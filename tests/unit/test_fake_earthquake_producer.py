"""Check Avro publication behavior without requiring Kafka or Schema Registry."""

import io
import json
from types import SimpleNamespace

import pytest
from fastavro import parse_schema, schemaless_reader

from seismic_pipeline.producers import fake_earthquake_producer as producer_module


class FakeRegistry:
    instances: list["FakeRegistry"] = []

    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.instances.append(self)

    def lookup_schema(self, subject: str, schema: object, normalize_schemas: bool = False):
        assert subject == "raw_earthquakes-value"
        assert schema.schema_type == "AVRO"
        return SimpleNamespace(schema_id=42, guid=None)

    def config(self) -> dict:
        return {}


class FakeMessage:
    def __init__(self, topic: str, key: bytes) -> None:
        self._topic = topic
        self._key = key

    def key(self) -> bytes:
        return self._key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 1


class FakeProducer:
    instances: list["FakeProducer"] = []
    delivery_error: str | None = None

    def __init__(self, config: dict[str, str | int]) -> None:
        self.config = config
        self.messages: list[dict] = []
        self.instances.append(self)

    def produce(self, **message: object) -> None:
        self.messages.append(message)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        for message in self.messages:
            report = FakeMessage(message["topic"], message["key"])
            message["on_delivery"](self.delivery_error, report)
        return 0


def setup_fake_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeProducer.instances.clear()
    FakeRegistry.instances.clear()
    FakeProducer.delivery_error = None
    monkeypatch.setattr(producer_module, "Producer", FakeProducer)
    monkeypatch.setattr(producer_module, "SchemaRegistryClient", FakeRegistry)


def test_publishes_avro_with_event_id_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_fake_clients(monkeypatch)
    intervals: list[float] = []
    monkeypatch.setattr(producer_module.time, "sleep", intervals.append)

    producer_module.publish_fake_earthquakes(
        bootstrap_servers="localhost:9092",
        registry_url="http://localhost:8081",
        topic="raw_earthquakes",
        event_count=3,
        interval_seconds=0.25,
    )

    client = FakeProducer.instances[0]
    assert client.config["bootstrap.servers"] == "localhost:9092"
    assert FakeRegistry.instances[0].config["url"] == "http://localhost:8081"
    assert len(client.messages) == 3
    assert intervals == [0.25, 0.25]
    schema = parse_schema(json.loads(producer_module.SCHEMA_PATH.read_text(encoding="utf-8")))
    for message in client.messages:
        wire_value = message["value"]
        assert wire_value[:5] == bytes([0, 0, 0, 0, 42])
        event = schemaless_reader(io.BytesIO(wire_value[5:]), schema)
        assert message["topic"] == "raw_earthquakes"
        assert message["key"].decode("utf-8") == event["event_id"]
        assert event["source"] == "simulator"


def test_delivery_failure_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_fake_clients(monkeypatch)
    FakeProducer.delivery_error = "broker unavailable"

    with pytest.raises(RuntimeError, match="delivery failed"):
        producer_module.publish_fake_earthquakes(
            bootstrap_servers="localhost:9092",
            registry_url="http://localhost:8081",
            topic="raw_earthquakes",
            event_count=1,
            interval_seconds=0,
        )


def test_invalid_event_is_not_published(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_fake_clients(monkeypatch)
    payload = producer_module.generate_fake_earthquake().to_dict()
    payload.pop("event_id")
    invalid_event = SimpleNamespace(event_id="invalid", to_dict=lambda: payload)
    monkeypatch.setattr(producer_module, "generate_fake_earthquake", lambda: invalid_event)

    with pytest.raises(ValueError, match="event_id"):
        producer_module.publish_fake_earthquakes(
            bootstrap_servers="localhost:9092",
            registry_url="http://localhost:8081",
            topic="raw_earthquakes",
            event_count=1,
            interval_seconds=0,
        )

    assert FakeProducer.instances[0].messages == []
