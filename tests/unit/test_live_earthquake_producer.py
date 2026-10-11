"""Exercise polling and Avro publication without network or Kafka services."""

import io
import json
from types import SimpleNamespace

import httpx
import pytest
from fastavro import parse_schema, schemaless_reader

from seismic_pipeline.producers import live_earthquake_producer as live


class FakeRegistry:
    def __init__(self, config: dict[str, str]) -> None:
        assert config == {"url": "http://localhost:8081"}

    def lookup_schema(self, subject: str, schema: object, normalize_schemas: bool = False):
        assert subject == "raw_earthquakes-value"
        return SimpleNamespace(schema_id=42, guid=None)

    def config(self) -> dict:
        return {}


class FakeMessage:
    def __init__(self, item: dict) -> None:
        self.item = item

    def key(self) -> bytes:
        return self.item["key"]

    def topic(self) -> str:
        return self.item["topic"]

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 1


class FakeProducer:
    instances: list["FakeProducer"] = []
    delivery_error: str | None = None

    def __init__(self, config: dict[str, str | int]) -> None:
        assert config["bootstrap.servers"] == "localhost:9092"
        self.messages: list[dict] = []
        self.pending: list[dict] = []
        self.instances.append(self)

    def produce(self, **item: object) -> None:
        self.messages.append(item)
        self.pending.append(item)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        for item in self.pending:
            item["on_delivery"](self.delivery_error, FakeMessage(item))
        self.pending.clear()
        return 0


class FakeResponse:
    def __init__(self, features: list) -> None:
        self.features = features

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return {"features": self.features}


def feature(event_id: str = "us7000abcd") -> dict:
    return {
        "id": event_id,
        "properties": {
            "mag": 4.2,
            "place": "La Serena, Chile",
            "time": 1790646900000,
            "updated": 1790647200000,
            "status": "reviewed",
            "tsunami": 0,
            "type": "earthquake",
            "url": "https://earthquake.usgs.gov/example",
        },
        "geometry": {"coordinates": [-71.2, -29.9, 45.8]},
    }


@pytest.fixture(autouse=True)
def fake_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeProducer.instances.clear()
    FakeProducer.delivery_error = None
    monkeypatch.setattr(live, "Producer", FakeProducer)
    monkeypatch.setattr(live, "SchemaRegistryClient", FakeRegistry)
    monkeypatch.setattr(live.time, "sleep", lambda _: None)


def run_producer(max_cycles: int = 1) -> int:
    return live.publish_live_earthquakes(
        bootstrap_servers="localhost:9092",
        registry_url="http://localhost:8081",
        topic="raw_earthquakes",
        feed_url="https://example.test/feed.geojson",
        poll_interval_seconds=0,
        max_cycles=max_cycles,
        request_timeout_seconds=2,
    )


def test_publishes_avro_once_across_repeated_polls(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = iter([FakeResponse([feature(), feature()]), FakeResponse([feature()])])
    requests: list[tuple[str, float]] = []

    def get(url: str, timeout: float) -> FakeResponse:
        requests.append((url, timeout))
        return next(responses)

    monkeypatch.setattr(live.httpx, "get", get)

    assert run_producer(max_cycles=2) == 1
    assert requests == [("https://example.test/feed.geojson", 2)] * 2
    messages = FakeProducer.instances[0].messages
    assert len(messages) == 1
    assert messages[0]["key"] == b"usgs:us7000abcd"
    assert messages[0]["topic"] == "raw_earthquakes"
    schema = parse_schema(json.loads(live.SCHEMA_PATH.read_text(encoding="utf-8")))
    wire_value = messages[0]["value"]
    assert wire_value[:5] == bytes([0, 0, 0, 0, 42])
    payload = schemaless_reader(io.BytesIO(wire_value[5:]), schema)
    assert payload["source"] == "usgs"
    assert payload["source_event_id"] == "us7000abcd"
    assert payload["magnitude_type"] is None


def test_empty_feed_and_invalid_feature_publish_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = iter([FakeResponse([]), FakeResponse([{"id": None}])])
    monkeypatch.setattr(live.httpx, "get", lambda url, timeout: next(responses))

    assert run_producer(max_cycles=2) == 0
    assert FakeProducer.instances[0].messages == []


def test_http_failure_in_one_shot_mode_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(url: str, timeout: float) -> FakeResponse:
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(live.httpx, "get", fail)

    with pytest.raises(RuntimeError, match="Feed fetch failed in all 1 cycle"):
        run_producer()


def test_kafka_delivery_failure_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(live.httpx, "get", lambda url, timeout: FakeResponse([feature()]))
    FakeProducer.delivery_error = "broker unavailable"

    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        run_producer()
