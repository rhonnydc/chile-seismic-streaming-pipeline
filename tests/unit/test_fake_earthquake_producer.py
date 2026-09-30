"""Check Kafka publication behavior without requiring a broker."""

import json

import pytest

from seismic_pipeline.producers import fake_earthquake_producer as producer_module


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


def test_publishes_json_with_event_id_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeProducer.instances.clear()
    FakeProducer.delivery_error = None
    intervals: list[float] = []
    monkeypatch.setattr(producer_module, "Producer", FakeProducer)
    monkeypatch.setattr(producer_module.time, "sleep", intervals.append)

    producer_module.publish_fake_earthquakes(
        bootstrap_servers="localhost:9092",
        topic="raw_earthquakes",
        event_count=3,
        interval_seconds=0.25,
    )

    client = FakeProducer.instances[0]
    assert client.config["bootstrap.servers"] == "localhost:9092"
    assert len(client.messages) == 3
    assert intervals == [0.25, 0.25]
    for message in client.messages:
        event = json.loads(message["value"])
        assert message["topic"] == "raw_earthquakes"
        assert message["key"].decode("utf-8") == event["event_id"]
        assert event["source"] == "simulator"


def test_delivery_failure_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeProducer.instances.clear()
    FakeProducer.delivery_error = "broker unavailable"
    monkeypatch.setattr(producer_module, "Producer", FakeProducer)

    with pytest.raises(RuntimeError, match="delivery failed"):
        producer_module.publish_fake_earthquakes(
            bootstrap_servers="localhost:9092",
            topic="raw_earthquakes",
            event_count=1,
            interval_seconds=0,
        )
