"""Check consumer delivery and offset behavior without connecting to Kafka."""

import base64
import json
from datetime import UTC, datetime
from random import Random
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError

from seismic_pipeline.consumers import enriching_consumer as consumer_module
from seismic_pipeline.consumers.error_policy import InvalidMessageError
from seismic_pipeline.consumers.retry import retry_recoverable
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake
from seismic_pipeline.sinks.postgres_sink import INSERT_SQL, persist_event


class FakeMessage:
    def __init__(self, event: dict[str, object] | bytes, *, key: bytes | None = None) -> None:
        self.event = event
        self._key = key

    def error(self) -> None:
        return None

    def value(self) -> dict[str, object] | bytes:
        return self.event

    def topic(self) -> str:
        return "raw_earthquakes"

    def key(self) -> bytes | None:
        return self._key

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 7


class FakeConsumer:
    instances: list["FakeConsumer"] = []
    next_message: FakeMessage | None = None
    crash_before_commit = False

    def __init__(self, config: dict[str, object]) -> None:
        self.config = config
        self.subscribed: list[str] = []
        self.commits: list[dict[str, object]] = []
        self.closed = False
        self.polled = False
        self.instances.append(self)

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def poll(self, timeout: float) -> FakeMessage:
        assert timeout == 1.0
        if self.polled:
            raise KeyboardInterrupt
        self.polled = True
        assert self.next_message is not None
        return self.next_message

    def commit(self, **kwargs: object) -> None:
        assert FakeProducer.delivery_reported is True
        if self.crash_before_commit:
            FakeConsumer.crash_before_commit = False
            raise RuntimeError("injected crash before raw offset commit")
        self.commits.append(kwargs)

    def close(self) -> None:
        self.closed = True


class FakeProducer:
    instances: list["FakeProducer"] = []
    delivery_error: str | None = None
    delivery_reported = False

    def __init__(self, config: dict[str, object]) -> None:
        self.config = config
        self.messages: list[dict[str, object]] = []
        self.instances.append(self)

    def produce(self, **message: object) -> None:
        self.messages.append(message)

    def flush(self, timeout: float) -> int:
        assert timeout == 15.0
        self.messages[-1]["on_delivery"](self.delivery_error, SimpleNamespace())
        FakeProducer.delivery_reported = True
        return 0


class FakeDeserializer:
    fail = False
    invalid_input = False
    registry_failures_remaining = 0
    calls = 0
    decoded_event: dict[str, object] | None = None

    def __init__(self, registry: object) -> None:
        pass

    def __call__(self, value: object, context: object) -> dict[str, object] | None:
        assert context.topic == "raw_earthquakes"
        FakeDeserializer.calls += 1
        if self.registry_failures_remaining:
            FakeDeserializer.registry_failures_remaining -= 1
            raise SchemaRegistryError(503, 50301, "registry unavailable")
        if self.invalid_input:
            raise InvalidMessageError("invalid Avro input")
        if self.fail:
            raise ValueError("bad Avro")
        return self.decoded_event if self.decoded_event is not None else value


class FakeSerializer:
    instances: list["FakeSerializer"] = []

    def __init__(self, registry: object, schema: str, conf: dict[str, object]) -> None:
        self.config = conf
        self.events: list[dict[str, object]] = []
        self.instances.append(self)

    def __call__(self, event: dict[str, object], context: object) -> bytes:
        assert context.topic == "enriched_earthquakes"
        self.events.append(event)
        return b"enriched-avro"


@pytest.fixture
def setup_clients(monkeypatch: pytest.MonkeyPatch) -> FakeMessage:
    event = {
        "event_id": "fake-123",
        "event_time_utc": "2026-10-03T12:00:00Z",
        "ingested_at": "2026-10-03T12:00:05Z",
        "magnitude": 4.5,
        "depth_km": 20.0,
    }
    message = FakeMessage(event)
    FakeConsumer.instances.clear()
    FakeProducer.instances.clear()
    FakeSerializer.instances.clear()
    FakeConsumer.next_message = message
    FakeConsumer.crash_before_commit = False
    FakeProducer.delivery_error = None
    FakeProducer.delivery_reported = False
    FakeDeserializer.fail = False
    FakeDeserializer.invalid_input = False
    FakeDeserializer.registry_failures_remaining = 0
    FakeDeserializer.calls = 0
    FakeDeserializer.decoded_event = None
    monkeypatch.setattr(consumer_module, "Consumer", FakeConsumer)
    monkeypatch.setattr(consumer_module, "Producer", FakeProducer)
    monkeypatch.setattr(consumer_module, "SchemaRegistryClient", lambda config: object())
    monkeypatch.setattr(consumer_module, "AvroDeserializer", FakeDeserializer)
    monkeypatch.setattr(consumer_module, "AvroSerializer", FakeSerializer)
    return message


def run_consumer() -> None:
    consumer_module.consume_and_enrich(
        bootstrap_servers="localhost:9092",
        registry_url="http://localhost:8081",
        raw_topic="raw_earthquakes",
        enriched_topic="enriched_earthquakes",
        group_id="seismic-enricher",
    )


def test_commits_only_after_enriched_delivery(setup_clients: FakeMessage) -> None:
    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    consumer = FakeConsumer.instances[0]
    producer = FakeProducer.instances[0]
    output = FakeSerializer.instances[0].events[0]
    assert consumer.config["group.id"] == "seismic-enricher"
    assert consumer.config["enable.auto.commit"] is False
    assert consumer.config["auto.offset.reset"] == "earliest"
    assert consumer.subscribed == ["raw_earthquakes"]
    assert consumer.commits == [{"message": setup_clients, "asynchronous": False}]
    assert consumer.closed is True
    assert producer.messages[0]["topic"] == "enriched_earthquakes"
    assert producer.messages[0]["key"] == b"fake-123"
    assert output["severity_level"] == "MODERATE"
    assert output["is_shallow"] is True
    assert FakeSerializer.instances[0].config["auto.register.schemas"] is False


def test_delivery_failure_does_not_commit(setup_clients: FakeMessage) -> None:
    FakeProducer.delivery_error = "broker unavailable"

    with pytest.raises(RuntimeError, match="Enriched delivery failed"):
        run_consumer()

    assert FakeConsumer.instances[0].commits == []
    assert FakeConsumer.instances[0].closed is True


def test_invalid_raw_record_does_not_publish_or_commit(setup_clients: FakeMessage) -> None:
    FakeConsumer.next_message = FakeMessage({"event_id": "fake-123"})

    with pytest.raises(KeyError, match="event_time_utc"):
        run_consumer()

    assert FakeProducer.instances[0].messages == []
    assert FakeConsumer.instances[0].commits == []
    assert FakeConsumer.instances[0].closed is True


def test_deserialization_failure_does_not_publish_or_commit(setup_clients: FakeMessage) -> None:
    FakeDeserializer.fail = True

    with pytest.raises(ValueError, match="bad Avro"):
        run_consumer()

    assert FakeProducer.instances[0].messages == []
    assert FakeConsumer.instances[0].commits == []
    assert FakeConsumer.instances[0].closed is True


def test_invalid_input_commits_only_after_dead_letter_delivery(
    setup_clients: FakeMessage,
) -> None:
    original = b"\x00\xffinvalid-avro"
    message = FakeMessage(original, key=b"source-key")
    FakeConsumer.next_message = message
    FakeDeserializer.invalid_input = True

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    consumer = FakeConsumer.instances[0]
    producer = FakeProducer.instances[0]
    assert len(producer.messages) == 1
    assert producer.messages[0]["topic"] == "dead_letter_earthquakes"
    assert producer.messages[0]["key"] == b"raw_earthquakes:1:7"
    envelope = json.loads(producer.messages[0]["value"])
    assert envelope["stage"] == "enricher"
    assert envelope["source_topic"] == "raw_earthquakes"
    assert envelope["source_partition"] == 1
    assert envelope["source_offset"] == 7
    assert envelope["event_id"] is None
    assert base64.b64decode(envelope["original_value_base64"]) == original
    assert consumer.commits == [{"message": message, "asynchronous": False}]


def test_failed_dead_letter_delivery_leaves_original_offset_pending(
    setup_clients: FakeMessage,
) -> None:
    FakeConsumer.next_message = FakeMessage(b"invalid-avro")
    FakeDeserializer.invalid_input = True
    FakeProducer.delivery_error = "broker unavailable"

    with pytest.raises(RuntimeError, match="Dead-letter delivery failed"):
        run_consumer()

    assert FakeProducer.instances[0].messages[0]["topic"] == "dead_letter_earthquakes"
    assert FakeConsumer.instances[0].commits == []
    assert FakeConsumer.instances[0].closed is True


def test_invalid_event_time_goes_to_dlq_with_known_event_id(
    setup_clients: FakeMessage,
) -> None:
    raw_event = setup_clients.event.copy()
    raw_event["event_time_utc"] = "2026-10-03T12:00:00"
    FakeDeserializer.decoded_event = raw_event
    message = FakeMessage(b"\x00\x00\x00\x00\x01payload", key=b"fake-123")
    FakeConsumer.next_message = message

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    producer = FakeProducer.instances[0]
    assert len(producer.messages) == 1
    assert producer.messages[0]["topic"] == "dead_letter_earthquakes"
    envelope = json.loads(producer.messages[0]["value"])
    assert envelope["error_code"] == "INVALID_EVENT_TIME"
    assert envelope["event_id"] == "fake-123"
    assert base64.b64decode(envelope["original_value_base64"]) == message.value()
    assert FakeConsumer.instances[0].commits == [{"message": message, "asynchronous": False}]


def test_crash_after_dead_letter_delivery_can_replay_same_source_message(
    setup_clients: FakeMessage,
) -> None:
    message = FakeMessage(b"invalid-avro", key=b"source-key")
    FakeConsumer.next_message = message
    FakeDeserializer.invalid_input = True
    FakeConsumer.crash_before_commit = True

    with pytest.raises(RuntimeError, match="injected crash before raw offset commit"):
        run_consumer()

    assert FakeConsumer.instances[0].commits == []
    assert FakeProducer.instances[0].messages[0]["topic"] == "dead_letter_earthquakes"

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    assert FakeConsumer.instances[1].commits == [{"message": message, "asynchronous": False}]
    assert [producer.messages[0]["key"] for producer in FakeProducer.instances] == [
        b"raw_earthquakes:1:7",
        b"raw_earthquakes:1:7",
    ]


def test_temporary_registry_failure_retries_before_normal_delivery(
    setup_clients: FakeMessage, monkeypatch: pytest.MonkeyPatch
) -> None:
    waits: list[float] = []
    monkeypatch.setattr(
        consumer_module,
        "retry_recoverable",
        lambda operation, **kwargs: retry_recoverable(operation, sleep=waits.append, **kwargs),
        raising=False,
    )
    FakeDeserializer.registry_failures_remaining = 2

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    assert FakeDeserializer.calls == 3
    assert waits == [1.0, 1.0]
    assert FakeProducer.instances[0].messages[0]["topic"] == "enriched_earthquakes"
    assert FakeConsumer.instances[0].commits == [{"message": setup_clients, "asynchronous": False}]


def test_persistent_registry_failure_stops_without_delivery_or_commit(
    setup_clients: FakeMessage, monkeypatch: pytest.MonkeyPatch
) -> None:
    waits: list[float] = []
    monkeypatch.setattr(
        consumer_module,
        "retry_recoverable",
        lambda operation, **kwargs: retry_recoverable(operation, sleep=waits.append, **kwargs),
        raising=False,
    )
    FakeDeserializer.registry_failures_remaining = 3

    with pytest.raises(SchemaRegistryError, match="registry unavailable"):
        run_consumer()

    assert FakeDeserializer.calls == 3
    assert waits == [1.0, 1.0]
    assert FakeProducer.instances[0].messages == []
    assert FakeConsumer.instances[0].commits == []


def test_crash_after_enriched_delivery_replays_raw_and_sink_deduplicates(
    setup_clients: FakeMessage,
) -> None:
    raw_event = generate_fake_earthquake(
        rng=Random(0), now=datetime(2026, 10, 23, 12, tzinfo=UTC)
    ).to_dict()
    raw_event["event_id"] = "phase9-enricher-crash"
    raw_event["source_event_id"] = raw_event["event_id"]
    message = FakeMessage(raw_event)
    FakeConsumer.next_message = message
    FakeConsumer.crash_before_commit = True

    with pytest.raises(RuntimeError, match="injected crash before raw offset commit"):
        run_consumer()

    assert FakeConsumer.instances[0].commits == []
    assert FakeConsumer.instances[0].closed is True

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    assert FakeConsumer.instances[1].commits == [{"message": message, "asynchronous": False}]
    published_events = [serializer.events[0] for serializer in FakeSerializer.instances]
    assert [event["event_id"] for event in published_events] == [
        "phase9-enricher-crash",
        "phase9-enricher-crash",
    ]
    assert [producer.messages[0]["key"] for producer in FakeProducer.instances] == [
        b"phase9-enricher-crash",
        b"phase9-enricher-crash",
    ]

    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    stored_ids: set[str] = set()

    def execute(sql: str, row: tuple[object, ...]) -> None:
        assert sql == INSERT_SQL
        event_id = row[0]
        assert isinstance(event_id, str)
        cursor.rowcount = int(event_id not in stored_ids)
        stored_ids.add(event_id)

    cursor.execute.side_effect = execute
    assert [persist_event(connection, event) for event in published_events] == [True, False]
    assert stored_ids == {"phase9-enricher-crash"}
    assert connection.commit.call_count == 2
