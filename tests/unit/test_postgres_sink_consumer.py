"""Check the Postgres consumer's offset boundary without external services."""

import base64
import json
from datetime import UTC, datetime
from random import Random
from unittest.mock import MagicMock

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError

from seismic_pipeline.consumers import postgres_sink_consumer as consumer_module
from seismic_pipeline.consumers.error_policy import InvalidMessageError
from seismic_pipeline.consumers.retry import retry_recoverable
from seismic_pipeline.processors.enrichment import enrich_earthquake
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake
from seismic_pipeline.sinks.postgres_sink import INSERT_SQL


@pytest.fixture
def clients(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock, MagicMock, MagicMock]:
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = b"\x00\x00\x00\x00\x01valid-avro"
    message.topic.return_value = "enriched_earthquakes"
    message.key.return_value = b"fake-123"
    message.partition.return_value = 1
    message.offset.return_value = 7

    consumer = MagicMock()
    consumer.poll.side_effect = [message, KeyboardInterrupt()]
    consumer_factory = MagicMock(return_value=consumer)
    monkeypatch.setattr(consumer_module, "Consumer", consumer_factory)

    connection = MagicMock()
    connector = MagicMock(return_value=connection)
    monkeypatch.setattr(consumer_module.psycopg, "connect", connector)
    monkeypatch.setattr(consumer_module, "SchemaRegistryClient", MagicMock())
    deserializer = MagicMock(return_value={"event_id": "fake-123"})
    monkeypatch.setattr(consumer_module, "AvroDeserializer", lambda _: deserializer)
    return message, consumer, consumer_factory, deserializer


def run_consumer() -> None:
    consumer_module.consume_to_postgres(
        bootstrap_servers="localhost:9092",
        registry_url="http://localhost:8081",
        enriched_topic="enriched_earthquakes",
        group_id="seismic-postgres-sink",
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="seismic",
        postgres_user="seismic_user",
        postgres_password="seismic_password",
    )


def test_commits_offset_after_database_write(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock], monkeypatch: pytest.MonkeyPatch
) -> None:
    message, consumer, consumer_factory, deserializer = clients
    actions: list[str] = []
    sink = MagicMock(side_effect=lambda *_: actions.append("database") or True)
    consumer.commit.side_effect = lambda **_: actions.append("offset")
    monkeypatch.setattr(consumer_module, "persist_event", sink)

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    assert actions == ["database", "offset"]
    assert consumer_factory.call_args.args[0]["group.id"] == "seismic-postgres-sink"
    assert consumer_factory.call_args.args[0]["enable.auto.commit"] is False
    assert consumer_factory.call_args.args[0]["auto.offset.reset"] == "earliest"
    consumer.subscribe.assert_called_once_with(["enriched_earthquakes"])
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)
    assert deserializer.call_count == 1
    consumer.close.assert_called_once()


def test_database_failure_does_not_commit_offset(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, consumer, _, _ = clients
    sink = MagicMock(side_effect=RuntimeError("Postgres unavailable"))
    monkeypatch.setattr(consumer_module, "persist_event", sink)

    with pytest.raises(RuntimeError, match="Postgres unavailable"):
        run_consumer()

    sink.assert_called_once()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_deserialization_failure_does_not_write_or_commit(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, consumer, _, deserializer = clients
    deserializer.side_effect = ValueError("bad Avro")
    sink = MagicMock()
    monkeypatch.setattr(consumer_module, "persist_event", sink)

    with pytest.raises(ValueError, match="bad Avro"):
        run_consumer()

    sink.assert_not_called()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_invalid_input_commits_only_after_dead_letter_delivery(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message, consumer, _, deserializer = clients
    deserializer.side_effect = InvalidMessageError("invalid Avro input")
    producer = MagicMock()
    actions: list[str] = []

    def report_delivery(_timeout: float) -> int:
        producer.produce.call_args.kwargs["on_delivery"](None, MagicMock())
        actions.append("DLQ delivery")
        return 0

    producer.flush.side_effect = report_delivery
    consumer.commit.side_effect = lambda **_: actions.append("offset commit")
    monkeypatch.setattr(
        consumer_module, "Producer", MagicMock(return_value=producer), raising=False
    )
    sink = MagicMock()
    monkeypatch.setattr(consumer_module, "persist_event", sink)

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    assert actions == ["DLQ delivery", "offset commit"]
    producer.produce.assert_called_once()
    assert producer.produce.call_args.kwargs["topic"] == "dead_letter_earthquakes"
    assert producer.produce.call_args.kwargs["key"] == b"enriched_earthquakes:1:7"
    envelope = json.loads(producer.produce.call_args.kwargs["value"])
    assert envelope["stage"] == "postgres_sink"
    assert envelope["event_id"] is None
    assert base64.b64decode(envelope["original_value_base64"]) == message.value()
    sink.assert_not_called()
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)


def test_invalid_avro_framing_goes_to_dlq_without_deserializing(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message, consumer, _, deserializer = clients
    message.value.return_value = b"bad"
    producer = MagicMock()

    def report_delivery(_timeout: float) -> int:
        producer.produce.call_args.kwargs["on_delivery"](None, MagicMock())
        return 0

    producer.flush.side_effect = report_delivery
    monkeypatch.setattr(consumer_module, "Producer", MagicMock(return_value=producer))

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    deserializer.assert_not_called()
    assert producer.produce.call_args.kwargs["topic"] == "dead_letter_earthquakes"
    envelope = json.loads(producer.produce.call_args.kwargs["value"])
    assert base64.b64decode(envelope["original_value_base64"]) == b"bad"
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)


def test_invalid_event_time_goes_to_dlq_before_database_write(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message, consumer, _, deserializer = clients
    raw_event = generate_fake_earthquake(
        rng=Random(0), now=datetime(2026, 10, 24, 12, tzinfo=UTC)
    ).to_dict()
    event = enrich_earthquake(raw_event)
    event["event_time_utc"] = "2026-10-24T12:00:00"
    deserializer.return_value = event
    producer = MagicMock()

    def report_delivery(_timeout: float) -> int:
        producer.produce.call_args.kwargs["on_delivery"](None, MagicMock())
        return 0

    producer.flush.side_effect = report_delivery
    monkeypatch.setattr(consumer_module, "Producer", MagicMock(return_value=producer))
    sink = MagicMock()
    monkeypatch.setattr(consumer_module, "persist_event", sink)

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    sink.assert_not_called()
    assert producer.produce.call_args.kwargs["topic"] == "dead_letter_earthquakes"
    envelope = json.loads(producer.produce.call_args.kwargs["value"])
    assert envelope["error_code"] == "INVALID_EVENT_TIME"
    assert envelope["event_id"] == event["event_id"]
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)


def test_temporary_registry_failure_retries_before_database_write(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message, consumer, _, deserializer = clients
    deserializer.side_effect = [
        SchemaRegistryError(503, 50301, "registry unavailable"),
        SchemaRegistryError(503, 50301, "registry unavailable"),
        {"event_id": "fake-123"},
    ]
    waits: list[float] = []
    monkeypatch.setattr(
        consumer_module,
        "retry_recoverable",
        lambda operation, **kwargs: retry_recoverable(operation, sleep=waits.append, **kwargs),
    )
    sink = MagicMock(return_value=True)
    monkeypatch.setattr(consumer_module, "persist_event", sink)

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    assert deserializer.call_count == 3
    assert waits == [1.0, 1.0]
    sink.assert_called_once()
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)


def test_persistent_registry_failure_stops_without_database_write_or_commit(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, consumer, _, deserializer = clients
    deserializer.side_effect = SchemaRegistryError(503, 50301, "registry unavailable")
    waits: list[float] = []
    monkeypatch.setattr(
        consumer_module,
        "retry_recoverable",
        lambda operation, **kwargs: retry_recoverable(operation, sleep=waits.append, **kwargs),
    )
    sink = MagicMock()
    monkeypatch.setattr(consumer_module, "persist_event", sink)

    with pytest.raises(SchemaRegistryError, match="registry unavailable"):
        run_consumer()

    assert deserializer.call_count == 3
    assert waits == [1.0, 1.0]
    sink.assert_not_called()
    consumer.commit.assert_not_called()


def test_failed_dead_letter_delivery_does_not_commit(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, consumer, _, deserializer = clients
    deserializer.side_effect = InvalidMessageError("invalid Avro input")
    producer = MagicMock()

    def report_failure(_timeout: float) -> int:
        producer.produce.call_args.kwargs["on_delivery"]("broker unavailable", MagicMock())
        return 0

    producer.flush.side_effect = report_failure
    monkeypatch.setattr(
        consumer_module, "Producer", MagicMock(return_value=producer), raising=False
    )

    with pytest.raises(RuntimeError, match="Dead-letter delivery failed"):
        run_consumer()

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_postgres_connection_failure_reconnects_before_retry(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message, consumer, _, _ = clients
    first_connection = MagicMock()
    second_connection = MagicMock()
    connector = consumer_module.psycopg.connect
    connector.side_effect = [first_connection, second_connection]
    waits: list[float] = []
    monkeypatch.setattr(
        consumer_module,
        "retry_recoverable",
        lambda operation, **kwargs: retry_recoverable(operation, sleep=waits.append, **kwargs),
        raising=False,
    )
    used_connections: list[MagicMock] = []

    def persist(connection: MagicMock, _event: dict[str, object]) -> bool:
        used_connections.append(connection)
        if connection is first_connection:
            raise consumer_module.psycopg.OperationalError("connection lost")
        return True

    monkeypatch.setattr(consumer_module, "persist_event", persist)

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    assert used_connections == [first_connection, second_connection]
    assert waits == [1.0]
    assert connector.call_count == 2
    first_connection.close.assert_called_once()
    second_connection.close.assert_called_once()
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)


def test_persistent_postgres_failure_stops_with_offset_pending(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, consumer, _, _ = clients
    connections = [MagicMock() for _ in range(3)]
    connector = consumer_module.psycopg.connect
    connector.side_effect = connections
    waits: list[float] = []
    monkeypatch.setattr(
        consumer_module,
        "retry_recoverable",
        lambda operation, **kwargs: retry_recoverable(operation, sleep=waits.append, **kwargs),
        raising=False,
    )
    sink = MagicMock(side_effect=consumer_module.psycopg.OperationalError("connection lost"))
    monkeypatch.setattr(consumer_module, "persist_event", sink)

    with pytest.raises(consumer_module.psycopg.OperationalError, match="connection lost"):
        run_consumer()

    assert sink.call_count == 3
    assert [call.args[0] for call in sink.call_args_list] == connections
    assert waits == [1.0, 1.0]
    assert connector.call_count == 3
    for connection in connections:
        connection.close.assert_called_once()
    consumer.commit.assert_not_called()


def test_crash_after_database_commit_replays_without_duplicate(
    clients: tuple[MagicMock, MagicMock, MagicMock, MagicMock], monkeypatch: pytest.MonkeyPatch
) -> None:
    message, first_consumer, consumer_factory, deserializer = clients
    raw_event = generate_fake_earthquake(
        rng=Random(0), now=datetime(2026, 10, 23, 12, tzinfo=UTC)
    ).to_dict()
    raw_event["event_id"] = "phase9-postgres-crash"
    raw_event["source_event_id"] = raw_event["event_id"]
    event = enrich_earthquake(raw_event, processed_at=datetime(2026, 10, 23, 12, tzinfo=UTC))
    deserializer.return_value = event

    replay_consumer = MagicMock()
    replay_consumer.poll.side_effect = [message, KeyboardInterrupt()]
    consumer_factory.side_effect = [first_consumer, replay_consumer]

    connection = consumer_module.psycopg.connect.return_value
    cursor = connection.cursor.return_value.__enter__.return_value
    committed_ids: set[str] = set()
    pending_id: str | None = None
    actions: list[str] = []
    inserted_results: list[bool] = []

    def execute(sql: str, row: tuple[object, ...]) -> None:
        nonlocal pending_id
        assert sql == INSERT_SQL
        event_id = row[0]
        assert isinstance(event_id, str)
        cursor.rowcount = int(event_id not in committed_ids)
        pending_id = event_id if cursor.rowcount else None

    def commit_database() -> None:
        nonlocal pending_id
        if pending_id is not None:
            committed_ids.add(pending_id)
        pending_id = None
        actions.append("database commit")

    cursor.execute.side_effect = execute
    connection.commit.side_effect = commit_database
    replay_consumer.commit.side_effect = lambda **_: actions.append("Kafka commit")

    real_persist_event = consumer_module.persist_event
    crash_once = True

    def persist_then_crash(connection: object, event: dict[str, object]) -> bool:
        nonlocal crash_once
        inserted = real_persist_event(connection, event)
        inserted_results.append(inserted)
        if crash_once:
            crash_once = False
            raise RuntimeError("injected crash after Postgres commit")
        return inserted

    monkeypatch.setattr(consumer_module, "persist_event", persist_then_crash)

    with pytest.raises(RuntimeError, match="injected crash after Postgres commit"):
        run_consumer()

    assert committed_ids == {event["event_id"]}
    first_consumer.commit.assert_not_called()
    first_consumer.close.assert_called_once()
    assert actions == ["database commit"]

    with pytest.raises(KeyboardInterrupt):
        run_consumer()

    assert committed_ids == {event["event_id"]}
    assert cursor.execute.call_count == 2
    assert connection.commit.call_count == 2
    assert inserted_results == [True, False]
    replay_consumer.commit.assert_called_once_with(message=message, asynchronous=False)
    replay_consumer.close.assert_called_once()
    assert actions == ["database commit", "database commit", "Kafka commit"]
    assert all(
        call.args[0]["group.id"] == "seismic-postgres-sink"
        for call in consumer_factory.call_args_list
    )
