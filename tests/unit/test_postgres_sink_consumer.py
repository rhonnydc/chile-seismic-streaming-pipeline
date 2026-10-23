"""Check the Postgres consumer's offset boundary without external services."""

from datetime import UTC, datetime
from random import Random
from unittest.mock import MagicMock

import pytest

from seismic_pipeline.consumers import postgres_sink_consumer as consumer_module
from seismic_pipeline.processors.enrichment import enrich_earthquake
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake
from seismic_pipeline.sinks.postgres_sink import INSERT_SQL


@pytest.fixture
def clients(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock, MagicMock, MagicMock]:
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = b"enriched-avro"
    message.partition.return_value = 1
    message.offset.return_value = 7

    consumer = MagicMock()
    consumer.poll.side_effect = [message, KeyboardInterrupt()]
    consumer_factory = MagicMock(return_value=consumer)
    monkeypatch.setattr(consumer_module, "Consumer", consumer_factory)

    connection = MagicMock()
    connector = MagicMock()
    connector.return_value.__enter__.return_value = connection
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

    connection = consumer_module.psycopg.connect.return_value.__enter__.return_value
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
