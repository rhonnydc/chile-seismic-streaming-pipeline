"""Check the Postgres consumer's offset boundary without external services."""

from unittest.mock import MagicMock

import pytest

from seismic_pipeline.consumers import postgres_sink_consumer as consumer_module


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
