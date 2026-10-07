"""Check the Postgres sink without Kafka or a live database."""

from datetime import UTC, date, datetime
from random import Random
from unittest.mock import MagicMock

import pytest

from seismic_pipeline.processors.enrichment import enrich_earthquake
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake
from seismic_pipeline.sinks.postgres_sink import (
    EVENT_COLUMNS,
    INSERT_SQL,
    event_to_row,
    persist_event,
)


@pytest.fixture
def enriched_event() -> dict[str, object]:
    raw_event = generate_fake_earthquake(
        rng=Random(0), now=datetime(2026, 10, 6, 12, tzinfo=UTC)
    ).to_dict()
    raw_event["magnitude_type"] = None
    raw_event["url"] = None
    return enrich_earthquake(raw_event, processed_at=datetime(2026, 10, 6, 12, 0, 6, tzinfo=UTC))


def test_mapping_preserves_values_and_converts_dates(enriched_event: dict[str, object]) -> None:
    row = dict(zip(EVENT_COLUMNS, event_to_row(enriched_event), strict=True))

    assert len(row) == 24
    assert row["event_id"] == enriched_event["event_id"]
    assert row["magnitude"] == enriched_event["magnitude"]
    assert row["region"] == enriched_event["region"]
    assert row["severity_level"] == enriched_event["severity_level"]
    assert row["magnitude_type"] is None
    assert row["url"] is None
    assert row["event_date_utc"] == date.fromisoformat(enriched_event["event_date_utc"])
    assert row["event_time_utc"].tzinfo == UTC
    assert row["processed_at_utc"].tzinfo == UTC


def test_insert_commits_after_execute(enriched_event: dict[str, object]) -> None:
    actions: list[str] = []
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.rowcount = 1
    cursor.execute.side_effect = lambda *_: actions.append("execute")
    connection.commit.side_effect = lambda: actions.append("commit")

    assert persist_event(connection, enriched_event) is True

    assert actions == ["execute", "commit"]
    cursor.execute.assert_called_once_with(INSERT_SQL, event_to_row(enriched_event))
    assert INSERT_SQL.count("%s") == len(EVENT_COLUMNS)
    assert "ON CONFLICT (event_id) DO NOTHING" in INSERT_SQL
    connection.rollback.assert_not_called()


def test_duplicate_is_committed_without_another_row(enriched_event: dict[str, object]) -> None:
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.rowcount = 0

    assert persist_event(connection, enriched_event) is False

    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()


def test_insert_failure_rolls_back(enriched_event: dict[str, object]) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = RuntimeError("Postgres unavailable")

    with pytest.raises(RuntimeError, match="Postgres unavailable"):
        persist_event(connection, enriched_event)

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
