"""Map enriched earthquake records to Postgres and persist them idempotently."""

from datetime import UTC, date, datetime
from typing import Any

EVENT_COLUMNS = (
    "event_id",
    "source",
    "source_event_id",
    "event_time_utc",
    "updated_at_utc",
    "place",
    "country",
    "region",
    "magnitude",
    "magnitude_type",
    "depth_km",
    "latitude",
    "longitude",
    "status",
    "event_type",
    "tsunami",
    "url",
    "ingested_at",
    "severity_level",
    "is_shallow",
    "event_date_utc",
    "event_hour_utc",
    "ingestion_latency_seconds",
    "processed_at_utc",
)

TIMESTAMP_COLUMNS = frozenset(
    {"event_time_utc", "updated_at_utc", "ingested_at", "processed_at_utc"}
)

INSERT_SQL = (
    f"INSERT INTO enriched_earthquake_events ({', '.join(EVENT_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(EVENT_COLUMNS))}) "
    "ON CONFLICT (event_id) DO NOTHING"
)


def _parse_utc(value: str) -> datetime:
    """Convert an ISO 8601 timestamp to a timezone-aware UTC datetime."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Earthquake timestamps must include a UTC offset")
    return parsed.astimezone(UTC)


def event_to_row(event: dict[str, Any]) -> tuple[Any, ...]:
    """Return SQL parameter values in the same order as EVENT_COLUMNS."""
    values = []
    for column in EVENT_COLUMNS:
        value = event[column]
        if column in TIMESTAMP_COLUMNS:
            value = _parse_utc(value)
        elif column == "event_date_utc":
            value = date.fromisoformat(value)
        values.append(value)
    return tuple(values)


def persist_event(connection: Any, event: dict[str, Any]) -> bool:
    """Commit one event; return False when its event_id already exists.

    The caller must supply a connection with autocommit disabled. On SQL failure,
    roll back the transaction and let the caller leave the Kafka offset uncommitted.
    """
    row = event_to_row(event)
    try:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_SQL, row)
            inserted = cursor.rowcount == 1
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return inserted
