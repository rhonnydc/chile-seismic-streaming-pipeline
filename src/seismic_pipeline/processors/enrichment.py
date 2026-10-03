"""Pure enrichment of a raw earthquake event."""

from datetime import UTC, datetime
from typing import Any


def _parse_utc(timestamp: str) -> datetime:
    """Parse an ISO 8601 timestamp and normalize it to UTC."""
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Earthquake timestamps must include a UTC offset")
    return parsed.astimezone(UTC)


def enrich_earthquake(
    raw_event: dict[str, Any], *, processed_at: datetime | None = None
) -> dict[str, Any]:
    """Copy a raw event and add simple fields derived from its measurements and times."""
    processed = processed_at if processed_at is not None else datetime.now(UTC)
    if processed.tzinfo is None:
        raise ValueError("processed_at must include a UTC offset")
    processed = processed.astimezone(UTC)

    event_time = _parse_utc(raw_event["event_time_utc"])
    ingested_at = _parse_utc(raw_event["ingested_at"])
    magnitude = raw_event["magnitude"]
    depth_km = raw_event["depth_km"]

    if magnitude < 4.0:
        severity_level = "LOW"
    elif magnitude < 6.0:
        severity_level = "MODERATE"
    else:
        severity_level = "HIGH"

    return {
        **raw_event,
        "severity_level": severity_level,
        "is_shallow": depth_km < 70.0,
        "event_date_utc": event_time.date().isoformat(),
        "event_hour_utc": event_time.hour,
        "ingestion_latency_seconds": (ingested_at - event_time).total_seconds(),
        "processed_at_utc": processed.isoformat().replace("+00:00", "Z"),
    }
