"""Check earthquake enrichment without Kafka or Schema Registry."""

from datetime import UTC, datetime

import pytest

from seismic_pipeline.processors.enrichment import enrich_earthquake


@pytest.fixture
def raw_event() -> dict[str, object]:
    return {
        "event_id": "fake-123",
        "source": "simulator",
        "event_time_utc": "2026-10-03T12:30:00Z",
        "ingested_at": "2026-10-03T12:30:05Z",
        "magnitude": 4.5,
        "depth_km": 20.0,
    }


@pytest.mark.parametrize(
    ("magnitude", "expected"),
    [(3.5, "LOW"), (4.0, "MODERATE"), (4.5, "MODERATE"), (6.0, "HIGH"), (6.2, "HIGH")],
)
def test_severity_boundaries(raw_event: dict[str, object], magnitude: float, expected: str) -> None:
    raw_event["magnitude"] = magnitude

    assert enrich_earthquake(raw_event)["severity_level"] == expected


@pytest.mark.parametrize(
    ("depth_km", "expected"), [(20.0, True), (69.9, True), (70.0, False), (120.0, False)]
)
def test_shallow_boundary(raw_event: dict[str, object], depth_km: float, expected: bool) -> None:
    raw_event["depth_km"] = depth_km

    assert enrich_earthquake(raw_event)["is_shallow"] is expected


def test_derived_times_and_original_fields(raw_event: dict[str, object]) -> None:
    original = raw_event.copy()
    processed_at = datetime(2026, 10, 3, 12, 30, 6, tzinfo=UTC)

    enriched = enrich_earthquake(raw_event, processed_at=processed_at)

    assert enriched["event_date_utc"] == "2026-10-03"
    assert enriched["event_hour_utc"] == 12
    assert enriched["ingestion_latency_seconds"] == 5.0
    assert enriched["processed_at_utc"] == "2026-10-03T12:30:06Z"
    assert enriched["event_id"] == "fake-123"
    assert enriched["source"] == "simulator"
    assert raw_event == original


def test_processed_at_defaults_to_utc_now(raw_event: dict[str, object]) -> None:
    enriched = enrich_earthquake(raw_event)

    assert datetime.fromisoformat(enriched["processed_at_utc"].replace("Z", "+00:00")).tzinfo == UTC
