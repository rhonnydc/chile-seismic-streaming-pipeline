"""Contract checks for simulated earthquake events."""

import json
from datetime import UTC, datetime
from random import Random

from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake


def test_generated_event_is_serializable_and_source_independent() -> None:
    now = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)

    event = generate_fake_earthquake(rng=Random(0), now=now)
    payload = json.loads(json.dumps(event.to_dict()))

    assert payload["event_id"].startswith("fake-20260929-")
    assert payload["source"] == "simulator"
    assert payload["source_event_id"] == payload["event_id"]
    assert payload["event_time_utc"] == "2026-09-29T12:00:00Z"
    assert payload["updated_at_utc"] == payload["event_time_utc"]
    assert payload["ingested_at"] == payload["event_time_utc"]
    assert payload["country"] == "Chile"
    assert payload["region"]
    assert payload["place"].endswith(", Chile")
    assert 2.5 <= payload["magnitude"] <= 6.5
    assert 5.0 <= payload["depth_km"] <= 180.0
    assert -54.0 <= payload["latitude"] <= -23.0
    assert -74.0 <= payload["longitude"] <= -70.0
    assert payload["tsunami"] is False
    assert payload["url"] is None


def test_generated_events_have_distinct_ids() -> None:
    now = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)

    first = generate_fake_earthquake(rng=Random(0), now=now)
    second = generate_fake_earthquake(rng=Random(0), now=now)

    assert first.event_id != second.event_id
