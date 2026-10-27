"""Verify one event across real Kafka, Schema Registry, consumers, and Postgres."""

from dataclasses import replace
from datetime import UTC, datetime
from random import Random

import pytest

from seismic_pipeline.producers import fake_earthquake_producer
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake


@pytest.mark.integration
def test_valid_event_reaches_postgres_with_confirmed_offsets(
    integration_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = integration_harness
    env = harness.env
    event = replace(
        generate_fake_earthquake(rng=Random(11), now=datetime.now(UTC)),
        event_time_utc="2026-10-26T12:34:00Z",
        updated_at_utc="2026-10-26T12:34:00Z",
        ingested_at="2026-10-26T12:34:05Z",
        magnitude=6.2,
        depth_km=12.0,
    )
    raw_event = event.to_dict()
    key = event.event_id.encode("utf-8")
    monkeypatch.setattr(fake_earthquake_producer, "generate_fake_earthquake", lambda: event)

    harness.start_consumer("enriching_consumer")
    harness.start_consumer("postgres_sink_consumer")
    fake_earthquake_producer.publish_fake_earthquakes(
        bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"],
        registry_url=env["SCHEMA_REGISTRY_URL"],
        topic=env["KAFKA_RAW_EARTHQUAKES_TOPIC"],
        event_count=1,
        interval_seconds=0,
    )

    raw_message = harness.wait_for_message(env["KAFKA_RAW_EARTHQUAKES_TOPIC"], key)
    assert harness.decode(raw_message) == raw_event

    enriched_message = harness.wait_for_message(env["KAFKA_ENRICHED_EARTHQUAKES_TOPIC"], key)
    enriched = harness.decode(enriched_message)
    for field, value in raw_event.items():
        assert enriched[field] == value
    assert enriched["severity_level"] == "HIGH"
    assert enriched["is_shallow"] is True
    assert enriched["event_date_utc"] == "2026-10-26"
    assert enriched["event_hour_utc"] == 12
    assert enriched["ingestion_latency_seconds"] == 5.0
    assert datetime.fromisoformat(enriched["processed_at_utc"].replace("Z", "+00:00")).tzinfo

    row = harness.wait_for_row(event.event_id)
    assert row["event_id"] == event.event_id
    assert row["source"] == event.source
    assert row["place"] == event.place
    assert row["magnitude"] == 6.2
    assert row["depth_km"] == 12.0
    assert row["severity_level"] == "HIGH"
    assert row["is_shallow"] is True
    assert row["event_date_utc"].isoformat() == "2026-10-26"
    assert row["event_hour_utc"] == 12
    assert row["ingestion_latency_seconds"] == 5.0
    assert row["event_time_utc"] == datetime(2026, 10, 26, 12, 34, tzinfo=UTC)
    assert row["processed_at_utc"].isoformat() == enriched["processed_at_utc"].replace(
        "Z", "+00:00"
    )
    assert len(harness.rows(event.event_id)) == 1

    raw_next_offset = raw_message.offset() + 1
    enriched_next_offset = enriched_message.offset() + 1
    assert harness.wait_for_commit(
        raw_message.topic(), raw_message.partition(), env["CONSUMER_GROUP_ID"], raw_next_offset
    ) == raw_next_offset
    assert harness.wait_for_commit(
        enriched_message.topic(),
        enriched_message.partition(),
        env["POSTGRES_SINK_GROUP_ID"],
        enriched_next_offset,
    ) == enriched_next_offset
