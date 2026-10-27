"""Verify that replaying a delivered event cannot add a second Postgres row."""

from datetime import UTC, datetime
from random import Random

import pytest

from seismic_pipeline.producers import fake_earthquake_producer
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake


@pytest.mark.integration
def test_new_sink_group_replays_event_without_duplicate_row(
    integration_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = integration_harness
    env = harness.env
    event = generate_fake_earthquake(rng=Random(23), now=datetime.now(UTC))
    key = event.event_id.encode("utf-8")
    monkeypatch.setattr(fake_earthquake_producer, "generate_fake_earthquake", lambda: event)
    env["CONSUMER_GROUP_ID"] = f"{env['CONSUMER_GROUP_ID']}-replay-test"
    env["POSTGRES_SINK_GROUP_ID"] = f"{env['POSTGRES_SINK_GROUP_ID']}-replay-test"

    harness.start_consumer("enriching_consumer")
    harness.start_consumer("postgres_sink_consumer")
    fake_earthquake_producer.publish_fake_earthquakes(
        bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"],
        registry_url=env["SCHEMA_REGISTRY_URL"],
        topic=env["KAFKA_RAW_EARTHQUAKES_TOPIC"],
        event_count=1,
        interval_seconds=0,
    )

    enriched_message = harness.wait_for_message(env["KAFKA_ENRICHED_EARTHQUAKES_TOPIC"], key)
    original_group = env["POSTGRES_SINK_GROUP_ID"]
    expected_offset = enriched_message.offset() + 1
    first_row = harness.wait_for_row(event.event_id)
    assert harness.wait_for_commit(
        enriched_message.topic(), enriched_message.partition(), original_group, expected_offset
    ) == expected_offset
    assert len(harness.rows(event.event_id)) == 1

    harness.stop_consumers()
    replay_group = f"{original_group}-replay"
    assert (
        harness.committed(enriched_message.topic(), enriched_message.partition(), replay_group) < 0
    )
    env["POSTGRES_SINK_GROUP_ID"] = replay_group
    harness.start_consumer("postgres_sink_consumer")

    assert harness.wait_for_commit(
        enriched_message.topic(), enriched_message.partition(), replay_group, expected_offset
    ) == expected_offset
    assert harness.rows(event.event_id) == [first_row]
