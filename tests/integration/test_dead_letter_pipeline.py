"""Verify a bad raw Kafka record reaches DLQ without blocking the next record."""

import base64
import json
from datetime import UTC, datetime
from random import Random

import pytest

from seismic_pipeline.producers import fake_earthquake_producer
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake


@pytest.mark.integration
def test_invalid_raw_bytes_reach_dlq_and_next_event_is_processed(
    integration_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = integration_harness
    env = harness.env
    env["CONSUMER_GROUP_ID"] = f"{env['CONSUMER_GROUP_ID']}-dlq-test"
    env["POSTGRES_SINK_GROUP_ID"] = f"{env['POSTGRES_SINK_GROUP_ID']}-dlq-test"
    event = generate_fake_earthquake(rng=Random(37), now=datetime.now(UTC))
    key = event.event_id.encode("utf-8")
    invalid_value = b"\x01invalid-confluent-avro"
    monkeypatch.setattr(fake_earthquake_producer, "generate_fake_earthquake", lambda: event)

    harness.start_consumer("enriching_consumer")
    harness.start_consumer("postgres_sink_consumer")
    source = harness.publish(env["KAFKA_RAW_EARTHQUAKES_TOPIC"], key, invalid_value)
    source_position = f"{source.topic()}:{source.partition()}:{source.offset()}"
    dead_letter = harness.wait_for_message(
        env["KAFKA_DEAD_LETTER_TOPIC"], source_position.encode("utf-8")
    )
    envelope = json.loads(dead_letter.value())
    assert envelope["schema_version"] == 1
    assert envelope["stage"] == "enricher"
    assert envelope["error_code"] == "INVALID_AVRO"
    assert envelope["attempts"] == 1
    assert envelope["source_topic"] == source.topic()
    assert envelope["source_partition"] == source.partition()
    assert envelope["source_offset"] == source.offset()
    assert envelope["event_id"] is None
    assert base64.b64decode(envelope["original_key_base64"]) == key
    assert base64.b64decode(envelope["original_value_base64"]) == invalid_value
    assert harness.wait_for_commit(
        source.topic(), source.partition(), env["CONSUMER_GROUP_ID"], source.offset() + 1
    ) == source.offset() + 1

    fake_earthquake_producer.publish_fake_earthquakes(
        bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"],
        registry_url=env["SCHEMA_REGISTRY_URL"],
        topic=env["KAFKA_RAW_EARTHQUAKES_TOPIC"],
        event_count=1,
        interval_seconds=0,
    )
    enriched_message = harness.wait_for_message(env["KAFKA_ENRICHED_EARTHQUAKES_TOPIC"], key)
    assert harness.decode(enriched_message)["event_id"] == event.event_id
    assert harness.wait_for_row(event.event_id)["event_id"] == event.event_id
    assert harness.wait_for_commit(
        source.topic(), source.partition(), env["CONSUMER_GROUP_ID"], source.offset() + 2
    ) == source.offset() + 2
    assert harness.wait_for_commit(
        enriched_message.topic(),
        enriched_message.partition(),
        env["POSTGRES_SINK_GROUP_ID"],
        enriched_message.offset() + 1,
    ) == enriched_message.offset() + 1
