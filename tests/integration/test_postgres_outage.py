"""Verify an enriched event stays pending while Postgres is unavailable."""

import time
from datetime import UTC, datetime
from pathlib import Path
from random import Random

import pytest
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from seismic_pipeline.consumers.enriching_consumer import ENRICHED_SCHEMA_PATH
from seismic_pipeline.processors.enrichment import enrich_earthquake
from seismic_pipeline.producers import fake_earthquake_producer
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake


@pytest.mark.integration
def test_postgres_outage_leaves_offset_pending_until_sink_restart(
    integration_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = integration_harness
    env = harness.env
    env["CONSUMER_GROUP_ID"] = f"{env['CONSUMER_GROUP_ID']}-outage-test"
    env["POSTGRES_SINK_GROUP_ID"] = f"{env['POSTGRES_SINK_GROUP_ID']}-outage-test"
    baseline = generate_fake_earthquake(rng=Random(41), now=datetime.now(UTC))
    monkeypatch.setattr(fake_earthquake_producer, "generate_fake_earthquake", lambda: baseline)

    harness.start_consumer("enriching_consumer")
    harness.start_consumer("postgres_sink_consumer")
    fake_earthquake_producer.publish_fake_earthquakes(
        bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"],
        registry_url=env["SCHEMA_REGISTRY_URL"],
        topic=env["KAFKA_RAW_EARTHQUAKES_TOPIC"],
        event_count=1,
        interval_seconds=0,
    )
    baseline_message = harness.wait_for_message(
        env["KAFKA_ENRICHED_EARTHQUAKES_TOPIC"], baseline.event_id.encode("utf-8")
    )
    assert harness.wait_for_row(baseline.event_id)["event_id"] == baseline.event_id
    assert harness.wait_for_commit(
        baseline_message.topic(),
        baseline_message.partition(),
        env["POSTGRES_SINK_GROUP_ID"],
        baseline_message.offset() + 1,
    ) == baseline_message.offset() + 1

    pending = generate_fake_earthquake(rng=Random(43), now=datetime.now(UTC))
    pending_event = enrich_earthquake(pending.to_dict())
    serializer = AvroSerializer(
        harness.registry,
        ENRICHED_SCHEMA_PATH.read_text(encoding="utf-8"),
        conf={
            "auto.register.schemas": False,
            "subject.name.strategy": topic_subject_name_strategy,
            "validate.strict": True,
        },
    )
    topic = env["KAFKA_ENRICHED_EARTHQUAKES_TOPIC"]
    value = serializer(pending_event, SerializationContext(topic, MessageField.VALUE))
    assert isinstance(value, bytes)

    harness.stop_postgres()
    try:
        pending_message = harness.publish(topic, pending.event_id.encode("utf-8"), value)
        committed_before = harness.committed(
            topic, pending_message.partition(), env["POSTGRES_SINK_GROUP_ID"]
        )
        sink_log_path = Path(harness.processes[1][1].name)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            sink_log = sink_log_path.read_text(encoding="utf-8")
            if "Recoverable persist failure" in sink_log:
                break
            time.sleep(0.2)
        else:
            pytest.fail(f"Sink did not report a Postgres write failure:\n{sink_log}")
        assert harness.committed(
            topic, pending_message.partition(), env["POSTGRES_SINK_GROUP_ID"]
        ) == committed_before
        assert committed_before <= pending_message.offset()
    finally:
        harness.stop_consumers()
        harness.start_postgres()

    assert harness.rows(pending.event_id) == []
    admin = AdminClient({"bootstrap.servers": env["KAFKA_BOOTSTRAP_SERVERS"]})
    deadline = time.monotonic() + 75
    while time.monotonic() < deadline:
        description = admin.describe_consumer_groups(
            [env["POSTGRES_SINK_GROUP_ID"]], request_timeout=5
        )[env["POSTGRES_SINK_GROUP_ID"]].result(timeout=6)
        if not description.members:
            break
        time.sleep(0.5)
    else:
        pytest.fail("The stopped sink remained a Kafka group member after 75 seconds")
    harness.start_consumer("postgres_sink_consumer")
    assert harness.wait_for_row(pending.event_id)["event_id"] == pending.event_id
    assert len(harness.rows(pending.event_id)) == 1
    assert harness.wait_for_commit(
        topic,
        pending_message.partition(),
        env["POSTGRES_SINK_GROUP_ID"],
        pending_message.offset() + 1,
    ) == pending_message.offset() + 1
