"""Publish simulated earthquake events to Kafka as Avro."""

import logging
import os
import time
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError
from httpx import HTTPError

from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake

LOGGER = logging.getLogger(__name__)
SCHEMA_PATH = Path(__file__).resolve().parents[3] / "schemas" / "raw_earthquake_event.avsc"


def publish_fake_earthquakes(
    *,
    bootstrap_servers: str,
    registry_url: str,
    topic: str,
    event_count: int,
    interval_seconds: float,
) -> None:
    """Serialize generated events as Avro, then wait for Kafka delivery reports."""
    if not bootstrap_servers.strip() or not registry_url.strip() or not topic.strip():
        raise ValueError("Kafka servers, Schema Registry URL, and topic must not be empty")
    if event_count < 0 or interval_seconds < 0:
        raise ValueError("Event count and interval must not be negative")

    registry_client = SchemaRegistryClient({"url": registry_url})
    serializer = AvroSerializer(
        registry_client,
        SCHEMA_PATH.read_text(encoding="utf-8"),
        conf={
            "auto.register.schemas": False,
            "subject.name.strategy": topic_subject_name_strategy,
            "validate.strict": True,
        },
    )
    producer = Producer({"bootstrap.servers": bootstrap_servers, "message.timeout.ms": 10000})
    delivery_errors: list[str] = []

    def on_delivery(error: object, message: object) -> None:
        event_id = message.key().decode("utf-8")
        if error is not None:
            delivery_errors.append(f"{event_id}: {error}")
            LOGGER.error("Delivery failed event_id=%s error=%s", event_id, error)
        else:
            LOGGER.info(
                "Delivered event_id=%s topic=%s partition=%s offset=%s",
                event_id,
                message.topic(),
                message.partition(),
                message.offset(),
            )

    try:
        for index in range(event_count):
            event = generate_fake_earthquake()
            value = serializer(event.to_dict(), SerializationContext(topic, MessageField.VALUE))
            if value is None:
                raise ValueError(f"Avro serializer returned no value for {event.event_id}")
            producer.produce(
                topic=topic,
                key=event.event_id.encode("utf-8"),
                value=value,
                on_delivery=on_delivery,
            )
            LOGGER.info(
                "Queued event_id=%s magnitude=%.1f region=%s",
                event.event_id,
                event.magnitude,
                event.region,
            )
            producer.poll(0)
            if index + 1 < event_count:
                time.sleep(interval_seconds)
    finally:
        remaining = producer.flush(15.0)

    if remaining:
        raise RuntimeError(f"{remaining} Kafka message(s) were not delivered before timeout")
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed for {len(delivery_errors)} message(s)")

    LOGGER.info("Finished: %s event(s) delivered to %s", event_count, topic)


def main() -> int:
    """Run the fake producer using environment variables."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        publish_fake_earthquakes(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            topic=os.getenv("KAFKA_RAW_EARTHQUAKES_TOPIC", "raw_earthquakes"),
            event_count=int(os.getenv("FAKE_PRODUCER_EVENT_COUNT", "10")),
            interval_seconds=float(os.getenv("FAKE_PRODUCER_INTERVAL_SECONDS", "1")),
        )
    except (
        BufferError,
        HTTPError,
        KafkaException,
        OSError,
        RuntimeError,
        SchemaRegistryError,
        SerializationError,
        TypeError,
        ValueError,
    ) as error:
        LOGGER.error("Fake producer failed: %s", error)
        return 1
    except KeyboardInterrupt:
        LOGGER.warning("Fake producer interrupted")
        return 130

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
