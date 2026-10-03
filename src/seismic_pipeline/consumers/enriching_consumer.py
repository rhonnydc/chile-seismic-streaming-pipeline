"""Consume raw Avro earthquakes and publish enriched Avro events."""

import logging
import os
from functools import partial
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError
from httpx import HTTPError

from seismic_pipeline.processors.enrichment import enrich_earthquake

LOGGER = logging.getLogger(__name__)
ENRICHED_SCHEMA_PATH = (
    Path(__file__).resolve().parents[3] / "schemas" / "enriched_earthquake_event.avsc"
)


def _record_delivery(error: object, _message: object, *, reports: list[object]) -> None:
    reports.append(error)


def consume_and_enrich(
    *,
    bootstrap_servers: str,
    registry_url: str,
    raw_topic: str,
    enriched_topic: str,
    group_id: str,
) -> None:
    """Process one input at a time and commit only after output delivery succeeds."""
    if not all(
        value.strip()
        for value in (bootstrap_servers, registry_url, raw_topic, enriched_topic, group_id)
    ):
        raise ValueError("Kafka, Schema Registry, topic, and group settings must not be empty")
    if raw_topic == enriched_topic:
        raise ValueError("Raw and enriched topics must be different")

    registry = SchemaRegistryClient({"url": registry_url})
    deserializer = AvroDeserializer(registry)
    serializer = AvroSerializer(
        registry,
        ENRICHED_SCHEMA_PATH.read_text(encoding="utf-8"),
        conf={
            "auto.register.schemas": False,
            "subject.name.strategy": topic_subject_name_strategy,
            "validate.strict": True,
        },
    )
    producer = Producer({"bootstrap.servers": bootstrap_servers, "message.timeout.ms": 10000})
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    try:
        consumer.subscribe([raw_topic])
        LOGGER.info("Consuming topic=%s group=%s", raw_topic, group_id)
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            context = SerializationContext(raw_topic, MessageField.VALUE)
            raw_event = deserializer(message.value(), context)
            if not isinstance(raw_event, dict):
                raise ValueError("Expected a non-null Avro record from raw_earthquakes")
            enriched_event = enrich_earthquake(raw_event)
            event_id = enriched_event["event_id"]
            value = serializer(
                enriched_event, SerializationContext(enriched_topic, MessageField.VALUE)
            )
            if value is None:
                raise ValueError(f"Avro serializer returned no value for {event_id}")

            delivery_reports: list[object] = []
            producer.produce(
                topic=enriched_topic,
                key=event_id.encode("utf-8"),
                value=value,
                on_delivery=partial(_record_delivery, reports=delivery_reports),
            )
            remaining = producer.flush(15.0)
            if remaining or len(delivery_reports) != 1 or delivery_reports[0] is not None:
                raise RuntimeError(
                    f"Enriched delivery failed for event_id={event_id}: "
                    f"remaining={remaining}, reports={delivery_reports}"
                )

            consumer.commit(message=message, asynchronous=False)
            LOGGER.info(
                "Enriched event_id=%s raw_partition=%s raw_offset=%s group=%s",
                event_id,
                message.partition(),
                message.offset(),
                group_id,
            )
    finally:
        consumer.close()
        LOGGER.info("Consumer closed group=%s", group_id)


def main() -> int:
    """Run the enricher with local environment settings."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        consume_and_enrich(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            raw_topic=os.getenv("KAFKA_RAW_EARTHQUAKES_TOPIC", "raw_earthquakes"),
            enriched_topic=os.getenv("KAFKA_ENRICHED_EARTHQUAKES_TOPIC", "enriched_earthquakes"),
            group_id=os.getenv("CONSUMER_GROUP_ID", "seismic-enricher"),
        )
    except KeyboardInterrupt:
        LOGGER.info("Enricher stopped by user")
        return 0
    except (
        BufferError,
        HTTPError,
        KafkaException,
        KeyError,
        OSError,
        RuntimeError,
        SchemaRegistryError,
        SerializationError,
        TypeError,
        ValueError,
    ) as error:
        LOGGER.error("Enricher failed: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
