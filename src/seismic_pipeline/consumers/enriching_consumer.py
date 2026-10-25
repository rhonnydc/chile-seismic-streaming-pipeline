"""Consume raw Avro earthquakes and publish enriched Avro events."""

import logging
import os
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError
from httpx import HTTPError

from seismic_pipeline.consumers.dead_letter import build_dead_letter_record
from seismic_pipeline.consumers.error_policy import (
    ErrorAction,
    FailureStage,
    InvalidMessageError,
    classify_error,
)
from seismic_pipeline.consumers.retry import retry_recoverable
from seismic_pipeline.processors.enrichment import enrich_earthquake

LOGGER = logging.getLogger(__name__)
ENRICHED_SCHEMA_PATH = (
    Path(__file__).resolve().parents[3] / "schemas" / "enriched_earthquake_event.avsc"
)


def _record_delivery(error: object, _message: object, *, reports: list[object]) -> None:
    reports.append(error)


def _validate_event_timestamps(raw_event: dict[str, object]) -> None:
    """Mark only demonstrably invalid input timestamps as poison data."""
    for field in ("event_time_utc", "ingested_at"):
        value = raw_event[field]
        if not isinstance(value, str):
            raise TypeError(f"{field} must be a string in the decoded Avro record")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise InvalidMessageError(f"{field} must be an ISO 8601 timestamp") from error
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise InvalidMessageError(f"{field} must include a UTC offset")


def _publish_dead_letter(
    producer: Producer,
    message: object,
    *,
    dead_letter_topic: str,
    error: InvalidMessageError,
    error_code: str,
    event_id: str | None = None,
) -> None:
    key, value = build_dead_letter_record(
        message,
        consumer_stage="enricher",
        error_code=error_code,
        error_message=str(error),
        attempts=1,
        failed_at=datetime.now(UTC),
        event_id=event_id,
    )
    reports: list[object] = []
    producer.produce(
        topic=dead_letter_topic,
        key=key,
        value=value,
        on_delivery=partial(_record_delivery, reports=reports),
    )
    remaining = producer.flush(15.0)
    if remaining or len(reports) != 1 or reports[0] is not None:
        raise RuntimeError(
            "Dead-letter delivery failed for "
            f"{message.topic()}:{message.partition()}:{message.offset()}: "
            f"remaining={remaining}, reports={reports}"
        )


def consume_and_enrich(
    *,
    bootstrap_servers: str,
    registry_url: str,
    raw_topic: str,
    enriched_topic: str,
    group_id: str,
    dead_letter_topic: str = "dead_letter_earthquakes",
) -> None:
    """Process one input at a time and commit only after output delivery succeeds."""
    if not all(
        value.strip()
        for value in (
            bootstrap_servers,
            registry_url,
            raw_topic,
            enriched_topic,
            group_id,
            dead_letter_topic,
        )
    ):
        raise ValueError("Kafka, Schema Registry, topic, and group settings must not be empty")
    if len({raw_topic, enriched_topic, dead_letter_topic}) != 3:
        raise ValueError("Raw, enriched, and dead-letter topics must be different")

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
            try:
                raw_value = message.value()
                if raw_value is None or (
                    isinstance(raw_value, bytes) and (len(raw_value) <= 5 or raw_value[0] != 0)
                ):
                    raise InvalidMessageError("Invalid Confluent Avro framing")
                raw_event = retry_recoverable(
                    lambda value=raw_value, serialization_context=context: deserializer(
                        value, serialization_context
                    ),
                    stage=FailureStage.DESERIALIZE,
                )
                if not isinstance(raw_event, dict):
                    raise InvalidMessageError(
                        "Expected a non-null Avro record from raw_earthquakes"
                    )
            except Exception as error:
                if (
                    classify_error(error, stage=FailureStage.DESERIALIZE)
                    is not ErrorAction.DEAD_LETTER
                ):
                    raise
                _publish_dead_letter(
                    producer,
                    message,
                    dead_letter_topic=dead_letter_topic,
                    error=error,
                    error_code="INVALID_AVRO",
                )
                consumer.commit(message=message, asynchronous=False)
                LOGGER.warning(
                    "Dead-lettered raw_partition=%s raw_offset=%s group=%s error=%s",
                    message.partition(),
                    message.offset(),
                    group_id,
                    error,
                )
                continue
            try:
                _validate_event_timestamps(raw_event)
                enriched_event = enrich_earthquake(raw_event)
            except Exception as error:
                if classify_error(error, stage=FailureStage.PROCESS) is not ErrorAction.DEAD_LETTER:
                    raise
                known_id = raw_event.get("event_id")
                _publish_dead_letter(
                    producer,
                    message,
                    dead_letter_topic=dead_letter_topic,
                    error=error,
                    error_code="INVALID_EVENT_TIME",
                    event_id=known_id if isinstance(known_id, str) else None,
                )
                consumer.commit(message=message, asynchronous=False)
                LOGGER.warning(
                    "Dead-lettered raw_partition=%s raw_offset=%s group=%s error=%s",
                    message.partition(),
                    message.offset(),
                    group_id,
                    error,
                )
                continue
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
            dead_letter_topic=os.getenv("KAFKA_DEAD_LETTER_TOPIC", "dead_letter_earthquakes"),
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
