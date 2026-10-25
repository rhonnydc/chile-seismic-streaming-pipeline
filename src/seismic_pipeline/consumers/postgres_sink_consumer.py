"""Consume enriched Avro earthquakes and persist them in Postgres."""

import logging
import os
from datetime import UTC, datetime
from functools import partial

import psycopg
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
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
from seismic_pipeline.sinks.postgres_sink import TIMESTAMP_COLUMNS, event_to_row, persist_event

LOGGER = logging.getLogger(__name__)


def _record_delivery(error: object, _message: object, *, reports: list[object]) -> None:
    reports.append(error)


def consume_to_postgres(
    *,
    bootstrap_servers: str,
    registry_url: str,
    enriched_topic: str,
    group_id: str,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    postgres_user: str,
    postgres_password: str,
    dead_letter_topic: str = "dead_letter_earthquakes",
) -> None:
    """Commit each Kafka offset only after its database transaction commits."""
    if not all(
        value.strip()
        for value in (
            bootstrap_servers,
            registry_url,
            enriched_topic,
            group_id,
            postgres_host,
            postgres_db,
            postgres_user,
            postgres_password,
            dead_letter_topic,
        )
    ):
        raise ValueError("Kafka, Schema Registry, and Postgres settings must not be empty")
    if not 1 <= postgres_port <= 65535:
        raise ValueError("Postgres port must be between 1 and 65535")
    if enriched_topic == dead_letter_topic:
        raise ValueError("Enriched and dead-letter topics must be different")

    registry = SchemaRegistryClient({"url": registry_url})
    deserializer = AvroDeserializer(registry)
    producer: Producer | None = None

    connection_settings = {
        "host": postgres_host,
        "port": postgres_port,
        "dbname": postgres_db,
        "user": postgres_user,
        "password": postgres_password,
        "autocommit": False,
    }
    connection = psycopg.connect(**connection_settings)

    def reconnect() -> None:
        nonlocal connection
        previous = connection
        connection = None
        if previous is not None:
            previous.close()
        connection = psycopg.connect(**connection_settings)

    try:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        try:
            consumer.subscribe([enriched_topic])
            LOGGER.info("Consuming topic=%s group=%s", enriched_topic, group_id)
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())

                event: dict[str, object] | None = None
                error_code = "INVALID_AVRO"
                try:
                    enriched_value = message.value()
                    if enriched_value is None or (
                        isinstance(enriched_value, bytes)
                        and (len(enriched_value) <= 5 or enriched_value[0] != 0)
                    ):
                        raise InvalidMessageError("Invalid Confluent Avro framing")
                    context = SerializationContext(enriched_topic, MessageField.VALUE)
                    event = retry_recoverable(
                        lambda value=enriched_value, serialization_context=context: deserializer(
                            value, serialization_context
                        ),
                        stage=FailureStage.DESERIALIZE,
                    )
                    if not isinstance(event, dict):
                        raise InvalidMessageError(
                            "Expected a non-null Avro record from enriched_earthquakes"
                        )
                    error_code = "INVALID_EVENT_TIME"
                    try:
                        if any(column in event for column in TIMESTAMP_COLUMNS):
                            event_to_row(event)
                    except ValueError as error:
                        raise InvalidMessageError(
                            "Invalid enriched event date or timestamp"
                        ) from error
                except Exception as error:
                    if (
                        classify_error(error, stage=FailureStage.DESERIALIZE)
                        is not ErrorAction.DEAD_LETTER
                    ):
                        raise
                    known_id = event.get("event_id") if isinstance(event, dict) else None
                    key, value = build_dead_letter_record(
                        message,
                        consumer_stage="postgres_sink",
                        error_code=error_code,
                        error_message=str(error),
                        attempts=1,
                        failed_at=datetime.now(UTC),
                        event_id=known_id if isinstance(known_id, str) else None,
                    )
                    if producer is None:
                        producer = Producer(
                            {"bootstrap.servers": bootstrap_servers, "message.timeout.ms": 10000}
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
                        ) from error
                    consumer.commit(message=message, asynchronous=False)
                    LOGGER.warning(
                        "Dead-lettered enriched_partition=%s enriched_offset=%s group=%s error=%s",
                        message.partition(),
                        message.offset(),
                        group_id,
                        error,
                    )
                    continue

                inserted = retry_recoverable(
                    lambda record=event: persist_event(connection, record),
                    stage=FailureStage.PERSIST,
                    on_retry=reconnect,
                )
                consumer.commit(message=message, asynchronous=False)
                LOGGER.info(
                    "Stored event_id=%s inserted=%s partition=%s offset=%s group=%s",
                    event["event_id"],
                    inserted,
                    message.partition(),
                    message.offset(),
                    group_id,
                )
        finally:
            consumer.close()
            LOGGER.info("Consumer closed group=%s", group_id)
    finally:
        if connection is not None:
            connection.close()


def main() -> int:
    """Run the sink consumer with local environment settings."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        consume_to_postgres(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            enriched_topic=os.getenv("KAFKA_ENRICHED_EARTHQUAKES_TOPIC", "enriched_earthquakes"),
            group_id=os.getenv("POSTGRES_SINK_GROUP_ID", "seismic-postgres-sink"),
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "seismic"),
            postgres_user=os.getenv("POSTGRES_USER", "seismic_user"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "seismic_password"),
            dead_letter_topic=os.getenv("KAFKA_DEAD_LETTER_TOPIC", "dead_letter_earthquakes"),
        )
    except KeyboardInterrupt:
        LOGGER.info("Postgres sink stopped by user")
        return 0
    except (
        BufferError,
        HTTPError,
        KafkaException,
        KeyError,
        OSError,
        psycopg.Error,
        RuntimeError,
        SchemaRegistryError,
        SerializationError,
        TypeError,
        ValueError,
    ) as error:
        LOGGER.error("Postgres sink failed: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
