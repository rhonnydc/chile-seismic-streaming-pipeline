"""Consume enriched Avro earthquakes and persist them in Postgres."""

import logging
import os

import psycopg
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError
from httpx import HTTPError

from seismic_pipeline.sinks.postgres_sink import persist_event

LOGGER = logging.getLogger(__name__)


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
        )
    ):
        raise ValueError("Kafka, Schema Registry, and Postgres settings must not be empty")
    if not 1 <= postgres_port <= 65535:
        raise ValueError("Postgres port must be between 1 and 65535")

    registry = SchemaRegistryClient({"url": registry_url})
    deserializer = AvroDeserializer(registry)

    with psycopg.connect(
        host=postgres_host,
        port=postgres_port,
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password,
        autocommit=False,
    ) as connection:
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

                event = deserializer(
                    message.value(), SerializationContext(enriched_topic, MessageField.VALUE)
                )
                if not isinstance(event, dict):
                    raise ValueError("Expected a non-null Avro record from enriched_earthquakes")

                inserted = persist_event(connection, event)
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
        )
    except KeyboardInterrupt:
        LOGGER.info("Postgres sink stopped by user")
        return 0
    except (
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
