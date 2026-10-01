"""Register the raw earthquake Avro contract in Schema Registry."""

import json
import logging
import os
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError
from fastavro import parse_schema
from httpx import HTTPError

LOGGER = logging.getLogger(__name__)
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "raw_earthquake_event.avsc"
COMPATIBILITY = "BACKWARD_TRANSITIVE"


def register_raw_earthquake_schema(*, registry_url: str, topic: str) -> tuple[str, int, int]:
    """Register the Avro schema and return its subject, version, and ID."""
    if not registry_url.strip() or not topic.strip():
        raise ValueError("Schema Registry URL and topic must not be empty")

    schema_text = SCHEMA_PATH.read_text(encoding="utf-8")
    parse_schema(json.loads(schema_text))
    schema = Schema(schema_text, schema_type="AVRO")
    subject = f"{topic}-value"
    client = SchemaRegistryClient({"url": registry_url})

    # Configure the subject before registration so all versions are checked.
    client.set_compatibility(subject_name=subject, level=COMPATIBILITY)
    schema_id = client.register_schema(subject, schema)
    version = client.lookup_schema(subject, schema).version
    LOGGER.info("Registered %s version=%s id=%s", subject, version, schema_id)
    return subject, version, schema_id


def main() -> int:
    """Read local settings and register the raw earthquake schema."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        register_raw_earthquake_schema(
            registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            topic=os.getenv("KAFKA_RAW_EARTHQUAKES_TOPIC", "raw_earthquakes"),
        )
    except (SchemaRegistryError, HTTPError, OSError, ValueError) as error:
        LOGGER.error("Could not register raw earthquake schema: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
