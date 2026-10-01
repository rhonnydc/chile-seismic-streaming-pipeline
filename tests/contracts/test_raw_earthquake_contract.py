"""Verify the raw earthquake Avro contract without running Kafka or Schema Registry."""

import json
from dataclasses import fields
from datetime import UTC, datetime
from pathlib import Path
from random import Random
from types import SimpleNamespace

import pytest
from confluent_kafka.schema_registry import Schema, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastavro import parse_schema

from seismic_pipeline.common.earthquake_event import EarthquakeEvent
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "raw_earthquake_event.avsc"
CONTEXT = SerializationContext("raw_earthquakes", MessageField.VALUE)


class FakeRegistry:
    """Supply a known schema ID without contacting Schema Registry."""

    def __init__(self, schema_text: str) -> None:
        self.schema = Schema(schema_text, schema_type="AVRO")

    def lookup_schema(self, subject: str, schema: Schema, normalize_schemas: bool = False):
        assert subject == "raw_earthquakes-value"
        assert json.loads(schema.schema_str) == json.loads(self.schema.schema_str)
        return SimpleNamespace(schema_id=42, guid=None)

    def get_schema(self, schema_id: int, subject: str | None = None, fmt: str | None = None):
        assert schema_id == 42
        return self.schema

    def config(self) -> dict:
        return {}


@pytest.fixture
def codecs() -> tuple[AvroSerializer, AvroDeserializer]:
    schema_text = SCHEMA_PATH.read_text(encoding="utf-8")
    registry = FakeRegistry(schema_text)
    serializer = AvroSerializer(
        registry,
        schema_text,
        conf={
            "auto.register.schemas": False,
            "subject.name.strategy": topic_subject_name_strategy,
            "validate.strict": True,
        },
    )
    deserializer = AvroDeserializer(
        registry,
        conf={"subject.name.strategy": topic_subject_name_strategy},
    )
    return serializer, deserializer


def test_schema_loads_and_matches_internal_event() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    parsed = parse_schema(schema)

    assert parsed["name"] == "com.rhonnydc.seismic.RawEarthquakeEvent"
    assert [field["name"] for field in schema["fields"]] == [
        field.name for field in fields(EarthquakeEvent)
    ]


def test_fake_event_survives_serializer_round_trip(
    codecs: tuple[AvroSerializer, AvroDeserializer],
) -> None:
    serializer, deserializer = codecs
    event = generate_fake_earthquake(
        rng=Random(0), now=datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
    ).to_dict()

    wire_value = serializer(event, CONTEXT)

    assert isinstance(wire_value, bytes)
    assert wire_value[:5] == bytes([0, 0, 0, 0, 42])
    assert deserializer(wire_value, CONTEXT) == event


@pytest.mark.parametrize(
    ("change", "expected_error"),
    [
        (lambda payload: payload.pop("event_id"), "event_id"),
        (lambda payload: payload.update(magnitude="not-a-number"), "not-a-number"),
    ],
)
def test_invalid_event_cannot_be_serialized(
    codecs: tuple[AvroSerializer, AvroDeserializer], change, expected_error: str
) -> None:
    serializer, _ = codecs
    payload = generate_fake_earthquake(rng=Random(0)).to_dict()
    change(payload)

    with pytest.raises((TypeError, ValueError), match=expected_error):
        serializer(payload, CONTEXT)
