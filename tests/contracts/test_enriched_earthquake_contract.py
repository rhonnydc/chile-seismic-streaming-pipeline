"""Verify the enriched Avro contract without Kafka or Schema Registry."""

import io
import json
from datetime import UTC, datetime
from pathlib import Path
from random import Random

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from seismic_pipeline.processors.enrichment import enrich_earthquake
from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake

SCHEMAS_DIR = Path(__file__).resolve().parents[2] / "schemas"


def test_enriched_schema_preserves_raw_fields_and_adds_derived_fields() -> None:
    raw_schema = json.loads((SCHEMAS_DIR / "raw_earthquake_event.avsc").read_text(encoding="utf-8"))
    enriched_schema = json.loads(
        (SCHEMAS_DIR / "enriched_earthquake_event.avsc").read_text(encoding="utf-8")
    )

    assert [field["name"] for field in enriched_schema["fields"]] == [
        field["name"] for field in raw_schema["fields"]
    ] + [
        "severity_level",
        "is_shallow",
        "event_date_utc",
        "event_hour_utc",
        "ingestion_latency_seconds",
        "processed_at_utc",
    ]
    assert parse_schema(enriched_schema)["name"] == "com.rhonnydc.seismic.EnrichedEarthquakeEvent"


def test_enriched_event_survives_avro_round_trip() -> None:
    schema = parse_schema(
        json.loads((SCHEMAS_DIR / "enriched_earthquake_event.avsc").read_text(encoding="utf-8"))
    )
    raw_event = generate_fake_earthquake(
        rng=Random(0), now=datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
    ).to_dict()
    enriched_event = enrich_earthquake(
        raw_event, processed_at=datetime(2026, 10, 3, 12, 0, 6, tzinfo=UTC)
    )
    buffer = io.BytesIO()

    schemaless_writer(buffer, schema, enriched_event)
    buffer.seek(0)

    assert schemaless_reader(buffer, schema) == enriched_event
