"""Specify the JSON dead-letter envelope without requiring Kafka or Avro."""

import base64
import json
from datetime import UTC, datetime

from seismic_pipeline.consumers.dead_letter import build_dead_letter_record


class FakeMessage:
    def __init__(self, *, key: bytes | None, value: bytes | None) -> None:
        self._key = key
        self._value = value

    def topic(self) -> str:
        return "raw_earthquakes"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 17

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value


def test_envelope_preserves_undecodable_input_without_event_id() -> None:
    message = FakeMessage(key=b"\xff\x00", value=b"\x00\xffinvalid-avro")
    failed_at = datetime(2026, 10, 24, 12, 30, tzinfo=UTC)

    key, value = build_dead_letter_record(
        message,
        consumer_stage="enricher",
        error_code="INVALID_AVRO",
        error_message="Avro payload cannot be decoded",
        attempts=1,
        failed_at=failed_at,
    )

    envelope = json.loads(value.decode("utf-8"))
    assert key == b"raw_earthquakes:2:17"
    assert envelope == {
        "schema_version": 1,
        "stage": "enricher",
        "error_code": "INVALID_AVRO",
        "error_message": "Avro payload cannot be decoded",
        "failed_at_utc": "2026-10-24T12:30:00Z",
        "attempts": 1,
        "source_topic": "raw_earthquakes",
        "source_partition": 2,
        "source_offset": 17,
        "original_key_base64": base64.b64encode(message.key()).decode("ascii"),
        "original_value_base64": base64.b64encode(message.value()).decode("ascii"),
        "event_id": None,
    }
    assert base64.b64decode(envelope["original_value_base64"]) == message.value()


def test_envelope_keeps_known_event_id_and_null_kafka_key() -> None:
    message = FakeMessage(key=None, value=b"enriched-avro")

    key, value = build_dead_letter_record(
        message,
        consumer_stage="postgres_sink",
        error_code="INVALID_EVENT_TIME",
        error_message="event_time_utc must include a UTC offset",
        attempts=1,
        failed_at=datetime(2026, 10, 24, 12, 31, tzinfo=UTC),
        event_id="fake-123",
    )

    envelope = json.loads(value)
    assert key == b"raw_earthquakes:2:17"
    assert envelope["stage"] == "postgres_sink"
    assert envelope["event_id"] == "fake-123"
    assert envelope["original_key_base64"] is None
    assert base64.b64decode(envelope["original_value_base64"]) == message.value()
