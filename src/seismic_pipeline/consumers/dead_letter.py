"""Build a dead-letter record that preserves the original Kafka bytes."""

import base64
import json
from datetime import UTC, datetime
from typing import Protocol


class SourceMessage(Protocol):
    """Kafka message fields needed to investigate and replay an input."""

    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...

    def key(self) -> bytes | None: ...

    def value(self) -> bytes | None: ...


def _base64_or_none(value: bytes | None) -> str | None:
    return None if value is None else base64.b64encode(value).decode("ascii")


def build_dead_letter_record(
    message: SourceMessage,
    *,
    consumer_stage: str,
    error_code: str,
    error_message: str,
    attempts: int,
    failed_at: datetime,
    event_id: str | None = None,
) -> tuple[bytes, bytes]:
    """Return a stable source-position key and a UTF-8 JSON DLQ value.

    The value is an envelope, not an Avro earthquake. Its base64 fields allow
    investigation even when the original payload cannot be deserialized.
    """
    if failed_at.tzinfo is None:
        raise ValueError("failed_at must include a UTC offset")
    if attempts < 1:
        raise ValueError("attempts must be at least 1")

    source_topic = message.topic()
    source_partition = message.partition()
    source_offset = message.offset()
    source_key = f"{source_topic}:{source_partition}:{source_offset}".encode()
    envelope = {
        "schema_version": 1,
        "stage": consumer_stage,
        "error_code": error_code,
        "error_message": error_message,
        "failed_at_utc": failed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "attempts": attempts,
        "source_topic": source_topic,
        "source_partition": source_partition,
        "source_offset": source_offset,
        "original_key_base64": _base64_or_none(message.key()),
        "original_value_base64": _base64_or_none(message.value()),
        "event_id": event_id,
    }
    return source_key, json.dumps(envelope, ensure_ascii=False).encode("utf-8")
