"""Classify consumer failures without treating unknown exceptions as bad data."""

from enum import StrEnum

import psycopg
from confluent_kafka.schema_registry.error import SchemaRegistryError
from httpx import TransportError


class ErrorAction(StrEnum):
    """The action a consumer may take after a failed operation."""

    RETRY = "retry"
    DEAD_LETTER = "dead_letter"
    STOP = "stop"


class FailureStage(StrEnum):
    """The operation that failed for the current Kafka message."""

    DESERIALIZE = "deserialize"
    PROCESS = "process"
    PERSIST = "persist"
    PUBLISH = "publish"


class InvalidMessageError(ValueError):
    """An input was proven permanently invalid by an explicit validation step."""


def classify_error(error: Exception, *, stage: FailureStage) -> ErrorAction:
    """Return a conservative action; unknown failures always stop processing.

    Callers must establish that a malformed input caused InvalidMessageError.
    A deserializer exception alone is not proof: Schema Registry may be down.
    """
    if isinstance(error, InvalidMessageError):
        if stage in (FailureStage.DESERIALIZE, FailureStage.PROCESS, FailureStage.PERSIST):
            return ErrorAction.DEAD_LETTER
        return ErrorAction.STOP

    if stage is FailureStage.DESERIALIZE:
        if isinstance(error, SchemaRegistryError):
            status = error.http_status_code
            if status == 429 or 500 <= status <= 599:
                return ErrorAction.RETRY
        if isinstance(error, TransportError):
            return ErrorAction.RETRY

    if stage is FailureStage.PERSIST and isinstance(error, psycopg.OperationalError):
        return ErrorAction.RETRY

    return ErrorAction.STOP
