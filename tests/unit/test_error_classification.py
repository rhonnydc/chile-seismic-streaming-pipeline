"""Define which consumer failures may retry, enter a DLQ, or stop processing."""

import psycopg
import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationError

from seismic_pipeline.consumers.error_policy import (
    ErrorAction,
    FailureStage,
    InvalidMessageError,
    classify_error,
)


@pytest.mark.parametrize("stage", [FailureStage.DESERIALIZE, FailureStage.PROCESS])
def test_explicitly_invalid_input_goes_to_dlq(stage: FailureStage) -> None:
    error = InvalidMessageError("invalid input record")

    assert classify_error(error, stage=stage) is ErrorAction.DEAD_LETTER


def test_postgres_connection_failure_can_retry_only_during_persistence() -> None:
    error = psycopg.OperationalError("connection lost")

    assert classify_error(error, stage=FailureStage.PERSIST) is ErrorAction.RETRY
    assert classify_error(error, stage=FailureStage.PROCESS) is ErrorAction.STOP


def test_temporary_registry_failure_can_retry_deserialization() -> None:
    error = SchemaRegistryError(503, 50301, "registry unavailable")

    assert classify_error(error, stage=FailureStage.DESERIALIZE) is ErrorAction.RETRY


@pytest.mark.parametrize(
    "error",
    [
        ValueError("unexpected conversion error"),
        KeyError("event_time_utc"),
        SerializationError("deserialization failed without a known cause"),
        psycopg.DataError("database rejected a value"),
        RuntimeError("programming failure"),
    ],
)
def test_ambiguous_or_unexpected_errors_stop_instead_of_entering_dlq(
    error: Exception,
) -> None:
    assert classify_error(error, stage=FailureStage.PROCESS) is ErrorAction.STOP


def test_invalid_message_marker_outside_input_stages_does_not_hide_output_bug() -> None:
    error = InvalidMessageError("unexpected output failure")

    assert classify_error(error, stage=FailureStage.PUBLISH) is ErrorAction.STOP
