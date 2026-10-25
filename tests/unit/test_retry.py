"""Specify bounded retries for operations with classified transient failures."""

import logging

import psycopg
import pytest

from seismic_pipeline.consumers.error_policy import FailureStage, InvalidMessageError
from seismic_pipeline.consumers.retry import retry_recoverable


def test_transient_failure_succeeds_on_third_attempt(caplog: pytest.LogCaptureFixture) -> None:
    calls = 0
    waits: list[float] = []

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise psycopg.OperationalError("connection lost")
        return "stored"

    with caplog.at_level(logging.WARNING):
        result = retry_recoverable(
            operation,
            stage=FailureStage.PERSIST,
            sleep=waits.append,
        )

    assert result == "stored"
    assert calls == 3
    assert waits == [1.0, 1.0]
    assert "attempt=1/3" in caplog.text
    assert "attempt=2/3" in caplog.text


def test_transient_failure_stops_after_three_attempts() -> None:
    calls = 0
    waits: list[float] = []

    def operation() -> None:
        nonlocal calls
        calls += 1
        raise psycopg.OperationalError("connection lost")

    with pytest.raises(psycopg.OperationalError, match="connection lost"):
        retry_recoverable(operation, stage=FailureStage.PERSIST, sleep=waits.append)

    assert calls == 3
    assert waits == [1.0, 1.0]


@pytest.mark.parametrize(
    "error",
    [InvalidMessageError("invalid input"), RuntimeError("unexpected bug")],
)
def test_nonrecoverable_failure_is_not_retried(error: Exception) -> None:
    calls = 0
    waits: list[float] = []

    def operation() -> None:
        nonlocal calls
        calls += 1
        raise error

    with pytest.raises(type(error)):
        retry_recoverable(operation, stage=FailureStage.PERSIST, sleep=waits.append)

    assert calls == 1
    assert waits == []


def test_reconnection_callback_runs_before_next_attempt() -> None:
    actions: list[str] = []

    def operation() -> str:
        actions.append("operation")
        if actions.count("operation") == 1:
            raise psycopg.OperationalError("connection lost")
        return "stored"

    result = retry_recoverable(
        operation,
        stage=FailureStage.PERSIST,
        sleep=lambda _: actions.append("wait"),
        on_retry=lambda: actions.append("reconnect"),
    )

    assert result == "stored"
    assert actions == ["operation", "wait", "reconnect", "operation"]
