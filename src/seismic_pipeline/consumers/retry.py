"""Bounded in-process retries for explicitly recoverable consumer operations."""

import logging
from collections.abc import Callable
from time import sleep as default_sleep
from typing import TypeVar

from seismic_pipeline.consumers.error_policy import ErrorAction, FailureStage, classify_error

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


def retry_recoverable(
    operation: Callable[[], T],
    *,
    stage: FailureStage,
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    sleep: Callable[[float], None] = default_sleep,
    on_retry: Callable[[], None] | None = None,
) -> T:
    """Retry a classified transient failure; propagate all other failures.

    The caller owns transaction boundaries and connection replacement. An
    on_retry callback may replace a broken connection before the next attempt.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if delay_seconds < 0:
        raise ValueError("delay_seconds must not be negative")

    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except Exception as error:
            if classify_error(error, stage=stage) is not ErrorAction.RETRY:
                raise
            LOGGER.warning(
                "Recoverable %s failure attempt=%s/%s: %s",
                stage,
                attempt,
                max_attempts,
                error,
            )
            if attempt == max_attempts:
                raise
            sleep(delay_seconds)
            if on_retry is not None:
                on_retry()

    raise AssertionError("Retry loop ended without a result or exception")
