"""Exercise quality evaluation offline and float checks on Postgres when available."""

import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import psycopg
import pytest

from seismic_pipeline.quality.checks import QUALITY_CHECKS, count_results, evaluate_check
from seismic_pipeline.quality.runner import run_quality_checks


def test_check_results_and_summary() -> None:
    passed = evaluate_check(QUALITY_CHECKS[0], 0)
    failed = evaluate_check(QUALITY_CHECKS[1], 2)

    assert passed.passed is True
    assert failed.passed is False
    assert failed.invalid_rows == 2
    assert count_results([passed, failed]) == (1, 1)


@pytest.mark.parametrize("invalid_rows", [-1, 1.5, True])
def test_invalid_check_count_is_rejected(invalid_rows: object) -> None:
    with pytest.raises(ValueError, match="non-negative integer"):
        evaluate_check(QUALITY_CHECKS[0], invalid_rows)


@pytest.mark.parametrize("failed_count, expected_exit", [(0, 0), (2, 1)])
def test_runner_exit_code_and_report(
    failed_count: int, expected_exit: int, capsys: pytest.CaptureFixture[str]
) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [(0,)] * (len(QUALITY_CHECKS) - 1) + [(failed_count,)]

    assert run_quality_checks(connection) == expected_exit

    report = capsys.readouterr().out
    assert "DATA QUALITY CHECKS" in report
    assert "PASS row_count_check" in report
    assert f"{len(QUALITY_CHECKS) - expected_exit} passed" in report
    assert f"{expected_exit} failed" in report
    if failed_count:
        assert "FAIL stored_at_not_null_check" in report
        assert f"invalid_rows: {failed_count}" in report
    assert cursor.execute.call_count == len(QUALITY_CHECKS) + 1


@pytest.mark.parametrize(
    "magnitude, severity, expected_invalid",
    [
        (3.9, "LOW", 0),
        (4.0, "MODERATE", 0),
        (5.9, "MODERATE", 0),
        (6.0, "HIGH", 0),
        (4.0, "LOW", 1),
        (6.0, "MODERATE", 1),
    ],
)
def test_severity_sql_boundaries(magnitude: float, severity: str, expected_invalid: int) -> None:
    sql = next(
        check.sql for check in QUALITY_CHECKS if check.name == "severity_matches_magnitude_check"
    )
    with sqlite3.connect(":memory:") as connection:
        connection.execute(
            "CREATE TABLE enriched_earthquake_events (magnitude REAL, severity_level TEXT)"
        )
        connection.execute(
            "INSERT INTO enriched_earthquake_events VALUES (?, ?)", (magnitude, severity)
        )
        assert connection.execute(sql).fetchone()[0] == expected_invalid


@pytest.mark.parametrize(
    "depth_km, is_shallow, expected_invalid",
    [(69.9, True, 0), (70.0, False, 0), (70.0, True, 1), (20.0, False, 1)],
)
def test_shallow_sql_boundary(depth_km: float, is_shallow: bool, expected_invalid: int) -> None:
    sql = next(
        check.sql for check in QUALITY_CHECKS if check.name == "is_shallow_matches_depth_check"
    )
    with sqlite3.connect(":memory:") as connection:
        connection.execute(
            "CREATE TABLE enriched_earthquake_events (depth_km REAL, is_shallow INTEGER)"
        )
        connection.execute(
            "INSERT INTO enriched_earthquake_events VALUES (?, ?)", (depth_km, is_shallow)
        )
        assert connection.execute(sql).fetchone()[0] == expected_invalid


@pytest.mark.integration
def test_non_finite_measurements_are_rejected_by_postgres() -> None:
    """Use a temporary table because SQLite does not share Postgres float ordering."""
    run_id = os.getenv("INTEGRATION_RUN_ID", "")
    project = os.getenv("INTEGRATION_COMPOSE_PROJECT", "")
    compose_file = Path(os.getenv("INTEGRATION_COMPOSE_FILE", ""))
    expected_compose_file = Path(__file__).resolve().parents[2] / "docker-compose.integration.yml"
    if (
        not run_id
        or project != f"seismic-it-{run_id}"
        or compose_file.resolve() != expected_compose_file.resolve()
        or os.getenv("POSTGRES_DB") != "seismic_integration"
    ):
        pytest.fail("Run Postgres integration tests through scripts/run_integration.py")
    try:
        connection = psycopg.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=3,
        )
    except psycopg.OperationalError as error:
        pytest.fail(f"Integration Postgres unavailable: {error}")

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TEMP TABLE enriched_earthquake_events (
                    depth_km DOUBLE PRECISION,
                    ingestion_latency_seconds DOUBLE PRECISION
                )"""
            )
            cursor.execute(
                """INSERT INTO enriched_earthquake_events VALUES
                    (0, 0), (-0.1, -0.1),
                    ('NaN'::float8, 'NaN'::float8),
                    ('Infinity'::float8, 'Infinity'::float8),
                    ('-Infinity'::float8, '-Infinity'::float8)"""
            )
            for name in ("depth_non_negative_check", "latency_non_negative_check"):
                sql = next(check.sql for check in QUALITY_CHECKS if check.name == name)
                cursor.execute(sql)
                assert cursor.fetchone()[0] == 4
