"""Run data quality checks against the Postgres analytical table."""

import os
import sys
from typing import Any

import psycopg

from seismic_pipeline.quality.checks import (
    QUALITY_CHECKS,
    CheckResult,
    count_results,
    evaluate_check,
)


def execute_checks(connection: Any) -> list[CheckResult]:
    """Run all checks against one consistent database snapshot."""
    results = []
    with connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        for check in QUALITY_CHECKS:
            cursor.execute(check.sql)
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Check {check.name} returned no result")
            results.append(evaluate_check(check, row[0]))
    return results


def print_report(results: list[CheckResult]) -> None:
    """Print one status per check and a final summary."""
    print("DATA QUALITY CHECKS\n")
    for result in results:
        print(f"{'PASS' if result.passed else 'FAIL'} {result.name}")
        if not result.passed:
            print(f"  invalid_rows: {result.invalid_rows}")
    passed, failed = count_results(results)
    print(f"\nSummary:\n{passed} passed\n{failed} failed")


def run_quality_checks(connection: Any) -> int:
    """Return 0 when every check passes, otherwise 1."""
    results = execute_checks(connection)
    print_report(results)
    return int(any(not result.passed for result in results))


def main() -> int:
    """Connect using the existing Postgres environment settings."""
    try:
        with psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "seismic"),
            user=os.getenv("POSTGRES_USER", "seismic_user"),
            password=os.getenv("POSTGRES_PASSWORD", "seismic_password"),
        ) as connection:
            return run_quality_checks(connection)
    except (OSError, psycopg.Error, ValueError) as error:
        print(f"Data quality checks could not run: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
