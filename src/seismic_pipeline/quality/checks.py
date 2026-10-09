"""SQL checks for the Postgres analytical earthquake table."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class QualityCheck:
    name: str
    sql: str


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    invalid_rows: int

    @property
    def passed(self) -> bool:
        return self.invalid_rows == 0


def evaluate_check(check: QualityCheck, invalid_rows: int) -> CheckResult:
    """Interpret a count returned by a quality query."""
    if not isinstance(invalid_rows, int) or isinstance(invalid_rows, bool) or invalid_rows < 0:
        raise ValueError("invalid_rows must be a non-negative integer")
    return CheckResult(name=check.name, invalid_rows=invalid_rows)


def count_results(results: list[CheckResult]) -> tuple[int, int]:
    """Return the number of passed and failed checks."""
    passed = sum(result.passed for result in results)
    return passed, len(results) - passed


QUALITY_CHECKS = (
    QualityCheck(
        "row_count_check",
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM enriched_earthquake_events) THEN 0 ELSE 1 END",
    ),
    QualityCheck(
        "not_null_event_id_check",
        "SELECT COUNT(*) FROM enriched_earthquake_events WHERE event_id IS NULL",
    ),
    QualityCheck(
        "unique_event_id_check",
        "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM enriched_earthquake_events",
    ),
    QualityCheck(
        "required_fields_not_null_check",
        """SELECT COUNT(*) FROM enriched_earthquake_events WHERE
            source IS NULL OR source_event_id IS NULL OR event_time_utc IS NULL OR
            updated_at_utc IS NULL OR place IS NULL OR country IS NULL OR region IS NULL OR
            magnitude IS NULL OR depth_km IS NULL OR latitude IS NULL OR longitude IS NULL OR
            status IS NULL OR event_type IS NULL OR tsunami IS NULL OR ingested_at IS NULL OR
            severity_level IS NULL OR is_shallow IS NULL OR event_date_utc IS NULL OR
            event_hour_utc IS NULL OR ingestion_latency_seconds IS NULL OR
            processed_at_utc IS NULL""",
    ),
    QualityCheck(
        "magnitude_range_check",
        "SELECT COUNT(*) FROM enriched_earthquake_events WHERE magnitude NOT BETWEEN 0 AND 10",
    ),
    QualityCheck(
        "depth_non_negative_check",
        """SELECT COUNT(*) FROM enriched_earthquake_events
            WHERE depth_km < 0 OR depth_km >= 'Infinity'::float8""",
    ),
    QualityCheck(
        "latitude_range_check",
        "SELECT COUNT(*) FROM enriched_earthquake_events WHERE latitude NOT BETWEEN -90 AND 90",
    ),
    QualityCheck(
        "longitude_range_check",
        "SELECT COUNT(*) FROM enriched_earthquake_events WHERE longitude NOT BETWEEN -180 AND 180",
    ),
    QualityCheck(
        "severity_allowed_values_check",
        """SELECT COUNT(*) FROM enriched_earthquake_events
            WHERE severity_level NOT IN ('LOW', 'MODERATE', 'HIGH')""",
    ),
    QualityCheck(
        "severity_matches_magnitude_check",
        """SELECT COUNT(*) FROM enriched_earthquake_events
            WHERE severity_level IS DISTINCT FROM CASE
                WHEN magnitude < 4 THEN 'LOW'
                WHEN magnitude < 6 THEN 'MODERATE'
                ELSE 'HIGH'
            END""",
    ),
    QualityCheck(
        "is_shallow_matches_depth_check",
        """SELECT COUNT(*) FROM enriched_earthquake_events
            WHERE is_shallow IS DISTINCT FROM (depth_km < 70)""",
    ),
    QualityCheck(
        "event_hour_range_check",
        "SELECT COUNT(*) FROM enriched_earthquake_events WHERE event_hour_utc NOT BETWEEN 0 AND 23",
    ),
    QualityCheck(
        "latency_non_negative_check",
        """SELECT COUNT(*) FROM enriched_earthquake_events
            WHERE ingestion_latency_seconds < 0
               OR ingestion_latency_seconds >= 'Infinity'::float8""",
    ),
    QualityCheck(
        "timestamp_order_check",
        """SELECT COUNT(*) FROM enriched_earthquake_events
            WHERE processed_at_utc < ingested_at""",
    ),
    QualityCheck(
        "stored_at_not_null_check",
        "SELECT COUNT(*) FROM enriched_earthquake_events WHERE stored_at_utc IS NULL",
    ),
)
