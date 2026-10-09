# Data Quality Checks

Phase 6 checks whether the analytical Postgres table `enriched_earthquake_events` is suitable for basic analysis. The checks inspect persisted rows; they do not change data or replace validation in the producer, enrichment stage, or database schema. Each SQL query counts invalid rows and the Python runner reports PASS when that count is zero.

## Rules

| Check | Rule and reason |
| --- | --- |
| `row_count_check` | At least one row exists; an empty table cannot demonstrate data quality. |
| `not_null_event_id_check` | Every event has an identifier for tracing and deduplication. |
| `unique_event_id_check` | Each identifier appears once, so analytical counts do not double count an event. |
| `required_fields_not_null_check` | Required source, location, measurement, status, timestamp, and derived fields are present. `magnitude_type` and `url` are optional. |
| `magnitude_range_check` | Magnitude is between 0 and 10, inclusive. |
| `depth_non_negative_check` | Depth is finite and at least 0 km; `NaN` and infinities fail. |
| `latitude_range_check` | Latitude is between -90 and 90, inclusive. |
| `longitude_range_check` | Longitude is between -180 and 180, inclusive. |
| `severity_allowed_values_check` | Severity is `LOW`, `MODERATE`, or `HIGH`. |
| `severity_matches_magnitude_check` | Magnitude below 4 is `LOW`; 4 to below 6 is `MODERATE`; 6 or above is `HIGH`. This matches the enrichment code. |
| `is_shallow_matches_depth_check` | `is_shallow` is true exactly when `depth_km < 70`. This matches the enrichment code. |
| `event_hour_range_check` | UTC event hour is between 0 and 23, inclusive. |
| `latency_non_negative_check` | Ingestion latency is finite and at least 0 seconds; `NaN` and infinities fail. |
| `timestamp_order_check` | Processing time is not before ingestion time. |
| `stored_at_not_null_check` | Postgres has recorded a storage timestamp. |

The table schema already enforces some of these properties, including the primary key, required columns, and hour range. The quality report checks the analytical output explicitly and also covers relationships between fields that the schema does not enforce.

## Run the checks

Start the local stack and load enriched events into Postgres before running the report. The enrichment and sink consumers run continuously in separate terminals; publish fake events in a third terminal, then run the checks after the sink has stored them.

```bash
make up
make register-schemas
make create-topics
make init-db
# In separate terminals: make consume-enrich and make consume-sink
make produce-fake
make quality-checks
```

`make quality-checks` uses the existing `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` settings from `.env` or the Makefile defaults. With no Make installation, set any nondefault Postgres variables in the shell and run `python -m seismic_pipeline.quality.runner` from the installed project environment. Run the unit tests with `make test-quality`, or `python -m pytest tests/unit/test_quality.py` without Make.

The report prints one PASS or FAIL per rule. A failed rule includes its invalid row count; the final summary gives passed and failed check counts. Exit code `0` means all rules passed, `1` means at least one rule failed, and `2` means the checks could not run (for example, a database connection or SQL error). An empty table fails `row_count_check` even though row-level checks may pass.

## Scope and future use

This phase uses Python, SQL, and pytest so the rules stay visible and easy to change without a data quality framework. It does not add a live API, dead-letter topic, aggregate Kafka metrics, dashboards, Terraform, or CI/CD configuration. A later CI job could run `make test-quality` for code changes and `make quality-checks` against a populated test database, using the exit code as its gate.
