# Roadmap

## Phase 0: Base Design

Create the repository structure, documentation, naming conventions, environment example, Makefile, Python package skeleton, and infrastructure directories.

## Phase 1: Local Docker Foundation

Add Docker Compose services for Kafka, Schema Registry, Kpow, and Postgres.

## Phase 2: Fake Event Producer

Implement a Python producer that generates fake Chilean seismic events and publishes them to `raw_earthquakes`.

## Phase 3: Contracts and Validation

Register schemas, validate event contracts, and add contract tests.

## Phase 4: Consumer and Processor

Consume Avro events from `raw_earthquakes`, apply Python enrichment, and publish Avro events to `enriched_earthquakes` using `event_id` as the key. Track progress with a consumer group and manual offset commits. Metrics and dead-letter handling are outside Phase 4.

## Phase 5: Analytical Sink

Consume Avro events from `enriched_earthquakes` with a separate consumer group and persist them idempotently in Postgres. Expose simple analytical SQL queries. Aggregate metrics remain outside Phase 5.

## Phase 6: Data Quality

Add data quality checks over the Postgres analytical table and a command that reports pass or fail.

## Phase 7: Testing and CI

Run fast unit and contract tests with Ruff through `make ci` and GitHub Actions. Keep service-dependent integration tests separate.

## Phase 8: Live Earthquake Producer

Poll a configurable USGS GeoJSON feed, normalize usable earthquake features into the existing `EarthquakeEvent` model, and publish them as Avro to `raw_earthquakes`. Preserve the fake producer and downstream contracts. Deduplicate within one process run; country and region inference remains intentionally limited.

## Phase 9: Fault Recovery and Replay Testing

Document and verify local consumer restarts, lag, a Postgres outage, and crashes
at both delivery boundaries. Keep the existing groups and idempotent Postgres
write. See [recovery testing](recovery-testing.md) for observed evidence.

## Phase 10: Retry, Error Handling, and Dead-Letter Queue

Classify transient, permanent-input, and unexpected failures in both Python
consumers. Apply bounded in-process retries to known recoverable operations and
route proven invalid inputs to `dead_letter_earthquakes` with original bytes
and source coordinates. Commit the source offset only after successful output,
Postgres, or DLQ delivery as appropriate. Deterministic tests, `make ci`, and
controlled local Kafka/Postgres/Kpow checks are complete. See
[retry and dead-letter operations](retry-dead-letter.md) and [ADR 009](adr/009-retry-and-dead-letter.md).

## Future: Optional Cloud Infrastructure

Consider Terraform only after the local pipeline has a concrete deployment need.
