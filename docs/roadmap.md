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

## Phase 6: Data Quality and Testing

Add data quality checks, unit tests, integration tests, and CI checks with GitHub Actions.

## Phase 7: Cloud-ready Layer

Add Terraform as an optional infrastructure layer without blocking local execution.
