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

Consume raw events, enrich them, publish successful events, generate metrics, and route invalid events to `dead_letter_earthquakes`.

## Phase 5: Analytical Sink

Persist enriched events and metrics in Postgres.

## Phase 6: Data Quality and Testing

Add data quality checks, unit tests, integration tests, and CI checks with GitHub Actions.

## Phase 7: Cloud-ready Layer

Add Terraform as an optional infrastructure layer without blocking local execution.
