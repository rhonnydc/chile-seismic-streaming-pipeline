# Technical Decisions

This document records the project's early design direction. See the [architecture decision records](adr/README.md) for phase-specific decisions and their status.

## Local execution is the default path

Docker Compose will be the primary runtime for development and demos. A reviewer should be able to run the project locally without cloud credentials.

## Fake data comes before live ingestion

The first producer generates synthetic seismic events. This keeps development deterministic and prevents the pipeline from depending on API availability, rate limits, or external schema changes.

Live ingestion can be added later as a second producer that publishes to the same raw topic contract.

## Kafka is the streaming backbone

Kafka is used to model the project as an event-driven pipeline. `raw_earthquakes` is the current event topic; enriched output, metrics, and failed-event topics belong to later phases.

## Event contracts are first-class artifacts

Schemas live in `schemas/` and are version-controlled. The Phase 3 code uses Avro as the `raw_earthquakes` wire format so Schema Registry can version the contract and enforce compatibility rules. The producer requires an explicitly registered schema under `raw_earthquakes-value`.

Phase 2 published plain JSON. Its retained local messages must be removed before Avro publication; the reset command is limited to the disposable local raw topic. The raw `.avsc` now matches the internal event model, while the other `.avsc` files remain scaffolds for later phases. [ADR 003](adr/003-wire-contract.md) is accepted after the Schema Registry compatibility setting and the end-to-end local cutover were verified.

## Python owns application logic

Python will be used for producers, processors, validation helpers, and sink logic. This keeps the first implementation approachable while still allowing production-style structure and tests.

## Postgres is the first analytical sink

Postgres is sufficient for the initial analytical layer: it is easy to run locally, inspect with SQL, and validate in integration tests.

## Kpow is used for Kafka observability

Kpow provides operational visibility into topics, messages, and Schema Registry during local development.

## Terraform is optional infrastructure

Terraform belongs in the roadmap, but it must not block the local pipeline. The local Docker Compose path remains the source of truth for the MVP.

## Tools intentionally excluded from the MVP

Flink, Spark, Airflow, Kubernetes, Iceberg, Prometheus, and Grafana are out of scope for the first implementation. The project should prove the streaming path before adding distributed processing, orchestration, lakehouse storage, or a monitoring stack.
