# Technical Decisions

This document records the project's early design direction. See the [architecture decision records](adr/README.md) for phase-specific decisions and their status.

## Local execution is the default path

Docker Compose will be the primary runtime for development and demos. A reviewer should be able to run the project locally without cloud credentials.

## Fake data comes before live ingestion

The first producer will generate synthetic seismic events. This keeps development deterministic and prevents the pipeline from depending on API availability, rate limits, or external schema changes.

Live ingestion can be added later as a second producer that publishes to the same raw topic contract.

## Kafka is the streaming backbone

Kafka is used to model the project as an event-driven pipeline. The first topics separate raw input, enriched output, metrics, and failed events.

## Event contracts are first-class artifacts

Schemas live in `schemas/` and are version-controlled. Avro is the target formal wire contract because Schema Registry can enforce compatibility rules.

Phase 2 currently publishes JSON; the existing `.avsc` files are scaffolds. [ADR 003](adr/003-wire-contract.md) covers the contract and format cutover.

## Python owns application logic

Python will be used for producers, processors, validation helpers, and sink logic. This keeps the first implementation approachable while still allowing production-style structure and tests.

## Postgres is the first analytical sink

Postgres is sufficient for the initial analytical layer: it is easy to run locally, inspect with SQL, and validate in integration tests.

## Kpow is used for Kafka observability

Kpow will be added once Kafka is running. Its role is operational visibility into topics, messages, and consumer groups during local development.

## Terraform is optional infrastructure

Terraform belongs in the roadmap, but it must not block the local pipeline. The local Docker Compose path remains the source of truth for the MVP.

## Tools intentionally excluded from the MVP

Flink, Spark, Airflow, Kubernetes, Iceberg, Prometheus, and Grafana are out of scope for the first implementation. The project should prove the streaming path before adding distributed processing, orchestration, lakehouse storage, or a monitoring stack.
