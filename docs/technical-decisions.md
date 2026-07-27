# Technical Decisions

This document records early project decisions so later phases can extend the pipeline without changing its basic direction.

## Local execution is the default path

Docker Compose will be the primary runtime for development and demos. A reviewer should be able to run the project locally without cloud credentials.

## Fake data comes before live ingestion

The first producer will generate synthetic seismic events. This keeps development deterministic and prevents the pipeline from depending on API availability, rate limits, or external schema changes.

Live ingestion can be added later as a second producer that publishes to the same raw topic contract.

## Kafka is the streaming backbone

Kafka is used to model the project as an event-driven pipeline. The first topics separate raw input, enriched output, metrics, and failed events.

## Event contracts are first-class artifacts

Schemas live in `schemas/` and are version-controlled. The initial format is Avro because it works well with Schema Registry and makes compatibility rules explicit.

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
