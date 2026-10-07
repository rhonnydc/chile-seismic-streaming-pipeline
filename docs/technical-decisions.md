# Technical Decisions

This document records the project's early design direction. See the [architecture decision records](adr/README.md) for phase-specific decisions and their status.

## Local execution is the default path

Docker Compose will be the primary runtime for development and demos. A reviewer should be able to run the project locally without cloud credentials.

## Fake data comes before live ingestion

The first producer generates synthetic seismic events. This keeps development deterministic and prevents the pipeline from depending on API availability, rate limits, or external schema changes.

Live ingestion can be added later as a second producer that publishes to the same raw topic contract.

## Kafka is the streaming backbone

Kafka carries the `raw_earthquakes` input stream and the `enriched_earthquakes` output stream. Metrics and failed-event topics belong to later phases.

## Event contracts are first-class artifacts

Schemas live in `schemas/` and are version-controlled. Phase 3 uses Avro for `raw_earthquakes`; Phase 4 uses Avro for `enriched_earthquakes`. Schema Registry versions the separate value contracts under `raw_earthquakes-value` and `enriched_earthquakes-value`. Both serializers require explicit registration.

Phase 2 published plain JSON. Its retained local messages must be removed before Avro publication; the reset command is limited to the disposable local raw topic. The raw `.avsc` matches the internal event model, and the enriched `.avsc` adds six derived fields. The metric `.avsc` remains a scaffold for a later phase. [ADR 003](adr/003-wire-contract.md) records the verified raw Avro cutover.

## Python owns application logic

Python runs the fake producer, Avro enrichment consumer, and separate Postgres sink consumer. The sink maps enriched events to SQL independently of Kafka and commits each database transaction before its consumer commits the Kafka offset.

## Postgres is the first analytical sink

Postgres stores enriched events in `enriched_earthquake_events` with `event_id` as the primary key. The sink uses `ON CONFLICT (event_id) DO NOTHING` to make replays safe; the first stored version wins. The versioned SQL includes table initialization and simple analytical queries. [ADR 005](adr/005-postgres-sink.md) records the accepted choice and its local verification.

## Kpow is used for Kafka observability

Kpow inspects topics, Avro messages, Schema Registry subjects, the consumer group, committed offsets, and lag during local development.

## Terraform is optional infrastructure

Terraform belongs in the roadmap, but it must not block the local pipeline. The local Docker Compose path remains the source of truth for the MVP.

## Tools intentionally excluded from the MVP

Flink, Spark, Airflow, Kubernetes, Iceberg, Prometheus, and Grafana are out of scope for the first implementation.
