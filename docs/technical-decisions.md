# Technical Decisions

## Local-first development

The pipeline will be designed to run locally using Docker Compose before introducing cloud infrastructure.

## Fake data first

The first producer will generate fake seismic events. A live API producer may be added later, but the project must not depend on an external API to work.

## Kafka as the streaming backbone

Kafka will be used as the central event streaming platform.

## Schema Registry and contracts

Events will be validated using explicit schemas to demonstrate data contract practices.

## Python for producers and consumers

Python will be used for ingestion, processing, validation, and sink logic.

## Postgres as the analytical sink

Postgres will be used as the first analytical destination because it is simple, inspectable, and appropriate for a local portfolio project.

## Kpow for Kafka observability

Kpow will be introduced after Kafka is running locally to inspect topics, messages, and consumer behavior.

## Terraform as cloud-ready infrastructure

Terraform will be included as a future infrastructure layer, but it must not be required to run the local MVP.

## Excluded from the initial MVP

Flink, Kubernetes, Spark, Airflow, Iceberg, Prometheus, and Grafana are intentionally excluded to keep the first version focused.
