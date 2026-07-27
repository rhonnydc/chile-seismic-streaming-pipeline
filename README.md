# Chilean Seismic Streaming Data Pipeline

Portfolio-grade Data Engineering project inspired by Chilean seismic activity.

The goal is to build a local-first streaming data pipeline that can simulate or ingest earthquake events, publish them to Kafka, validate them with data contracts, process and enrich them, generate analytical metrics, and persist results in an analytical sink.

## Target Architecture

```text
Earthquake Source / Event Poller
-> Kafka topic: raw_earthquakes
-> Schema Registry
-> Python Consumer / Processor
-> Kafka topics: enriched_earthquakes, seismic_metrics, dead_letter_earthquakes
-> Postgres Analytical Sink
-> Data Quality Checks
-> Kpow Observability
```

## Project Goals

- Build a streaming pipeline that is understandable for recruiters and technically defensible for Data Engineering profiles.
- Start with fake seismic events so the project can run without depending on an external API.
- Keep Docker Compose as the main local execution mechanism in later phases.
- Use explicit schemas to introduce data contracts from the beginning.
- Add automated testing, observability, reproducible infrastructure, and technical documentation incrementally.
- Keep Terraform cloud-ready, without making it required for local execution.

## Current Phase

**Phase 0: Base project design and repository structure**

This phase defines the initial repository layout, naming conventions, documentation structure, configuration placeholders, and technical decisions. It does not implement the full streaming pipeline yet.

See [docs/phase-0-design.md](docs/phase-0-design.md) for the detailed scope and completion checklist.

## Planned Tools

- Python
- Docker Compose
- Apache Kafka
- Schema Registry
- Kpow
- Postgres
- pytest
- GitHub Actions
- Terraform

## Initial Repository Structure

```text
.
├── docs/
├── infra/
├── schemas/
├── scripts/
├── src/
├── tests/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── README.md
```

## Out Of Scope For The Initial MVP

The first version intentionally avoids the following tools to keep the project focused:

- Flink
- Kubernetes
- Spark
- Airflow
- Iceberg
- Prometheus
- Grafana

## Local Setup

Phase 0 does not start any streaming services yet. The commands below are prepared so the project can grow without changing its basic workflow.

```bash
make help
make install
make test
```

## Roadmap

See [docs/roadmap.md](docs/roadmap.md).
