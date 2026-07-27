# Chilean Seismic Streaming Data Pipeline

Streaming data pipeline based on Chilean seismic events.

The project is designed to run locally first and evolve in small phases: event generation, Kafka ingestion, schema validation, processing, analytical persistence, quality checks, tests, and observability.

## Architecture

```text
Fake or live earthquake source
-> raw_earthquakes
-> Schema Registry
-> Python processor
-> enriched_earthquakes
-> seismic_metrics
-> dead_letter_earthquakes
-> Postgres analytical sink
-> Kpow for Kafka inspection
```

## Current Status

Phase 0: repository foundation.

This phase defines the repository structure, base documentation, configuration examples, initial schemas, Python package layout, and development conventions.

## Scope

The first working version will prioritize:

- Local execution with Docker Compose.
- Fake seismic events as the default input source.
- Kafka topics for raw, enriched, metrics, and dead-letter events.
- Avro schemas for event contracts.
- Python producers, processors, and sink logic.
- Postgres as the first analytical store.
- pytest-based unit, contract, and integration tests.
- Kpow for Kafka visibility during development.

Terraform is planned as an optional cloud-ready layer. It must not be required for local development.

## Repository Layout

```text
.
+-- docs/              Technical documentation and project decisions
+-- infra/             Docker and Terraform project files
+-- schemas/           Avro event contracts
+-- scripts/           Operational helper scripts
+-- src/               Python package source
+-- tests/             Unit, integration, and contract tests
+-- .env.example       Local configuration template
+-- docker-compose.yml Local services entrypoint, added in later phases
+-- Makefile           Development command shortcuts
+-- pyproject.toml     Python package and tooling configuration
```

## Planned Topics

| Topic | Purpose |
| --- | --- |
| `raw_earthquakes` | Raw events produced by fake or live sources. |
| `enriched_earthquakes` | Validated and enriched seismic events. |
| `seismic_metrics` | Aggregated or derived operational metrics. |
| `dead_letter_earthquakes` | Invalid or unprocessable events with failure context. |

## Documentation

- [Architecture](docs/architecture.md)
- [Phase 0 Design](docs/phase-0-design.md)
- [Naming Conventions](docs/naming-conventions.md)
- [Technical Decisions](docs/technical-decisions.md)
- [Roadmap](docs/roadmap.md)

## Local Commands

```bash
make install
make lint
make test
```

`make docker-up` and `make docker-down` are reserved for the local runtime once the Docker Compose stack is added.

## Not In The Initial MVP

The initial version avoids Flink, Spark, Airflow, Kubernetes, Iceberg, Prometheus, and Grafana. Those tools can be valuable in larger systems, but they would add operational weight before the core streaming path is proven.
