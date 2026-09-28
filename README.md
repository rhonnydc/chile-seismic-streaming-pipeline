# Chilean Seismic Streaming Data Pipeline

Local-first streaming data pipeline for Chilean seismic events.

The project evolves in small phases. Phase 1 provides a reproducible Docker Compose stack with Kafka, Schema Registry, Kpow, and Postgres. Producers, consumers, processing logic, tests, and cloud infrastructure are intentionally left for later phases.

## Phase 1 Scope

This phase includes:

- Kafka as the local event broker.
- Schema Registry as the local schema service.
- Kpow configured as the local Kafka inspection UI, pending a local license.
- Postgres as the local analytical database.
- Docker Compose commands for starting, inspecting, stopping, and cleaning the stack.

This phase prepares the local runtime for Phase 2, where a fake producer can publish events to `raw_earthquakes`.

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
+-- docker-compose.yml Local Docker Compose stack
+-- Makefile           Development command shortcuts
+-- pyproject.toml     Python package and tooling configuration
```

## Requirements

- Docker Desktop
- Docker Compose v2
- Make, optional on Windows

Kafka, Schema Registry, Kpow, and Postgres do not need to be installed directly on the host machine.

## Local Configuration

Create a local `.env` file from the versioned template:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

`.env.example` is versioned because it documents the variables required by the project. `.env` is ignored by Git because it is the local machine-specific copy.

Kpow requires a local license before the UI is usable. Store license variables in `.env.kpow`; this file is ignored by Git through the existing `.env.*` rule.

Create the local Kpow license file from the versioned template:

```bash
cp .env.kpow.example .env.kpow
```

On Windows PowerShell:

```powershell
Copy-Item .env.kpow.example .env.kpow
```

Then fill `.env.kpow` with the license values from the Kpow email:

```env
LICENSE_ID=
LICENSE_CODE=
LICENSEE=
LICENSE_CREDITS=
LICENSE_EXPIRY=
LICENSE_SIGNATURE=
```

## Running The Stack

```bash
docker compose config
docker compose up -d
docker compose ps
```

If `make` is available:

```bash
make up
make ps
```

## Local Services

| Service | Local URL or port |
| --- | --- |
| Kafka | `localhost:9092` |
| Schema Registry | `http://localhost:8081` |
| Kpow | Pending `.env.kpow`, then `http://localhost:3000` |
| Postgres | `localhost:5432` |

Inside Docker, services communicate through the Compose network. For example, Schema Registry and Kpow use `kafka:29092`, while clients running on the host use `localhost:9092`.

## Logs

```bash
docker compose logs kafka
docker compose logs schema-registry
docker compose logs kpow
docker compose logs postgres
```

If `make` is available:

```bash
make logs
```

## Stopping The Stack

```bash
docker compose down
```

If `make` is available:

```bash
make down
```

## Cleaning Local Volumes

This removes local Kafka and Postgres data volumes for the project:

```bash
docker compose down --volumes --remove-orphans
```

If `make` is available:

```bash
make clean
```

## Not Included Yet

Phase 1 does not include Python producers, Python consumers, fake events, live earthquake API ingestion, definitive topic creation, dead-letter workflows, complex tests, CI/CD, Terraform, Flink, Spark, Airflow, Kubernetes, Iceberg, Prometheus, Grafana, or ClickHouse.

## Documentation

- [Architecture](docs/architecture.md)
- [Observability](docs/observability.md)
- [Phase 0 Design](docs/phase-0-design.md)
- [Naming Conventions](docs/naming-conventions.md)
- [Technical Decisions](docs/technical-decisions.md)
- [Roadmap](docs/roadmap.md)
