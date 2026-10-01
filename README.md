# Chilean Seismic Streaming Data Pipeline

Local-first streaming data pipeline for Chilean seismic events.

The project evolves in small phases. Phase 1 provides a reproducible Docker Compose stack with Kafka, Schema Registry, Kpow, and Postgres. Phase 2 introduced a Python fake earthquake producer. Phase 3 publishes its events as Avro using the versioned `raw_earthquakes` contract in Schema Registry.

## Phase 1 Scope

Phase 1 includes:

- Kafka as the local event broker.
- Schema Registry as the local schema service.
- Kpow as the local Kafka inspection UI, using a local license.
- Postgres as the local analytical database.
- Docker Compose commands for starting, inspecting, stopping, and cleaning the stack.

The current flow is: fake producer -> Avro serializer and Schema Registry -> Kafka `raw_earthquakes` -> Kpow.

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
- Python 3.11 or newer and pip for the fake producer and contract tests
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

If the provided environment block omits `LICENSE_CREDITS`, use the `Cluster Credits` value from the license certificate. Copy the signature directly from the original text to avoid transcription errors.

## Running The Stack

```bash
docker compose config --quiet
docker compose up -d
docker compose ps
```

If `make` is available:

```bash
make up
make ps
```

Wait until Kafka is healthy before creating the topic.

## Fake Producer and Phase 3 Avro Contract

Create a Python virtual environment and install the project:

```bash
python -m venv .venv
# Activate the environment, then:
python -m pip install -e ".[dev]"
```

On Windows PowerShell, activate it with `.\.venv\Scripts\Activate.ps1`; on macOS or Linux, use `source .venv/bin/activate`. Set `FAKE_PRODUCER_EVENT_COUNT` and `FAKE_PRODUCER_INTERVAL_SECONDS` in `.env` to change the default 10 events and 1 second between events.

After `make up` and `make ps`, register and check the contract before producing:

```bash
make register-schemas
make list-schemas
make test-contracts
make reset-raw-topic
make create-topics
make produce-fake
```

`make reset-raw-topic` deletes and recreates the disposable local topic. Run it before the first Avro publication if Phase 2 JSON messages may still be retained; it is unnecessary for an empty new topic. The producer requires the registered schema and does not register it automatically.

Without Make, run `python scripts/register_schemas.py`, `python -m pytest tests/contracts tests/unit/test_fake_earthquake_producer.py`, `python scripts/reset_raw_topic.py` when cutting over from JSON, `python scripts/create_topics.py`, and `python -m seismic_pipeline.producers.fake_earthquake_producer`. Direct Python commands read process environment variables or use local defaults; Make loads `.env` for them.

Open Kpow at `http://localhost:3000`, inspect `raw_earthquakes` for Avro messages keyed by `event_id`, and check that the subject `raw_earthquakes-value` appears in Schema Registry. You can also verify the subject directly with `curl http://localhost:8081/subjects` (use `curl.exe` in PowerShell).

The producer still generates the source-independent internal event model introduced in Phase 2. See [Data Contracts](docs/data-contracts.md) for its 18 fields, Avro schema, compatibility rule, and safe local cutover. Live ingestion, consumers, and Postgres writes are outside this phase.

## Local Services

| Service | Local URL or port |
| --- | --- |
| Kafka | `localhost:9092` |
| Schema Registry | `http://localhost:8081` |
| Kpow | `http://localhost:3000` after configuring `.env.kpow` |
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

## Documentation

- [Architecture](docs/architecture.md)
- [Data Contracts](docs/data-contracts.md)
- [Observability](docs/observability.md)
- [Phase 0 Design](docs/phase-0-design.md)
- [Naming Conventions](docs/naming-conventions.md)
- [Technical Decisions](docs/technical-decisions.md)
- [Architecture Decision Records](docs/adr/README.md)
- [Roadmap](docs/roadmap.md)
