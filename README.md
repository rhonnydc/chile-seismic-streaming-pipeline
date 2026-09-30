# Chilean Seismic Streaming Data Pipeline

Local-first streaming data pipeline for Chilean seismic events.

The project evolves in small phases. Phase 1 provides a reproducible Docker Compose stack with Kafka, Schema Registry, Kpow, and Postgres. Phase 2 adds a Python fake earthquake producer that publishes JSON events to `raw_earthquakes`.

## Phase 1 Scope

Phase 1 includes:

- Kafka as the local event broker.
- Schema Registry as the local schema service.
- Kpow as the local Kafka inspection UI, using a local license.
- Postgres as the local analytical database.
- Docker Compose commands for starting, inspecting, stopping, and cleaning the stack.

Phase 2 uses this local runtime for the flow: fake producer -> Kafka `raw_earthquakes` -> Kpow.

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
- Python 3.11 or newer and pip for Phase 2
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

## Phase 2: Fake Earthquake Producer

Create a Python virtual environment and install the project:

```bash
python -m venv .venv
# Activate the environment, then:
python -m pip install -e ".[dev]"
```

On Windows PowerShell, activate it with `.\.venv\Scripts\Activate.ps1`; on macOS or Linux, use `source .venv/bin/activate`. Set `FAKE_PRODUCER_EVENT_COUNT` and `FAKE_PRODUCER_INTERVAL_SECONDS` in `.env` to change the default 10 events and 1 second between events.

After `make up` and `make ps`:

```bash
make create-topics
make produce-fake
```

Without Make, run `python scripts/create_topics.py` and `python -m seismic_pipeline.producers.fake_earthquake_producer`. Direct Python commands read process environment variables, or use the local defaults (`localhost:9092`, `raw_earthquakes`, 10 events, 1 second); Make loads `.env` for these settings.

Open Kpow at `http://localhost:3000`, go to **Data → Inspect**, select `raw_earthquakes`, and click **Search**. The local Compose configuration enables topic inspection and binds Kpow to `127.0.0.1`, so message keys and values are visible from this host. Each message has an `event_id` key and a JSON value like:

```json
{
  "event_id": "fake-20260929-a1b2c3d4e5f6",
  "source": "simulator",
  "source_event_id": "fake-20260929-a1b2c3d4e5f6",
  "event_time_utc": "2026-09-29T12:00:00Z",
  "updated_at_utc": "2026-09-29T12:00:00Z",
  "place": "Near La Serena, Chile",
  "country": "Chile",
  "region": "Coquimbo",
  "magnitude": 4.2,
  "magnitude_type": "ml",
  "depth_km": 45.8,
  "latitude": -29.9,
  "longitude": -71.2,
  "status": "simulated",
  "event_type": "earthquake",
  "tsunami": false,
  "url": null,
  "ingested_at": "2026-09-29T12:00:00Z"
}
```

The producer generates the project's internal event model, rather than copying a source-specific GeoJSON feature. A future adapter can map USGS `id`, `properties`, and `geometry.coordinates` into the same fields. This phase uses fake events and plain JSON only; it does not ingest a live API, consume events, write to Postgres, or register a formal schema.

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
- [Observability](docs/observability.md)
- [Phase 0 Design](docs/phase-0-design.md)
- [Naming Conventions](docs/naming-conventions.md)
- [Technical Decisions](docs/technical-decisions.md)
- [Roadmap](docs/roadmap.md)
