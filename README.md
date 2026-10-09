# Chilean Seismic Streaming Data Pipeline

Local-first streaming data pipeline for Chilean seismic events.

The project evolves in small phases. Phase 1 provides a reproducible Docker Compose stack with Kafka, Schema Registry, Kpow, and Postgres. Phase 2 introduced a Python fake earthquake producer. Phase 3 publishes its events as Avro using the versioned `raw_earthquakes` contract in Schema Registry. Phase 4 consumes those Avro events, enriches them in Python, and publishes Avro events to `enriched_earthquakes`. Phase 5 consumes the enriched events into an analytical Postgres table.

## Phase 1 Scope

Phase 1 includes:

- Kafka as the local event broker.
- Schema Registry as the local schema service.
- Kpow as the local Kafka inspection UI, using a local license.
- Postgres as the local analytical database.
- Docker Compose commands for starting, inspecting, stopping, and cleaning the stack.

The current flow is: fake producer -> Avro `raw_earthquakes` -> Python enrichment consumer -> Avro `enriched_earthquakes` -> Python Postgres sink consumer -> `enriched_earthquake_events`.

## Repository Layout

```text
.
+-- docs/              Technical documentation and project decisions
+-- infra/             Docker and Terraform project files
+-- schemas/           Avro event contracts
+-- scripts/           Operational helper scripts
+-- sql/               Postgres table and analytical queries
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
- Python 3.11 or newer and pip for the producer, consumer, and tests
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

The producer still generates the source-independent internal event model introduced in Phase 2. See [Data Contracts](docs/data-contracts.md) for its 18 fields, Avro schema, compatibility rule, and safe local cutover. Live ingestion is outside the current implementation.

## Phase 4: Consume and Enrich

The enricher reads `raw_earthquakes` with the `seismic-enricher` consumer group, deserializes values through Schema Registry, and copies each raw record with six derived fields: `severity_level`, `is_shallow`, `event_date_utc`, `event_hour_utc`, `ingestion_latency_seconds`, and `processed_at_utc`. Ingestion latency is `ingested_at - event_time_utc` in seconds. It publishes the result as Avro to `enriched_earthquakes` with `event_id` as the message key.

Start the stack and register both schemas before publishing or consuming. If the disposable raw topic still contains Phase 2 JSON messages, follow the reset procedure above before producing new Avro messages. Then run:

```bash
make up
make ps
make register-schemas
make create-topics
make produce-fake
make test-enrichment
make consume-enrich
```

`make consume-enrich` keeps running until interrupted. It uses manual offset commits after each enriched delivery report. A restart can therefore publish a duplicate if delivery succeeded but the process stopped before its source offset was committed. The default `auto.offset.reset=earliest` lets a new group process raw events already on the topic. A previously used group resumes from its committed offsets.

On Windows without Make, the equivalent Phase 4 commands are:

```powershell
docker compose up -d
.\.venv\Scripts\python.exe scripts/register_schemas.py
.\.venv\Scripts\python.exe scripts/create_topics.py
.\.venv\Scripts\python.exe -m seismic_pipeline.producers.fake_earthquake_producer
.\.venv\Scripts\python.exe -m pytest tests/unit/test_enrichment.py tests/contracts/test_enriched_earthquake_contract.py tests/unit/test_enriching_consumer.py
.\.venv\Scripts\python.exe -m seismic_pipeline.consumers.enriching_consumer
```

The direct Python commands use process environment variables or their local defaults; unlike Make, they do not load `.env` automatically. Set `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `KAFKA_RAW_EARTHQUAKES_TOPIC`, `KAFKA_ENRICHED_EARTHQUAKES_TOPIC`, and `CONSUMER_GROUP_ID` in the shell if you change their defaults.

In Kpow at `http://localhost:3000`, confirm that both topics exist, `enriched_earthquakes` receives records with derived fields and `event_id` keys, and the `seismic-enricher` group has committed offsets on `raw_earthquakes`. Its lag should decrease as records are processed. An invalid raw record stops this simple consumer without committing that record; this phase does not add a dead-letter topic.

## Phase 5: Postgres Analytical Sink

The second consumer reads Avro from `enriched_earthquakes` through Schema Registry and writes each event to `enriched_earthquake_events`. It uses the separate `seismic-postgres-sink` group. The table has `event_id` as its primary key and the sink uses `ON CONFLICT (event_id) DO NOTHING`: a replay cannot create a second row, and the first stored version wins. The sink commits the Postgres transaction before synchronously committing the Kafka offset. If either step fails, a replay may occur; the primary key makes that replay safe. Corrections to an existing `event_id` are not reflected by this Phase 5 policy.

With Make, initialize the table and run the sink tests after starting the stack:

```bash
make up
make register-schemas
make create-topics
make init-db
make test-sink
```

Run `make consume-enrich` and `make consume-sink` in separate terminals; both keep running until interrupted. In a third terminal, run `make produce-fake`. After the consumer logs show stored events, run `make query-db` to see event counts, average magnitude, high-severity events, and average ingestion latency by region.

On Windows PowerShell without Make, start the stack, register schemas, create topics, initialize Postgres, and run the sink tests:

```powershell
docker compose up -d
.\.venv\Scripts\python.exe scripts/register_schemas.py
.\.venv\Scripts\python.exe scripts/create_topics.py
docker compose cp .\sql\init.sql postgres:/tmp/init.sql
docker compose exec -T postgres psql -U seismic_user -d seismic -v ON_ERROR_STOP=1 -f /tmp/init.sql
.\.venv\Scripts\python.exe -B -m pytest tests/unit/test_postgres_sink.py tests/unit/test_postgres_sink_consumer.py
```

Run `.\.venv\Scripts\python.exe -m seismic_pipeline.consumers.enriching_consumer` in one terminal and `.\.venv\Scripts\python.exe -m seismic_pipeline.consumers.postgres_sink_consumer` in another. Publish events in a third terminal with `.\.venv\Scripts\python.exe -m seismic_pipeline.producers.fake_earthquake_producer`. Stop the sink with Ctrl+C, then run the analytical queries:

```powershell
docker compose cp .\sql\analytics_queries.sql postgres:/tmp/analytics_queries.sql
docker compose exec -T postgres psql -U seismic_user -d seismic -v ON_ERROR_STOP=1 -f /tmp/analytics_queries.sql
```

Direct Python commands use process environment variables or their local defaults; they do not load `.env` automatically. Set `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_SINK_GROUP_ID`, `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, and `KAFKA_ENRICHED_EARTHQUAKES_TOPIC` in the shell if you change their defaults. Make loads `.env` and exports these settings.

Check `SELECT COUNT(*) FROM enriched_earthquake_events;` in Postgres. In Kpow, inspect the `seismic-postgres-sink` group on `enriched_earthquakes`: committed offsets should advance and lag should reach zero after queued events are stored. Restarting a group resumes at its committed offsets; to deliberately replay retained events for an idempotency check, run the sink with a new `POSTGRES_SINK_GROUP_ID` and confirm the row count does not increase.

## Phase 6: Data Quality Checks

With Postgres running and `enriched_earthquake_events` populated by the Phase 5 sink, run:

```bash
make test-quality
make quality-checks
```

`quality-checks` prints PASS/FAIL for each rule and a summary. It exits with code `1` if any data rule fails (including an empty table), or `2` if the checks cannot run. Make loads the existing Postgres settings from `.env`. On Windows without Make, use `.\.venv\Scripts\python.exe -m pytest tests/unit/test_quality.py` and `.\.venv\Scripts\python.exe -m seismic_pipeline.quality.runner`; direct Python commands use process environment variables or local defaults. See [Data Quality Checks](docs/data-quality.md) for the rules and report interpretation.

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
- [Data Quality Checks](docs/data-quality.md)
- [Observability](docs/observability.md)
- [Phase 0 Design](docs/phase-0-design.md)
- [Naming Conventions](docs/naming-conventions.md)
- [Technical Decisions](docs/technical-decisions.md)
- [Architecture Decision Records](docs/adr/README.md)
- [Roadmap](docs/roadmap.md)
