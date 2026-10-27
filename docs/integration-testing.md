# Phase 11 — Integration and resilience testing

## What runs

`make ci` runs Ruff and tests without external services. `make test-integration`
starts real Kafka, Schema Registry, and Postgres through
`docker-compose.integration.yml`, runs `pytest -m integration`, and removes the
test stack. Run the integration suite through the Make target or
`python scripts/run_integration.py`; invoking pytest directly does not prepare
the services and fails the environment checks.

Kpow, USGS, cloud credentials, and historical Kafka data are not required.

## Local prerequisites and commands

Install Python 3.11 or newer, Docker with Compose, and Make. Docker must be
running. From a clean checkout, create a virtual environment, then install the
project dependencies:

```powershell
# Windows PowerShell
python -m venv .venv
make install
make ci
make test-integration
```

```sh
# Linux
python3 -m venv .venv
make install
make ci
make test-integration
```

The integration command normally takes longer than the fast suite because it
starts the services and waits for Kafka consumer group membership to expire
after the controlled sink interruption. To run one scenario, use the same
executor with a pytest path, for example:

```powershell
.\.venv\Scripts\python.exe scripts/run_integration.py tests/integration/test_end_to_end.py -q
```

On Linux, replace that Python path with `.venv/bin/python`.

By default the test services bind only to `127.0.0.1` on ports `19092` (Kafka),
`18081` (Schema Registry), and `15432` (Postgres). If a port is occupied, set
`INTEGRATION_KAFKA_PORT`, `INTEGRATION_SCHEMA_REGISTRY_PORT`, or
`INTEGRATION_POSTGRES_PORT` in the shell before running the command. The ports
must be distinct. The Kafka advertised listener follows the Kafka port
override. A port collision or unavailable Docker daemon is a setup failure,
not a passing or skipped test.

## Isolation and preparation

The executor generates a `seismic-it-<run-id>` Compose project. Its containers,
network, Kafka volume, and Postgres volume belong to that project. It does not
use the fixed container or network names in `docker-compose.yml`. It overrides
the ordinary `.env` connection settings for its child processes and assigns
run-specific topic names and consumer groups. Postgres uses the dedicated
`seismic_integration` database; its table comes from the existing `sql/init.sql`.

After the services are available, the executor reuses `scripts/create_topics.py`
and `scripts/register_schemas.py`. The tests fail if required services or the
SQL table cannot be reached. Python consumers run as test-owned processes and
are stopped by the pytest fixture. The executor calls Compose `down --volumes`
for its generated project in a `finally` block, including after a test failure.
It never removes the main stack's volumes or resets its topics or offsets.

The project name is printed at startup. If the executor itself is forcibly
killed and leaves resources behind, first inspect the exact printed project,
then remove only that project:

```sh
docker compose -f docker-compose.integration.yml -p seismic-it-REPLACE_WITH_PRINTED_ID down --volumes
```

Do not substitute the main Compose project or use `make clean` for this cleanup.

## What each test proves

| Test | Real-service evidence |
| --- | --- |
| End-to-end | The existing fake producer publishes a controlled raw Avro event. An observer reads raw and enriched records through the real Registry. The test checks derived fields, one SQL row, and the two consumer groups' committed offsets. |
| Replay | A new sink group reads an already stored enriched record from the beginning. Its offset advances, while Postgres retains exactly the original row for that `event_id`. |
| Dead letter | Invalid raw bytes reach the JSON DLQ. The test checks the source-position key, topic, partition, offset, and base64-decoded original key and payload. The raw offset advances after DLQ delivery. A valid event with the same Kafka key then passes through that partition and reaches Postgres. |
| Postgres outage | The sink first writes a baseline event. The test stops only its isolated Postgres service and publishes an enriched event. It observes a persistence failure and no offset advance, stops the sink, restores Postgres, and verifies that the event is still absent. After Kafka removes the old group member, restarting the same sink group stores the event and confirms its offset. |
| Postgres quality | A temporary table checks non-finite measurements against real Postgres behavior. Missing integration infrastructure fails the test. |

The tests use delivery reports, Kafka messages and committed offsets, SQL
queries, and bounded polling. An offset alone does not prove a correct row;
each scenario checks the relevant output as well. Failed assertions leave a
nonzero pytest result. The Postgres outage test intentionally stops the sink
after observing the connection failure; it does not claim automatic recovery
while Postgres is down.

## CI and limits

GitHub Actions has independent `checks` and `integration` jobs. `checks` keeps
running `make ci`. The integration job runs `make test-integration` on Linux
with a 20-minute job limit. It uses the same Compose stack and cleanup path as
local runs, without Kpow.

The pipeline has at-least-once consumer boundaries. The Postgres primary key
and `ON CONFLICT DO NOTHING` prevent duplicate rows for an `event_id`, but a
replay can still produce another enriched Kafka record or DLQ envelope. These
tests do not establish end-to-end exactly-once processing, broker or disk-loss
durability, automatic DLQ replay, or recovery without restarting the sink.
Phase 9 already has deterministic unit tests for crashes after Postgres commit
and after enriched delivery; those fault injections are not duplicated here
as slower real-service tests.
