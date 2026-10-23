# Phase 9 — Fault recovery and replay testing

These experiments use the existing Compose stack and the real `seismic-enricher` and
`seismic-postgres-sink` groups. They were run locally on 2026-10-23. The numeric offsets and
`event_id` values below are observed evidence, not values a later run must reproduce.

## Boundaries being tested

The enricher disables automatic offset commits and synchronously commits each raw message only
after its enriched Kafka delivery succeeds. The sink commits its Postgres transaction before
synchronously committing the enriched message's Kafka offset. `event_id` is the Postgres primary
key, and the insert uses `ON CONFLICT (event_id) DO NOTHING`.

This is **at-least-once processing at both consumer boundaries**. A crash between the two actions
at either boundary can replay an input. It can create a duplicate in `enriched_earthquakes`, but
the sink preserves one Postgres row per `event_id`. This is not an end-to-end exactly-once
transaction. The first stored version wins, including when a replay has a different
`processed_at_utc`.

## Safe setup and observation

Use PowerShell from the repository root with the project dependencies installed. The commands
below use the defaults in `.env.example`; direct Python commands do **not** load `.env`. If your
configuration differs, set matching process environment variables before running them. Keep a
separate terminal for each long-running consumer. Stop one with Ctrl+C when instructed.

Do not run `make clean`, `make reset-raw-topic`, delete topics, reset group offsets, or remove
volumes for these experiments. `docker compose stop postgres` and `docker compose start postgres`
preserve its volume. Use the fake producer's UUID-based `event_id` values and record each ID from
its delivery log. Run only one consumer per tested group, and pause other producers during each
measurement.

```powershell
docker compose up -d
docker compose ps
.\.venv\Scripts\python.exe scripts/register_schemas.py
.\.venv\Scripts\python.exe scripts/create_topics.py
# On a fresh database only; CREATE TABLE IF NOT EXISTS preserves existing rows.
make init-db
```

The following commands only observe state. `CURRENT-OFFSET` is the **next offset** the group
would read in that partition, not the last processed offset. `LOG-END-OFFSET` is the next offset
Kafka would append. Their difference is lag. An inactive group can retain committed offsets; a
group with no committed offset displays `-`, which must not be interpreted as lag zero.

```powershell
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group seismic-enricher --state
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group seismic-enricher
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group seismic-postgres-sink --state
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group seismic-postgres-sink
docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic enriched_earthquakes
docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic enriched_earthquakes --time earliest
docker compose exec -T postgres psql -U seismic_user -d seismic -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) AS rows, COUNT(DISTINCT event_id) AS distinct_event_ids FROM enriched_earthquake_events;"
```

In Kpow at <http://localhost:3000>, inspect Consumer Groups for each group's members, assigned
partitions, committed offsets, log end offsets, and lag. Inspect the raw and enriched topics for
the recorded `event_id` and key. The UI requires the configured local Kpow license. Kpow does
not show Postgres rows; use SQL for them. The intermediate failure states below were captured
with Kafka CLI, Python logs, and SQL. Kpow was inspected after recovery; no screenshot was saved.

## A — Stopped enricher accumulates lag

**Initial state:** Verify `seismic-enricher` has no other active member and has committed offsets.
If it has no offsets, run it and publish a small batch first, then stop it with Ctrl+C. Do not
start a second consumer in the same group: it would change the partition assignment. Record the
per-partition offset and lag baseline before publishing.

**Action:** Keep the enricher stopped. Publish a small batch, record the three delivered IDs,
then inspect the group again. Restart the enricher with the same group and inspect its logs and
the group again.

```powershell
$env:FAKE_PRODUCER_EVENT_COUNT='3'
$env:FAKE_PRODUCER_INTERVAL_SECONDS='0'
.\.venv\Scripts\python.exe -m seismic_pipeline.producers.fake_earthquake_producer
# In a separate terminal, after observing increased lag:
.\.venv\Scripts\python.exe -m seismic_pipeline.consumers.enriching_consumer
```

**Expected:** While stopped, committed offsets do not move and lag grows by the number of new
records. After restart, logs show each pending ID, committed offsets reach the log ends, and lag
returns to zero. The group moves from `Empty` to `Stable` with an assignment; a transient
rebalance state may be too brief to capture.

**Observed:** The initial raw offsets on partitions 0/1/2 were `14/12/5`, equal to log ends.
Three events landed at `0:14`, `0:15`, and `2:5`. While stopped, committed offsets stayed
`14/12/5`, log ends became `16/12/6`, and lag was `2/0/1`. On restart, the enricher logged all
three IDs, committed offsets became `16/12/6`, and lag returned to `0/0/0`. Kafka reported one
member assigned partitions `(0,1,2)`.

## E — Same-group restart versus intentional replay

**Initial state:** With the enricher caught up, record its group offsets and the enriched topic's
log ends. Stop the enricher with Ctrl+C and confirm that the group has zero members while its
offsets remain. Do not publish new events. Restart it with the same `CONSUMER_GROUP_ID`.

**Expected:** There are no `Enriched event_id=...` logs, committed offsets remain at the raw log
ends, and enriched log ends do not change. A *different* group without offsets would use
`auto.offset.reset=earliest` to start at the earliest **retained** messages; that is a deliberate
replay and can republish historical data. Do not run that replay against these shared topics
unless its effects are intended.

**Observed:** Before and after restart, raw committed/log end offsets were `16/12/6`, all lag
zero. The enriched log ends stayed `16/12/6`, and no enriched event was logged during the
observation. The group moved `Empty` → `Stable` with one new member.

## B — Postgres unavailable

**Initial state:** Start the enricher and sink in separate terminals and wait for both groups to
reach lag zero. Start the sink **while Postgres is available**: it opens its database connection
before joining Kafka. If a sink group has no committed offset on a partition, publish a small
warm-up batch and wait until it has processed a record there. Record each partition's committed
and log end offset, plus the SQL row count.

```powershell
.\.venv\Scripts\python.exe -m seismic_pipeline.consumers.enriching_consumer
# In another terminal:
.\.venv\Scripts\python.exe -m seismic_pipeline.consumers.postgres_sink_consumer
```

**Action:** Stop only Postgres; leave the two consumers running. Publish one event and record its
ID. Wait for the sink to report an error and exit. Compare its group offsets with the enriched
log ends. Restore Postgres, query the ID **before** restarting the sink, then restart it normally
with the same group and query again.

```powershell
docker compose stop postgres
$env:FAKE_PRODUCER_EVENT_COUNT='1'
$env:FAKE_PRODUCER_INTERVAL_SECONDS='0'
.\.venv\Scripts\python.exe -m seismic_pipeline.producers.fake_earthquake_producer
# Observe sink error and group lag before restoring Postgres.
docker compose start postgres
$target='REPLACE_WITH_DELIVERED_EVENT_ID'
docker compose exec -T postgres psql -U seismic_user -d seismic -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) FROM enriched_earthquake_events WHERE event_id = '$target';"
# Restart in the sink terminal:
.\.venv\Scripts\python.exe -m seismic_pipeline.consumers.postgres_sink_consumer
```

**Expected:** A failed database write does not advance the corresponding Kafka offset. The sink
exits; it does not reconnect automatically. Once Postgres is restored and the sink is restarted,
the pending event is written and its offset is committed.

**Observed:** The sink baseline on enriched partitions 0/1/2 was `18/15/7`, all lag zero, and
Postgres had 40 distinct rows. After Postgres stopped, event
`fake-20261023-3beb8dfd5d96` reached enriched partition 1 at offset 15. The sink logged
`the connection is lost` and exited. Partition 1 retained committed offset `15`, log end `16`,
lag `1`; other partitions remained at lag zero. After Postgres restarted, SQL showed zero rows
for the ID. The normal sink then logged `inserted=True`, SQL showed one row, committed offset
advanced to `16`, and lag returned to zero. The table had 41 distinct rows.

## C — Crash after Postgres commit, before Kafka commit

First run the deterministic unit test without external services:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/test_postgres_sink_consumer.py -q
```

For a real-service reproduction, wait for the sink to reach lag zero, stop **only the sink** with
Ctrl+C, and verify the group has zero members. Keep the enricher running. Publish one fake event
and wait until it reaches `enriched_earthquakes`. Record its ID and the sink group's partition,
committed offset, log end, and lag. SQL should show zero rows for the ID. Replace the target
below with that exact ID. The inline Python runs only in this terminal and exits only after
`persist_event` returns for the target; it does not alter production files.

```powershell
$env:FAKE_PRODUCER_EVENT_COUNT='1'
$env:FAKE_PRODUCER_INTERVAL_SECONDS='0'
.\.venv\Scripts\python.exe -m seismic_pipeline.producers.fake_earthquake_producer
$env:PHASE9_TARGET_EVENT_ID='REPLACE_WITH_DELIVERED_EVENT_ID'
@'
import os
from seismic_pipeline.consumers import postgres_sink_consumer as module

target = os.environ["PHASE9_TARGET_EVENT_ID"]
persist_event = module.persist_event

def persist_then_exit(connection, event):
    if event["event_id"] != target:
        raise RuntimeError(f"Unexpected event before target: {event['event_id']}")
    inserted = persist_event(connection, event)
    print(f"INJECTED_EXIT_AFTER_POSTGRES_COMMIT event_id={target} inserted={inserted}", flush=True)
    os._exit(86)

module.persist_event = persist_then_exit
raise SystemExit(module.main())
'@ | .\.venv\Scripts\python.exe -
```

The process exits with an intentional nonzero status. Confirm **one SQL row** for the ID while
the sink committed offset is unchanged and lag remains one. Because the process exits without
closing its consumer, Kafka may briefly show a stale member. Wait until `--state` reports
`Empty` before restarting the normal sink. Its log should show `inserted=False`; then verify
one SQL row and lag zero. The exact SQL and group observation commands are in the setup section.

**Observed:** For `fake-20261023-e347cf1f1791`, the injected process logged
`INJECTED_EXIT_AFTER_POSTGRES_COMMIT ... inserted=True`. SQL showed one row, while enriched
partition 0 still had committed offset `18`, log end `19`, lag `1`. After Kafka expired the
abruptly stopped member, the normal sink replayed offset `0:18`, logged `inserted=False`, kept
one row, and committed offset `19` with lag zero.

## D — Crash after enriched delivery, before raw commit

The deterministic unit test is in `tests/unit/test_enriching_consumer.py`. For a real-service
reproduction, keep the sink running and caught up. Stop **only the enricher** with Ctrl+C,
confirm `seismic-enricher` has zero members, and record raw group offsets and enriched topic log
ends. Publish one fake event and record its ID and raw partition/offset. The raw group should
show lag one; SQL should show zero rows for that ID. Replace the target in this inline process:

```powershell
$env:FAKE_PRODUCER_EVENT_COUNT='1'
$env:FAKE_PRODUCER_INTERVAL_SECONDS='0'
.\.venv\Scripts\python.exe -m seismic_pipeline.producers.fake_earthquake_producer
$env:PHASE9_TARGET_EVENT_ID='REPLACE_WITH_DELIVERED_EVENT_ID'
@'
import os
from seismic_pipeline.consumers import enriching_consumer as module

target = os.environ["PHASE9_TARGET_EVENT_ID"]
real_consumer = module.Consumer

class ExitBeforeCommit:
    def __init__(self, config):
        self.inner = real_consumer(config)

    def subscribe(self, topics):
        return self.inner.subscribe(topics)

    def poll(self, timeout):
        return self.inner.poll(timeout)

    def commit(self, **kwargs):
        message = kwargs["message"]
        event_id = message.key().decode("utf-8")
        if event_id != target:
            raise RuntimeError(f"Unexpected event before target: {event_id}")
        if kwargs["asynchronous"] is not False:
            raise RuntimeError("Expected synchronous offset commit")
        print(
            f"INJECTED_EXIT_AFTER_ENRICHED_DELIVERY event_id={event_id} "
            f"raw_partition={message.partition()} raw_offset={message.offset()}",
            flush=True,
        )
        os._exit(87)

    def close(self):
        return self.inner.close()

module.Consumer = ExitBeforeCommit
raise SystemExit(module.main())
'@ | .\.venv\Scripts\python.exe -
```

The enricher calls this overridden `commit` only **after** its producer has reported successful
enriched delivery. Confirm that the enriched log end advanced, that the sink stored one row,
and that the raw group's committed offset stayed behind by one. Wait for the stale enricher
member to expire and the group to become `Empty`; restart the normal enricher. Confirm a second
enriched record with the same `event_id`, sink log `inserted=False`, one SQL row, and lag zero
for both groups.

**Observed:** For `fake-20261023-a478aba3f063`, the injected process exited before committing
raw offset `0:19`. Raw committed/log end offsets stayed `19/20` (lag `1`), while the enriched
partition 0 log end advanced from `19` to `20`. The sink logged `inserted=True` and Postgres had
one row. After restart, raw offset `0:19` was processed again, enriched partition 0 advanced to
log end `21`, and the sink processed enriched offset `0:20` with `inserted=False`. SQL still
showed one row; enricher committed raw offset `20`, sink committed enriched offset `21`, and
both groups had lag zero. The table had 43 rows with 43 distinct IDs.

**Kpow cross-check after recovery:** Consumers → Details showed `seismic-enricher` and
`seismic-postgres-sink` both `Stable`, each with one member, three assignments, and zero lag.
The assignment table showed raw offsets/end `20/20`, `16/16`, `7/7` for the enricher and
enriched offsets/end `21/21`, `16/16`, `7/7` for the sink. Data → Inspect on
`enriched_earthquakes` displayed two Avro records with key and `event_id`
`fake-20261023-a478aba3f063` at partition 0 offsets **19 and 20**. Their
`processed_at_utc` values differed (`21:36:22.194447Z` and `21:37:49.781089Z`), showing that
re-enrichment created a second version. SQL, not Kpow, established that Postgres kept one row.

**Final shutdown:** Both normal consumers were stopped with Ctrl+C. Kafka reported both groups
`Empty` with zero members. The enricher retained raw committed/log end offsets `20/20`, `16/16`,
`7/7`; the sink retained enriched committed/log end offsets `21/21`, `16/16`, `7/7`. All partition
lags remained zero. Kafka, Schema Registry, Postgres, and Kpow stayed running in Compose.

## What the evidence does and does not prove

The two added unit tests make the commit windows repeatable without services. The local runs
above demonstrate A, E, B, C, and D with the existing Kafka, Schema Registry, and Postgres.
The observed logs, Kafka CLI offsets, SQL queries, and final Kpow inspection support the stated
outcomes. Kpow was not observed during the intermediate lag spikes, so those measurements rest
on the Kafka CLI evidence. No Kpow screenshot was captured in this run.

Kafka retention limits replay. During preparation, `seismic-postgres-sink` did not exist in
Kafka, though Postgres already had 31 rows; its earlier offsets were unavailable for an
unconfirmed reason. `enriched_earthquakes` had log ends `16/12/6`, but its earliest retained
offsets were `14/12/5`. A new group starting at `earliest` could therefore read only those
retained records, not the entire historical stream. No offsets or topics were reset here.

These experiments do not test broker or disk failure, expiration of an unprocessed message,
multiple concurrent members per group, automatic process restart, or updates to a previously
stored `event_id`. They do not add a dead-letter queue, retry topics, or new monitoring services.
The Postgres outage required a manual sink restart. A zero-lag group alone does not prove that
Postgres has the expected rows; compare Kafka state with SQL and consumer logs.

The existing CI gate remains the fast suite: `make ci` ran after the two unit tests were added
and passed Ruff plus 72 tests, with one integration-marked test excluded. The local fault runs
are documented procedures, not GitHub Actions jobs. `make test-integration` also passed its
existing Postgres-backed data-quality test (1 test); it does not run these fault procedures.
