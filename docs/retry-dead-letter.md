# Phase 10 — Retry, error handling, and dead-letter queue

This document describes the implemented behavior of the `seismic-enricher` and
`seismic-postgres-sink` groups. Both consumers use manual, synchronous Kafka
offset commits. The local observations below were made on 2026-10-24 in Chile
(2026-10-25 UTC); offsets and event IDs are evidence from that run, not values
that a later run must reproduce.

## Error policy

| Failure | Action | Reason |
| --- | --- | --- |
| Schema Registry HTTP 429 or 5xx, or an HTTP transport failure while deserializing | Retry the deserialization operation | Communication may recover without changing the input. |
| `psycopg.OperationalError` while persisting | Retry after closing and reopening the Postgres connection | A broken connection should not be reused. |
| Missing or clearly invalid Confluent Avro framing; a decoded null/non-record value | Publish the original message to DLQ | The same bytes will not become a valid Avro earthquake on retry. |
| A decoded event timestamp without an offset or with invalid ISO 8601 syntax | Publish the original message to DLQ | These values violate an explicit processing requirement. |
| An enriched event with a date/timestamp rejected by `event_to_row` | Publish the original message to DLQ | Its temporal fields cannot be stored as required. |
| Kafka poll, output publication, DLQ publication, or offset commit failure | Stop; leave any uncommitted input pending | An infrastructure problem must not be disguised as bad data. |
| Unknown exceptions, missing decoded fields, output serialization errors, ambiguous Avro errors, and other SQL errors | Stop; leave the input pending | Their cause is not proven to be a permanent input defect. |

Classification is intentionally conservative. A generic `ValueError`,
`SerializationError`, `KeyError`, or database error does **not** automatically
enter the DLQ. For example, a failure to look up a writer schema may reflect
Registry state or configuration rather than corrupt input. The framing check
covers a missing value, at most five bytes, or a first byte other than the
Confluent magic byte `0`. Other malformed Avro that cannot be distinguished
reliably from infrastructure or schema problems currently stops the consumer.

## Bounded retry

An eligible operation has **three attempts total**, with a **fixed one-second
wait** between attempts. Each classified transient failure is logged with the
stage and attempt number. An exhausted retry raises the last error and stops the
consumer. No source offset is committed and nothing is sent to DLQ for a
persistent infrastructure failure. The Postgres sink replaces the connection
between persistence attempts. Connection failure at startup, Kafka delivery,
and commits are not covered by this in-process retry.

An immediate retry can overload a service that is temporarily unavailable.
Exponential backoff is useful for longer outages but needs more timing and
poll-interval decisions. The fixed delay keeps this local phase small and
bounded. No retry topics or automatic service recovery are used.

## Dead-letter topic and JSON contract

`make create-topics` creates `dead_letter_earthquakes` idempotently with three
partitions and replication factor one in the local single-broker stack. The
same topic serves both stages. The `stage` field identifies `enricher` or
`postgres_sink`; a future operational need for separate retention or access
controls could justify splitting it. The DLQ value is UTF-8 JSON, so publishing
an error does not require Schema Registry. Raw and enriched Avro schemas remain
unchanged. The topic must exist before testing the route.

The Kafka key is the UTF-8 string `source_topic:source_partition:source_offset`.
It identifies a source position across possible duplicate DLQ deliveries. The
JSON value contains:

| Field | Meaning |
| --- | --- |
| `schema_version` | Envelope version, currently `1`. |
| `stage` | Consumer stage that isolated the input. |
| `error_code`, `error_message` | Classified cause and a short diagnostic message. |
| `failed_at_utc`, `attempts` | UTC failure time and attempts for this input operation. |
| `source_topic`, `source_partition`, `source_offset` | Exact Kafka source position. |
| `original_key_base64`, `original_value_base64` | Original bytes, or `null` when the original field was null. |
| `event_id` | Decoded event ID when available; otherwise `null`. |

The original bytes are read from the Kafka message and base64 encoded before
constructing the JSON. Do not rely on `event_id` for an undecodable record.
`attempts` is `1` for the currently implemented DLQ cases because permanent
input failures are not retried.

Example, abridged from the local run:

```json
{
  "schema_version": 1,
  "stage": "enricher",
  "error_code": "INVALID_AVRO",
  "error_message": "Invalid Confluent Avro framing",
  "failed_at_utc": "2026-10-25T02:46:47.919328Z",
  "attempts": 1,
  "source_topic": "raw_earthquakes",
  "source_partition": 0,
  "source_offset": 20,
  "original_key_base64": "cGhhc2UxMC1pbnZhbGlkLWZyYW1pbmctOWMzZTQwYTUzOGQw",
  "original_value_base64": "bm90LWF2cm8tcGhhc2UxMA==",
  "event_id": null
}
```

## Delivery and commit boundary

For a valid raw input, the enricher waits for successful delivery to
`enriched_earthquakes`, then commits its raw offset. For a valid enriched
input, the sink commits its Postgres transaction, then commits its Kafka
offset. `event_id` is the Postgres primary key and `ON CONFLICT DO NOTHING`
keeps one stored row per ID.

For an invalid input, either consumer publishes its JSON DLQ record, flushes
the producer, checks for one successful delivery report, and **only then**
commits the original offset synchronously. A failed or missing DLQ report,
producer exception, or failed commit stops processing. If DLQ delivery
succeeds but the process crashes before the source commit, replay can publish
a second DLQ record with the same source-position key. This is
**at-least-once delivery**, not an end-to-end exactly-once transaction. The
unit test reproduces this duplicate window without external services.

## Local observation and Kpow

The local stack was started with `docker compose up -d`. The enricher group
was empty with raw committed/log-end offsets `20/20`, `16/16`, and `7/7` on
partitions 0/1/2. The new DLQ had log ends `0/0/0`. `make create-topics`
left raw and enriched untouched and created the DLQ.

One raw test message with value `not-avro-phase10` reached
`raw_earthquakes:0:20`. The enricher logged an invalid-framing DLQ action.
The corresponding DLQ record appeared at partition 2, offset 0 with key
`raw_earthquakes:0:20`, and decoding `original_value_base64` recovered the
exact test bytes. The raw group advanced to offset 21 on partition 0. A
subsequent valid fake event, `fake-20261025-c89b3317338c`, reached raw
partition 2, offset 7; the same enricher processed it and advanced that
partition to offset 8. Its group ended with lag zero.

The sink group initially had one pending enriched record on partition 2. A
separate test value `not-avro-sink-phase10` reached
`enriched_earthquakes:1:16`. The sink sent it to DLQ partition 1, offset 0
with key `enriched_earthquakes:1:16`, then committed enriched offset 17 on
partition 1. It also stored the pending valid fake event: SQL returned one
row for that `event_id`. The sink group ended at `21/21`, `17/17`, `8/8`
and lag zero on partitions 0/1/2. Both consumers were stopped after the run;
the stack and test messages were left in place.

In Kpow at <http://localhost:3000>, select **Data → Inspect**, choose
`dead_letter_earthquakes`, keep **Slice / Recent** and automatic key/value
deserializers, then select **Search**. This run showed **2 / 2** records. The
raw entry displayed `stage=enricher`; the enriched entry displayed
`stage=postgres_sink`. Inspect their Kafka keys, source coordinates, error
codes, `event_id`, and base64 fields. Under **Consumer Groups → Details**,
inspect each group's state, members, committed offsets, log ends, and lag.
Kpow's updates may lag the CLI briefly; use **Pull latest data** when needed.

The same state can be checked without Kpow:

```powershell
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group seismic-enricher --state
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group seismic-enricher
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group seismic-postgres-sink --state
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group seismic-postgres-sink
docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic dead_letter_earthquakes
```

An **Empty** group with no members is stopped. A **Stable** group with lag is
running but behind; inspect its logs to distinguish slow processing from a
blocked input. A DLQ record identifies an isolated input by stage and source
position. Persistent infrastructure errors stop a consumer and leave lag
until recovery and restart; they do not create DLQ records. Lag zero alone
does not prove a Postgres row exists, so compare SQL and logs for sink checks.

## Verification and limits

`make ci` passed Ruff and 104 non-integration tests after the Phase 10 code
changes. Deterministic tests cover successful flow, bounded retry,
classification, failed DLQ delivery without offset commit, unexpected errors,
and the DLQ replay window. The controlled local run verified actual Kafka
delivery, source offset advancement, continued enrichment, sink persistence,
and Kpow visibility. It did **not** reproduce a live DLQ publication failure,
retry exhaustion, or Postgres reconnection; those paths have unit coverage.
The single-broker local replication factor is not a durability guarantee
against broker or disk loss. Automatic DLQ replay is deliberately absent.

Manual replay from the DLQ requires decoding the original bytes, resolving the
cause, and publishing to the correct source topic while handling duplicate
source positions. Not every DLQ record has an `event_id`.
