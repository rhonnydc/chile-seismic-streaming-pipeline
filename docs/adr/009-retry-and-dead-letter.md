# 009 — Bounded retry and dead-letter routing

**Status:** Accepted · **Phase:** 10

**Decision.** Keep `seismic-enricher` and `seismic-postgres-sink` as separate
single-message consumers with manual, synchronous offset commits. Classify
known transient errors narrowly: retry Schema Registry 429/5xx and transport
failures during deserialization, and retry `psycopg.OperationalError` during
persistence after replacing the Postgres connection. Use at most three attempts
with a fixed one-second delay. Stop on persistent infrastructure or unknown
failures, leaving the input offset uncommitted.

Route only explicitly identified permanent input defects to the shared
`dead_letter_earthquakes` topic. Its key identifies the source
`topic:partition:offset`. Its versioned UTF-8 JSON envelope records the stage,
cause, source coordinates, original key and value as base64, and an optional
`event_id`. This route does not depend on Schema Registry and does not change
the raw or enriched Avro contracts. Confirm a source offset only after Kafka
reports successful DLQ delivery.

**Reason.** A bad input should not block its partition indefinitely when its
bytes can be isolated. A service outage or programming defect should not be
hidden in DLQ. Keeping the original bytes allows investigation when Avro
deserialization or event-ID extraction is impossible. A failed DLQ delivery
leaves the source message pending rather than silently dropping it.

**Tradeoff.** A crash after DLQ delivery but before source commit can create
duplicate DLQ records with the same key. Normal enrichment can likewise be
replayed, while the Postgres primary key still keeps one row per `event_id`.
These are at-least-once boundaries, not end-to-end exactly-once processing.
The shared topic is simple locally but cannot give each stage separate
retention or access policy. JSON avoids a Registry dependency on the error
path but requires an explicit envelope version and consumer discipline.
Ambiguous Avro failures stop the consumer; some genuinely corrupt records may
therefore require investigation before a future classifier can isolate them.
There is no automatic DLQ replay or retry topic.

**Verification.** Unit tests cover classification, bounded attempts, connection
replacement, successful and failed DLQ delivery, commit ordering, and the
DLQ duplicate window. `make ci` passed Ruff and 104 non-integration tests.
In a controlled local run, one invalid raw message and one invalid enriched
message reached DLQ with their original bytes and source positions. The two
groups advanced only after delivery, then processed a valid event through
Postgres; both ended with zero lag. Kpow displayed both DLQ records. See
[Phase 10 testing and operations](../retry-dead-letter.md) for exact evidence,
commands, and untested failure modes.
