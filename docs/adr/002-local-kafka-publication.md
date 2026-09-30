# 002 — Local Kafka publication

**Status:** Accepted · **Phase:** 2

**Decision.** The fake producer publishes UTF-8 JSON values to `raw_earthquakes`, keyed by `event_id`. Topic creation is an explicit, idempotent command: three partitions and replication factor one on the single-broker local stack. The producer checks delivery callbacks and flushes before exit. Kpow inspects the resulting records from localhost.

**Reason.** This verifies generation, serialization, broker delivery, and inspection without coupling the first runtime flow to Schema Registry or an external API.

**Consequence.** JSON is a temporary wire format. Replication factor one is a local constraint, not a production setting. The Phase 3 cutover must account for existing JSON records before introducing Avro on this stream.
