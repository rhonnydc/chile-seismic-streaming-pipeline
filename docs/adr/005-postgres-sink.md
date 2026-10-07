# 005 — Postgres analytical sink

**Status:** Accepted · **Phase:** 5

**Decision.** Consume Avro records from `enriched_earthquakes` with a separate `seismic-postgres-sink` group. Persist each record in `enriched_earthquake_events` with `event_id` as the primary key and `INSERT ... ON CONFLICT (event_id) DO NOTHING`. Store event, update, ingestion, and processing times as timezone-aware timestamps. Commit each Postgres transaction before synchronously committing that Kafka message's offset.

**Reason.** A crash between the database commit and the Kafka offset commit can replay an event. The primary key and conflict policy leave one row per `event_id`, allowing simple at-least-once consumption. A separate consumer keeps enrichment independent of analytical storage and gives the sink its own offsets and lag.

**Tradeoff.** `DO NOTHING` preserves the first stored version; later corrections with the same `event_id` do not update that row. This phase stores enriched events only. Metric tables and their grain are outside its scope.

**Verification.** Unit tests cover mapping, duplicate handling, rollback, and the database-before-offset ordering. In the local stack, the sink consumed 23 retained enriched Avro events and 3 newly produced events. Postgres contained 26 rows with 26 distinct `event_id` values, and all four analytical queries returned results. Kpow showed the `seismic-postgres-sink` group at lag 0 on all three enriched topic partitions. A new group replayed all 26 events, logged `inserted=False` for each, reached lag 0, and left the table at 26 rows. The full test suite passed (41 tests), as did Ruff checks and formatting.
