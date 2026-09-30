# 005 — Postgres analytical sink

**Status:** Proposed · **Phase:** 5

**Decision.** Persist enriched events in Postgres with a unique `event_id` and an upsert for replays. Store event and ingestion times as timezone-aware timestamps. Write a Kafka batch in one database transaction, then commit offsets only after the transaction succeeds. Give metrics their own table and grain.

**Reason.** Kafka delivery and consumer restarts can replay records. A database key and transaction boundary make those replays safe for analytical queries.

**Adoption gate.** Prove replay leaves one row per event, a failed transaction leaves offsets uncommitted, and metric uniqueness matches its declared grain.
