# 003 — Versioned wire contract

**Status:** Accepted · **Phase:** 3

**Decision.** Use `schemas/raw_earthquake_event.avsc` as the versioned Avro contract for the 18 fields of the Phase 2 `EarthquakeEvent` model. Register it explicitly under `raw_earthquakes-value` in Schema Registry with `BACKWARD_TRANSITIVE` compatibility. The producer serializes message values as Avro using the registered schema ID and keeps `event_id` as the UTF-8 Kafka key. Check proposed schema changes against all registered versions before adoption.

**Reason.** A registered contract lets producers and consumers evolve independently and catches incompatible field changes before runtime.

**Verification.** Contract and producer tests passed. In the local stack, Schema Registry registered version 1 of `raw_earthquakes-value` with `BACKWARD_TRANSITIVE` compatibility, and the compatibility check passed. After resetting the disposable local topic, the fake producer published 10 records. All 10 were decoded with the registered Avro schema, and Kpow displayed their fields.

**Consequence.** Phase 2 JSON records and Phase 3 Avro records must not share one consumer path. Reset the disposable local topic before the Avro cutover; use a new topic when retained records must be preserved.
