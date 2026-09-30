# 003 — Versioned wire contract

**Status:** Proposed · **Phase:** 3

**Decision.** Make `schemas/raw_earthquake_event.avsc` match the published event model, register it in Schema Registry, and serialize new records as Avro. Set `BACKWARD_TRANSITIVE` compatibility and check changes against all registered versions before merge. Treat the current `.avsc` file as a scaffold: its fields do not yet match Phase 2 JSON.

**Reason.** A registered contract lets producers and consumers evolve independently and catches incompatible field changes before runtime.

**Adoption gate.** Demonstrate a serializer round trip and compatibility check. Do not mix unframed JSON and Avro in one consumer path: reset the disposable local topic or cut over to a new topic when retained records matter.
