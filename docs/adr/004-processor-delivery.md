# 004 — Processor delivery boundary

**Status:** Accepted · **Phase:** 4

**Decision.** Consume Avro records from `raw_earthquakes` with the `seismic-enricher` group, enrich them in Python, and publish Avro records to `enriched_earthquakes` keyed by `event_id`. Disable automatic offset commits and commit each source offset only after the enriched output is acknowledged. A deserialization, enrichment, or delivery failure stops the consumer without committing that input. Dead-letter routing is outside Phase 4.

**Reason.** The source position must not advance before the corresponding output is delivered. A crash after delivery but before the commit can replay the input and produce a duplicate.

**Verification.** Unit tests with simulated clients cover successful delivery, failed delivery, invalid input, and offset commits. In the local stack, the fake producer published 10 new raw events; the consumer processed all 23 retained raw events and published 23 enriched Avro records. Kpow decoded the enriched values and displayed `event_id` keys and derived fields. The `seismic-enricher` group's committed offsets were 9, 11, and 3, matching the raw log end offsets on the three partitions (lag 0). This boundary is at least once; downstream writes must tolerate duplicates.
