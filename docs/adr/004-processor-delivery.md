# 004 — Processor delivery boundary

**Status:** Proposed · **Phase:** 4

**Decision.** Consume `raw_earthquakes` with a named group and explicit offset commits. Validate before enrichment; send successful records to `enriched_earthquakes` and deterministic data failures to `dead_letter_earthquakes` with error context and the original payload. Retry infrastructure failures. Commit the source offset only after the corresponding output is acknowledged.

**Reason.** A process crash must not silently discard an input record. Invalid data must remain inspectable without blocking the rest of the partition.

**Adoption gate.** Integration tests cover valid input, invalid input, and restart after an output acknowledgment but before offset commit. This boundary is at-least-once; downstream writes must tolerate duplicates.
