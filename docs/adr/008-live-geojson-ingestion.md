# 008 — Live GeoJSON ingestion

**Status:** Accepted · **Phase:** 8

**Decision.** Poll a configurable USGS GeoJSON feed from a separate live producer. Normalize each usable earthquake feature into the existing `EarthquakeEvent` model and publish it to `raw_earthquakes` with the registered raw Avro schema. Preserve the provider `id` as `source_event_id`; use `usgs:<source_event_id>` as the stable `event_id` and Kafka key to avoid collisions with other sources. Convert epoch milliseconds to UTC and read coordinates as longitude, latitude, depth. Keep the fake producer and downstream contracts unchanged.

**Reason.** The HTTP feed is near-real-time but does not push to Kafka. Polling adapts it to the streaming pipeline, while normalization prevents consumers from depending on provider JSON. A stable ID makes repeated observations of one source event identifiable.

**Behavior.** Track successfully delivered `source_event_id` values in memory and suppress repeats during one process run. Log and skip unusable features. An empty response publishes no events; a failed one-shot request exits with an error, while a multi-cycle run retries. The default feed is worldwide and the producer does not filter to Chile. Infer country and Chilean region only from simple place-name rules; non-Chilean locations and unrecognized regions use `UNKNOWN`. Coordinates are not used for country or administrative-region lookup. No external geocoding or persistent deduplication is part of this phase.

**Consequence.** A restart can republish an event still present in the feed. Postgres avoids a second row through its `event_id` primary key and `ON CONFLICT DO NOTHING`. A provider update has the same ID and is skipped during a running producer; if replayed after restart, the existing Postgres row still wins. Handling revisions as updates requires a later decision.

**Future scope.** Only if a real use case justifies it, decide whether to restrict ingestion geographically to Chile or add reliable worldwide country and region enrichment. That decision should follow the product's geographic requirements rather than be added to this learning phase.

**Verification.** Unit tests cover missing fields, coordinate order, timestamp conversion, Avro publication, repeated polls, empty responses, and HTTP and delivery failures with simulated clients. These tests do not require a live API, Kafka, or Schema Registry; an end-to-end local run remains a separate check.
