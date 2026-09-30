# 008 — Live GeoJSON ingestion

**Status:** Proposed · **Phase:** Later

**Decision.** Add a separate live-source adapter and producer that map GeoJSON into `EarthquakeEvent` and publish under the active raw-event contract. Preserve provider `id` as `source_event_id`; derive a stable pipeline ID from provider and source ID. Convert epoch milliseconds to UTC and read coordinates as longitude, latitude, depth. Treat provider updates as revisions of the same event.

**Reason.** Provider payloads and polling behavior can change without forcing changes in consumers or the fake generator.

**Adoption gate.** Fixture tests cover missing fields, coordinate order, timestamp conversion, repeated polls, and updated events. No live API call is required for contract tests.
