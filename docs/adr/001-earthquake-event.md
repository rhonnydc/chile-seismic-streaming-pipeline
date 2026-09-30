# 001 — Source-independent earthquake event

**Status:** Accepted · **Phase:** 2

**Decision.** Producers create `EarthquakeEvent` before publishing. The event carries a pipeline `event_id`, source identity, UTC event/update/ingestion times, seismic measurements, location, and source metadata. Kafka uses `event_id` as the message key. Source-specific parsing stays outside this model.

**Reason.** Downstream code needs one shape regardless of whether an event is simulated or comes from GeoJSON. Separate source and ingestion timestamps preserve update and latency analysis.

**Consequence.** The fake generator fills the model directly. A live adapter must map provider fields into it and define stable IDs for provider updates. The dataclass is an internal interface; it does not enforce the Kafka wire contract.
