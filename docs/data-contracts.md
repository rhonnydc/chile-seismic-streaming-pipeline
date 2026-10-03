# Data Contracts

## Raw Earthquakes: Avro v1

A data contract defines the shape and types that a producer writes and a future consumer can expect. The contract for the value of each `raw_earthquakes` message is the Avro record in [`schemas/raw_earthquake_event.avsc`](../schemas/raw_earthquake_event.avsc). The fake producer maps its `EarthquakeEvent` object to this record before sending bytes to Kafka. The message key remains the UTF-8 `event_id`.

Avro is the wire format from Phase 3 onward. Schema Registry stores versions of the contract under the subject `raw_earthquakes-value`: the topic name followed by `-value` because the schema describes message values. The producer uses the registered schema ID in the Avro message framing and requires the schema to be registered before it publishes.

The record has 18 fields, matching the Phase 2 internal model:

| Field | Avro type | Meaning |
| --- | --- | --- |
| `event_id` | `string` | Pipeline event identifier |
| `source` | `string` | Event source, currently `simulator` |
| `source_event_id` | `string` | Identifier at the source |
| `event_time_utc` | `string` | Event time in UTC |
| `updated_at_utc` | `string` | Source update time in UTC |
| `place` | `string` | Human-readable location |
| `country` | `string` | Country |
| `region` | `string` | Chilean region |
| `magnitude` | `double` | Earthquake magnitude |
| `magnitude_type` | `["null", "string"]` | Magnitude scale, when known |
| `depth_km` | `double` | Depth in kilometers |
| `latitude` | `double` | Latitude |
| `longitude` | `double` | Longitude |
| `status` | `string` | Event status |
| `event_type` | `string` | Event type |
| `tsunami` | `boolean` | Tsunami indicator |
| `url` | `["null", "string"]` | Source URL, when available |
| `ingested_at` | `string` | Pipeline ingestion time in UTC |

The 16 non-nullable fields have no Avro default and must be supplied. `magnitude_type` and `url` can contain `null`; the current Python model still includes both keys in every event. Timestamps stay as ISO 8601 UTC strings for this phase. Avro checks their type, not whether a string is a valid UTC date. It likewise checks numeric types but does not impose magnitude or coordinate ranges.

## Enriched Earthquakes: Avro v1

The value contract for `enriched_earthquakes` is [`schemas/enriched_earthquake_event.avsc`](../schemas/enriched_earthquake_event.avsc), registered under `enriched_earthquakes-value`. Its first 18 fields match the raw record, in the same order. The enrichment function copies those values and adds six fields:

| Field | Avro type | Meaning |
| --- | --- | --- |
| `severity_level` | `string` | `LOW` below magnitude 4.0, `MODERATE` from 4.0 to below 6.0, `HIGH` from 6.0 |
| `is_shallow` | `boolean` | `true` when `depth_km < 70` |
| `event_date_utc` | `string` | UTC date derived from `event_time_utc`, in `YYYY-MM-DD` format |
| `event_hour_utc` | `int` | UTC hour derived from `event_time_utc`, from 0 to 23 |
| `ingestion_latency_seconds` | `double` | `ingested_at - event_time_utc`, in seconds |
| `processed_at_utc` | `string` | Time the consumer enriched the event, as an ISO 8601 UTC string |

The enriched message key is still the UTF-8 `event_id`; the key is not part of the Avro value. This preserves the source event alongside derived attributes and gives downstream consumers a separate contract for the processed stream. The enriched schema describes types, but it does not enforce the severity thresholds, UTC string format, or a nonnegative latency; the Python transformation defines those semantics.

## Compatibility and evolution

The registration command sets `BACKWARD_TRANSITIVE` independently on `raw_earthquakes-value` and `enriched_earthquakes-value` before registering each schema. A future schema version must be able to read data written with *every* earlier version under its subject. For example, adding a field in v2 generally requires a suitable default so the new reader can handle v1 records. Test the proposed change against the registered versions before adopting it; changing a field's type or removing a field can break compatibility.

Schema Registry versions the subject. The repository keeps the `.avsc` as the source of truth for the contract being deployed; an initially empty Registry will register it as version 1.

## Register and verify

Start the local stack, then register both schemas before producing Avro messages:

```bash
make up
make register-schemas
make list-schemas
make test-contracts
```

Direct Schema Registry checks:

```bash
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/raw_earthquakes-value/versions/latest
curl http://localhost:8081/subjects/enriched_earthquakes-value/versions/latest
curl http://localhost:8081/config/raw_earthquakes-value
curl http://localhost:8081/config/enriched_earthquakes-value
```

The subject list should include both value subjects. The raw contract has 18 fields and the enriched contract has 24; both should have type `AVRO` and `BACKWARD_TRANSITIVE` compatibility. In PowerShell, use `curl.exe` for these commands. Without Make, run `python scripts/register_schemas.py` and `python -m pytest tests/contracts tests/unit/test_fake_earthquake_producer.py` from the project environment.

## Safe local cutover from JSON

Phase 2 wrote plain JSON values to `raw_earthquakes`. An Avro reader cannot interpret those retained JSON bytes as Avro. For this disposable local topic, stop any running producer and reset the topic **before the first Avro publication**:

```bash
make reset-raw-topic
make create-topics
make produce-fake
```

`reset-raw-topic` deletes all retained messages in `raw_earthquakes`, waits for deletion, and recreates it with three partitions and replication factor one. The script accepts only the project topic on `localhost:9092` or `127.0.0.1:9092`; it does not reset other topics or remote brokers. `create-topics` is safe to run afterward because it leaves an existing topic in place. Do not run the reset once the topic contains data that must be retained; a later migration would need a separate topic or another explicit cutover plan.

In Kpow, inspect `raw_earthquakes` for Avro records keyed by `event_id`, `enriched_earthquakes` for records with the six derived fields, and Schema Registry for both value subjects. The local Kpow UI requires the project's license configuration. Phase 4 adds enrichment; a Postgres sink and live source remain outside the current implementation.
