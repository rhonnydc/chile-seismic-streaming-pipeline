# Architecture

The implemented flow is synthetic or USGS events → raw Avro topic → enrichment
consumer → enriched Avro topic → PostgreSQL sink consumer → analytical table.
Both consumers can send proven invalid inputs to the shared dead-letter topic.
Data quality checks query the stored table; Kpow inspects Kafka when licensed.
The [README diagram](../README.md#architecture) shows these runtime paths.
The sections below retain the phase history and explain individual boundaries.

## Local runtime

```text
Host machine
  |-- localhost:9092  -> Kafka
  |-- localhost:8081  -> Schema Registry
  |-- localhost:3000  -> Kpow (requires local license)
  |-- localhost:5432  -> Postgres

Docker Compose network
  |-- schema-registry -> kafka:29092
  |-- kpow            -> kafka:29092
  |-- kpow            -> schema-registry:8081
```

This Compose stack provides the services used by the implemented producers,
consumers, topics, and analytical table.

## Components

| Component | Responsibility |
| --- | --- |
| Kafka | Local event broker for seismic events. |
| Schema Registry | Local schema service connected to Kafka. |
| Kpow | Local UI for inspecting Kafka and Schema Registry; requires a local license. |
| Postgres | Local database for analytical persistence of enriched events. |

## Kafka Connectivity

Kafka is configured with separate listeners for host access and container-to-container access:

| Context | Address |
| --- | --- |
| Host machine clients | `localhost:9092` |
| Docker Compose services | `kafka:29092` |

This matters because `localhost` has a different meaning depending on where the client runs. From the host machine, `localhost` points to the Windows or macOS environment running Docker Desktop. From inside a container, `localhost` points to that same container, not to Kafka.

`KAFKA_ADVERTISED_LISTENERS` tells Kafka which address clients should use after they connect. The external advertised listener is `localhost:9092` for host tools and the local Python producer and consumer. The internal advertised listener is `kafka:29092` for Schema Registry, Kpow, and future containers.

## Schema Registry Connectivity

Schema Registry is published to the host at `http://localhost:8081`.

Inside Docker Compose, it connects to Kafka through:

```text
kafka:29092
```

Schema Registry stores schema metadata in Kafka. Phase 1 started it without an event schema; Phase 3 added `raw_earthquakes-value`, and Phase 4 adds `enriched_earthquakes-value`. Both subjects are registered explicitly before their producers run.

## Kpow Connectivity

Kpow publishes to the host at `http://localhost:3000` when a valid local license is configured in `.env.kpow`.

Inside Docker Compose, it connects to:

```text
kafka:29092
schema-registry:8081
```

License values should be copied from `.env.kpow.example` into `.env.kpow`. The template is versioned, while `.env.kpow` is ignored by Git.

The local Kafka cluster has one broker, so Kpow uses `REPLICATION_FACTOR=1` for its internal topics.

## Postgres Connectivity

Postgres is published to the host at `localhost:5432`.

The local database defaults are:

| Setting | Value |
| --- | --- |
| Database | `seismic` |
| User | `seismic_user` |
| Password | `seismic_password` |

Postgres data is stored in a named Docker volume so regular `docker compose down` does not delete local data.

## Volumes And Network

The stack uses named volumes for local persistence:

| Volume | Purpose |
| --- | --- |
| `kafka-data` | Kafka broker data. |
| `postgres-data` | Postgres database files. |

The stack uses a dedicated Docker network:

```text
chile-seismic-network
```

This gives services stable DNS names such as `kafka`, `schema-registry`, and `postgres`.

## Phase 2 Event Flow

```text
Python fake generator -> Python Kafka producer -> raw_earthquakes -> Kpow
```

Phase 2 introduced the generator, which creates `EarthquakeEvent` objects with plausible Chilean data. At that point the producer converted each object to plain JSON, used `event_id` as the Kafka message key, and waited for delivery reports. A separate command created `raw_earthquakes` with three partitions and replication factor one if needed. Kpow made the topic, keys, and JSON values visible.

Both Python commands run on the host and connect through `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`. A future containerized producer would instead connect to `kafka:29092`. Phase 2 did not register or enforce an Avro schema; its retained JSON messages must be removed from the disposable local topic before Avro publication.

### Source Data And Internal Model

The internal event model is independent of a particular data provider. The fake generator fills its fields directly. Phase 8 adds `normalize_usgs_feature(feature)` to map a USGS GeoJSON feature into the same model:

| Internal field | Possible GeoJSON source |
| --- | --- |
| `event_id`, `source` | `usgs:<feature id>`; provider name (`usgs`) |
| `source_event_id` | Feature `id` |
| `magnitude`, `magnitude_type` | `properties.mag` and `properties.magType` |
| `place`, `status`, `event_type`, `tsunami`, `url` | Other feature `properties` |
| `event_time_utc`, `updated_at_utc` | `properties.time` and `properties.updated`, converted from epoch milliseconds to UTC |
| `longitude`, `latitude`, `depth_km` | `geometry.coordinates`, in that order |
| `ingested_at` | Time the pipeline receives the event |
| `country`, `region` | Inferred from simple place-name rules; otherwise `UNKNOWN` |

This boundary lets both implemented producers use one stable event shape for the enrichment consumer and PostgreSQL sink.

## Phase 3 Avro Event Flow

```text
Fake generator -> EarthquakeEvent -> AvroSerializer -> raw_earthquakes -> Kpow
                                        |
                                        +-- schema ID lookup -> Schema Registry
```

The versioned record in [`schemas/raw_earthquake_event.avsc`](../schemas/raw_earthquake_event.avsc) matches the 18 fields of `EarthquakeEvent`. A registration command sets `BACKWARD_TRANSITIVE` compatibility and registers it under `raw_earthquakes-value`. The producer uses that registered schema to encode values as Avro with a schema ID, while the Kafka key remains the UTF-8 `event_id`. Automatic schema registration is disabled, so registration must happen first. Serialization occurs before the Kafka `produce` call; an invalid record is not queued.

The safe local cutover deletes and recreates only the disposable `raw_earthquakes` topic before the first Avro message. This prevents retained Phase 2 JSON and new Avro values from sharing one topic. Kpow can then inspect the new messages and the registered subject. The [data contract guide](data-contracts.md) gives the exact commands and verification steps.

## Phase 4 Enrichment Flow

```text
Fake producer
  -> raw_earthquakes (Avro, event_id key)
  -> Python consumer, group seismic-enricher
  -> pure enrichment function
  -> enriched_earthquakes (Avro, event_id key)
  -> Kpow inspection
```

The consumer runs on the host and uses the same external Kafka listener (`localhost:9092`) and Schema Registry endpoint (`http://localhost:8081`) as the fake producer. `AvroDeserializer` reads the schema ID carried with each raw value and resolves it through Schema Registry. The processing function copies the 18 raw fields and adds six derived fields. `AvroSerializer` writes the result using the separately registered `enriched_earthquakes-value` contract. The registration and topic-creation scripts now handle both topics.

The `seismic-enricher` consumer group tracks its position in each raw topic partition. Automatic commits are disabled. For each valid input, the consumer waits for an enriched delivery report and then commits the source offset synchronously. If output delivery fails, it does not commit that input. A crash after output delivery but before the commit can publish the same input again after restart, so this boundary is at least once. Phase 10 adds selective error handling below; the Phase 4 implementation originally stopped on all invalid inputs.

A new consumer group starts at the earliest available raw offset; an existing group resumes from its committed offsets. Kpow can show the group, per-partition committed offsets, and lag, while its topic browser can show the original and enriched messages. The [observability guide](observability.md) lists the specific checks.

## Phase 5 Analytical Sink Flow

```text
raw_earthquakes (Avro)
  -> seismic-enricher consumer -> enrichment -> enriched_earthquakes (Avro)
  -> seismic-postgres-sink consumer -> enriched_earthquake_events (Postgres)
  -> sql/analytics_queries.sql
```

The sink is a second Python consumer with its own `seismic-postgres-sink` group. It reads `enriched_earthquakes` using `AvroDeserializer` and Schema Registry, then calls a Postgres module that has no Kafka dependency. This separation lets the sink replay retained enriched events without re-running enrichment, change its storage behavior independently, scale its consumer group separately, and expose its own offsets and lag in Kpow.

The versioned [`sql/init.sql`](../sql/init.sql) creates `enriched_earthquake_events` with the 24 enriched fields and a database-generated `stored_at_utc`. Avro timestamp strings become `TIMESTAMPTZ`, `event_date_utc` becomes `DATE`, and Avro doubles become `DOUBLE PRECISION`. Only `magnitude_type` and `url` are nullable under the current contract. The [`sql/analytics_queries.sql`](../sql/analytics_queries.sql) file queries regional counts, average magnitude, high-severity events, and average ingestion latency.

Each event is inserted in one database transaction with `event_id` as the primary key and `ON CONFLICT (event_id) DO NOTHING`. The first stored version wins if the same ID appears again. After the database commit succeeds, the consumer commits that Kafka message's offset synchronously. A database failure leaves the offset uncommitted. A crash after the database commit but before the offset commit can cause a replay, which the primary key handles without adding another row. This is at-least-once delivery with an idempotent sink, not an atomic transaction across Kafka and Postgres. Phase 10 adds bounded retry for lost database connections and selective DLQ routing for invalid inputs.

## Phase 10 Error Flow

```text
raw_earthquakes (Avro) -> seismic-enricher -> enriched_earthquakes (Avro)
                                |                         |
                         invalid input               seismic-postgres-sink -> Postgres
                                |                         |
                                +-> dead_letter_earthquakes <- invalid input
```

Both consumers classify failures before deciding whether to retry, send an
input to the shared JSON DLQ, or stop. A DLQ envelope holds the original Kafka
bytes in base64 along with the stage, source topic, partition, offset, and
error. It does not alter the raw or enriched Avro contracts. Successful DLQ
delivery precedes the original source offset commit; a crash between those
actions may duplicate the DLQ entry. See [Phase 10 operations](retry-dead-letter.md)
and [ADR 009](adr/009-retry-and-dead-letter.md) for the exact policy.

## Phase 6 Data Quality Flow

```text
enriched_earthquake_events (Postgres)
  -> SQL checks over one consistent snapshot
  -> Python PASS/FAIL report and process exit code
```

The quality runner reads the analytical table after the sink has stored events. Each query counts rows that violate a rule, including range, required-field, and enrichment-consistency checks. An empty table fails its own check. The runner reports each result and exits with a nonzero code when data is invalid or the checks cannot run. It does not modify rows or participate in Kafka consumption. The [data quality guide](data-quality.md) lists the rules and commands.

## Phase 8 Live Source Flow

```text
Fake generator -------------------------------> EarthquakeEvent
                                                  |
USGS GeoJSON feed -> HTTP polling -> normalizer --+-> raw Avro serializer
                                                        |
                                                        v
                                                raw_earthquakes
                                                        |
                                                        v
                                  enrichment -> enriched Avro -> Postgres sink
```

The USGS feed is a near-real-time HTTP source, not a WebSocket or push stream. The live producer requests its GeoJSON `features` array once by default or repeatedly at a configurable interval. The default feed is worldwide, and the producer does not filter events by country. The normalizer converts epoch milliseconds to UTC and reads coordinates as longitude, latitude, and depth. It keeps the provider ID in `source_event_id`, derives the stable pipeline ID `usgs:<source_event_id>`, and records when the producer ingested the feature. Simple place-name rules recognize some Chilean regions; other countries and unrecognized regions remain `UNKNOWN` because the normalizer does not derive administrative areas from coordinates. Features with missing critical fields or measurements outside the existing quality ranges are logged and skipped. Non-earthquake feature types are also skipped.

Both producers use the same `EarthquakeEvent` model, `raw_earthquake_event.avsc`, registered `raw_earthquakes-value` subject, Avro serialization settings, and `event_id` Kafka key. The enrichment consumer, enriched schema, Postgres sink, and data quality checks therefore require no source-specific changes. The existing `make produce-fake` remains available; `make produce-live` runs the HTTP polling producer.

During one run, the live producer tracks successfully delivered `source_event_id` values in memory and suppresses repeats from later polls. An empty feed produces no messages. An HTTP error is logged and retried on a later cycle; a one-shot run with a failed request exits with an error. After a restart, the in-memory set is empty and a feature still present in the feed can be published again. Postgres uses `event_id` as its primary key and `ON CONFLICT DO NOTHING` to avoid a second stored row. Because the first row wins, later USGS revisions of the same event do not update it in this phase.

If a real use case emerges, the geographic scope should be decided from its requirements. A Chile-only product could filter geographically; a global product would need a reliable country and administrative-region source. Neither extension is needed for the current learning pipeline.
