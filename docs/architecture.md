# Architecture

## Phase 1 Runtime

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

Phase 1 creates the local infrastructure layer only. It does not create producers, consumers, processing jobs, definitive topics, or analytical tables.

## Components

| Component | Responsibility |
| --- | --- |
| Kafka | Local event broker for seismic events. |
| Schema Registry | Local schema service connected to Kafka. |
| Kpow | Local UI for inspecting Kafka and Schema Registry; requires a local license. |
| Postgres | Local database prepared for later analytical persistence. |

## Kafka Connectivity

Kafka is configured with separate listeners for host access and container-to-container access:

| Context | Address |
| --- | --- |
| Host machine clients | `localhost:9092` |
| Docker Compose services | `kafka:29092` |

This matters because `localhost` has a different meaning depending on where the client runs. From the host machine, `localhost` points to the Windows or macOS environment running Docker Desktop. From inside a container, `localhost` points to that same container, not to Kafka.

`KAFKA_ADVERTISED_LISTENERS` tells Kafka which address clients should use after they connect. The external advertised listener is `localhost:9092` for host tools and the local Python producer. The internal advertised listener is `kafka:29092` for Schema Registry, Kpow, and future containers.

## Schema Registry Connectivity

Schema Registry is published to the host at `http://localhost:8081`.

Inside Docker Compose, it connects to Kafka through:

```text
kafka:29092
```

Schema Registry stores schema metadata in Kafka. Phase 1 started it without an event schema; Phase 3 registers the `raw_earthquakes-value` Avro subject explicitly before the producer runs.

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

The internal event model is independent of a particular data provider. The fake generator fills its fields directly. A future `normalize_usgs_event(raw_feature)` could map a USGS GeoJSON feature into the same model:

| Internal field | Possible GeoJSON source |
| --- | --- |
| `event_id`, `source` | Pipeline identifier derived from feature `id`; provider name (`usgs`) |
| `source_event_id` | Feature `id` |
| `magnitude`, `magnitude_type` | `properties.mag` and `properties.magType` |
| `place`, `status`, `event_type`, `tsunami`, `url` | Other feature `properties` |
| `event_time_utc`, `updated_at_utc` | `properties.time` and `properties.updated`, converted from epoch milliseconds to UTC |
| `longitude`, `latitude`, `depth_km` | `geometry.coordinates`, in that order |
| `ingested_at` | Time the pipeline receives the event |
| `country`, `region` | Derived from the place or coordinates when possible |

This boundary lets later processing use one stable event shape even when the source format changes. Consumers, enrichment, Postgres writes, and live API ingestion remain outside Phase 3.

## Phase 3 Avro Event Flow

```text
Fake generator -> EarthquakeEvent -> AvroSerializer -> raw_earthquakes -> Kpow
                                        |
                                        +-- schema ID lookup -> Schema Registry
```

The versioned record in [`schemas/raw_earthquake_event.avsc`](../schemas/raw_earthquake_event.avsc) matches the 18 fields of `EarthquakeEvent`. A registration command sets `BACKWARD_TRANSITIVE` compatibility and registers it under `raw_earthquakes-value`. The producer uses that registered schema to encode values as Avro with a schema ID, while the Kafka key remains the UTF-8 `event_id`. Automatic schema registration is disabled, so registration must happen first. Serialization occurs before the Kafka `produce` call; an invalid record is not queued.

The safe local cutover deletes and recreates only the disposable `raw_earthquakes` topic before the first Avro message. This prevents retained Phase 2 JSON and new Avro values from sharing one topic. Kpow can then inspect the new messages and the registered subject. The [data contract guide](data-contracts.md) gives the exact commands and verification steps.
