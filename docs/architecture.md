# Architecture

## Phase 1 Runtime

```text
Host machine
  |-- localhost:9092  -> Kafka
  |-- localhost:8081  -> Schema Registry
  |-- localhost:3000  -> Kpow, pending local license
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
| Kafka | Local event broker for future seismic events. |
| Schema Registry | Local schema service connected to Kafka. |
| Kpow | Local UI configured for inspecting Kafka and Schema Registry, pending a local license. |
| Postgres | Local database prepared for later analytical persistence. |

## Kafka Connectivity

Kafka is configured with separate listeners for host access and container-to-container access:

| Context | Address |
| --- | --- |
| Host machine clients | `localhost:9092` |
| Docker Compose services | `kafka:29092` |

This matters because `localhost` has a different meaning depending on where the client runs. From the host machine, `localhost` points to the Windows or macOS environment running Docker Desktop. From inside a container, `localhost` points to that same container, not to Kafka.

`KAFKA_ADVERTISED_LISTENERS` tells Kafka which address clients should use after they connect. The external advertised listener is `localhost:9092` for host tools and future local Python clients. The internal advertised listener is `kafka:29092` for Schema Registry, Kpow, and future containers.

## Schema Registry Connectivity

Schema Registry is published to the host at `http://localhost:8081`.

Inside Docker Compose, it connects to Kafka through:

```text
kafka:29092
```

Schema Registry stores schema metadata in Kafka. In this phase it is running and ready, but no event schemas are registered automatically yet.

## Kpow Connectivity

Kpow is configured to publish to the host at `http://localhost:3000`, but the UI is pending a local license in `.env.kpow`.

Inside Docker Compose, it connects to:

```text
kafka:29092
schema-registry:8081
```

License values should be copied from `.env.kpow.example` into `.env.kpow`. The template is versioned, while `.env.kpow` is ignored by Git.

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

## Phase 2 Extension

The next phase can add a fake producer that publishes seismic events to:

```text
raw_earthquakes
```

That producer should connect from the host using:

```text
localhost:9092
```

If a future producer runs as another Docker Compose service, it should use:

```text
kafka:29092
```

Later phases can add consumers, enriched topics, metrics topics, dead-letter handling, Postgres tables, tests, and cloud infrastructure without changing the basic local runtime model.
