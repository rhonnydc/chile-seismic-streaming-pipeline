# Observability

## Phase 1 Scope

Phase 1 configures Kpow as the local Kafka observability tool. Kpow access is pending a local Community Edition license in `.env.kpow`.

This phase does not include Prometheus, Grafana, alerting, distributed tracing, log aggregation, or production monitoring. The goal is to make the local Kafka stack inspectable while the pipeline is still small.

## Kpow Role

Kpow provides a browser UI for inspecting Kafka during local development. It is configured to publish at:

```text
http://localhost:3000
```

Inside Docker Compose, Kpow connects to:

```text
kafka:29092
schema-registry:8081
```

Kpow Community Edition requires a free local license before the UI is usable. Copy `.env.kpow.example` to `.env.kpow`, then fill the license values from the Kpow Community Edition email. `.env.kpow` is ignored by Git.

## What Can Be Observed

After the Kpow license is configured, and once producers, schemas, and consumers are added in later phases, Kpow can help inspect:

| Area | What it helps answer |
| --- | --- |
| Topics | Which topics exist, how many partitions they have, and how they are configured. |
| Messages | Whether events are being published and what their payloads look like. |
| Schemas | Which schemas are registered and how subjects evolve over time. |
| Consumer groups | Which applications are consuming from Kafka. |
| Offsets | How far each consumer group has read in each partition. |
| Consumer lag | Whether a consumer is falling behind the producer. |
| Partitions | How events are distributed across topic partitions. |

## Phase 1 Checks

Before any application code exists, the main observability checks are service-level:

```bash
docker compose ps
docker compose logs kafka
docker compose logs schema-registry
docker compose logs kpow
docker compose logs postgres
```

Kafka topics can be listed from inside the Kafka container:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Schema Registry can be checked from the host:

```bash
curl http://localhost:8081/subjects
```

On a fresh Phase 1 stack, Kafka may only show internal topics such as:

```text
__consumer_offsets
_schemas
```

That is expected. Application topics and event data arrive in later phases.

## Why This Matters

In streaming systems, correctness is not only about whether services are running. A Data Engineer also needs to see whether data is flowing, whether consumers are keeping up, whether schemas match expectations, and whether partitions are behaving as intended.

Once licensed locally, Kpow gives visibility into those Kafka-specific questions before adding heavier production observability tools.
