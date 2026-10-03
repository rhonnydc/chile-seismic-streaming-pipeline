# Observability

## Phase 1 Scope

Phase 1 configures Kpow as the local Kafka observability tool. Its UI requires a valid local license in `.env.kpow`.

This phase does not include Prometheus, Grafana, alerting, distributed tracing, log aggregation, or production monitoring.

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

Kpow requires a local license before the UI is usable. Copy `.env.kpow.example` to `.env.kpow`, then fill the license values from the Kpow email. `.env.kpow` is ignored by Git.

## What Can Be Observed

With the local license configured, Kpow can inspect the `raw_earthquakes` and `enriched_earthquakes` messages and their registered value schemas. Phase 4 also makes consumer groups, offsets, and lag useful:

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

That is expected for Phase 1. Phase 3 adds `raw_earthquakes` and its Avro events; Phase 4 adds `enriched_earthquakes` and a consumer group.

## Phase 4 Checks In Kpow

After registering both schemas, creating both topics, and publishing fake raw events, start `make consume-enrich` (or the direct Python command in the README). With Kpow open at `http://localhost:3000`, check:

1. **Topics and messages:** `raw_earthquakes` contains Avro input; `enriched_earthquakes` receives Avro output with `event_id` keys and derived fields such as `severity_level`, `is_shallow`, and `processed_at_utc`.
2. **Schemas:** Schema Registry lists `raw_earthquakes-value` and `enriched_earthquakes-value`.
3. **Consumer group:** `seismic-enricher` consumes `raw_earthquakes`. Its members may disappear after the Python process stops, but the committed offsets remain associated with the group.
4. **Offsets:** The committed offset for a partition is the next record the group would read there. It advances after the corresponding enriched delivery succeeds.
5. **Lag:** Compare the partition's end offset with its committed offset. Lag should fall as the consumer processes queued raw events; if more raw events arrive, it can rise again.

The consumer logs `event_id`, source partition, and source offset after each successful commit. If processing stops on a bad input or output delivery failure, that input is not committed; inspect the logs and topic messages before restarting. This phase has no dead-letter topic or aggregate metrics.
