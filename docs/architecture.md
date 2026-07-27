# Architecture

## Target Flow

```text
Fake earthquake producer
        |
        v
raw_earthquakes
        |
        v
Schema Registry validation
        |
        v
Python processor
        |
        +--> enriched_earthquakes --> Postgres
        |
        +--> seismic_metrics -------> Postgres
        |
        +--> dead_letter_earthquakes
```

Kpow will be connected to the Kafka cluster for topic, message, and consumer group inspection.

## Components

| Component | Responsibility |
| --- | --- |
| Fake producer | Generate deterministic seismic events for local development and tests. |
| Live producer | Optional later source that reads from an external earthquake API. |
| Kafka | Transport events between source, processor, metrics, and dead-letter streams. |
| Schema Registry | Manage event contracts and compatibility. |
| Python processor | Validate, enrich, route, and publish events. |
| Postgres | Store enriched events and metrics for analytical queries. |
| Kpow | Provide visibility into Kafka during local development. |

## Topic Design

| Topic | Producer | Consumer |
| --- | --- | --- |
| `raw_earthquakes` | Fake or live source | Python processor |
| `enriched_earthquakes` | Python processor | Postgres sink |
| `seismic_metrics` | Python processor | Postgres sink |
| `dead_letter_earthquakes` | Python processor | Manual inspection or future replay workflow |

## Failure Handling

Invalid or unprocessable events should not stop the processor. They should be published to `dead_letter_earthquakes` with enough context to diagnose the failure.

Dead-letter event shape will be defined in a later phase once validation and processing rules are implemented.

## Local Runtime

The local runtime will be based on Docker Compose. Terraform is planned only as a later cloud-ready layer and is not required for the local MVP.
