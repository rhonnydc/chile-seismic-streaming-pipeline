# Architecture

## Target Architecture

```text
Earthquake Source / Event Poller
-> Kafka topic: raw_earthquakes
-> Schema Registry
-> Python Consumer / Processor
-> Kafka topics: enriched_earthquakes, seismic_metrics, dead_letter_earthquakes
-> Postgres Analytical Sink
-> Data Quality Checks
-> Kpow Observability
```

## Components

### Earthquake Source / Event Poller

The first source will generate fake seismic events so the project can run locally without depending on an external API.

A live API producer may be added later as an optional source.

### Kafka

Kafka will be the central streaming backbone. The initial planned topics are:

- `raw_earthquakes`
- `enriched_earthquakes`
- `seismic_metrics`
- `dead_letter_earthquakes`

### Schema Registry

Schema Registry will be used to manage event contracts and validate event structure over time.

### Python Processor

Python services will consume raw events, validate and enrich them, publish successful outputs, and route invalid events to a dead letter topic.

### Postgres Analytical Sink

Postgres will store processed events and metrics in a simple analytical model that can be inspected locally.

### Data Quality Checks

Data quality checks will be added incrementally to validate completeness, ranges, required fields, and analytical consistency.

### Kpow Observability

Kpow will be used later to inspect Kafka topics, consumer groups, messages, and pipeline behavior during local development.
