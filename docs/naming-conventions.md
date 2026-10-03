# Naming Conventions

## Kafka Topics

Topic names use lowercase snake case and describe the event stream, not the producing service.

| Topic | Description |
| --- | --- |
| `raw_earthquakes` | Simulated seismic events; a live source is planned. |
| `enriched_earthquakes` | Raw event fields plus six derived fields. |
| `seismic_metrics` | Planned aggregate metrics stream. |
| `dead_letter_earthquakes` | Planned failed-event stream. |

## Schemas

Schema filenames map one-to-one to event types.

| File | Event |
| --- | --- |
| `raw_earthquake_event.avsc` | Input event contract. |
| `enriched_earthquake_event.avsc` | Processed event contract. |
| `seismic_metric_event.avsc` | Metric event contract. |

## Python Modules

Python modules should be named by responsibility.

```text
fake_earthquake_producer.py
enriching_consumer.py
enrichment.py
```

Avoid generic names such as `main.py`, `utils.py`, or `handler.py` unless the module has a narrow and documented role.

## Tests

Tests are grouped by execution cost and dependency boundary.

```text
tests/unit/
tests/contracts/
tests/integration/
```

Examples:

```text
tests/unit/test_enrichment.py
tests/unit/test_enriching_consumer.py
tests/contracts/test_enriched_earthquake_contract.py
```

## Environment Variables

Environment variables use uppercase snake case and include the owning system when useful.

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_RAW_EARTHQUAKES_TOPIC
KAFKA_ENRICHED_EARTHQUAKES_TOPIC
CONSUMER_GROUP_ID
SCHEMA_REGISTRY_URL
POSTGRES_HOST
```
