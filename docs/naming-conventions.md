# Naming Conventions

Consistent names make the pipeline easier to inspect in Kafka, logs, tests, and documentation.

## Kafka Topics

Topic names use lowercase snake case and describe the event stream, not the producing service.

| Topic | Description |
| --- | --- |
| `raw_earthquakes` | Unprocessed seismic events from fake or live sources. |
| `enriched_earthquakes` | Validated events with derived fields added by the processor. |
| `seismic_metrics` | Aggregated or derived metrics produced from seismic events. |
| `dead_letter_earthquakes` | Events that failed validation or processing. |

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
live_earthquake_producer.py
earthquake_processor.py
postgres_sink.py
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
tests/unit/test_earthquake_processor.py
tests/contracts/test_raw_earthquake_schema.py
tests/integration/test_postgres_sink.py
```

## Environment Variables

Environment variables use uppercase snake case and include the owning system when useful.

```text
KAFKA_BOOTSTRAP_SERVERS
SCHEMA_REGISTRY_URL
POSTGRES_HOST
EARTHQUAKE_API_URL
```
