# Naming Conventions

## Kafka Topics

Use lowercase snake case and plural domain nouns.

```text
raw_earthquakes
enriched_earthquakes
seismic_metrics
dead_letter_earthquakes
```

## Schemas

Use lowercase snake case and include the event type.

```text
raw_earthquake_event.avsc
enriched_earthquake_event.avsc
seismic_metric_event.avsc
```

## Python Modules

Use lowercase snake case and name modules by responsibility.

```text
fake_earthquake_producer.py
live_earthquake_producer.py
earthquake_consumer.py
earthquake_processor.py
postgres_sink.py
```

## Tests

Use the `test_*.py` pattern and keep tests grouped by type.

```text
tests/unit/test_earthquake_processor.py
tests/integration/test_postgres_sink.py
tests/contracts/test_raw_earthquake_schema.py
```

## Documentation

Use lowercase kebab case or snake case consistently. This project uses kebab-style concepts in readable Markdown filenames only when useful, but current docs use snake case for simplicity.

```text
architecture.md
phase-0-design.md
naming-conventions.md
technical-decisions.md
roadmap.md
```
