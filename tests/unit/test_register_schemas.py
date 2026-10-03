"""Check schema registration behavior without a running Schema Registry."""

import json
from types import SimpleNamespace

import pytest

from scripts import register_schemas


class FakeRegistry:
    instances: list["FakeRegistry"] = []

    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.compatibility: tuple[str, str] | None = None
        self.registered_subject: str | None = None
        self.field_count: int | None = None
        self.instances.append(self)

    def set_compatibility(self, *, subject_name: str, level: str) -> None:
        self.compatibility = (subject_name, level)

    def register_schema(self, subject: str, schema: object) -> int:
        assert self.compatibility == (subject, "BACKWARD_TRANSITIVE")
        assert schema.schema_type == "AVRO"
        self.registered_subject = subject
        self.field_count = len(json.loads(schema.schema_str)["fields"])
        return 42

    def lookup_schema(self, subject: str, schema: object) -> SimpleNamespace:
        assert subject == self.registered_subject
        return SimpleNamespace(version=1)


@pytest.mark.parametrize(
    ("register", "topic", "field_count"),
    [
        (register_schemas.register_raw_earthquake_schema, "raw_earthquakes", 18),
        (register_schemas.register_enriched_earthquake_schema, "enriched_earthquakes", 24),
    ],
)
def test_registers_topic_value_contract(
    monkeypatch: pytest.MonkeyPatch, register, topic: str, field_count: int
) -> None:
    FakeRegistry.instances.clear()
    monkeypatch.setattr(register_schemas, "SchemaRegistryClient", FakeRegistry)

    result = register(registry_url="http://localhost:8081", topic=topic)

    client = FakeRegistry.instances[0]
    assert client.config == {"url": "http://localhost:8081"}
    assert client.registered_subject == f"{topic}-value"
    assert client.field_count == field_count
    assert result == (f"{topic}-value", 1, 42)


def test_main_requests_both_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[tuple[str, str]] = []

    def record_schema(*, registry_url: str, topic: str) -> tuple[str, int, int]:
        requests.append((registry_url, topic))
        return f"{topic}-value", 1, 42

    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    monkeypatch.setenv("KAFKA_RAW_EARTHQUAKES_TOPIC", "raw_earthquakes")
    monkeypatch.setenv("KAFKA_ENRICHED_EARTHQUAKES_TOPIC", "enriched_earthquakes")
    monkeypatch.setattr(register_schemas, "register_raw_earthquake_schema", record_schema)
    monkeypatch.setattr(register_schemas, "register_enriched_earthquake_schema", record_schema)

    assert register_schemas.main() == 0
    assert requests == [
        ("http://localhost:8081", "raw_earthquakes"),
        ("http://localhost:8081", "enriched_earthquakes"),
    ]
