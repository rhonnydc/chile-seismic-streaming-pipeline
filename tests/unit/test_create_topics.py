"""Check repeatable topic creation without a running Kafka broker."""

from types import SimpleNamespace

import pytest

from scripts import create_topics


class FakeFuture:
    def __init__(self) -> None:
        self.waited = False

    def result(self, timeout: float) -> None:
        assert timeout == 20
        self.waited = True


class FakeAdminClient:
    existing_topics: dict[str, object] = {}
    instances: list["FakeAdminClient"] = []

    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.created_topics: list = []
        self.future = FakeFuture()
        self.instances.append(self)

    def list_topics(self, timeout: float) -> SimpleNamespace:
        assert timeout == 10
        return SimpleNamespace(topics=self.existing_topics)

    def create_topics(self, topics: list, **kwargs: float) -> dict[str, FakeFuture]:
        self.created_topics.extend(topics)
        return {topic.topic: self.future for topic in topics}


def test_creates_raw_topic_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeAdminClient.instances.clear()
    FakeAdminClient.existing_topics = {}
    monkeypatch.setattr(create_topics, "AdminClient", FakeAdminClient)

    created = create_topics.ensure_raw_earthquakes_topic(
        bootstrap_servers="localhost:9092", topic="raw_earthquakes"
    )

    client = FakeAdminClient.instances[0]
    assert created is True
    assert client.config["bootstrap.servers"] == "localhost:9092"
    assert len(client.created_topics) == 1
    assert client.created_topics[0].topic == "raw_earthquakes"
    assert client.created_topics[0].num_partitions == 3
    assert client.created_topics[0].replication_factor == 1
    assert client.future.waited is True


def test_existing_topic_is_left_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeAdminClient.instances.clear()
    FakeAdminClient.existing_topics = {"raw_earthquakes": object()}
    monkeypatch.setattr(create_topics, "AdminClient", FakeAdminClient)

    created = create_topics.ensure_raw_earthquakes_topic(
        bootstrap_servers="localhost:9092", topic="raw_earthquakes"
    )

    assert created is False
    assert FakeAdminClient.instances[0].created_topics == []


def test_creates_enriched_topic_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeAdminClient.instances.clear()
    FakeAdminClient.existing_topics = {}
    monkeypatch.setattr(create_topics, "AdminClient", FakeAdminClient)

    created = create_topics.ensure_enriched_earthquakes_topic(
        bootstrap_servers="localhost:9092", topic="enriched_earthquakes"
    )

    client = FakeAdminClient.instances[0]
    assert created is True
    assert len(client.created_topics) == 1
    assert client.created_topics[0].topic == "enriched_earthquakes"
    assert client.created_topics[0].num_partitions == 3
    assert client.created_topics[0].replication_factor == 1
    assert client.future.waited is True


def test_main_requests_both_topics(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[tuple[str, str]] = []

    def record_topic(*, bootstrap_servers: str, topic: str) -> bool:
        requests.append((bootstrap_servers, topic))
        return True

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_RAW_EARTHQUAKES_TOPIC", "raw_earthquakes")
    monkeypatch.setenv("KAFKA_ENRICHED_EARTHQUAKES_TOPIC", "enriched_earthquakes")
    monkeypatch.setattr(create_topics, "ensure_raw_earthquakes_topic", record_topic)
    monkeypatch.setattr(create_topics, "ensure_enriched_earthquakes_topic", record_topic)

    assert create_topics.main() == 0
    assert requests == [
        ("localhost:9092", "raw_earthquakes"),
        ("localhost:9092", "enriched_earthquakes"),
    ]
