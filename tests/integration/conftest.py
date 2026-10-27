"""Real-service helpers shared by the Phase 11 integration scenarios."""

import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, Message, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[2]
TIMEOUT_SECONDS = 30


class IntegrationHarness:
    """Own consumer processes and query only this run's Kafka and Postgres resources."""

    def __init__(self, env: dict[str, str], log_dir: Path) -> None:
        self.env = env
        self.log_dir = log_dir
        self.processes: list[tuple[subprocess.Popen, object]] = []
        self.registry = SchemaRegistryClient({"url": env["SCHEMA_REGISTRY_URL"]})

    def start_consumer(self, module: str) -> subprocess.Popen:
        if module not in {"enriching_consumer", "postgres_sink_consumer"}:
            raise ValueError(f"Unknown consumer module: {module}")
        log = (self.log_dir / f"{module}-{len(self.processes)}.log").open(
            "w", encoding="utf-8"
        )
        process = subprocess.Popen(
            [sys.executable, "-m", f"seismic_pipeline.consumers.{module}"],
            cwd=ROOT,
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.processes.append((process, log))
        return process

    def stop_consumers(self) -> None:
        for process, log in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            log.close()
        self.processes.clear()

    def _postgres_compose(self, action: str) -> None:
        """Control only the Postgres service owned by this integration run."""
        run_id = self.env.get("INTEGRATION_RUN_ID", "")
        project = self.env.get("INTEGRATION_COMPOSE_PROJECT", "")
        compose_file = Path(self.env.get("INTEGRATION_COMPOSE_FILE", ""))
        if not run_id or project != f"seismic-it-{run_id}":
            pytest.fail("Integration Compose project identity is invalid")
        if compose_file.resolve() != (ROOT / "docker-compose.integration.yml").resolve():
            pytest.fail("Integration Compose file identity is invalid")
        if action not in {"stop", "start"}:
            raise ValueError(f"Unsupported Postgres Compose action: {action}")
        command = ["docker", "compose", "-f", str(compose_file), "-p", project, action, "postgres"]
        result = subprocess.run(
            command, cwd=ROOT, env=self.env, capture_output=True, text=True, timeout=60, check=False
        )
        if result.returncode:
            pytest.fail(f"Could not {action} isolated Postgres:\n{result.stdout}\n{result.stderr}")

    def stop_postgres(self) -> None:
        self._postgres_compose("stop")

    def start_postgres(self) -> None:
        self._postgres_compose("start")
        deadline = time.monotonic() + 60
        last_error = "Postgres did not accept a connection"
        while time.monotonic() < deadline:
            try:
                with psycopg.connect(
                    host=self.env["POSTGRES_HOST"],
                    port=int(self.env["POSTGRES_PORT"]),
                    dbname=self.env["POSTGRES_DB"],
                    user=self.env["POSTGRES_USER"],
                    password=self.env["POSTGRES_PASSWORD"],
                    connect_timeout=2,
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT to_regclass('public.enriched_earthquake_events')")
                        if cursor.fetchone()[0] is not None:
                            return
                        last_error = "Integration table is missing after Postgres restart"
            except psycopg.OperationalError as error:
                last_error = str(error)
            time.sleep(0.5)
        pytest.fail(f"Isolated Postgres did not recover: {last_error}")

    def _check_processes(self) -> None:
        for process, log in self.processes:
            if process.poll() is not None:
                output = Path(log.name).read_text(encoding="utf-8")
                pytest.fail(f"Consumer exited with status {process.returncode}:\n{output}")

    def publish(self, topic: str, key: bytes, value: bytes) -> Message:
        """Return Kafka's delivery report, including the actual partition and offset."""
        reports: list[tuple[object, Message]] = []
        producer = Producer(
            {"bootstrap.servers": self.env["KAFKA_BOOTSTRAP_SERVERS"], "message.timeout.ms": 10000}
        )
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=lambda error, message: reports.append((error, message)),
        )
        remaining = producer.flush(15)
        assert remaining == 0 and len(reports) == 1, "Kafka did not report delivery"
        error, message = reports[0]
        assert error is None, f"Kafka delivery failed: {error}"
        return message

    def wait_for_message(
        self, topic: str, key: bytes, *, timeout: int = TIMEOUT_SECONDS
    ) -> Message:
        """Read from this run's topic without changing a pipeline consumer group's offsets."""
        metadata = Consumer(
            {"bootstrap.servers": self.env["KAFKA_BOOTSTRAP_SERVERS"], "group.id": uuid4().hex}
        )
        try:
            partitions = metadata.list_topics(topic=topic, timeout=5).topics[topic].partitions
            metadata.assign(
                [TopicPartition(topic, number, OFFSET_BEGINNING) for number in partitions]
            )
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                self._check_processes()
                message = metadata.poll(1)
                if message is None:
                    continue
                if message.error():
                    pytest.fail(f"Kafka observation failed: {message.error()}")
                if message.key() == key:
                    return message
        finally:
            metadata.close()
        pytest.fail(f"No message with key {key!r} appeared in {topic} within {timeout}s")

    def decode(self, message: Message) -> dict[str, object]:
        decoded = AvroDeserializer(self.registry)(
            message.value(), SerializationContext(message.topic(), MessageField.VALUE)
        )
        assert isinstance(decoded, dict)
        return decoded

    def committed(self, topic: str, partition: int, group_id: str) -> int:
        consumer = Consumer(
            {"bootstrap.servers": self.env["KAFKA_BOOTSTRAP_SERVERS"], "group.id": group_id}
        )
        try:
            return consumer.committed([TopicPartition(topic, partition)], timeout=5)[0].offset
        finally:
            consumer.close()

    def wait_for_commit(
        self, topic: str, partition: int, group_id: str, expected: int
    ) -> int:
        deadline = time.monotonic() + TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            self._check_processes()
            offset = self.committed(topic, partition, group_id)
            if offset >= expected:
                return offset
            time.sleep(0.2)
        pytest.fail(f"{group_id} did not commit {topic}:{partition} offset {expected}")

    def rows(self, event_id: str) -> list[dict[str, object]]:
        with psycopg.connect(
            host=self.env["POSTGRES_HOST"],
            port=int(self.env["POSTGRES_PORT"]),
            dbname=self.env["POSTGRES_DB"],
            user=self.env["POSTGRES_USER"],
            password=self.env["POSTGRES_PASSWORD"],
            connect_timeout=3,
            row_factory=dict_row,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM enriched_earthquake_events WHERE event_id = %s", (event_id,)
                )
                return cursor.fetchall()

    def wait_for_row(self, event_id: str) -> dict[str, object]:
        deadline = time.monotonic() + TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            self._check_processes()
            rows = self.rows(event_id)
            if rows:
                assert len(rows) == 1
                return rows[0]
            time.sleep(0.2)
        pytest.fail(f"Postgres has no row for {event_id} after {TIMEOUT_SECONDS}s")


@pytest.fixture
def integration_harness(tmp_path: Path) -> IntegrationHarness:
    """Fail explicitly if pytest was run against the regular local stack."""
    project = os.getenv("INTEGRATION_COMPOSE_PROJECT", "")
    if not project.startswith("seismic-it-"):
        pytest.fail("Run integration tests through scripts/run_integration.py")
    env = os.environ.copy()
    harness = IntegrationHarness(env, tmp_path)
    try:
        yield harness
    finally:
        harness.stop_consumers()
