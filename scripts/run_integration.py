"""Run integration tests against a disposable, isolated Compose project."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx
import psycopg
from confluent_kafka.admin import AdminClient

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.integration.yml"
READY_TIMEOUT_SECONDS = 120


def _port(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if not 1 <= value <= 65535:
        raise ValueError(f"{name} must be a TCP port between 1 and 65535")
    return value


def _run(command: list[str], *, env: dict[str, str], timeout: int) -> None:
    result = subprocess.run(command, cwd=ROOT, env=env, text=True, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: {' '.join(command)}"
        )


def _stop_pytest_tree(process: subprocess.Popen) -> None:
    """Stop pytest and its consumer children if the test run is interrupted."""
    if os.name == "nt":
        result = subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if result.returncode:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
            raise RuntimeError(f"Could not stop pytest process tree: {result.stderr}")
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait(timeout=10)


def _run_pytest(env: dict[str, str], *, timeout: int = 900) -> int:
    """Give pytest a bounded lifetime while retaining control of its process tree."""
    group_options = (
        {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        if os.name == "nt"
        else {"start_new_session": True}
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "pytest", "-m", "integration", *sys.argv[1:]],
        cwd=ROOT,
        env=env,
        **group_options,
    )
    try:
        return process.wait(timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        _stop_pytest_tree(process)
        raise


def _ready(env: dict[str, str]) -> None:
    """Require all services and the SQL table, rather than relying on container state."""
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    pending: dict[str, str] = {}
    while time.monotonic() < deadline:
        pending.clear()
        try:
            AdminClient({"bootstrap.servers": env["KAFKA_BOOTSTRAP_SERVERS"]}).list_topics(
                timeout=2
            )
        except Exception as error:
            pending["Kafka"] = str(error)

        try:
            response = httpx.get(f"{env['SCHEMA_REGISTRY_URL']}/subjects", timeout=2)
            response.raise_for_status()
        except (httpx.HTTPError, OSError) as error:
            pending["Schema Registry"] = str(error)

        try:
            with psycopg.connect(
                host=env["POSTGRES_HOST"],
                port=int(env["POSTGRES_PORT"]),
                dbname=env["POSTGRES_DB"],
                user=env["POSTGRES_USER"],
                password=env["POSTGRES_PASSWORD"],
                connect_timeout=2,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT to_regclass('public.enriched_earthquake_events')")
                    if cursor.fetchone()[0] is None:
                        pending["Postgres"] = "sql/init.sql has not created the table"
        except psycopg.Error as error:
            pending["Postgres"] = str(error)

        if not pending:
            return
        time.sleep(1)
    raise RuntimeError(f"Integration services did not become ready: {pending}")


def main() -> int:
    """Own one Compose project from startup through cleanup, including pytest failures."""
    kafka_port = _port("INTEGRATION_KAFKA_PORT", 19092)
    registry_port = _port("INTEGRATION_SCHEMA_REGISTRY_PORT", 18081)
    postgres_port = _port("INTEGRATION_POSTGRES_PORT", 15432)
    if len({kafka_port, registry_port, postgres_port}) != 3:
        raise ValueError("Integration host ports must be distinct")

    run_id = uuid4().hex[:12]
    project = f"seismic-it-{run_id}"
    env = os.environ.copy()
    env.update(
        {
            "INTEGRATION_KAFKA_PORT": str(kafka_port),
            "INTEGRATION_SCHEMA_REGISTRY_PORT": str(registry_port),
            "INTEGRATION_POSTGRES_PORT": str(postgres_port),
            "INTEGRATION_COMPOSE_PROJECT": project,
            "INTEGRATION_COMPOSE_FILE": str(COMPOSE_FILE),
            "INTEGRATION_RUN_ID": run_id,
            "KAFKA_BOOTSTRAP_SERVERS": f"127.0.0.1:{kafka_port}",
            "SCHEMA_REGISTRY_URL": f"http://127.0.0.1:{registry_port}",
            "POSTGRES_HOST": "127.0.0.1",
            "POSTGRES_PORT": str(postgres_port),
            "POSTGRES_DB": "seismic_integration",
            "POSTGRES_USER": "seismic_integration",
            "POSTGRES_PASSWORD": "seismic_integration",
            "KAFKA_RAW_EARTHQUAKES_TOPIC": f"raw_earthquakes_{run_id}",
            "KAFKA_ENRICHED_EARTHQUAKES_TOPIC": f"enriched_earthquakes_{run_id}",
            "KAFKA_DEAD_LETTER_TOPIC": f"dead_letter_earthquakes_{run_id}",
            "CONSUMER_GROUP_ID": f"seismic-enricher-{run_id}",
            "POSTGRES_SINK_GROUP_ID": f"seismic-postgres-sink-{run_id}",
        }
    )
    compose = ["docker", "compose", "-f", str(COMPOSE_FILE), "-p", project]
    print(f"Starting isolated Compose project {project}", flush=True)
    try:
        _run(compose + ["up", "-d", "--wait", "--wait-timeout", "180"], env=env, timeout=240)
        _ready(env)
        _run([sys.executable, str(ROOT / "scripts" / "create_topics.py")], env=env, timeout=60)
        _run([sys.executable, str(ROOT / "scripts" / "register_schemas.py")], env=env, timeout=60)
        return _run_pytest(env)
    finally:
        print(f"Removing isolated Compose project {project}", flush=True)
        _run(compose + ["down", "--volumes"], env=env, timeout=120)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"Integration setup or cleanup failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
