"""Delete and recreate the disposable local raw_earthquakes topic."""

import logging
import os
import time
from concurrent.futures import TimeoutError

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

LOGGER = logging.getLogger(__name__)
TOPIC = "raw_earthquakes"
LOCAL_BOOTSTRAP_SERVERS = {"localhost:9092", "127.0.0.1:9092"}


def reset_raw_topic(*, bootstrap_servers: str, topic: str) -> None:
    """Remove old JSON records and recreate only the local raw topic."""
    if bootstrap_servers.strip() not in LOCAL_BOOTSTRAP_SERVERS or topic != TOPIC:
        raise ValueError("Topic reset is limited to local raw_earthquakes on port 9092")

    admin = AdminClient({"bootstrap.servers": bootstrap_servers.strip()})
    if topic in admin.list_topics(timeout=10).topics:
        LOGGER.warning("Deleting %s and all its locally retained messages", topic)
        admin.delete_topics([topic], operation_timeout=15, request_timeout=20)[topic].result(
            timeout=25
        )

        deadline = time.monotonic() + 45
        while topic in admin.list_topics(timeout=10).topics:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for {topic} to be deleted")
            time.sleep(0.5)
    else:
        LOGGER.info("Topic %s does not exist; creating it", topic)

    deadline = time.monotonic() + 45
    while True:
        result = admin.create_topics(
            [NewTopic(topic, num_partitions=3, replication_factor=1)],
            operation_timeout=10,
            request_timeout=15,
        )[topic]
        try:
            result.result(timeout=20)
            break
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting to recreate {topic}") from error
            time.sleep(0.5)

    LOGGER.info("Recreated %s with 3 partitions and replication factor 1", topic)


def main() -> int:
    """Reset the local topic using the project's Kafka settings."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        reset_raw_topic(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_RAW_EARTHQUAKES_TOPIC", TOPIC),
        )
    except (KafkaException, TimeoutError, ValueError) as error:
        LOGGER.error("Could not reset local raw topic: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
