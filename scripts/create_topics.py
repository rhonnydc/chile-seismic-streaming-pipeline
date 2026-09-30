"""Create the raw earthquake Kafka topic when it does not exist."""

import logging
import os
from concurrent.futures import TimeoutError

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

LOGGER = logging.getLogger(__name__)


def ensure_raw_earthquakes_topic(*, bootstrap_servers: str, topic: str) -> bool:
    """Return True if the topic was created, or False if it already existed."""
    if not bootstrap_servers.strip() or not topic.strip():
        raise ValueError("Kafka bootstrap servers and topic must not be empty")

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    if topic in metadata.topics:
        LOGGER.info("Topic %s already exists", topic)
        return False

    result = admin.create_topics(
        [NewTopic(topic, num_partitions=3, replication_factor=1)],
        operation_timeout=10,
        request_timeout=15,
    )[topic]
    try:
        result.result(timeout=20)
    except KafkaException as error:
        if error.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            LOGGER.info("Topic %s already exists", topic)
            return False
        raise

    LOGGER.info("Created topic %s with 3 partitions and replication factor 1", topic)
    return True


def main() -> int:
    """Use the local Kafka settings from environment variables."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        ensure_raw_earthquakes_topic(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_RAW_EARTHQUAKES_TOPIC", "raw_earthquakes"),
        )
    except (KafkaException, TimeoutError, ValueError) as error:
        LOGGER.error("Could not create topic: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
