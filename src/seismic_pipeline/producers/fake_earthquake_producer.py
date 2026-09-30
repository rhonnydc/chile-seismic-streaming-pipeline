"""Publish simulated earthquake events to Kafka as JSON."""

import json
import logging
import os
import time

from confluent_kafka import KafkaException, Producer

from seismic_pipeline.producers.fake_earthquake_generator import generate_fake_earthquake

LOGGER = logging.getLogger(__name__)


def publish_fake_earthquakes(
    *, bootstrap_servers: str, topic: str, event_count: int, interval_seconds: float
) -> None:
    """Generate events, send them to Kafka, and wait for delivery reports."""
    if not bootstrap_servers.strip() or not topic.strip():
        raise ValueError("Kafka bootstrap servers and topic must not be empty")
    if event_count < 0 or interval_seconds < 0:
        raise ValueError("Event count and interval must not be negative")

    producer = Producer({"bootstrap.servers": bootstrap_servers, "message.timeout.ms": 10000})
    delivery_errors: list[str] = []

    def on_delivery(error: object, message: object) -> None:
        event_id = message.key().decode("utf-8")
        if error is not None:
            delivery_errors.append(f"{event_id}: {error}")
            LOGGER.error("Delivery failed event_id=%s error=%s", event_id, error)
        else:
            LOGGER.info(
                "Delivered event_id=%s topic=%s partition=%s offset=%s",
                event_id,
                message.topic(),
                message.partition(),
                message.offset(),
            )

    try:
        for index in range(event_count):
            event = generate_fake_earthquake()
            producer.produce(
                topic=topic,
                key=event.event_id.encode("utf-8"),
                value=json.dumps(event.to_dict(), ensure_ascii=False).encode("utf-8"),
                on_delivery=on_delivery,
            )
            LOGGER.info(
                "Queued event_id=%s magnitude=%.1f region=%s",
                event.event_id,
                event.magnitude,
                event.region,
            )
            producer.poll(0)
            if index + 1 < event_count:
                time.sleep(interval_seconds)
    finally:
        remaining = producer.flush(15.0)

    if remaining:
        raise RuntimeError(f"{remaining} Kafka message(s) were not delivered before timeout")
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed for {len(delivery_errors)} message(s)")

    LOGGER.info("Finished: %s event(s) delivered to %s", event_count, topic)


def main() -> int:
    """Run the fake producer using environment variables."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        publish_fake_earthquakes(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_RAW_EARTHQUAKES_TOPIC", "raw_earthquakes"),
            event_count=int(os.getenv("FAKE_PRODUCER_EVENT_COUNT", "10")),
            interval_seconds=float(os.getenv("FAKE_PRODUCER_INTERVAL_SECONDS", "1")),
        )
    except (BufferError, KafkaException, RuntimeError, ValueError) as error:
        LOGGER.error("Fake producer failed: %s", error)
        return 1
    except KeyboardInterrupt:
        LOGGER.warning("Fake producer interrupted")
        return 130

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
