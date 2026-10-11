"""Poll a USGS GeoJSON feed and publish normalized earthquakes as raw Avro."""

import logging
import os
import time
from pathlib import Path

import httpx
from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

from seismic_pipeline.producers.usgs_geojson_normalizer import normalize_usgs_feature

LOGGER = logging.getLogger(__name__)
SCHEMA_PATH = Path(__file__).resolve().parents[3] / "schemas" / "raw_earthquake_event.avsc"
DEFAULT_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"


def publish_live_earthquakes(
    *,
    bootstrap_servers: str,
    registry_url: str,
    topic: str,
    feed_url: str,
    poll_interval_seconds: float,
    max_cycles: int,
    request_timeout_seconds: float,
) -> int:
    """Poll the feed; return delivered count. Zero max_cycles means run until interrupted."""
    if not all(value.strip() for value in (bootstrap_servers, registry_url, topic, feed_url)):
        raise ValueError("Kafka, Schema Registry, topic, and feed URL must not be empty")
    if poll_interval_seconds < 0 or max_cycles < 0 or request_timeout_seconds <= 0:
        raise ValueError(
            "Polling interval and cycles must be nonnegative; timeout must be positive"
        )

    registry_client = SchemaRegistryClient({"url": registry_url})
    serializer = AvroSerializer(
        registry_client,
        SCHEMA_PATH.read_text(encoding="utf-8"),
        conf={
            "auto.register.schemas": False,
            "subject.name.strategy": topic_subject_name_strategy,
            "validate.strict": True,
        },
    )
    producer = Producer({"bootstrap.servers": bootstrap_servers, "message.timeout.ms": 10000})
    published_ids: set[str] = set()
    delivery_errors: list[str] = []
    successful_fetches = 0
    cycle = 0

    def on_delivery(error: object, message: object) -> None:
        event_id = message.key().decode("utf-8")
        if error is not None:
            delivery_errors.append(f"{event_id}: {error}")
            LOGGER.error("Delivery failed event_id=%s error=%s", event_id, error)
        else:
            published_ids.add(event_id.removeprefix("usgs:"))
            LOGGER.info(
                "Delivered event_id=%s topic=%s partition=%s offset=%s",
                event_id,
                message.topic(),
                message.partition(),
                message.offset(),
            )

    try:
        while max_cycles == 0 or cycle < max_cycles:
            cycle += 1
            LOGGER.info("Fetching feed cycle=%s url=%s", cycle, feed_url)
            try:
                response = httpx.get(feed_url, timeout=request_timeout_seconds)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not isinstance(payload.get("features"), list):
                    raise ValueError("GeoJSON response must contain a features array")
            except (httpx.HTTPError, ValueError) as error:
                LOGGER.warning("Feed fetch failed cycle=%s error=%s", cycle, error)
            else:
                successful_fetches += 1
                features = payload["features"]
                if not features:
                    LOGGER.info("Feed returned no events cycle=%s", cycle)
                queued_ids: set[str] = set()
                skipped = 0
                for feature in features:
                    try:
                        event = normalize_usgs_feature(feature)
                    except ValueError as error:
                        skipped += 1
                        LOGGER.warning("Skipping invalid feature cycle=%s error=%s", cycle, error)
                        continue
                    if event.event_type != "earthquake":
                        skipped += 1
                        continue
                    if (
                        event.source_event_id in published_ids
                        or event.source_event_id in queued_ids
                    ):
                        skipped += 1
                        continue

                    value = serializer(
                        event.to_dict(), SerializationContext(topic, MessageField.VALUE)
                    )
                    if value is None:
                        raise ValueError(f"Avro serializer returned no value for {event.event_id}")
                    producer.produce(
                        topic=topic,
                        key=event.event_id.encode("utf-8"),
                        value=value,
                        on_delivery=on_delivery,
                    )
                    queued_ids.add(event.source_event_id)
                    producer.poll(0)

                remaining = producer.flush(15.0)
                if remaining:
                    raise RuntimeError(
                        f"{remaining} Kafka message(s) were not delivered before timeout"
                    )
                if delivery_errors:
                    raise RuntimeError(
                        f"Kafka delivery failed for {len(delivery_errors)} message(s)"
                    )
                LOGGER.info(
                    "Completed cycle=%s feed_events=%s published=%s skipped=%s",
                    cycle,
                    len(features),
                    len(queued_ids),
                    skipped,
                )

            if max_cycles == 0 or cycle < max_cycles:
                time.sleep(poll_interval_seconds)
    finally:
        remaining = producer.flush(15.0)
        if remaining:
            LOGGER.error("Producer closed with %s undelivered message(s)", remaining)

    if not successful_fetches:
        raise RuntimeError(f"Feed fetch failed in all {cycle} cycle(s)")
    LOGGER.info("Finished: %s unique event(s) delivered to %s", len(published_ids), topic)
    return len(published_ids)


def main() -> int:
    """Run the polling producer with local environment settings."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        publish_live_earthquakes(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            topic=os.getenv("KAFKA_RAW_EARTHQUAKES_TOPIC", "raw_earthquakes"),
            feed_url=os.getenv("LIVE_EARTHQUAKE_FEED_URL", DEFAULT_FEED_URL),
            poll_interval_seconds=float(os.getenv("LIVE_PRODUCER_POLL_INTERVAL_SECONDS", "60")),
            max_cycles=int(os.getenv("LIVE_PRODUCER_MAX_CYCLES", "1")),
            request_timeout_seconds=float(os.getenv("LIVE_PRODUCER_REQUEST_TIMEOUT_SECONDS", "10")),
        )
    except KeyboardInterrupt:
        LOGGER.info("Live producer stopped by user")
        return 0
    except (
        BufferError,
        httpx.HTTPError,
        KafkaException,
        OSError,
        RuntimeError,
        SchemaRegistryError,
        SerializationError,
        TypeError,
        ValueError,
    ) as error:
        LOGGER.error("Live producer failed: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
