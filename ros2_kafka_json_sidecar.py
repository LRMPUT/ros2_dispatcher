#!/usr/bin/env python3
"""
ROS 2 → Kafka JSON sidecar transformer.

Consumes CDR-serialized ROS 2 messages produced by the ROS 2 Kafka bridge,
deserializes them using rclpy, converts them to JSON, and publishes the JSON
payloads to a mirrored Kafka topic with a configurable suffix.
"""

import argparse
import json
import logging
import signal
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, Producer
import rclpy
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.convert import message_to_ordereddict
from rosidl_runtime_py.utilities import get_message


HeaderList = Optional[List[Tuple[str, Optional[bytes]]]]


def parse_headers(headers: HeaderList) -> Dict[str, Any]:
    """Convert Kafka headers to a string-keyed dictionary."""
    header_dict: Dict[str, Any] = {}
    if headers is None:
        return header_dict

    for key, value in headers:
        if value is None:
            header_dict[key] = None
        elif isinstance(value, (bytes, bytearray)):
            header_dict[key] = value.decode("utf-8", errors="replace")
        else:
            header_dict[key] = value
    return header_dict


def load_message_class(ros_type: str):
    """
    Dynamically load a ROS 2 message class using its type string
    (e.g., 'std_msgs/msg/String').
    """
    return get_message(ros_type)


def deserialize_to_json(payload: bytes, ros_type: str) -> str:
    """Deserialize a CDR payload into JSON using the provided ROS type."""
    message_class = load_message_class(ros_type)
    ros_message = deserialize_message(payload, message_class)
    message_dict = message_to_ordereddict(ros_message)
    return json.dumps(message_dict, default=str)


def delivery_report(err: Optional[KafkaError], msg) -> None:
    """Log the status of produced messages."""
    if err is not None:
        logging.error("Failed to deliver message to %s [%d]: %s", msg.topic(), msg.partition(), err)
    else:
        logging.debug(
            "Delivered message to %s [%d] at offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def build_consumer(args: argparse.Namespace) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "enable.auto.commit": True,
            "auto.offset.reset": args.offset_reset,
            "allow.auto.create.topics": False,
        }
    )


def build_producer(args: argparse.Namespace) -> Producer:
    return Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "linger.ms": args.linger_ms,
        }
    )


def subscribe_topics(consumer: Consumer, topic: Optional[str], pattern: Optional[str]) -> None:
    if topic:
        consumer.subscribe([topic])
        logging.info("Subscribed to topic '%s'", topic)
    elif pattern:
        consumer.subscribe([pattern])
        logging.info("Subscribed to topics matching pattern '%s'", pattern)
    else:
        raise ValueError("Either --topic or --topic-pattern must be provided.")


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ROS 2 Kafka JSON sidecar transformer")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (host:port). Default: %(default)s",
    )
    parser.add_argument(
        "--group-id",
        default="ros2-json-sidecar",
        help="Consumer group id. Default: %(default)s",
    )
    parser.add_argument(
        "--topic",
        help="Kafka topic to consume from. Mutually exclusive with --topic-pattern.",
    )
    parser.add_argument(
        "--topic-pattern",
        default="^ros2\\..*",
        help="Regex pattern for topic subscription when --topic is not provided. Default: %(default)s",
    )
    parser.add_argument(
        "--output-suffix",
        default=".json",
        help="Suffix appended to the input topic name for output messages. Default: %(default)s",
    )
    parser.add_argument(
        "--offset-reset",
        default="latest",
        choices=["earliest", "latest"],
        help="Offset reset policy. Default: %(default)s",
    )
    parser.add_argument(
        "--linger-ms",
        type=int,
        default=5,
        help="Producer linger.ms setting. Default: %(default)s",
    )
    parser.add_argument(
        "--poll-timeout",
        type=float,
        default=1.0,
        help="Consumer poll timeout in seconds. Default: %(default)s",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level. Default: %(default)s",
    )
    return parser.parse_args(list(argv))


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    rclpy.init(args=None)
    consumer = build_consumer(args)
    producer = build_producer(args)

    running = True

    def handle_signal(signum, _frame):
        nonlocal running
        logging.info("Received signal %s, shutting down...", signum)
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        subscribe_topics(consumer, args.topic, args.topic_pattern)

        while running:
            producer.poll(0)
            message = consumer.poll(args.poll_timeout)
            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logging.error("Kafka error: %s", message.error())
                continue

            headers = parse_headers(message.headers())
            ros_type = headers.get("ros_type")
            if not ros_type:
                logging.warning("Missing 'ros_type' header for topic %s; skipping message.", message.topic())
                continue

            try:
                json_payload = deserialize_to_json(message.value(), ros_type)
            except ModuleNotFoundError:
                logging.error("Unknown ROS 2 type '%s'; skipping message.", ros_type)
                continue
            except Exception as exc:
                logging.error("Failed to deserialize/convert message for type '%s': %s", ros_type, exc)
                continue

            output_topic = f"{message.topic()}{args.output_suffix}"
            output_headers = [(key, str(value)) for key, value in headers.items()]

            try:
                producer.produce(
                    topic=output_topic,
                    value=json_payload.encode("utf-8"),
                    key=message.key(),
                    headers=output_headers,
                    on_delivery=delivery_report,
                )
            except BufferError as exc:
                logging.error("Local producer queue is full: %s", exc)
            except Exception as exc:
                logging.error("Failed to produce message to %s: %s", output_topic, exc)
    finally:
        logging.info("Flushing producer and closing consumer...")
        producer.flush(5)
        consumer.close()
        rclpy.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
