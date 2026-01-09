#!/usr/bin/env python3
"""Kafka CDR  ROS 2 message republisher.

This client consumes Kafka messages whose value is a raw ROS 2 CDR payload (as produced by
`kafka_sink` when `kafka.payload_format` is `cdr`) and republishes the decoded ROS 2
message on a ROS 2 topic.

It relies on Kafka headers set by `kafka_sink`:
- `ros_topic`: original ROS topic name (e.g., "/sensing/...")
- `ros_type`: ROS 2 message type string (e.g., "geometry_msgs/msg/PoseStamped")

If headers are missing, you can override them via CLI args.
"""

import argparse
import logging
import signal
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message


HeaderList = Optional[List[Tuple[str, Optional[bytes]]]]


def parse_headers(headers: HeaderList) -> Dict[str, Any]:
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
    parser = argparse.ArgumentParser(description="Consume Kafka CDR payloads and republish to ROS 2")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (host:port). Default: %(default)s",
    )
    parser.add_argument(
        "--group-id",
        default="ros2-cdr-to-ros",
        help="Consumer group id. Default: %(default)s",
    )
    parser.add_argument(
        "--topic",
        help="Kafka topic to consume from. Mutually exclusive with --topic-pattern.",
    )
    parser.add_argument(
        "--topic-pattern",
        default=None,
        help="Regex pattern for topic subscription when --topic is not provided.",
    )
    parser.add_argument(
        "--offset-reset",
        default="latest",
        choices=["earliest", "latest"],
        help="Offset reset policy. Default: %(default)s",
    )
    parser.add_argument(
        "--poll-timeout",
        type=float,
        default=1.0,
        help="Consumer poll timeout in seconds. Default: %(default)s",
    )
    parser.add_argument(
        "--qos-depth",
        type=int,
        default=10,
        help="ROS 2 publisher QoS depth (KeepLast). Default: %(default)s",
    )

    parser.add_argument(
        "--ros-topic",
        default=None,
        help="Override output ROS topic (otherwise uses Kafka header 'ros_topic').",
    )
    parser.add_argument(
        "--ros-type",
        default=None,
        help="Override ROS type (otherwise uses Kafka header 'ros_type').",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level. Default: %(default)s",
    )
    return parser.parse_args(list(argv))


class CdrToRosRepublisher(Node):
    def __init__(self, qos_depth: int):
        super().__init__("kafka_cdr_to_ros")
        if qos_depth <= 0:
            raise ValueError("--qos-depth must be > 0")
        self._qos = QoSProfile(depth=qos_depth)
        self._msg_classes: Dict[str, Any] = {}

    def _get_message_class(self, ros_type: str):
        if ros_type not in self._msg_classes:
            self._msg_classes[ros_type] = get_message(ros_type)
        return self._msg_classes[ros_type]

    def publish_cdr(self, payload: bytes, ros_topic: str, ros_type: str) -> None:
        try:
            msg_cls = self._get_message_class(ros_type)
            ros_msg = deserialize_message(payload, msg_cls)
            # Create publisher on-the-fly (don't cache to avoid rclpy._publishers dict issues)
            pub = self.create_publisher(msg_cls, ros_topic, self._qos)
            pub.publish(ros_msg)
        except Exception as e:
            import traceback
            self.get_logger().error(
                f"Failed to deserialize/publish {ros_type} to {ros_topic}: {str(e)} (payload size: {len(payload)} bytes)\n{traceback.format_exc()}"
            )


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    rclpy.init(args=None)
    node = CdrToRosRepublisher(qos_depth=args.qos_depth)

    consumer = build_consumer(args)
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
            message = consumer.poll(args.poll_timeout)
            rclpy.spin_once(node, timeout_sec=0.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logging.error("Kafka error: %s", message.error())
                continue

            headers = parse_headers(message.headers())
            header_ros_type = headers.get("ros_type")

            ros_type = args.ros_type or header_ros_type

            if not ros_type:
                logging.warning(
                    "Missing ros_type in headers and no --ros-type provided. Skipping message."
                )
                continue

            # Derive ros_topic: CLI override > Kafka header > derived from Kafka topic with /kafka_decoded/ prefix
            if args.ros_topic:
                ros_topic = args.ros_topic
            elif "ros_topic" in headers and headers["ros_topic"]:
                ros_topic = headers["ros_topic"]
            else:
                # Derive from Kafka topic: "ros2.sensing.gnss.xyz" -> "/kafka_decoded/sensing/gnss/xyz"
                kafka_topic = message.topic()
                # Remove "ros2." prefix if present
                if kafka_topic.startswith("ros2."):
                    ros_topic = "/kafka_decoded/" + kafka_topic[5:].replace(".", "/")
                else:
                    ros_topic = "/kafka_decoded/" + kafka_topic.replace(".", "/")

            try:
                node.publish_cdr(message.value(), ros_topic, ros_type)
            except ModuleNotFoundError:
                logging.error("Unknown ROS 2 type '%s'; skipping.", ros_type)
            except Exception as exc:
                logging.error("Failed to deserialize/publish type '%s' to '%s': %s", ros_type, ros_topic, exc)

    finally:
        consumer.close()
        node.destroy_node()
        rclpy.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
