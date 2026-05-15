#!/usr/bin/env python3
"""Consume Kafka or MQTT records carrying CDR-serialized NavSatFix.

Subscribes to all `robot_*` topics, deserializes CDR, extracts the
publish-time t0_ns from header.stamp, captures t1_ns at receive, and
writes one JSONL row per delivered message.
"""
from __future__ import annotations

import argparse
import json
import re
import signal
import time
from typing import IO

from rclpy.serialization import deserialize_message
from nav_msgs.msg import Odometry
from sensor_msgs.msg import LaserScan, NavSatFix, PointCloud2


TYPE_CONFIG = {
    "navsatfix":   (NavSatFix,   "gnss"),
    "odometry":    (Odometry,    "odom"),
    "laserscan":   (LaserScan,   "scan"),
    "pointcloud2": (PointCloud2, "points"),
}

# suffix → (short_type_name, python_class)
SUFFIX_TO_TYPE = {suffix: (short, cls) for short, (cls, suffix) in TYPE_CONFIG.items()}


def _build_kafka_re(suffix: str):
    return re.compile(rf"^ros2\.robot_(\d+)\.{re.escape(suffix)}$")


def _build_mqtt_re(suffix: str):
    return re.compile(rf"^ros2/robot_(\d+)/{re.escape(suffix)}$")


# Backwards-compatible NavSatFix-specific helpers (kept so the unit tests work).
_KAFKA_ROBOT_RE = _build_kafka_re("gnss")
_MQTT_ROBOT_RE = _build_mqtt_re("gnss")


def deserialize_navsatfix(data: bytes) -> NavSatFix:
    return deserialize_message(data, NavSatFix)


def extract_t0_ns(msg) -> int:
    return msg.header.stamp.sec * 1_000_000_000 + msg.header.stamp.nanosec


def robot_id_from_kafka_topic(topic: str, regex=None) -> int:
    m = (regex or _KAFKA_ROBOT_RE).match(topic)
    if not m:
        raise ValueError(f"Unrecognised Kafka topic: {topic}")
    return int(m.group(1))


def robot_id_from_mqtt_topic(topic: str, regex=None) -> int:
    m = (regex or _MQTT_ROBOT_RE).match(topic)
    if not m:
        raise ValueError(f"Unrecognised MQTT topic: {topic}")
    return int(m.group(1))


def format_row(robot_id: int, topic: str, t0_ns: int, t1_ns: int, bytes_: int) -> str:
    return json.dumps(
        {
            "robot_id": robot_id,
            "topic": topic,
            "t0_ns": t0_ns,
            "t1_ns": t1_ns,
            "latency_ns": t1_ns - t0_ns,
            "bytes": bytes_,
        }
    )


def _now_ns() -> int:
    return time.time_ns()


def _kafka_topic_to_robot_and_class(topic: str):
    """Return (robot_id, python_class) for a `ros2.robot_<i>.<suffix>` topic.
    Used for multi-topic dispatch.  Returns None if topic doesn't match."""
    m = re.match(r"^ros2\.robot_(\d+)\.(gnss|odom|scan|points)$", topic)
    if not m:
        return None
    suffix = m.group(2)
    short, cls = SUFFIX_TO_TYPE[suffix]
    return int(m.group(1)), cls


def _mqtt_topic_to_robot_and_class(topic: str):
    m = re.match(r"^ros2/robot_(\d+)/(gnss|odom|scan|points)$", topic)
    if not m:
        return None
    suffix = m.group(2)
    short, cls = SUFFIX_TO_TYPE[suffix]
    return int(m.group(1)), cls


def _consume_kafka(
    bootstrap: str,
    pattern: str,
    warmup_s: float,
    duration_s: float,
    out: IO[str],
    msg_class=None,
    topic_re=None,
    multi: bool = False,
) -> None:
    """Consume from Kafka. In single-type mode pass msg_class+topic_re; in
    multi-topic mode pass multi=True and the function dispatches by topic
    suffix."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"e2e-consumer-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "topic.metadata.refresh.interval.ms": 1000,
        }
    )
    consumer.subscribe([pattern])

    t_start = time.monotonic()
    while True:
        elapsed = time.monotonic() - t_start
        if elapsed >= warmup_s + duration_s:
            break
        msg = consumer.poll(timeout=0.5)
        if msg is None or msg.error():
            continue
        t1_ns = _now_ns()
        if elapsed < warmup_s:
            continue
        try:
            if multi:
                lookup = _kafka_topic_to_robot_and_class(msg.topic())
                if lookup is None:
                    continue
                robot_id, cls = lookup
                decoded = deserialize_message(msg.value(), cls)
            else:
                decoded = deserialize_message(msg.value(), msg_class)
                robot_id = robot_id_from_kafka_topic(msg.topic(), regex=topic_re)
        except (ValueError, Exception):  # noqa: BLE001
            continue
        t0_ns = extract_t0_ns(decoded)
        out.write(format_row(robot_id, msg.topic(), t0_ns, t1_ns, len(msg.value())) + "\n")
    consumer.close()


def _consume_mqtt(
    host: str,
    port: int,
    pattern: str,
    warmup_s: float,
    duration_s: float,
    out: IO[str],
    msg_class=None,
    topic_re=None,
    multi: bool = False,
) -> None:
    import paho.mqtt.client as mqtt

    t_start = time.monotonic()

    def _on_message(_client, _userdata, msg):
        elapsed = time.monotonic() - t_start
        if elapsed < warmup_s:
            return
        t1_ns = _now_ns()
        try:
            if multi:
                lookup = _mqtt_topic_to_robot_and_class(msg.topic)
                if lookup is None:
                    return
                robot_id, cls = lookup
                decoded = deserialize_message(msg.payload, cls)
            else:
                decoded = deserialize_message(msg.payload, msg_class)
                robot_id = robot_id_from_mqtt_topic(msg.topic, regex=topic_re)
        except (ValueError, Exception):  # noqa: BLE001
            return
        t0_ns = extract_t0_ns(decoded)
        out.write(format_row(robot_id, msg.topic, t0_ns, t1_ns, len(msg.payload)) + "\n")

    client = mqtt.Client()
    client.on_message = _on_message
    client.connect(host, port, keepalive=60)
    client.subscribe(pattern)
    client.loop_start()
    while time.monotonic() - t_start < warmup_s + duration_s:
        time.sleep(0.5)
    client.loop_stop()
    client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", choices=["kafka", "mqtt"], required=True)
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--msg-type",
                        choices=sorted(TYPE_CONFIG.keys()) + ["multi"], default="navsatfix",
                        help="Message type to deserialize. 'multi' dispatches by topic suffix.")
    parser.add_argument("--kafka-pattern", default=None,
                        help="Override Kafka regex (default derived from --msg-type).")
    parser.add_argument("--mqtt-pattern", default=None,
                        help="Override MQTT wildcard (default derived from --msg-type).")
    parser.add_argument("--warmup", type=float, default=10.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--output", required=True, help="JSONL output path")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    multi = (args.msg_type == "multi")

    if multi:
        # Subscribe to all 4 suffixes at once; dispatch by topic.
        kafka_pattern = args.kafka_pattern or r"^ros2\.robot_[0-9]+\.(gnss|odom|scan|points)$"
        # MQTT wildcards: + matches one whole topic level. We need separate
        # subscriptions per suffix because MQTT cannot OR multiple suffixes.
        # The consumer calls subscribe() once with a list of (topic, qos) tuples.
        mqtt_pattern = args.mqtt_pattern or "ros2/+/+"
        with open(args.output, "w", buffering=1) as out:
            if args.broker == "kafka":
                _consume_kafka(args.bootstrap, kafka_pattern, args.warmup, args.duration, out,
                               multi=True)
            else:
                _consume_mqtt(args.mqtt_host, args.mqtt_port, mqtt_pattern, args.warmup, args.duration,
                              out, multi=True)
    else:
        msg_class, suffix = TYPE_CONFIG[args.msg_type]
        kafka_pattern = args.kafka_pattern or rf"^ros2\.robot_.*\.{suffix}$"
        mqtt_pattern = args.mqtt_pattern or f"ros2/+/{suffix}"
        kafka_re = _build_kafka_re(suffix)
        mqtt_re = _build_mqtt_re(suffix)

        with open(args.output, "w", buffering=1) as out:
            if args.broker == "kafka":
                _consume_kafka(args.bootstrap, kafka_pattern, args.warmup, args.duration, out,
                               msg_class=msg_class, topic_re=kafka_re)
            else:
                _consume_mqtt(args.mqtt_host, args.mqtt_port, mqtt_pattern, args.warmup, args.duration,
                              out, msg_class=msg_class, topic_re=mqtt_re)


if __name__ == "__main__":
    main()
