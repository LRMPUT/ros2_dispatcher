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
from sensor_msgs.msg import NavSatFix


_KAFKA_ROBOT_RE = re.compile(r"^ros2\.robot_(\d+)\.gnss$")
_MQTT_ROBOT_RE = re.compile(r"^ros2/robot_(\d+)/gnss$")


def deserialize_navsatfix(data: bytes) -> NavSatFix:
    return deserialize_message(data, NavSatFix)


def extract_t0_ns(msg: NavSatFix) -> int:
    return msg.header.stamp.sec * 1_000_000_000 + msg.header.stamp.nanosec


def robot_id_from_kafka_topic(topic: str) -> int:
    m = _KAFKA_ROBOT_RE.match(topic)
    if not m:
        raise ValueError(f"Unrecognised Kafka topic: {topic}")
    return int(m.group(1))


def robot_id_from_mqtt_topic(topic: str) -> int:
    m = _MQTT_ROBOT_RE.match(topic)
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


def _consume_kafka(
    bootstrap: str,
    pattern: str,
    warmup_s: float,
    duration_s: float,
    out: IO[str],
) -> None:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"e2e-consumer-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            # Default is 5 min; refresh fast so we pick up new robot topics as
            # they're created at experiment start.
            "topic.metadata.refresh.interval.ms": 1000,
        }
    )
    consumer.subscribe([pattern])  # confluent-kafka supports regex with `^` prefix

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
            continue  # skip warmup window
        try:
            nav = deserialize_navsatfix(msg.value())
            robot_id = robot_id_from_kafka_topic(msg.topic())
        except (ValueError, Exception):  # noqa: BLE001
            continue
        t0_ns = extract_t0_ns(nav)
        out.write(format_row(robot_id, msg.topic(), t0_ns, t1_ns, len(msg.value())) + "\n")
    consumer.close()


def _consume_mqtt(
    host: str,
    port: int,
    pattern: str,
    warmup_s: float,
    duration_s: float,
    out: IO[str],
) -> None:
    import paho.mqtt.client as mqtt

    t_start = time.monotonic()

    def _on_message(_client, _userdata, msg):
        elapsed = time.monotonic() - t_start
        if elapsed < warmup_s:
            return
        t1_ns = _now_ns()
        try:
            nav = deserialize_navsatfix(msg.payload)
            robot_id = robot_id_from_mqtt_topic(msg.topic)
        except (ValueError, Exception):  # noqa: BLE001
            return
        t0_ns = extract_t0_ns(nav)
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
    parser.add_argument("--kafka-pattern", default="^ros2\\.robot_.*\\.gnss$")
    # MQTT wildcards: `+` matches one whole topic level, so we cannot put it
    # inside `robot_X`. Use `+` for the whole second level and rely on
    # robot_id_from_mqtt_topic regex to filter received topics.
    parser.add_argument("--mqtt-pattern", default="ros2/+/gnss")
    parser.add_argument("--warmup", type=float, default=10.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--output", required=True, help="JSONL output path")
    args = parser.parse_args()

    # Graceful shutdown so files flush
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    with open(args.output, "w", buffering=1) as out:
        if args.broker == "kafka":
            _consume_kafka(args.bootstrap, args.kafka_pattern, args.warmup, args.duration, out)
        else:
            _consume_mqtt(args.mqtt_host, args.mqtt_port, args.mqtt_pattern, args.warmup, args.duration, out)


if __name__ == "__main__":
    main()
