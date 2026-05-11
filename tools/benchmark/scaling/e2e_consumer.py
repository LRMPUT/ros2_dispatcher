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
