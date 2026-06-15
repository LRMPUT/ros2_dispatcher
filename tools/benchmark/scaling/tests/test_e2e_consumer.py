"""Unit tests for e2e_consumer helpers."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rclpy.serialization import serialize_message
from sensor_msgs.msg import NavSatFix

from e2e_consumer import deserialize_navsatfix, extract_t0_ns, format_row, robot_id_from_kafka_topic, robot_id_from_mqtt_topic


def _make_fix_with_stamp(t_ns: int) -> bytes:
    msg = NavSatFix()
    msg.header.stamp.sec = t_ns // 1_000_000_000
    msg.header.stamp.nanosec = t_ns % 1_000_000_000
    msg.latitude = 51.1
    msg.longitude = 17.0
    return serialize_message(msg)


def test_deserialize_navsatfix_roundtrip():
    payload = _make_fix_with_stamp(1_234_567_890_123_456_789)
    msg = deserialize_navsatfix(payload)
    assert abs(msg.latitude - 51.1) < 1e-12


def test_extract_t0_ns_combines_sec_and_nanosec():
    payload = _make_fix_with_stamp(1_234_567_890_123_456_789)
    msg = deserialize_navsatfix(payload)
    assert extract_t0_ns(msg) == 1_234_567_890_123_456_789


def test_extract_t0_ns_zero():
    payload = _make_fix_with_stamp(0)
    msg = deserialize_navsatfix(payload)
    assert extract_t0_ns(msg) == 0


def test_robot_id_from_kafka_topic():
    assert robot_id_from_kafka_topic("ros2.robot_7.gnss") == 7
    assert robot_id_from_kafka_topic("ros2.robot_50.gnss") == 50


def test_robot_id_from_mqtt_topic():
    assert robot_id_from_mqtt_topic("ros2/robot_7/gnss") == 7
    assert robot_id_from_mqtt_topic("ros2/robot_50/gnss") == 50


def test_format_row_jsonl():
    row = format_row(robot_id=3, topic="ros2.robot_3.gnss", t0_ns=100, t1_ns=250, bytes_=104)
    parsed = json.loads(row)
    assert parsed == {
        "robot_id": 3,
        "topic": "ros2.robot_3.gnss",
        "t0_ns": 100,
        "t1_ns": 250,
        "latency_ns": 150,
        "bytes": 104,
    }
