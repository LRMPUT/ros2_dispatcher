"""Unit tests for robot_replay helpers."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sensor_msgs.msg import NavSatFix

from robot_replay import shift_navsatfix, restamp_ns


def _make_fix(lat: float, lon: float, alt: float) -> NavSatFix:
    msg = NavSatFix()
    msg.latitude = lat
    msg.longitude = lon
    msg.altitude = alt
    return msg


def test_shift_navsatfix_robot_0_is_identity():
    msg = _make_fix(51.1, 17.0, 120.0)
    shift_navsatfix(msg, robot_id=0)
    assert msg.latitude == 51.1
    assert msg.longitude == 17.0
    assert msg.altitude == 120.0


def test_shift_navsatfix_offset_scales_with_robot_id():
    msg = _make_fix(51.1, 17.0, 120.0)
    shift_navsatfix(msg, robot_id=5)
    assert abs(msg.latitude - (51.1 + 0.0001 * 5)) < 1e-12
    assert abs(msg.longitude - (17.0 + 0.0001 * 5)) < 1e-12
    # altitude unchanged
    assert msg.altitude == 120.0


def test_restamp_ns_writes_sec_and_nanosec_correctly():
    msg = _make_fix(0.0, 0.0, 0.0)
    restamp_ns(msg, t_ns=1_234_567_890_123_456_789)
    assert msg.header.stamp.sec == 1_234_567_890
    assert msg.header.stamp.nanosec == 123_456_789


def test_restamp_ns_zero():
    msg = _make_fix(0.0, 0.0, 0.0)
    restamp_ns(msg, t_ns=0)
    assert msg.header.stamp.sec == 0
    assert msg.header.stamp.nanosec == 0


TEST_BAG = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "test_data",
    "test_bag",
)


@pytest.mark.skipif(not os.path.exists(TEST_BAG), reason="test bag not generated")
def test_bag_looper_yields_navsatfix_messages():
    from robot_replay import BagLooper

    looper = BagLooper(TEST_BAG, topic_type="sensor_msgs/msg/NavSatFix")
    msg = next(looper)
    assert hasattr(msg, "latitude")
    assert hasattr(msg, "longitude")


@pytest.mark.skipif(not os.path.exists(TEST_BAG), reason="test bag not generated")
def test_bag_looper_loops_past_end():
    from robot_replay import BagLooper

    looper = BagLooper(TEST_BAG, topic_type="sensor_msgs/msg/NavSatFix")
    # The fixture bag has 50 messages — iterate 75 and confirm no StopIteration
    msgs = []
    for _ in range(75):
        msgs.append(next(looper))
    assert len(msgs) == 75
    # Looped → first and 51st messages should have identical latitude
    assert msgs[0].latitude == msgs[50].latitude
