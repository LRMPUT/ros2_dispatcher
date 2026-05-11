#!/usr/bin/env python3
"""Replay a NavSatFix bag with per-robot origin shift and live re-stamping.

Container entrypoint: derive ROBOT_ID from the hostname suffix, open the
bag at $BAG_PATH, loop forever, publish at $RATE_HZ. The published
message's header.stamp encodes time.time_ns() at publish time, which
the e2e consumer subtracts to compute latency.
"""
from __future__ import annotations

import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
from sensor_msgs.msg import NavSatFix

LAT_OFFSET_DEG_PER_ID = 0.0001  # ~11 m at the equator; close enough
LON_OFFSET_DEG_PER_ID = 0.0001


def shift_navsatfix(msg: NavSatFix, robot_id: int) -> None:
    """Apply a deterministic per-robot offset to lat/lon. In-place."""
    msg.latitude += LAT_OFFSET_DEG_PER_ID * robot_id
    msg.longitude += LON_OFFSET_DEG_PER_ID * robot_id


def restamp_ns(msg: NavSatFix, t_ns: int) -> None:
    """Set header.stamp to a wall-clock ns timestamp. In-place."""
    msg.header.stamp.sec = t_ns // 1_000_000_000
    msg.header.stamp.nanosec = t_ns % 1_000_000_000


class BagLooper:
    """Iterate messages from a rosbag2 bag, looping forever.

    Filters to a single topic type. Reuses the underlying reader; on EOF,
    closes and re-opens it.
    """

    def __init__(self, bag_path: str, topic_type: str) -> None:
        self._bag_path = bag_path
        self._topic_type_str = topic_type
        self._msg_class = get_message(topic_type)
        self._reader = None
        self._open_reader()

    def _open_reader(self) -> None:
        if self._reader is not None:
            del self._reader
        try:
            self._reader = rosbag2_py.SequentialReader()
            storage_options = rosbag2_py.StorageOptions(uri=self._bag_path, storage_id="sqlite3")
            converter_options = rosbag2_py.ConverterOptions("", "")
            self._reader.open(storage_options, converter_options)
            # Filter to topics matching our type (the bag may contain other types)
            topics_meta = self._reader.get_all_topics_and_types()
            wanted = [m.name for m in topics_meta if m.type == self._topic_type_str]
            if not wanted:
                raise RuntimeError(
                    f"No topic of type {self._topic_type_str} found in {self._bag_path}"
                )
            self._reader.set_filter(rosbag2_py.StorageFilter(topics=wanted))
        except Exception as e:
            # Mark iterator as permanently broken. Re-opening failed (bag deleted,
            # corrupted, or permission denied mid-loop). Next __next__ will fail
            # with a clear error, not an AttributeError on a half-open reader.
            self._reader = None
            raise RuntimeError(f"BagLooper failed to re-open {self._bag_path}: {e}") from e

    def __iter__(self):
        return self

    def __next__(self):
        if not self._reader.has_next():
            self._open_reader()
        _topic, data, _t = self._reader.read_next()
        return deserialize_message(data, self._msg_class)
