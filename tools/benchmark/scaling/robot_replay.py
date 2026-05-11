#!/usr/bin/env python3
"""Replay a NavSatFix bag with per-robot origin shift and live re-stamping.

Container entrypoint: derive ROBOT_ID from the hostname suffix, open the
bag at $BAG_PATH, loop forever, publish at $RATE_HZ. The published
message's header.stamp encodes time.time_ns() at publish time, which
the e2e consumer subtracts to compute latency.
"""
from __future__ import annotations

import argparse
import os
import socket
import time

import rosbag2_py
import rclpy
from rclpy.node import Node
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
from nav_msgs.msg import Odometry
from sensor_msgs.msg import LaserScan, NavSatFix, PointCloud2

LAT_OFFSET_DEG_PER_ID = 0.0001  # ~11 m at the equator; close enough
LON_OFFSET_DEG_PER_ID = 0.0001


# Maps the short MSG_TYPE env value (set by orchestrator) to:
#   (ros_msg_type_string, python_class, output_topic_suffix, default_rate_hz)
# default_rate_hz is the bag's native rate, used in multi-topic fleet mode.
TYPE_CONFIG = {
    "navsatfix":   ("sensor_msgs/msg/NavSatFix",   NavSatFix,   "gnss",   10.0),
    "odometry":    ("nav_msgs/msg/Odometry",       Odometry,    "odom",   20.0),
    "laserscan":   ("sensor_msgs/msg/LaserScan",   LaserScan,   "scan",   50.0),
    "pointcloud2": ("sensor_msgs/msg/PointCloud2", PointCloud2, "points", 12.5),
}

# Multi-topic preset: every simulated robot publishes all 4 streams at their
# native bag rates.  Selected when MSG_TYPE=multi (or --msg-type=multi).
MULTI_TYPES = ("navsatfix", "odometry", "laserscan", "pointcloud2")


def shift_navsatfix(msg: NavSatFix, robot_id: int) -> None:
    """Apply a deterministic per-robot offset to lat/lon. In-place."""
    msg.latitude += LAT_OFFSET_DEG_PER_ID * robot_id
    msg.longitude += LON_OFFSET_DEG_PER_ID * robot_id


def shift_message(msg, robot_id: int, msg_type: str) -> None:
    """Apply per-robot offset where meaningful for the chosen type."""
    if msg_type == "navsatfix":
        shift_navsatfix(msg, robot_id)
    # Odometry / PointCloud2: no per-robot shift (the topic name already
    # distinguishes robots; spatial offset of pose or cloud is not load-relevant).


def restamp_ns(msg, t_ns: int) -> None:
    """Set header.stamp to a wall-clock ns timestamp. In-place.

    Works for any message with a std_msgs/Header at `.header`.
    """
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
        if self._reader is None:
            raise RuntimeError("BagLooper is permanently broken after failed re-open")
        if not self._reader.has_next():
            self._open_reader()
        _topic, data, _t = self._reader.read_next()
        return deserialize_message(data, self._msg_class)


def derive_robot_id_from_hostname() -> int:
    """Compose --scale assigns hostnames like '<project>-robot-3'. Take the trailing int."""
    host = socket.gethostname()
    parts = host.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return int(parts[1])
    return 0


class RobotReplay(Node):
    """Single-stream replay node: one robot publishes one message type."""

    def __init__(self, robot_id: int, bag_path: str, rate_hz: float, msg_type: str = "navsatfix") -> None:
        super().__init__(f"robot_replay_{robot_id}")
        if msg_type not in TYPE_CONFIG:
            raise ValueError(f"Unknown msg_type {msg_type!r}; expected one of {list(TYPE_CONFIG)}")
        type_str, type_class, suffix, _native_rate = TYPE_CONFIG[msg_type]
        self._robot_id = robot_id
        self._rate_hz = rate_hz
        self._msg_type = msg_type
        self._looper = BagLooper(bag_path, topic_type=type_str)
        self._pub = self.create_publisher(type_class, f"/robot_{robot_id}/{suffix}", 10)
        self._timer = self.create_timer(1.0 / rate_hz, self._tick)
        self.get_logger().info(
            f"Replaying bag={bag_path} as robot_id={robot_id} at {rate_hz} Hz "
            f"(type={msg_type}, topic=/robot_{robot_id}/{suffix})"
        )

    def _tick(self) -> None:
        msg = next(self._looper)
        shift_message(msg, self._robot_id, self._msg_type)
        restamp_ns(msg, time.time_ns())
        self._pub.publish(msg)


class MultiTopicRobotReplay(Node):
    """Multi-stream replay node: one robot publishes ALL configured message
    types simultaneously, each at its bag-native rate. Used by the multi-topic
    scalability benchmark."""

    def __init__(self, robot_id: int, bag_path: str, types=MULTI_TYPES) -> None:
        super().__init__(f"robot_multi_{robot_id}")
        self._robot_id = robot_id
        self._streams = []
        for short in types:
            if short not in TYPE_CONFIG:
                continue
            type_str, type_class, suffix, native_rate = TYPE_CONFIG[short]
            looper = BagLooper(bag_path, topic_type=type_str)
            pub = self.create_publisher(type_class, f"/robot_{robot_id}/{suffix}", 10)
            # Capture default-arg pattern so each timer binds to its own stream.
            def make_tick(_looper=looper, _pub=pub, _short=short):
                def tick():
                    try:
                        msg = next(_looper)
                    except RuntimeError:
                        return
                    shift_message(msg, self._robot_id, _short)
                    restamp_ns(msg, time.time_ns())
                    _pub.publish(msg)
                return tick
            timer = self.create_timer(1.0 / native_rate, make_tick())
            self._streams.append((short, suffix, native_rate, timer))
            self.get_logger().info(
                f"robot_id={robot_id} stream type={short} topic=/robot_{robot_id}/{suffix} rate={native_rate}Hz"
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--robot-id",
        type=int,
        default=int(os.environ.get("ROBOT_ID", "-1")),
        help="Single-robot mode: this container's robot_id. If <0, falls back to hostname or NUM_ROBOTS fleet mode.",
    )
    parser.add_argument(
        "--num-robots",
        type=int,
        default=int(os.environ.get("NUM_ROBOTS", "0")),
        help="Fleet mode: spin N RobotReplay nodes (robot_id=1..N) in a single process. If >0, takes precedence over --robot-id.",
    )
    parser.add_argument(
        "--bag-path",
        default=os.environ.get("BAG_PATH"),
        help="Path to a rosbag2 directory containing NavSatFix messages",
    )
    parser.add_argument(
        "--rate-hz",
        type=float,
        default=float(os.environ.get("RATE_HZ", "10")),
    )
    parser.add_argument(
        "--msg-type",
        # 'multi' = multi-topic mode (4 streams per robot at bag-native rates)
        choices=sorted(TYPE_CONFIG.keys()) + ["multi"],
        default=os.environ.get("MSG_TYPE", "navsatfix"),
        help="Which message type to replay. Use 'multi' for the multi-topic "
             "fleet mode (navsatfix+odometry+laserscan+pointcloud2 in parallel).",
    )
    args = parser.parse_args()

    if not args.bag_path:
        raise SystemExit("BAG_PATH (env or --bag-path) is required")

    multi = (args.msg_type == "multi")

    rclpy.init()
    nodes = []
    try:
        if args.num_robots > 0:
            # Fleet mode: spin N robots in this process via MultiThreadedExecutor.
            from rclpy.executors import MultiThreadedExecutor
            # Multi-topic robots have 4 timers each, so more threads help.
            nthreads = min(max(args.num_robots, 4) * (4 if multi else 1), 32)
            executor = MultiThreadedExecutor(num_threads=nthreads)
            for robot_id in range(1, args.num_robots + 1):
                if multi:
                    node = MultiTopicRobotReplay(robot_id, args.bag_path)
                else:
                    node = RobotReplay(robot_id, args.bag_path, args.rate_hz, args.msg_type)
                nodes.append(node)
                executor.add_node(node)
            print(f"[robot_replay] Fleet mode: {args.num_robots} robots "
                  f"type={args.msg_type} threads={nthreads}", flush=True)
            executor.spin()
        else:
            robot_id = args.robot_id if args.robot_id >= 0 else derive_robot_id_from_hostname()
            if multi:
                node = MultiTopicRobotReplay(robot_id, args.bag_path)
            else:
                node = RobotReplay(robot_id, args.bag_path, args.rate_hz, args.msg_type)
            nodes.append(node)
            # Use a multi-threaded executor for the multi-topic case so the
            # 4 timers can fire on separate threads.
            if multi:
                from rclpy.executors import MultiThreadedExecutor
                ex = MultiThreadedExecutor(num_threads=4)
                ex.add_node(node)
                ex.spin()
            else:
                rclpy.spin(node)
    finally:
        for node in nodes:
            node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
