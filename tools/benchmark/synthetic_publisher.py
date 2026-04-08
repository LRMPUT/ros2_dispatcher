#!/usr/bin/env python3
"""Publish synthetic ROS 2 messages at a controlled rate for benchmarking.

Supports three message types representing small/medium/large payloads:
  - sensor_msgs/NavSatFix       (~small,  ~100 bytes CDR)
  - nav_msgs/Odometry           (~medium, ~700 bytes CDR)
  - sensor_msgs/PointCloud2     (~large,  configurable)

Usage:
  ros2 run benchmark synthetic_publisher.py --msg-type navsatfix --rate 100
  ros2 run benchmark synthetic_publisher.py --msg-type pointcloud2 --rate 10 --num-points 10000
"""

import argparse
import math
import struct
import time

import rclpy
from rclpy.node import Node

from nav_msgs.msg import Odometry
from sensor_msgs.msg import NavSatFix, NavSatStatus, PointCloud2, PointField
from geometry_msgs.msg import (
    PoseWithCovariance,
    TwistWithCovariance,
    Pose,
    Twist,
    Point,
    Quaternion,
    Vector3,
)
from std_msgs.msg import Header


MSG_TYPE_CHOICES = ["navsatfix", "odometry", "odometry_fullcov", "pointcloud2"]


def make_navsatfix(seq: int, stamp) -> NavSatFix:
    """Small message (~100 bytes CDR)."""
    msg = NavSatFix()
    msg.header = Header()
    msg.header.stamp = stamp
    msg.header.frame_id = "gps_link"
    msg.status.status = NavSatStatus.STATUS_FIX
    msg.status.service = NavSatStatus.SERVICE_GPS
    msg.latitude = 51.1079 + 0.0001 * math.sin(seq * 0.01)
    msg.longitude = 17.0385 + 0.0001 * math.cos(seq * 0.01)
    msg.altitude = 120.5 + 0.1 * math.sin(seq * 0.05)
    msg.position_covariance = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 4.0]
    msg.position_covariance_type = NavSatFix.COVARIANCE_TYPE_DIAGONAL_KNOWN
    return msg


def make_odometry(seq: int, stamp) -> Odometry:
    """Medium message (~700 bytes CDR) with full covariance matrices."""
    msg = Odometry()
    msg.header = Header()
    msg.header.stamp = stamp
    msg.header.frame_id = "odom"
    msg.child_frame_id = "base_link"

    t = seq * 0.01
    msg.pose.pose.position = Point(x=t * 0.5, y=math.sin(t) * 0.3, z=0.0)
    msg.pose.pose.orientation = Quaternion(x=0.0, y=0.0, z=math.sin(t / 2), w=math.cos(t / 2))
    msg.pose.covariance = [0.01 if i % 7 == 0 else 0.0 for i in range(36)]

    msg.twist.twist.linear = Vector3(x=0.5, y=0.0, z=0.0)
    msg.twist.twist.angular = Vector3(x=0.0, y=0.0, z=0.1 * math.sin(t))
    msg.twist.covariance = [0.001 if i % 7 == 0 else 0.0 for i in range(36)]

    return msg


def make_odometry_fullcov(seq: int, stamp) -> Odometry:
    """Medium message with realistic (non-zero) covariance matrices.

    All 72 covariance entries are non-zero floats with varying precision,
    producing longer JSON representation than zero-filled arrays.
    """
    msg = Odometry()
    msg.header = Header()
    msg.header.stamp = stamp
    msg.header.frame_id = "odom"
    msg.child_frame_id = "base_link"

    t = seq * 0.01
    msg.pose.pose.position = Point(x=t * 0.5, y=math.sin(t) * 0.3, z=0.05 * math.cos(t))
    msg.pose.pose.orientation = Quaternion(x=0.0, y=0.0, z=math.sin(t / 2), w=math.cos(t / 2))

    # Realistic symmetric positive-definite-ish covariance (all entries non-zero)
    msg.pose.covariance = [
        0.025 + 0.001 * math.sin(i * 0.7 + t) for i in range(36)
    ]
    # Make diagonal dominant
    for i in range(6):
        msg.pose.covariance[i * 7] = 0.1 + 0.01 * abs(math.sin(i + t))

    msg.twist.twist.linear = Vector3(x=0.5 + 0.01 * math.sin(t), y=0.02, z=0.001)
    msg.twist.twist.angular = Vector3(x=0.001, y=0.002, z=0.1 * math.sin(t))
    msg.twist.covariance = [
        0.005 + 0.0005 * math.cos(i * 0.3 + t) for i in range(36)
    ]
    for i in range(6):
        msg.twist.covariance[i * 7] = 0.02 + 0.002 * abs(math.cos(i + t))

    return msg


def make_pointcloud2(seq: int, stamp, num_points: int) -> PointCloud2:
    """Large message (configurable size via num_points).

    Each point has x, y, z, intensity (4 x float32 = 16 bytes per point).
    10000 points ~ 160 KB payload.
    """
    msg = PointCloud2()
    msg.header = Header()
    msg.header.stamp = stamp
    msg.header.frame_id = "velodyne"
    msg.height = 1
    msg.width = num_points
    msg.is_dense = True
    msg.is_bigendian = False
    msg.point_step = 16  # 4 floats x 4 bytes
    msg.row_step = msg.point_step * num_points
    msg.fields = [
        PointField(name="x", offset=0, datatype=PointField.FLOAT32, count=1),
        PointField(name="y", offset=4, datatype=PointField.FLOAT32, count=1),
        PointField(name="z", offset=8, datatype=PointField.FLOAT32, count=1),
        PointField(name="intensity", offset=12, datatype=PointField.FLOAT32, count=1),
    ]

    # Generate deterministic point data with slight variation per seq
    t = seq * 0.001
    data = bytearray(msg.row_step)
    for i in range(num_points):
        angle = 2.0 * math.pi * i / num_points + t
        r = 10.0 + 0.1 * math.sin(i * 0.1)
        x = r * math.cos(angle)
        y = r * math.sin(angle)
        z = 0.5 * math.sin(i * 0.05 + t)
        intensity = 50.0 + 50.0 * math.sin(i * 0.02)
        struct.pack_into("<ffff", data, i * 16, x, y, z, intensity)

    msg.data = bytes(data)
    return msg


class SyntheticPublisher(Node):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__("synthetic_publisher")
        self._msg_type = args.msg_type
        self._rate_hz = args.rate
        self._num_points = args.num_points
        self._count = args.count
        self._seq = 0
        self._sent = 0

        topic = args.topic or f"/benchmark/{self._msg_type}"

        if self._msg_type == "navsatfix":
            self._pub = self.create_publisher(NavSatFix, topic, 10)
            self._make = lambda seq, stamp: make_navsatfix(seq, stamp)
        elif self._msg_type == "odometry":
            self._pub = self.create_publisher(Odometry, topic, 10)
            self._make = lambda seq, stamp: make_odometry(seq, stamp)
        elif self._msg_type == "odometry_fullcov":
            self._pub = self.create_publisher(Odometry, topic, 10)
            self._make = lambda seq, stamp: make_odometry_fullcov(seq, stamp)
        elif self._msg_type == "pointcloud2":
            self._pub = self.create_publisher(PointCloud2, topic, 10)
            self._make = lambda seq, stamp: make_pointcloud2(seq, stamp, self._num_points)

        self.get_logger().info(
            f"Publishing {self._msg_type} on {topic} at {self._rate_hz} Hz"
        )
        self._timer = self.create_timer(1.0 / self._rate_hz, self._tick)

    def _tick(self) -> None:
        if self._count and self._sent >= self._count:
            self.get_logger().info(f"Published {self._sent} messages, stopping.")
            raise SystemExit(0)

        self._seq += 1
        stamp = self.get_clock().now().to_msg()
        msg = self._make(self._seq, stamp)
        self._pub.publish(msg)
        self._sent += 1

        if self._sent % (self._rate_hz * 10) == 0:
            self.get_logger().info(f"Published {self._sent} messages")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish synthetic ROS 2 messages for CDR/JSON benchmarking."
    )
    parser.add_argument(
        "--msg-type",
        choices=MSG_TYPE_CHOICES,
        required=True,
        help="Message type to publish.",
    )
    parser.add_argument("--topic", default="", help="Override topic name.")
    parser.add_argument("--rate", type=float, default=10.0, help="Publish rate in Hz.")
    parser.add_argument(
        "--num-points",
        type=int,
        default=10000,
        help="Number of points for PointCloud2 (ignored for other types).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of messages to publish (0 = indefinite).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rclpy.init()
    node = SyntheticPublisher(args)
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
