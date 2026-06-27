#!/usr/bin/env python3
# Synthetic NavSatFix publisher for one robot. Toggles across the P12 boundary
# (inside lat 48.05 / outside lat 47.9, lon 3.05) at RATE Hz with header.stamp=now,
# so the cloud zenoh_gis_query node emits a geofence crossing alert each toggle.
import sys
import time
import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix

INSIDE = (48.05, 3.05)
OUTSIDE = (47.9, 3.05)


def main() -> None:
    robot = sys.argv[1] if len(sys.argv) > 1 else "robot_1"
    rate = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0
    rclpy.init()
    node = Node("bench_pub")
    pub = node.create_publisher(NavSatFix, f"/{robot}/gps/fix", 10)
    period = 1.0 / rate
    state = False
    while rclpy.ok():
        state = not state
        lat, lon = INSIDE if state else OUTSIDE
        msg = NavSatFix()
        msg.header.frame_id = robot
        msg.header.stamp = node.get_clock().now().to_msg()
        msg.latitude = lat
        msg.longitude = lon
        pub.publish(msg)
        time.sleep(period)


if __name__ == "__main__":
    main()
