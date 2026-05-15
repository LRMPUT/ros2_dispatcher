#!/usr/bin/env python3
"""Test-only mock publisher for /leader/gps/fix and /leader/localisation/filtered_odom.

Used by the GIS4IoRT-ksqlDB end-to-end test to drive kafka_sink + ksqlDB adapter
without needing a real robot or a rosbag.
"""

import math

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix
from nav_msgs.msg import Odometry


class MockLeaderPublisher(Node):
    def __init__(self):
        super().__init__('mock_leader_publisher')
        self.gps_pub = self.create_publisher(NavSatFix, '/leader/gps/fix', 10)
        self.odom_pub = self.create_publisher(
            Odometry, '/leader/localisation/filtered_odom', 10)
        self._t = 0.0
        self.create_timer(0.5, self._tick)

    def _tick(self):
        self._t += 0.5
        now = self.get_clock().now().to_msg()

        gps = NavSatFix()
        gps.header.stamp = now
        gps.header.frame_id = 'gps'
        # Slowly drift lat/lon, valid finite values
        gps.latitude = 50.0 + 0.0001 * self._t
        gps.longitude = 17.0 + 0.0001 * self._t
        gps.altitude = 100.0 + math.sin(self._t)
        self.gps_pub.publish(gps)

        odom = Odometry()
        odom.header.stamp = now
        odom.header.frame_id = 'odom'
        odom.child_frame_id = 'base_link'
        odom.pose.pose.position.x = 10.0 + self._t
        odom.pose.pose.position.y = 5.0 + 0.5 * self._t
        odom.pose.pose.position.z = 0.0
        odom.pose.pose.orientation.w = 1.0
        self.odom_pub.publish(odom)


def main():
    rclpy.init()
    node = MockLeaderPublisher()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
