#!/usr/bin/env python3
import argparse
import json
import time

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


class LatencyPublisher(Node):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__("latency_publisher")
        self._topic = args.topic
        self._rate_hz = args.rate
        self._payload_bytes = args.payload_bytes
        self._count = args.count
        self._log_file = open(args.log_file, "a", buffering=1) if args.log_file else None
        self._publisher = self.create_publisher(String, self._topic, 10)
        self._msg_id = 0
        self._sent = 0
        self._payload = "x" * self._payload_bytes
        self._timer = self.create_timer(1.0 / self._rate_hz, self._publish_once)

    def _publish_once(self) -> None:
        if self._count and self._sent >= self._count:
            self.get_logger().info("Completed publishing requested count.")
            rclpy.shutdown()
            return

        self._msg_id += 1
        t0_ns = time.time_ns()
        payload = {
            "msg_id": self._msg_id,
            "t0_ns": t0_ns,
            "payload": self._payload,
        }
        msg = String()
        msg.data = json.dumps(payload, separators=(",", ":"))
        self._publisher.publish(msg)
        self._sent += 1

        if self._log_file:
            record = {
                "msg_id": self._msg_id,
                "t0_ns": t0_ns,
                "topic": self._topic,
                "payload_bytes": len(msg.data.encode("utf-8")),
            }
            self._log_file.write(json.dumps(record) + "\n")

    def close(self) -> None:
        if self._log_file:
            self._log_file.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish timestamped latency test messages.")
    parser.add_argument("--topic", default="/latency_test")
    parser.add_argument("--rate", type=float, default=10.0)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--count", type=int, default=0, help="0 means publish indefinitely.")
    parser.add_argument("--log-file", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rclpy.init()
    node = LatencyPublisher(args)
    try:
        rclpy.spin(node)
    finally:
        node.close()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
