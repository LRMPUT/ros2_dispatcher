#!/usr/bin/env python3
import argparse
import json
import time

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


class LatencyConsumer(Node):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__("latency_consumer")
        self._topic = args.topic
        self._count = args.count
        self._received = 0
        self._log_file = open(args.log_file, "a", buffering=1) if args.log_file else None
        self._subscription = self.create_subscription(String, self._topic, self._on_msg, 10)
        self._subscription  # keep reference

    def _on_msg(self, msg: String) -> None:
        t1_ns = time.time_ns()
        try:
            payload = json.loads(msg.data)
        except json.JSONDecodeError:
            self.get_logger().warn("Failed to decode JSON payload.")
            return

        msg_id = payload.get("msg_id")
        t0_ns = payload.get("t0_ns")
        payload_bytes = len(msg.data.encode("utf-8"))
        if msg_id is None or t0_ns is None:
            self.get_logger().warn("Missing msg_id or t0_ns in payload.")
            return

        latency_ns = t1_ns - int(t0_ns)
        self._received += 1

        if self._log_file:
            record = {
                "msg_id": msg_id,
                "t0_ns": int(t0_ns),
                "t1_ns": t1_ns,
                "latency_ns": latency_ns,
                "payload_bytes": payload_bytes,
                "topic": self._topic,
            }
            self._log_file.write(json.dumps(record) + "\n")

        if self._count and self._received >= self._count:
            self.get_logger().info("Received requested count; shutting down.")
            rclpy.shutdown()

    def close(self) -> None:
        if self._log_file:
            self._log_file.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume latency test messages.")
    parser.add_argument("--topic", default="/kafka_decoded/latency_test")
    parser.add_argument("--count", type=int, default=0, help="0 means run indefinitely.")
    parser.add_argument("--log-file", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rclpy.init()
    node = LatencyConsumer(args)
    try:
        rclpy.spin(node)
    finally:
        node.close()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
