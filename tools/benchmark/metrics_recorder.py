#!/usr/bin/env python3
"""Subscribe to kafka_sink/metrics and record to CSV for analysis.

The kafka_sink publishes a JSON array on std_msgs/String at a configurable
interval (default 1 s). Each array element contains per-topic metrics.

This script flattens the nested JSON into CSV rows with one row per
topic per metrics interval.

Usage:
  python3 metrics_recorder.py --output results/navsatfix_cdr_10hz.csv
  python3 metrics_recorder.py --output results/run.csv --duration 65
"""

import argparse
import csv
import json
import os
import signal
import sys
import time

import rclpy
from rclpy.node import Node
from std_msgs.msg import String

# CSV columns — flattened from the kafka_sink metrics JSON
CSV_COLUMNS = [
    "wall_time",
    "ros_topic",
    "kafka_topic",
    "msg_type",
    "payload_format",
    "interval_ms",
    # delta
    "delta_received",
    "delta_sent_ok",
    "delta_dropped",
    "delta_errors",
    "delta_bytes",
    # rates
    "received_per_sec",
    "sent_per_sec",
    # message size
    "msg_size_avg_bytes",
    "msg_size_min_bytes",
    "msg_size_max_bytes",
    # latency
    "serialize_avg_ns",
    "serialize_p95_ns",
    "serialize_p99_ns",
    "serialize_max_ns",
    "send_avg_ns",
    "send_p95_ns",
    "send_p99_ns",
    "send_max_ns",
    # throughput
    "serialize_mb_per_sec",
    "send_mb_per_sec",
    # cpu
    "ns_per_byte",
    "bytes_per_cpu_ms",
    # totals
    "total_received",
    "total_sent_ok",
    "total_dropped",
    "total_errors",
    "total_bytes",
]


def flatten_entry(entry: dict, wall_time: float) -> dict:
    """Flatten nested metrics JSON into a flat dict matching CSV_COLUMNS."""
    d = entry.get("delta", {})
    r = entry.get("rates", {})
    s = entry.get("message_size", {})
    lat = entry.get("latency_ns", {})
    tp = entry.get("throughput", {})
    cpu = entry.get("cpu_efficiency", {})
    tot = entry.get("totals", {})

    return {
        "wall_time": f"{wall_time:.3f}",
        "ros_topic": entry.get("ros_topic", ""),
        "kafka_topic": entry.get("kafka_topic", ""),
        "msg_type": entry.get("msg_type", ""),
        "payload_format": entry.get("payload_format", ""),
        "interval_ms": entry.get("interval_ms", 0),
        "delta_received": d.get("received", 0),
        "delta_sent_ok": d.get("sent_ok", 0),
        "delta_dropped": d.get("dropped", 0),
        "delta_errors": d.get("errors", 0),
        "delta_bytes": d.get("bytes", 0),
        "received_per_sec": r.get("received_per_sec", 0.0),
        "sent_per_sec": r.get("sent_per_sec", 0.0),
        "msg_size_avg_bytes": s.get("avg_bytes", 0.0),
        "msg_size_min_bytes": s.get("min_bytes", 0),
        "msg_size_max_bytes": s.get("max_bytes", 0),
        "serialize_avg_ns": lat.get("serialize_avg", 0),
        "serialize_p95_ns": lat.get("serialize_p95", 0),
        "serialize_p99_ns": lat.get("serialize_p99", 0),
        "serialize_max_ns": lat.get("serialize_max", 0),
        "send_avg_ns": lat.get("send_avg", 0),
        "send_p95_ns": lat.get("send_p95", 0),
        "send_p99_ns": lat.get("send_p99", 0),
        "send_max_ns": lat.get("send_max", 0),
        "serialize_mb_per_sec": tp.get("serialize_mb_per_sec", 0.0),
        "send_mb_per_sec": tp.get("send_mb_per_sec", 0.0),
        "ns_per_byte": cpu.get("ns_per_byte", 0.0),
        "bytes_per_cpu_ms": cpu.get("bytes_per_cpu_ms", 0.0),
        "total_received": tot.get("received", 0),
        "total_sent_ok": tot.get("sent_ok", 0),
        "total_dropped": tot.get("dropped", 0),
        "total_errors": tot.get("errors", 0),
        "total_bytes": tot.get("bytes", 0),
    }


class MetricsRecorder(Node):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__("metrics_recorder")
        self._output_path = args.output
        self._duration = args.duration
        self._warmup = args.warmup
        self._metrics_topic = args.metrics_topic
        self._rows_written = 0
        self._start_time = time.time()

        os.makedirs(os.path.dirname(self._output_path) or ".", exist_ok=True)
        self._csvfile = open(self._output_path, "w", newline="", buffering=1)
        self._writer = csv.DictWriter(self._csvfile, fieldnames=CSV_COLUMNS)
        self._writer.writeheader()

        self.create_subscription(
            String, self._metrics_topic, self._on_metrics, 10
        )
        self.get_logger().info(
            f"Recording {self._metrics_topic} -> {self._output_path} "
            f"(warmup={self._warmup}s, duration={self._duration}s)"
        )

    def _on_metrics(self, msg: String) -> None:
        wall = time.time()
        elapsed = wall - self._start_time

        if elapsed < self._warmup:
            return

        if self._duration > 0 and elapsed > (self._warmup + self._duration):
            self.get_logger().info(
                f"Duration reached. Wrote {self._rows_written} rows to {self._output_path}"
            )
            self.close()
            raise SystemExit(0)

        try:
            entries = json.loads(msg.data)
        except json.JSONDecodeError as e:
            self.get_logger().warn(f"Bad metrics JSON: {e}")
            return

        if not isinstance(entries, list):
            entries = [entries]

        for entry in entries:
            row = flatten_entry(entry, wall)
            self._writer.writerow(row)
            self._rows_written += 1

    def close(self) -> None:
        if self._csvfile and not self._csvfile.closed:
            self._csvfile.close()
            self.get_logger().info(f"CSV closed: {self._output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Record kafka_sink metrics to CSV."
    )
    parser.add_argument(
        "--output", "-o", required=True, help="Output CSV file path."
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Recording duration in seconds after warmup (0 = indefinite).",
    )
    parser.add_argument(
        "--warmup",
        type=float,
        default=5.0,
        help="Seconds to skip at startup before recording.",
    )
    parser.add_argument(
        "--metrics-topic",
        default="kafka_sink/metrics",
        help="Metrics topic name.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rclpy.init()
    node = MetricsRecorder(args)

    def shutdown_handler(sig, frame):
        node.get_logger().info(f"Signal {sig}, shutting down.")
        node.close()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        rclpy.spin(node)
    except SystemExit:
        pass
    finally:
        node.close()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
