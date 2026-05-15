#!/usr/bin/env python3
"""Generate a tiny NavSatFix bag for tests.

Run once:
    cd tools/benchmark/scaling/test_data
    python3 generate_test_bag.py --output ./test_bag
"""
import argparse
import os
import shutil

import rclpy
from rclpy.serialization import serialize_message
from sensor_msgs.msg import NavSatFix, NavSatStatus
from std_msgs.msg import Header

import rosbag2_py


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, help="output bag directory")
    parser.add_argument("--rate-hz", type=float, default=10.0)
    parser.add_argument("--duration-s", type=float, default=5.0)
    parser.add_argument("--topic", default="/source/gnss")
    args = parser.parse_args()

    if os.path.exists(args.output):
        shutil.rmtree(args.output)

    storage_options = rosbag2_py.StorageOptions(uri=args.output, storage_id="sqlite3")
    converter_options = rosbag2_py.ConverterOptions("", "")
    writer = rosbag2_py.SequentialWriter()
    writer.open(storage_options, converter_options)
    writer.create_topic(
        rosbag2_py.TopicMetadata(
            name=args.topic,
            type="sensor_msgs/msg/NavSatFix",
            serialization_format="cdr",
        )
    )

    n_msgs = int(args.rate_hz * args.duration_s)
    period_ns = int(1e9 / args.rate_hz)
    t0_ns = 0
    for i in range(n_msgs):
        msg = NavSatFix()
        msg.header = Header()
        msg.header.frame_id = "gps_link"
        ts_ns = t0_ns + i * period_ns
        msg.header.stamp.sec = ts_ns // 1_000_000_000
        msg.header.stamp.nanosec = ts_ns % 1_000_000_000
        msg.status.status = NavSatStatus.STATUS_FIX
        msg.status.service = NavSatStatus.SERVICE_GPS
        msg.latitude = 51.1079 + 0.00001 * i
        msg.longitude = 17.0385 + 0.00001 * i
        msg.altitude = 120.0
        writer.write(args.topic, serialize_message(msg), ts_ns)

    del writer
    print(f"Wrote {n_msgs} messages to {args.output}")


if __name__ == "__main__":
    main()
