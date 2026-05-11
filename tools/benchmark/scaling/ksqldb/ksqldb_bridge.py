#!/usr/bin/env python3
"""Fan-in bridge: consume CDR NavSatFix records from the scaling pipeline's
per-robot Kafka topics (`ros2.robot_<i>.gnss`), deserialize, and re-emit as
JSON to a flat topic (default `ros_gps_fix`) that ksqlDB consumes.

The original publisher's wall-clock nanosecond `t0_ns` is preserved as an
explicit field on the JSON payload so the ksqlDB query can pass it through
to the output topic and the downstream consumer can compute end-to-end
paradigm latency `t1_ns - t0_ns`.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import time

from confluent_kafka import Consumer, Producer
from rclpy.serialization import deserialize_message
from sensor_msgs.msg import NavSatFix


_ROBOT_TOPIC_RE = re.compile(r"^ros2\.robot_(\d+)\.gnss$")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--input-pattern",
        # librdkafka regex is POSIX (no \d, no \w). Use [0-9]+.
        default=os.environ.get("INPUT_PATTERN", r"^ros2\.robot_[0-9]+\.gnss$"),
        help="confluent-kafka regex pattern to subscribe to",
    )
    parser.add_argument(
        "--output-topic",
        default=os.environ.get("OUTPUT_TOPIC", "ros_gps_fix"),
        help="Flat output Kafka topic for ksqlDB to consume",
    )
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"ksqldb-bridge-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            # Default 5 min is too slow; we need to pick up new per-robot
            # topics as soon as the sink starts producing them.
            "topic.metadata.refresh.interval.ms": 1000,
        }
    )
    consumer.subscribe([args.input_pattern])

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "acks": "1",
            "linger.ms": 0,
        }
    )

    # SIGTERM → KeyboardInterrupt so the finally block flushes.
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    print(f"[ksqldb-bridge] {args.input_pattern} → {args.output_topic} via {args.bootstrap}", flush=True)

    forwarded = 0
    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None or msg.error():
                continue
            m = _ROBOT_TOPIC_RE.match(msg.topic())
            if not m:
                continue
            robot_id_str = m.group(1)
            try:
                nav = deserialize_message(msg.value(), NavSatFix)
            except Exception:  # noqa: BLE001
                continue
            t0_ns = nav.header.stamp.sec * 1_000_000_000 + nav.header.stamp.nanosec
            payload = {
                "robot_id": robot_id_str,
                "latitude": float(nav.latitude),
                "longitude": float(nav.longitude),
                "altitude": float(nav.altitude),
                # ksqlDB likes millisecond TIMESTAMP fields; we keep the
                # nanosecond original alongside.
                "timestamp": t0_ns // 1_000_000,
                "t0_ns": t0_ns,
            }
            producer.produce(
                args.output_topic,
                key=robot_id_str.encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
            )
            producer.poll(0)
            forwarded += 1
            if forwarded % 1000 == 0:
                print(f"[ksqldb-bridge] forwarded {forwarded}", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(5.0)
        consumer.close()
        print(f"[ksqldb-bridge] exit; total forwarded={forwarded}", flush=True)


if __name__ == "__main__":
    main()
