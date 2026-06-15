#!/usr/bin/env python3
"""Consume JSON output from a ksqlDB query (default topic
`robot_geofence_alerts`), extract the `t0_ns` set by the upstream bridge,
capture `t1_ns` at receive, and write one JSONL row per delivered message —
schema-compatible with `e2e_consumer.py` so the same `analyze_scaling.py`
aggregator works.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import time

from confluent_kafka import Consumer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default="robot_geofence_alerts")
    parser.add_argument("--warmup", type=float, default=10.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--output", required=True, help="JSONL output path")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"ksqldb-output-consumer-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "topic.metadata.refresh.interval.ms": 1000,
        }
    )
    consumer.subscribe([args.topic])

    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    t_start = time.monotonic()
    received = 0
    try:
        with open(args.output, "w", buffering=1) as out:
            while True:
                elapsed = time.monotonic() - t_start
                if elapsed >= args.warmup + args.duration:
                    break
                msg = consumer.poll(timeout=0.5)
                if msg is None or msg.error():
                    continue
                t1_ns = time.time_ns()
                if elapsed < args.warmup:
                    continue
                try:
                    payload = json.loads(msg.value())
                except json.JSONDecodeError:
                    continue
                # ksqlDB may uppercase column names. Accept both forms.
                t0_ns_raw = payload.get("t0_ns", payload.get("T0_NS"))
                robot_raw = payload.get("robot_id", payload.get("ROBOT_ID"))
                if t0_ns_raw is None or robot_raw is None:
                    continue
                t0_ns = int(t0_ns_raw)
                try:
                    robot_id = int(robot_raw)
                except (TypeError, ValueError):
                    robot_id = str(robot_raw)
                row = {
                    "robot_id": robot_id,
                    "topic": msg.topic(),
                    "t0_ns": t0_ns,
                    "t1_ns": t1_ns,
                    "latency_ns": t1_ns - t0_ns,
                    "bytes": len(msg.value()),
                }
                out.write(json.dumps(row) + "\n")
                received += 1
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"[ksqldb-consumer] exit; received={received}", flush=True)


if __name__ == "__main__":
    main()
