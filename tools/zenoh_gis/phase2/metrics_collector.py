#!/usr/bin/env python3
# Cloud-side resilience metrics. Subscribes the positions republished by the
# cloud zenoh_source (/zenoh_decoded/robot_K/gps/fix) and prints one CSV line per
# INTERVAL: t_s,recv_per_sec,lat_avg_ms,robots_seen. Run on the cloud node while
# the host applies/clears tc netem — the received-rate time series (drop under
# loss/partition, return after clear) is the resilience result. No extra deps.
#
# Usage: metrics_collector.py <n_robots> <interval_s> <duration_s>
import sys
import time
import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    interval = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    duration = float(sys.argv[3]) if len(sys.argv) > 3 else 40.0
    rclpy.init()
    node = Node("metrics_collector")
    state = {"count": 0, "lat_sum": 0.0, "robots": set()}

    def make_cb(robot):
        def cb(msg):
            state["count"] += 1
            state["robots"].add(robot)
            stamp = msg.header.stamp.sec * 1_000_000_000 + msg.header.stamp.nanosec
            if stamp > 0:
                now = node.get_clock().now().nanoseconds
                state["lat_sum"] += (now - stamp) / 1e6  # ms
        return cb

    for k in range(1, n + 1):
        node.create_subscription(NavSatFix, f"/zenoh_decoded/robot_{k}/gps/fix", make_cb(f"robot_{k}"), 50)

    print("t_s,recv_per_sec,lat_avg_ms,robots_seen", flush=True)
    t0 = time.time()
    next_tick = t0 + interval
    while time.time() - t0 < duration and rclpy.ok():
        rclpy.spin_once(node, timeout_sec=0.05)
        if time.time() >= next_tick:
            c = state["count"]
            lat = (state["lat_sum"] / c) if c else 0.0
            print(f"{time.time()-t0:.1f},{c/interval:.2f},{lat:.2f},{len(state['robots'])}", flush=True)
            state["count"] = 0
            state["lat_sum"] = 0.0
            state["robots"] = set()
            next_tick += interval
    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
