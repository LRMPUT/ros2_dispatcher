#!/usr/bin/env python3
"""Aggregate per-cell consumer JSONL into summary.csv + plots."""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import statistics
from typing import Dict, List


def percentile(samples: List[float], q: float) -> float:
    if not samples:
        return 0
    s = sorted(samples)
    # Nearest-rank method, 1-indexed
    k = max(1, math.ceil(q * len(s)))
    return s[k - 1]


def expected_messages(n: int, rate_hz: float, duration_s: float) -> int:
    return int(n * rate_hz * duration_s)


def load_jsonl(path: str) -> List[dict]:
    out: List[dict] = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def aggregate_cell(cell_dir: str, n: int, rate_hz: float, duration_s: float) -> dict:
    jsonl_path = os.path.join(cell_dir, "consumer.jsonl")
    rows = load_jsonl(jsonl_path)
    latencies = [r["latency_ns"] for r in rows]
    received = len(rows)
    expected = expected_messages(n, rate_hz, duration_s)
    drop_rate = 1.0 - (received / expected) if expected > 0 else 0.0

    return {
        "received": received,
        "expected": expected,
        "drop_rate": drop_rate,
        "throughput_msgs_per_s": received / duration_s if duration_s > 0 else 0,
        "latency_avg_ns": int(statistics.mean(latencies)) if latencies else 0,
        "latency_p50_ns": percentile(latencies, 0.50),
        "latency_p95_ns": percentile(latencies, 0.95),
        "latency_p99_ns": percentile(latencies, 0.99),
    }


_CELL_RE = re.compile(r"^N=(\d+)_broker=(\w+)_run=([\w]+)$")


def discover_cells(results_dir: str) -> List[Dict]:
    out = []
    for entry in sorted(os.listdir(results_dir)):
        m = _CELL_RE.match(entry)
        if not m:
            continue
        out.append(
            {
                "dir": os.path.join(results_dir, entry),
                "n": int(m.group(1)),
                "broker": m.group(2),
                "run": m.group(3),
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="results directory")
    parser.add_argument("--output", required=True, help="output directory (summary.csv + plots/)")
    parser.add_argument("--rate-hz", type=float, default=10.0)
    parser.add_argument("--duration-s", type=float, default=60.0)
    args = parser.parse_args()

    os.makedirs(os.path.join(args.output, "plots"), exist_ok=True)
    cells = discover_cells(args.input)

    rows = []
    for cell in cells:
        if cell["run"] == "smoke":
            continue  # exclude smoke gate runs
        try:
            agg = aggregate_cell(cell["dir"], cell["n"], args.rate_hz, args.duration_s)
        except FileNotFoundError:
            continue
        rows.append({"n": cell["n"], "broker": cell["broker"], "run": cell["run"], **agg})

    csv_path = os.path.join(args.output, "summary.csv")
    if rows:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {csv_path}")
    else:
        print("No cells with data found.")
        return

    _plot_summary(rows, os.path.join(args.output, "plots"))
    _write_markdown_table(rows, args.output)


def _write_markdown_table(rows: List[dict], output_dir: str) -> None:
    """Write a per-(N, broker) summary as Markdown for paper paste-in."""
    if not rows:
        return
    # Aggregate across reps
    by_cell: Dict[tuple, List[dict]] = {}
    for r in rows:
        by_cell.setdefault((r["broker"], r["n"]), []).append(r)
    out_path = os.path.join(output_dir, "summary_table.md")
    with open(out_path, "w") as f:
        f.write("| Broker | N | P50 lat (ms) | P95 lat (ms) | P99 lat (ms) | Throughput (msg/s) | Drop rate |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        for (broker, n) in sorted(by_cell.keys(), key=lambda t: (t[0], t[1])):
            reps = by_cell[(broker, n)]
            mean_p50 = statistics.mean(r["latency_p50_ns"] for r in reps) / 1e6
            mean_p95 = statistics.mean(r["latency_p95_ns"] for r in reps) / 1e6
            mean_p99 = statistics.mean(r["latency_p99_ns"] for r in reps) / 1e6
            mean_tput = statistics.mean(r["throughput_msgs_per_s"] for r in reps)
            mean_drop = statistics.mean(r["drop_rate"] for r in reps)
            f.write(f"| {broker} | {n} | {mean_p50:.2f} | {mean_p95:.2f} | {mean_p99:.2f} | {mean_tput:.1f} | {mean_drop:.4f} |\n")
    print(f"Wrote {out_path}")


def _plot_summary(rows: List[dict], plot_dir: str) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:  # noqa: BLE001
        print(f"matplotlib not available; skipping plots ({exc})")
        return

    # Group by broker → list of (n, mean_p95_ns) across runs
    by_broker: Dict[str, Dict[int, List[float]]] = {}
    by_broker_drop: Dict[str, Dict[int, List[float]]] = {}
    by_broker_tput: Dict[str, Dict[int, List[float]]] = {}
    for r in rows:
        by_broker.setdefault(r["broker"], {}).setdefault(r["n"], []).append(r["latency_p95_ns"])
        by_broker_drop.setdefault(r["broker"], {}).setdefault(r["n"], []).append(r["drop_rate"])
        by_broker_tput.setdefault(r["broker"], {}).setdefault(r["n"], []).append(r["throughput_msgs_per_s"])

    def _plot(data: Dict[str, Dict[int, List[float]]], ylabel: str, fname: str, scale: float = 1.0) -> None:
        plt.figure()
        for broker, points in data.items():
            xs = sorted(points.keys())
            ys = [statistics.mean(points[x]) * scale for x in xs]
            errs = [statistics.pstdev(points[x]) * scale for x in xs]
            plt.errorbar(xs, ys, yerr=errs, marker="o", label=broker)
        plt.xlabel("Number of robots")
        plt.ylabel(ylabel)
        plt.legend()
        plt.grid(True, alpha=0.3)
        out = os.path.join(plot_dir, fname)
        plt.savefig(out, dpi=140, bbox_inches="tight")
        print(f"Wrote {out}")

    _plot(by_broker, "P95 e2e latency (ms)", "latency_vs_n.png", scale=1e-6)
    _plot(by_broker_tput, "Throughput (msgs/s)", "throughput_vs_n.png")
    _plot(by_broker_drop, "Drop rate", "drop_rate_vs_n.png")


if __name__ == "__main__":
    main()
