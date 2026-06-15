#!/usr/bin/env python3
"""Cross-pass comparison: aggregate the three pass summaries and emit a
combined Markdown table + grouped plots for paper inclusion.

Inputs (hardcoded paths relative to script dir):
  results_archive/pass1_defaults/summary.csv
  results_archive/pass2_fair_fleet/summary.csv
  results_archive/pass3_fair_per_container/summary.csv

Outputs (alongside the inputs):
  results_archive/comparison.md
  results_archive/comparison_latency.png
  results_archive/comparison_drop_rate.png
"""
from __future__ import annotations

import csv
import os
import statistics
from collections import defaultdict


PASSES = [
    ("pass1_defaults",      "Defaults (fleet)",      "results_archive/pass1_defaults"),
    ("pass2_fair_fleet",    "Fair (fleet)",          "results_archive/pass2_fair_fleet"),
    ("pass3_fair_percont",  "Fair (per-container)",  "results_archive/pass3_fair_per_container"),
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def load_pass(path: str) -> dict:
    cells: dict = defaultdict(list)
    with open(os.path.join(SCRIPT_DIR, path, "summary.csv")) as f:
        for r in csv.DictReader(f):
            cells[(r["broker"], int(r["n"]))].append(r)
    return cells


def mean_ms(reps, key):
    return statistics.mean(int(r[key]) for r in reps) / 1e6


def mean_drop(reps):
    return statistics.mean(float(r["drop_rate"]) for r in reps)


def write_markdown(passes: list, out_path: str) -> None:
    with open(out_path, "w") as f:
        f.write("# Scalability Matrix — Cross-Pass Comparison\n\n")
        f.write("Three configurations × {Kafka, MQTT} × N ∈ {1, 5, 10, 25, 50}, "
                "3 reps each, 60 s per cell.\n\n")
        f.write("Each pass uses the same publisher → broker → consumer pipeline "
                "and bag (`rorbots_follower_leader_parcelle_1MONT`, replayed at "
                "10 Hz per simulated robot with deterministic per-robot lat/lon "
                "shift). The differences are:\n\n")
        f.write("- **pass1 (defaults, fleet)**: Kafka producer with `acks=all`, "
                "`linger.ms=5` (librdkafka defaults); one container hosts N rclpy "
                "publishers (MultiThreadedExecutor).\n")
        f.write("- **pass2 (fair, fleet)**: Kafka tuned to match MQTT semantics — "
                "`acks=1`, `linger.ms=0` (no batching). One container, N publishers.\n")
        f.write("- **pass3 (fair, per-container)**: same Kafka tuning as pass2; "
                "**N distinct Docker containers**, one publisher each. Matches the "
                "original spec topology.\n\n")
        f.write("MQTT is unchanged across passes (mosquitto with `qos=1`).\n\n")

        for broker in ["kafka", "mqtt"]:
            f.write(f"## {broker.upper()} — P50 / P95 / P99 latency (ms)\n\n")
            f.write("| N | " + " | ".join(p[1] for p in PASSES) + " |\n")
            f.write("|" + "---|" * (len(PASSES) + 1) + "\n")
            for n in [1, 5, 10, 25, 50]:
                cells = [str(n)]
                for key, _label, _path in PASSES:
                    reps = passes[key][(broker, n)]
                    if not reps:
                        cells.append("—")
                    else:
                        p50 = mean_ms(reps, "latency_p50_ns")
                        p95 = mean_ms(reps, "latency_p95_ns")
                        p99 = mean_ms(reps, "latency_p99_ns")
                        cells.append(f"{p50:.2f} / {p95:.2f} / {p99:.2f}")
                f.write("| " + " | ".join(cells) + " |\n")
            f.write("\n")

        for broker in ["kafka", "mqtt"]:
            f.write(f"## {broker.upper()} — Drop rate "
                    "(negative = consumer caught warmup overlap; positive = real loss)\n\n")
            f.write("| N | " + " | ".join(p[1] for p in PASSES) + " |\n")
            f.write("|" + "---|" * (len(PASSES) + 1) + "\n")
            for n in [1, 5, 10, 25, 50]:
                cells = [str(n)]
                for key, _label, _path in PASSES:
                    reps = passes[key][(broker, n)]
                    cells.append(f"{mean_drop(reps):+.4f}" if reps else "—")
                f.write("| " + " | ".join(cells) + " |\n")
            f.write("\n")


def write_plots(passes: list, out_dir: str) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        print(f"matplotlib unavailable, skipping plots ({exc})")
        return

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for ax, broker in zip(axes, ["kafka", "mqtt"]):
        for key, label, _ in PASSES:
            xs = sorted({n for (b, n) in passes[key].keys() if b == broker})
            ys = [mean_ms(passes[key][(broker, n)], "latency_p95_ns") for n in xs]
            ax.plot(xs, ys, marker="o", label=label)
        ax.set_yscale("log")
        ax.set_title(f"{broker.upper()} — P95 e2e latency vs N")
        ax.set_xlabel("Number of robots")
        ax.set_ylabel("P95 latency (ms)")
        ax.grid(True, alpha=0.3)
        ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "comparison_latency.png"), dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out_dir}/comparison_latency.png")

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for ax, broker in zip(axes, ["kafka", "mqtt"]):
        for key, label, _ in PASSES:
            xs = sorted({n for (b, n) in passes[key].keys() if b == broker})
            ys = [max(0, mean_drop(passes[key][(broker, n)])) for n in xs]
            ax.plot(xs, ys, marker="o", label=label)
        ax.set_title(f"{broker.upper()} — Drop rate vs N")
        ax.set_xlabel("Number of robots")
        ax.set_ylabel("Drop rate (clamped ≥0)")
        ax.grid(True, alpha=0.3)
        ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "comparison_drop_rate.png"), dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out_dir}/comparison_drop_rate.png")


def main() -> None:
    passes = {key: load_pass(path) for key, _, path in PASSES}
    out_dir = os.path.join(SCRIPT_DIR, "results_archive")
    md_path = os.path.join(out_dir, "comparison.md")
    write_markdown(passes, md_path)
    print(f"Wrote {md_path}")
    write_plots(passes, out_dir)


if __name__ == "__main__":
    main()
