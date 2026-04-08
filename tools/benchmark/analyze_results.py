#!/usr/bin/env python3
"""Analyze CDR vs JSON benchmark results and generate paper-ready figures.

Reads all CSVs from results/, computes summary statistics across repetitions,
and produces:
  - figures/serialize_latency_bar.pdf    — avg serialization latency by type & format
  - figures/serialize_latency_tail.pdf   — p95/p99 tail latency comparison
  - figures/message_size_bar.pdf         — message size CDR vs JSON
  - figures/throughput_vs_rate.pdf        — throughput vs publish rate
  - figures/drop_rate.pdf                — drop rate at high frequencies
  - figures/summary_table.tex            — LaTeX table for the paper

Usage:
  python3 analyze_results.py
  python3 analyze_results.py --results-dir results --output-dir figures
"""

import argparse
import glob
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── Styling ──
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.labelsize": 12,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.dpi": 300,
})

C_CDR = "#2196F3"
C_JSON = "#FF9800"
MSG_TYPE_LABELS = {
    "navsatfix": "NavSatFix\n(~128 B)",
    "odometry": "Odometry\n(zeros cov)",
    "odometry_fullcov": "Odometry\n(full cov)",
    "pointcloud2": "PointCloud2\n(~160 KB)",
}
MSG_TYPE_ORDER = ["navsatfix", "odometry", "odometry_fullcov", "pointcloud2"]


def parse_filename(path):
    """Extract msg_type, format, rate, run_id from filename."""
    basename = os.path.basename(path)
    m = re.match(r"(\w+)_(cdr|json)_(\d+)hz_run(\d+)\.csv", basename)
    if not m:
        return None
    return {
        "msg_type": m.group(1),
        "format": m.group(2),
        "rate": int(m.group(3)),
        "run_id": int(m.group(4)),
    }


def load_results(results_dir):
    """Load all CSVs and compute per-run averages (skipping first row as warmup residual)."""
    import csv

    records = []
    for path in sorted(glob.glob(os.path.join(results_dir, "*.csv"))):
        meta = parse_filename(path)
        if not meta:
            continue

        rows = []
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)

        if len(rows) < 2:
            print(f"  WARN: {os.path.basename(path)} has {len(rows)} rows, skipping")
            continue

        # Skip first row (warmup residual)
        rows = rows[1:]

        # Compute means for this run
        def mean_field(field):
            vals = [float(r[field]) for r in rows if float(r[field]) > 0]
            return np.mean(vals) if vals else 0.0

        def max_field(field):
            vals = [float(r[field]) for r in rows]
            return np.max(vals) if vals else 0.0

        def sum_field(field):
            return sum(float(r[field]) for r in rows)

        total_received = sum_field("delta_received")
        total_dropped = sum_field("delta_dropped")

        rec = {
            **meta,
            "serialize_avg_ns": mean_field("serialize_avg_ns"),
            "serialize_p95_ns": mean_field("serialize_p95_ns"),
            "serialize_p99_ns": mean_field("serialize_p99_ns"),
            "serialize_max_ns": max_field("serialize_max_ns"),
            "send_avg_ns": mean_field("send_avg_ns"),
            "send_p95_ns": mean_field("send_p95_ns"),
            "send_p99_ns": mean_field("send_p99_ns"),
            "send_max_ns": max_field("send_max_ns"),
            "msg_size_avg": mean_field("msg_size_avg_bytes"),
            "msg_size_min": mean_field("msg_size_min_bytes"),
            "msg_size_max": mean_field("msg_size_max_bytes"),
            "serialize_mb_per_sec": mean_field("serialize_mb_per_sec"),
            "send_mb_per_sec": mean_field("send_mb_per_sec"),
            "received_per_sec": mean_field("received_per_sec"),
            "drop_rate": total_dropped / total_received if total_received > 0 else 0.0,
            "n_rows": len(rows),
        }
        records.append(rec)

    return records


def group_by(records, *keys):
    """Group records by keys, return dict of key_tuple -> [records]."""
    groups = {}
    for r in records:
        k = tuple(r[key] for key in keys)
        groups.setdefault(k, []).append(r)
    return groups


def agg_across_runs(records, field):
    """Return (mean, std) of a field across repetitions."""
    vals = [r[field] for r in records]
    return np.mean(vals), np.std(vals)


def fig_serialize_latency_bar(records, output_dir):
    """Bar chart: avg serialization latency by message type, CDR vs JSON."""
    fig, ax = plt.subplots(figsize=(7, 4))

    # Use only 10 Hz rate for clean comparison (low rate, no contention)
    subset = [r for r in records if r["rate"] == 10]
    groups = group_by(subset, "msg_type", "format")

    x = np.arange(len(MSG_TYPE_ORDER))
    width = 0.35

    for i, fmt in enumerate(["cdr", "json"]):
        means, stds = [], []
        for mt in MSG_TYPE_ORDER:
            recs = groups.get((mt, fmt), [])
            if recs:
                m, s = agg_across_runs(recs, "serialize_avg_ns")
                means.append(m / 1000)  # ns -> µs
                stds.append(s / 1000)
            else:
                means.append(0)
                stds.append(0)

        color = C_CDR if fmt == "cdr" else C_JSON
        offset = -width / 2 + i * width
        bars = ax.bar(x + offset, means, width, yerr=stds, label=fmt.upper(),
                      color=color, alpha=0.85, capsize=4, edgecolor="white", linewidth=0.5)

        # Value labels on bars
        for bar, val in zip(bars, means):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(stds) * 0.3,
                        f"{val:.1f}", ha="center", va="bottom", fontsize=8)

    ax.set_ylabel("Serialization latency (µs)")
    ax.set_title("Average Serialization Latency — CDR vs JSON (10 Hz)")
    ax.set_xticks(x)
    ax.set_xticklabels([MSG_TYPE_LABELS.get(mt, mt) for mt in MSG_TYPE_ORDER])
    ax.legend()
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    for ext in ["pdf", "png"]:
        fig.savefig(os.path.join(output_dir, f"serialize_latency_bar.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print("  -> serialize_latency_bar")


def fig_serialize_latency_tail(records, output_dir):
    """Grouped bar chart: p95 and p99 tail latencies."""
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=True)

    subset = [r for r in records if r["rate"] == 10]
    groups = group_by(subset, "msg_type", "format")

    for ax_idx, (percentile, field) in enumerate([("P95", "serialize_p95_ns"), ("P99", "serialize_p99_ns")]):
        ax = axes[ax_idx]
        x = np.arange(len(MSG_TYPE_ORDER))
        width = 0.35

        for i, fmt in enumerate(["cdr", "json"]):
            means = []
            for mt in MSG_TYPE_ORDER:
                recs = groups.get((mt, fmt), [])
                if recs:
                    m, _ = agg_across_runs(recs, field)
                    means.append(m / 1000)
                else:
                    means.append(0)

            color = C_CDR if fmt == "cdr" else C_JSON
            offset = -width / 2 + i * width
            ax.bar(x + offset, means, width, label=fmt.upper(),
                   color=color, alpha=0.85, edgecolor="white", linewidth=0.5)

        ax.set_title(f"{percentile} Serialization Latency")
        ax.set_xticks(x)
        ax.set_xticklabels([MSG_TYPE_LABELS.get(mt, mt) for mt in MSG_TYPE_ORDER])
        ax.grid(axis="y", alpha=0.3)
        if ax_idx == 0:
            ax.set_ylabel("Latency (µs)")
            ax.legend()

    fig.suptitle("Tail Serialization Latency — CDR vs JSON (10 Hz)", fontsize=13)
    fig.tight_layout()
    for ext in ["pdf", "png"]:
        fig.savefig(os.path.join(output_dir, f"serialize_latency_tail.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print("  -> serialize_latency_tail")


def fig_message_size_bar(records, output_dir):
    """Bar chart: message size CDR vs JSON with ratio annotation."""
    fig, ax = plt.subplots(figsize=(7, 4))

    subset = [r for r in records if r["rate"] == 10]
    groups = group_by(subset, "msg_type", "format")

    x = np.arange(len(MSG_TYPE_ORDER))
    width = 0.35

    size_data = {}
    for i, fmt in enumerate(["cdr", "json"]):
        means = []
        for mt in MSG_TYPE_ORDER:
            recs = groups.get((mt, fmt), [])
            if recs:
                m, _ = agg_across_runs(recs, "msg_size_avg")
                means.append(m)
            else:
                means.append(0)
        size_data[fmt] = means

        color = C_CDR if fmt == "cdr" else C_JSON
        offset = -width / 2 + i * width
        ax.bar(x + offset, means, width, label=fmt.upper(),
               color=color, alpha=0.85, edgecolor="white", linewidth=0.5)

    # Annotate ratio
    for j, mt in enumerate(MSG_TYPE_ORDER):
        cdr_s = size_data["cdr"][j]
        json_s = size_data["json"][j]
        if cdr_s > 0 and json_s > 0:
            ratio = json_s / cdr_s
            y_pos = max(cdr_s, json_s) * 1.05
            ax.text(j, y_pos, f"{ratio:.1f}×", ha="center", va="bottom",
                    fontsize=9, fontweight="bold", color="#333")

    ax.set_ylabel("Message size (bytes)")
    ax.set_title("Payload Size — CDR vs JSON")
    ax.set_xticks(x)
    ax.set_xticklabels([MSG_TYPE_LABELS.get(mt, mt) for mt in MSG_TYPE_ORDER])
    ax.legend()
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=0.3)

    # Use log scale if pointcloud2 makes small bars invisible
    if size_data["cdr"][-1] > 10000:
        ax.set_yscale("log")
        ax.set_ylabel("Message size (bytes, log scale)")

    fig.tight_layout()
    for ext in ["pdf", "png"]:
        fig.savefig(os.path.join(output_dir, f"message_size_bar.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print("  -> message_size_bar")


def fig_throughput_vs_rate(records, output_dir):
    """Line plot: serialization throughput vs publish rate, per message type."""
    fig, axes = plt.subplots(1, len(MSG_TYPE_ORDER), figsize=(14, 4), sharey=False)

    rates = sorted(set(r["rate"] for r in records))
    groups = group_by(records, "msg_type", "format", "rate")

    for ax_idx, mt in enumerate(MSG_TYPE_ORDER):
        ax = axes[ax_idx]
        for fmt in ["cdr", "json"]:
            means, stds, xs = [], [], []
            for rate in rates:
                recs = groups.get((mt, fmt, rate), [])
                if recs:
                    m, s = agg_across_runs(recs, "serialize_mb_per_sec")
                    means.append(m)
                    stds.append(s)
                    xs.append(rate)

            color = C_CDR if fmt == "cdr" else C_JSON
            ax.errorbar(xs, means, yerr=stds, marker="o", markersize=5,
                        label=fmt.upper(), color=color, capsize=3, linewidth=1.5)

        ax.set_title(MSG_TYPE_LABELS.get(mt, mt).replace("\n", " "))
        ax.set_xlabel("Publish rate (Hz)")
        if ax_idx == 0:
            ax.set_ylabel("Serialization throughput (MB/s)")
        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ax.grid(alpha=0.3)
        ax.legend()

    fig.suptitle("Serialization Throughput vs Publish Rate", fontsize=13)
    fig.tight_layout()
    for ext in ["pdf", "png"]:
        fig.savefig(os.path.join(output_dir, f"throughput_vs_rate.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print("  -> throughput_vs_rate")


def fig_drop_rate(records, output_dir):
    """Bar chart: drop rate at each rate, CDR vs JSON."""
    rates = sorted(set(r["rate"] for r in records))
    groups = group_by(records, "msg_type", "format", "rate")

    # Only show if there are any drops
    has_drops = any(r["drop_rate"] > 0 for r in records)
    if not has_drops:
        print("  -> drop_rate: SKIPPED (no drops observed)")
        return

    fig, axes = plt.subplots(1, len(MSG_TYPE_ORDER), figsize=(14, 4), sharey=True)

    for ax_idx, mt in enumerate(MSG_TYPE_ORDER):
        ax = axes[ax_idx]
        x = np.arange(len(rates))
        width = 0.35

        for i, fmt in enumerate(["cdr", "json"]):
            means = []
            for rate in rates:
                recs = groups.get((mt, fmt, rate), [])
                if recs:
                    m, _ = agg_across_runs(recs, "drop_rate")
                    means.append(m * 100)  # to percent
                else:
                    means.append(0)

            color = C_CDR if fmt == "cdr" else C_JSON
            offset = -width / 2 + i * width
            ax.bar(x + offset, means, width, label=fmt.upper(),
                   color=color, alpha=0.85, edgecolor="white", linewidth=0.5)

        ax.set_title(MSG_TYPE_LABELS.get(mt, mt).replace("\n", " "))
        ax.set_xticks(x)
        ax.set_xticklabels([str(r) for r in rates])
        ax.set_xlabel("Rate (Hz)")
        if ax_idx == 0:
            ax.set_ylabel("Drop rate (%)")
            ax.legend()
        ax.grid(axis="y", alpha=0.3)

    fig.suptitle("Message Drop Rate vs Publish Rate", fontsize=13)
    fig.tight_layout()
    for ext in ["pdf", "png"]:
        fig.savefig(os.path.join(output_dir, f"drop_rate.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print("  -> drop_rate")


def generate_latex_table(records, output_dir):
    """Generate LaTeX table with key metrics at 10 Hz."""
    subset = [r for r in records if r["rate"] == 10]
    groups = group_by(subset, "msg_type", "format")

    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{CDR vs JSON serialization comparison at 10\,Hz.}",
        r"\label{tab:cdr_vs_json}",
        r"\small",
        r"\setlength{\tabcolsep}{3pt}",
        r"\begin{tabular}{llrrrr}",
        r"\toprule",
        r"\textbf{Message type} & \textbf{Format} & \textbf{Size (B)} & \textbf{Serialize avg (\textmu s)} & \textbf{P99 (\textmu s)} & \textbf{Ratio} \\",
        r"\midrule",
    ]

    for mt in MSG_TYPE_ORDER:
        cdr_recs = groups.get((mt, "cdr"), [])
        json_recs = groups.get((mt, "json"), [])
        if not cdr_recs or not json_recs:
            continue

        cdr_size, _ = agg_across_runs(cdr_recs, "msg_size_avg")
        json_size, _ = agg_across_runs(json_recs, "msg_size_avg")
        cdr_lat, _ = agg_across_runs(cdr_recs, "serialize_avg_ns")
        json_lat, _ = agg_across_runs(json_recs, "serialize_avg_ns")
        cdr_p99, _ = agg_across_runs(cdr_recs, "serialize_p99_ns")
        json_p99, _ = agg_across_runs(json_recs, "serialize_p99_ns")

        size_ratio = json_size / cdr_size if cdr_size > 0 else 0
        lat_ratio = json_lat / cdr_lat if cdr_lat > 0 else 0

        label = mt.replace("navsatfix", "NavSatFix").replace("odometry", "Odometry").replace("pointcloud2", "PointCloud2")

        lines.append(
            f"{label} & CDR & {cdr_size:.0f} & {cdr_lat/1000:.1f} & {cdr_p99/1000:.1f} & --- \\\\"
        )
        lines.append(
            f" & JSON & {json_size:.0f} & {json_lat/1000:.1f} & {json_p99/1000:.1f} & {size_ratio:.1f}$\\times$ size, {lat_ratio:.0f}$\\times$ lat \\\\"
        )
        if mt != MSG_TYPE_ORDER[-1]:
            lines.append(r"\addlinespace")

    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ]

    tex_path = os.path.join(output_dir, "summary_table.tex")
    with open(tex_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"  -> summary_table.tex")


def print_summary(records):
    """Print a quick summary to stdout."""
    subset = [r for r in records if r["rate"] == 10]
    groups = group_by(subset, "msg_type", "format")

    print("\n=== Summary (10 Hz) ===")
    print(f"{'Type':<14} {'Format':<6} {'Size(B)':>8} {'Ser.avg(µs)':>12} {'Ser.p99(µs)':>12} {'Send.avg(µs)':>13}")
    print("-" * 72)

    for mt in MSG_TYPE_ORDER:
        for fmt in ["cdr", "json"]:
            recs = groups.get((mt, fmt), [])
            if not recs:
                continue
            size, _ = agg_across_runs(recs, "msg_size_avg")
            ser_avg, _ = agg_across_runs(recs, "serialize_avg_ns")
            ser_p99, _ = agg_across_runs(recs, "serialize_p99_ns")
            send_avg, _ = agg_across_runs(recs, "send_avg_ns")
            print(f"{mt:<14} {fmt:<6} {size:>8.0f} {ser_avg/1000:>12.1f} {ser_p99/1000:>12.1f} {send_avg/1000:>13.1f}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Analyze CDR vs JSON benchmark results.")
    parser.add_argument("--results-dir", default=os.path.join(os.path.dirname(__file__), "results"))
    parser.add_argument("--output-dir", default=os.path.join(os.path.dirname(__file__), "figures"))
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Loading results from {args.results_dir}...")
    records = load_results(args.results_dir)
    print(f"  Loaded {len(records)} runs")

    if not records:
        print("ERROR: No results found.")
        sys.exit(1)

    msg_types_found = sorted(set(r["msg_type"] for r in records))
    formats_found = sorted(set(r["format"] for r in records))
    rates_found = sorted(set(r["rate"] for r in records))
    print(f"  Types: {msg_types_found}")
    print(f"  Formats: {formats_found}")
    print(f"  Rates: {rates_found}")

    print_summary(records)

    print(f"\nGenerating figures in {args.output_dir}...")
    fig_serialize_latency_bar(records, args.output_dir)
    fig_serialize_latency_tail(records, args.output_dir)
    fig_message_size_bar(records, args.output_dir)
    fig_throughput_vs_rate(records, args.output_dir)
    fig_drop_rate(records, args.output_dir)
    generate_latex_table(records, args.output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
