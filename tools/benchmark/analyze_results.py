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
    """Bar chart: avg serialization latency — split panels for scale."""
    subset = [r for r in records if r["rate"] == 10]
    groups = group_by(subset, "msg_type", "format")

    small_types = [mt for mt in MSG_TYPE_ORDER if mt != "pointcloud2"]
    large_types = [mt for mt in MSG_TYPE_ORDER if mt == "pointcloud2"]
    has_large = bool(large_types) and any(
        groups.get((mt, fmt), []) for mt in large_types for fmt in ("cdr", "json")
    )

    if has_large:
        fig, (ax_small, ax_large) = plt.subplots(
            1, 2, figsize=(10, 4.5),
            gridspec_kw={"width_ratios": [len(small_types), len(large_types)]}
        )
        panels = [(ax_small, small_types), (ax_large, large_types)]
    else:
        fig, ax_small = plt.subplots(figsize=(7, 4.5))
        panels = [(ax_small, small_types)]

    width = 0.35

    for ax, types in panels:
        x = np.arange(len(types))
        is_large = (types == large_types)
        divisor = 1e6 if is_large else 1e3  # ns -> ms for large, ns -> µs for small

        for i, fmt in enumerate(["cdr", "json"]):
            means, stds = [], []
            for mt in types:
                recs = groups.get((mt, fmt), [])
                if recs:
                    m, s = agg_across_runs(recs, "serialize_avg_ns")
                    means.append(m / divisor)
                    stds.append(s / divisor)
                else:
                    means.append(0)
                    stds.append(0)

            color = C_CDR if fmt == "cdr" else C_JSON
            offset = -width / 2 + i * width
            bars = ax.bar(x + offset, means, width, yerr=stds, label=fmt.upper(),
                          color=color, alpha=0.85, capsize=4, edgecolor="white", linewidth=0.5)

            for bar, val in zip(bars, means):
                if val > 0:
                    unit = "ms" if is_large else "µs"
                    ax.text(bar.get_x() + bar.get_width() / 2,
                            bar.get_height() + max(stds) * 0.3,
                            f"{val:.2f} {unit}" if is_large else f"{val:.1f}",
                            ha="center", va="bottom", fontsize=8)

        ax.set_xticks(x)
        ax.set_xticklabels([MSG_TYPE_LABELS.get(mt, mt) for mt in types])
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", alpha=0.3)
        ax.legend()
        ax.set_ylabel("Serialization latency (ms)" if is_large else "Serialization latency (µs)")

    fig.suptitle("Average Serialization Latency — CDR vs JSON (10 Hz)", fontsize=13)
    fig.tight_layout()
    for ext in ["pdf", "png"]:
        fig.savefig(os.path.join(output_dir, f"serialize_latency_bar.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print("  -> serialize_latency_bar")


def fig_serialize_latency_tail(records, output_dir):
    """Grouped bar chart: p95 and p99 tail latencies — split panels for scale."""
    subset = [r for r in records if r["rate"] == 10]
    groups = group_by(subset, "msg_type", "format")

    small_types = [mt for mt in MSG_TYPE_ORDER if mt != "pointcloud2"]
    large_types = [mt for mt in MSG_TYPE_ORDER if mt == "pointcloud2"]
    has_large = bool(large_types) and any(
        groups.get((mt, fmt), []) for mt in large_types for fmt in ("cdr", "json")
    )

    if has_large:
        fig, axes = plt.subplots(1, 3, figsize=(14, 4.5),
                                 gridspec_kw={"width_ratios": [len(small_types), len(small_types), len(large_types)]})
        panel_configs = [
            (axes[0], small_types, "P95", "serialize_p95_ns"),
            (axes[1], small_types, "P99", "serialize_p99_ns"),
            (axes[2], large_types, "P95 / P99", None),
        ]
    else:
        fig, axes = plt.subplots(1, 2, figsize=(10, 4.5), sharey=True)
        panel_configs = [
            (axes[0], small_types, "P95", "serialize_p95_ns"),
            (axes[1], small_types, "P99", "serialize_p99_ns"),
        ]

    width = 0.35

    for ax, types, title, field in panel_configs:
        if field is not None:
            # Single percentile panel
            x = np.arange(len(types))
            for i, fmt in enumerate(["cdr", "json"]):
                means = []
                for mt in types:
                    recs = groups.get((mt, fmt), [])
                    if recs:
                        m, _ = agg_across_runs(recs, field)
                        means.append(m / 1000)
                    else:
                        means.append(0)

                color = C_CDR if fmt == "cdr" else C_JSON
                offset = -width / 2 + i * width
                bars = ax.bar(x + offset, means, width, label=fmt.upper(),
                              color=color, alpha=0.85, edgecolor="white", linewidth=0.5)

                for bar, val in zip(bars, means):
                    if val > 0:
                        label_text = f"{val/1000:.1f} ms" if val >= 1000 else f"{val:.0f}"
                        ax.text(bar.get_x() + bar.get_width() / 2,
                                bar.get_height() * 1.02,
                                label_text, ha="center", va="bottom", fontsize=7)

            ax.set_xticks(x)
            ax.set_xticklabels([MSG_TYPE_LABELS.get(mt, mt) for mt in types])
        else:
            # Combined P95+P99 for PointCloud2 — in ms
            x = np.arange(len(types))
            bar_w = 0.2
            for i, fmt in enumerate(["cdr", "json"]):
                for j, (pctl, pfield) in enumerate([("P95", "serialize_p95_ns"), ("P99", "serialize_p99_ns")]):
                    means = []
                    for mt in types:
                        recs = groups.get((mt, fmt), [])
                        if recs:
                            m, _ = agg_across_runs(recs, pfield)
                            means.append(m / 1e6)  # ns -> ms
                        else:
                            means.append(0)

                    color = C_CDR if fmt == "cdr" else C_JSON
                    alpha = 0.85 if j == 0 else 0.55
                    offset = -1.5 * bar_w + (i * 2 + j) * bar_w
                    bars = ax.bar(x + offset, means, bar_w,
                                  label=f"{fmt.upper()} {pctl}",
                                  color=color, alpha=alpha, edgecolor="white", linewidth=0.5)

                    for bar, val in zip(bars, means):
                        if val > 0:
                            ax.text(bar.get_x() + bar.get_width() / 2,
                                    bar.get_height() * 1.02,
                                    f"{val:.2f} ms", ha="center", va="bottom", fontsize=7)

            ax.set_xticks(x)
            ax.set_xticklabels([MSG_TYPE_LABELS.get(mt, mt) for mt in types])
            is_large_panel = True

        ax.set_title(title)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", alpha=0.3)
        ax.legend(fontsize=8)
        ax.set_ylabel("Latency (ms)" if (field is None) else "Latency (µs)")

    fig.suptitle("Tail Serialization Latency — CDR vs JSON (10 Hz)", fontsize=13)
    fig.tight_layout()
    for ext in ["pdf", "png"]:
        fig.savefig(os.path.join(output_dir, f"serialize_latency_tail.{ext}"), bbox_inches="tight")
    plt.close(fig)
    print("  -> serialize_latency_tail")


def fig_message_size_bar(records, output_dir):
    """Bar chart: message size CDR vs JSON — split into two panels to handle scale difference."""
    subset = [r for r in records if r["rate"] == 10]
    groups = group_by(subset, "msg_type", "format")

    # Separate small/medium types from large types
    small_types = [mt for mt in MSG_TYPE_ORDER if mt != "pointcloud2"]
    large_types = [mt for mt in MSG_TYPE_ORDER if mt == "pointcloud2"]
    has_large = bool(large_types) and any(
        groups.get((mt, fmt), []) for mt in large_types for fmt in ("cdr", "json")
    )

    if has_large:
        fig, (ax_small, ax_large) = plt.subplots(
            1, 2, figsize=(10, 4.5),
            gridspec_kw={"width_ratios": [len(small_types), len(large_types)]}
        )
        panels = [(ax_small, small_types), (ax_large, large_types)]
    else:
        fig, ax_small = plt.subplots(figsize=(7, 4.5))
        panels = [(ax_small, small_types)]

    width = 0.35

    for ax, types in panels:
        x = np.arange(len(types))
        size_data = {}

        for i, fmt in enumerate(["cdr", "json"]):
            means = []
            for mt in types:
                recs = groups.get((mt, fmt), [])
                if recs:
                    m, _ = agg_across_runs(recs, "msg_size_avg")
                    means.append(m)
                else:
                    means.append(0)
            size_data[fmt] = means

            color = C_CDR if fmt == "cdr" else C_JSON
            offset = -width / 2 + i * width
            bars = ax.bar(x + offset, means, width, label=fmt.upper(),
                          color=color, alpha=0.85, edgecolor="white", linewidth=0.5)

            # Value labels on bars
            for bar, val in zip(bars, means):
                if val > 0:
                    label_text = f"{val/1000:.0f} KB" if val >= 10000 else f"{val:.0f} B"
                    ax.text(bar.get_x() + bar.get_width() / 2,
                            bar.get_height() * 1.02,
                            label_text, ha="center", va="bottom", fontsize=8)

        # Annotate ratio
        for j, mt in enumerate(types):
            cdr_s = size_data["cdr"][j]
            json_s = size_data["json"][j]
            if cdr_s > 0 and json_s > 0:
                ratio = json_s / cdr_s
                y_pos = max(cdr_s, json_s) * 1.15
                ax.text(j, y_pos, f"{ratio:.1f}×", ha="center", va="bottom",
                        fontsize=10, fontweight="bold", color="#333")

        ax.set_xticks(x)
        ax.set_xticklabels([MSG_TYPE_LABELS.get(mt, mt) for mt in types])
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", alpha=0.3)
        ax.legend()

    # Axis labels
    if has_large:
        ax_small.set_ylabel("Message size (bytes)")
        ax_large.set_ylabel("Message size (bytes)")
        fig.suptitle("Payload Size — CDR vs JSON", fontsize=13)
    else:
        ax_small.set_ylabel("Message size (bytes)")
        ax_small.set_title("Payload Size — CDR vs JSON")

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


def _fmt_size(val):
    """Format byte size with thousand separators for LaTeX."""
    if val >= 10000:
        return f"{val:,.0f}".replace(",", r"\,")
    return f"{val:.0f}"


def _fmt_latency(val_ns, use_ms=False):
    """Format latency from ns to µs or ms string."""
    if use_ms:
        return f"{val_ns / 1e6:.2f}"
    return f"{val_ns / 1e3:.1f}"


TEX_LABELS = {
    "navsatfix": "NavSatFix",
    "odometry": "Odometry (zero cov.)",
    "odometry_fullcov": "Odometry (full cov.)",
    "pointcloud2": "PointCloud2 (10k pts)",
}


def generate_latex_table(records, output_dir):
    """Generate summary_table.tex and benchmark_report.tex."""
    subset_10 = [r for r in records if r["rate"] == 10]
    subset_500 = [r for r in records if r["rate"] == 500]
    groups_10 = group_by(subset_10, "msg_type", "format")
    groups_500 = group_by(subset_500, "msg_type", "format")

    active_types = [mt for mt in MSG_TYPE_ORDER
                    if groups_10.get((mt, "cdr")) and groups_10.get((mt, "json"))]

    # ── summary_table.tex (compact, single table) ──
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{CDR vs JSON serialization comparison at 10\,Hz publish rate.}",
        r"\label{tab:cdr_vs_json}",
        r"\small",
        r"\setlength{\tabcolsep}{3pt}",
        r"\begin{tabular}{llrrrr}",
        r"\toprule",
        r"\textbf{Message type} & \textbf{Format} & \textbf{Size} & \textbf{Avg} & \textbf{P99} & \textbf{Slowdown} \\",
        r"& & (B) & (\textmu s) & (\textmu s) & \\",
        r"\midrule",
    ]

    for mt in active_types:
        cdr_recs = groups_10[(mt, "cdr")]
        json_recs = groups_10[(mt, "json")]
        cdr_size, _ = agg_across_runs(cdr_recs, "msg_size_avg")
        json_size, _ = agg_across_runs(json_recs, "msg_size_avg")
        cdr_lat, _ = agg_across_runs(cdr_recs, "serialize_avg_ns")
        json_lat, _ = agg_across_runs(json_recs, "serialize_avg_ns")
        cdr_p99, _ = agg_across_runs(cdr_recs, "serialize_p99_ns")
        json_p99, _ = agg_across_runs(json_recs, "serialize_p99_ns")
        size_ratio = json_size / cdr_size if cdr_size > 0 else 0
        lat_ratio = json_lat / cdr_lat if cdr_lat > 0 else 0

        is_pc = (mt == "pointcloud2")
        unit_note = " (ms)" if is_pc else ""
        label = TEX_LABELS.get(mt, mt)

        lines.append(
            f"{label} & CDR & {_fmt_size(cdr_size)} & "
            f"{_fmt_latency(cdr_lat, is_pc)} & "
            f"{_fmt_latency(cdr_p99, is_pc)} & --- \\\\"
        )
        lines.append(
            f" & JSON & {_fmt_size(json_size)} & "
            f"{_fmt_latency(json_lat, is_pc)} & "
            f"{_fmt_latency(json_p99, is_pc)} & "
            f"{size_ratio:.1f}$\\times$ size, {lat_ratio:.0f}$\\times$ lat \\\\"
        )
        if mt != active_types[-1]:
            lines.append(r"\addlinespace")

    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\begin{flushleft}",
        r"\footnotesize PointCloud2 latencies reported in milliseconds; all others in microseconds.",
        r"\end{flushleft}",
        r"\end{table}",
    ]

    tex_path = os.path.join(output_dir, "summary_table.tex")
    with open(tex_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print("  -> summary_table.tex")

    # ── benchmark_report.tex (full multi-table report) ──
    report = []
    report.append(r"% CDR vs JSON Serialization Benchmark — LaTeX tables")
    report.append(r"% Auto-generated by analyze_results.py")
    report.append(r"% Include in paper with: \input{benchmark_report.tex}")
    report.append("")

    # TABLE 1: Payload size
    report += [
        r"%%% TABLE 1: Payload size comparison",
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{Payload size: CDR (fixed binary) vs JSON (text, varies with numeric precision).}",
        r"\label{tab:payload_size}",
        r"\small",
        r"\setlength{\tabcolsep}{4pt}",
        r"\begin{tabular}{lrrr}",
        r"\toprule",
        r"\textbf{Message type} & \textbf{CDR (B)} & \textbf{JSON (B)} & \textbf{Ratio} \\",
        r"\midrule",
    ]
    for mt in active_types:
        cdr_size, _ = agg_across_runs(groups_10[(mt, "cdr")], "msg_size_avg")
        json_size, _ = agg_across_runs(groups_10[(mt, "json")], "msg_size_avg")
        ratio = json_size / cdr_size if cdr_size > 0 else 0
        label = TEX_LABELS.get(mt, mt)
        report.append(
            f"{label} & {_fmt_size(cdr_size)} & {_fmt_size(json_size)} & {ratio:.1f}$\\times$ \\\\"
        )
    report += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]

    # TABLE 2: Serialization latency
    report += [
        r"%%% TABLE 2: Serialization latency at 10 Hz",
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{Serialization latency at 10\,Hz (mean over 3 repetitions, 60\,s each).}",
        r"\label{tab:serialize_latency}",
        r"\small",
        r"\setlength{\tabcolsep}{3pt}",
        r"\begin{tabular}{llrrrr}",
        r"\toprule",
        r"\textbf{Message type} & \textbf{Format} & \textbf{Avg} & \textbf{P95} & \textbf{P99} & \textbf{Slowdown} \\",
        r"\midrule",
    ]
    for mt in active_types:
        cdr_recs = groups_10[(mt, "cdr")]
        json_recs = groups_10[(mt, "json")]
        is_pc = (mt == "pointcloud2")
        unit = "ms" if is_pc else r"\textmu s"
        label = TEX_LABELS.get(mt, mt)

        cdr_avg, _ = agg_across_runs(cdr_recs, "serialize_avg_ns")
        cdr_p95, _ = agg_across_runs(cdr_recs, "serialize_p95_ns")
        cdr_p99, _ = agg_across_runs(cdr_recs, "serialize_p99_ns")
        json_avg, _ = agg_across_runs(json_recs, "serialize_avg_ns")
        json_p95, _ = agg_across_runs(json_recs, "serialize_p95_ns")
        json_p99, _ = agg_across_runs(json_recs, "serialize_p99_ns")
        slowdown = json_avg / cdr_avg if cdr_avg > 0 else 0

        report.append(
            f"{label} & CDR & {_fmt_latency(cdr_avg, is_pc)} & "
            f"{_fmt_latency(cdr_p95, is_pc)} & {_fmt_latency(cdr_p99, is_pc)} "
            f"& --- \\\\"
        )
        report.append(
            f" & JSON & {_fmt_latency(json_avg, is_pc)} & "
            f"{_fmt_latency(json_p95, is_pc)} & {_fmt_latency(json_p99, is_pc)} "
            f"& {slowdown:.0f}$\\times$ \\\\"
        )
        if mt != active_types[-1]:
            report.append(r"\addlinespace")

    report += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\begin{flushleft}",
        r"\footnotesize All values in \textmu s except PointCloud2 (ms).",
        r"\end{flushleft}",
        r"\end{table}",
        "",
    ]

    # TABLE 3: Throughput at 500 Hz
    report += [
        r"%%% TABLE 3: Serialization throughput at 500 Hz",
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{Serialization throughput at 500\,Hz.}",
        r"\label{tab:throughput}",
        r"\small",
        r"\begin{tabular}{lrrr}",
        r"\toprule",
        r"\textbf{Message type} & \textbf{CDR (MB/s)} & \textbf{JSON (MB/s)} & \textbf{Ratio} \\",
        r"\midrule",
    ]
    for mt in active_types:
        cdr_recs = groups_500.get((mt, "cdr"), [])
        json_recs = groups_500.get((mt, "json"), [])
        if not cdr_recs or not json_recs:
            continue
        cdr_tp, _ = agg_across_runs(cdr_recs, "serialize_mb_per_sec")
        json_tp, _ = agg_across_runs(json_recs, "serialize_mb_per_sec")
        ratio = cdr_tp / json_tp if json_tp > 0 else 0
        label = TEX_LABELS.get(mt, mt)
        cdr_str = f"{cdr_tp:,.0f}".replace(",", r"\,") if cdr_tp >= 1000 else f"{cdr_tp:.1f}"
        report.append(
            f"{label} & {cdr_str} & {json_tp:.1f} & {ratio:.0f}$\\times$ \\\\"
        )
    report += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]

    # TABLE 4: Size dependency
    if "odometry" in active_types and "odometry_fullcov" in active_types:
        cdr_z, _ = agg_across_runs(groups_10[("odometry", "cdr")], "msg_size_avg")
        json_z, _ = agg_across_runs(groups_10[("odometry", "json")], "msg_size_avg")
        cdr_f, _ = agg_across_runs(groups_10[("odometry_fullcov", "cdr")], "msg_size_avg")
        json_f, _ = agg_across_runs(groups_10[("odometry_fullcov", "json")], "msg_size_avg")
        pct = ((json_f - json_z) / json_z) * 100 if json_z > 0 else 0

        report += [
            r"%%% TABLE 4: Size dependency on data content",
            r"\begin{table}[ht]",
            r"\centering",
            r"\caption{JSON size depends on numeric content. Both Odometry variants have identical CDR size but JSON differs by %.1f$\times$ due to covariance values.}" % (json_f / json_z),
            r"\label{tab:size_dependency}",
            r"\small",
            r"\begin{tabular}{lrrl}",
            r"\toprule",
            r"\textbf{Variant} & \textbf{CDR (B)} & \textbf{JSON (B)} & \textbf{Covariance content} \\",
            r"\midrule",
            f"Odometry (zero cov.) & {cdr_z:.0f} & {json_z:.0f} & \\texttt{{0.0}} (1--3 chars/entry) \\\\",
            f"Odometry (full cov.) & {cdr_f:.0f} & {_fmt_size(json_f)} & \\texttt{{0.025...}} (8--12 chars/entry) \\\\",
            r"\addlinespace",
            f"\\multicolumn{{3}}{{l}}{{\\textit{{Size increase}}}} & $+{pct:.0f}\\%$ JSON, $+0\\%$ CDR \\\\",
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table}",
        ]

    report_path = os.path.join(output_dir, "benchmark_report.tex")
    with open(report_path, "w") as f:
        f.write("\n".join(report) + "\n")
    print("  -> benchmark_report.tex")


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
