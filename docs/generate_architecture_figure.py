#!/usr/bin/env python3
"""Generate the ROS 2 → Dispatcher → Kafka/MQTT architecture figure."""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

# Colors
C_ROS2 = "#E3F2FD"
C_ROS2_EDGE = "#2196F3"
C_CTRL = "#E8F5E9"
C_CTRL_EDGE = "#4CAF50"
C_SINK = "#FFF3E0"
C_SINK_EDGE = "#FF9800"
C_BROKER = "#FFEBEE"
C_BROKER_EDGE = "#F44336"
C_TOOL = "#F3E5F5"
C_TOOL_EDGE = "#9C27B0"
C_GRAY = "#455A64"
C_WHITE = "#FFFFFF"


def rounded_box(ax, x, y, w, h, label, sublabel=None, fc="#fff", ec="#000", lw=2.0):
    """Draw a rounded box with centered text."""
    box = FancyBboxPatch(
        (x - w / 2, y - h / 2), w, h,
        boxstyle="round,pad=0.08", fc=fc, ec=ec, lw=lw, zorder=2,
    )
    ax.add_patch(box)
    if sublabel:
        ax.text(x, y + 0.14, label, ha="center", va="center",
                fontsize=14, fontweight="bold", zorder=3)
        ax.text(x, y - 0.14, sublabel, ha="center", va="center",
                fontsize=10, fontstyle="italic", color=C_GRAY, zorder=3)
    else:
        ax.text(x, y, label, ha="center", va="center",
                fontsize=14, fontweight="bold", zorder=3)
    return box


def arrow(ax, x0, y0, x1, y1, color=C_GRAY, style="-", lw=2.0, label=None,
          label_side="right", connectionstyle="arc3,rad=0"):
    ls = "--" if style == "dashed" else "-"
    ax.annotate(
        "", xy=(x1, y1), xytext=(x0, y0),
        arrowprops=dict(
            arrowstyle="-|>", color=color, lw=lw, linestyle=ls,
            connectionstyle=connectionstyle, shrinkA=2, shrinkB=2,
        ),
        zorder=1,
    )
    if label:
        mx, my = (x0 + x1) / 2, (y0 + y1) / 2
        offset = 0.12 if label_side == "right" else -0.12
        ax.text(mx + offset, my, label, fontsize=14, color=color,
                ha="left" if label_side == "right" else "right",
                va="center", fontstyle="italic", zorder=3)


fig, ax = plt.subplots(1, 1, figsize=(8, 10))
ax.set_xlim(-5.5, 5.5)
ax.set_ylim(-5.8, 4.8)
ax.set_aspect("equal")
ax.axis("off")

# ── Row 0: ROS 2 Graph ──
rounded_box(ax, 0, 3.8, 7.5, 1.0,
            "ROS 2 Graph",
            "/camera/image    /odom    /scan    /cmd_vel    ...",
            fc=C_ROS2, ec=C_ROS2_EDGE)

# ── Row 1: introspection_manager ──
rounded_box(ax, 0, 2.2, 4.2, 0.7,
            "introspection_manager",
            "graph monitor & topic discovery",
            fc=C_ROS2, ec=C_ROS2_EDGE)

# ── Row 2: dispatcher_controller + GUI + topic_tools ──
rounded_box(ax, 0, 0.8, 4.2, 0.7,
            "dispatcher_controller",
            "selection \u2022 lifecycle \u2022 topic_tools",
            fc=C_CTRL, ec=C_CTRL_EDGE)

rounded_box(ax, -4.0, 0.8, 1.8, 0.6,
            "GUI / CLI", fc=C_WHITE, ec=C_GRAY)

rounded_box(ax, 4.0, 0.8, 2.4, 0.6,
            "topic_tools",
            "Throttle, Drop, Delay",
            fc=C_TOOL, ec=C_TOOL_EDGE)

# ── Row 3: Sinks ──
rounded_box(ax, -1.8, -0.8, 3.0, 0.85,
            "kafka_sink",
            "Lifecycle Node \u2022 CDR / JSON",
            fc=C_SINK, ec=C_SINK_EDGE)

rounded_box(ax, 1.8, -0.8, 3.0, 0.85,
            "mosquitto_sink",
            "Lifecycle Node \u2022 CDR / JSON",
            fc=C_SINK, ec=C_SINK_EDGE)

# ── Row 4: Brokers ──
rounded_box(ax, -1.8, -2.4, 3.0, 0.85,
            "Apache Kafka",
            "broker cluster",
            fc=C_BROKER, ec=C_BROKER_EDGE)

rounded_box(ax, 1.8, -2.4, 3.0, 0.85,
            "MQTT Broker",
            "Mosquitto",
            fc=C_BROKER, ec=C_BROKER_EDGE)

# ── Row 5: Downstream ──
rounded_box(ax, 0, -3.9, 4.0, 0.7,
            "Downstream",
            "analytics \u2022 ML \u2022 GIS",
            fc=C_WHITE, ec=C_GRAY)

# ── Arrows ──

# Graph -> introspection_manager
arrow(ax, 0, 3.28, 0, 2.57, color=C_ROS2_EDGE, label="topic discovery")

# introspection_manager -> dispatcher_controller
arrow(ax, 0, 1.85, 0, 1.17, color=C_ROS2_EDGE, label="TopicsInfo")

# GUI <-> dispatcher_controller
arrow(ax, -3.08, 0.85, -2.12, 0.85, color=C_GRAY, label_side="right")
arrow(ax, -2.12, 0.75, -3.08, 0.75, color=C_GRAY)

# dispatcher_controller -> topic_tools
arrow(ax, 2.12, 0.8, 2.78, 0.8, color=C_TOOL_EDGE, label_side="right")

# dispatcher_controller -> kafka_sink (configure/activate)
arrow(ax, -0.6, 0.43, -1.5, -0.35, color=C_CTRL_EDGE,
      label_side="left")

# dispatcher_controller -> mosquitto_sink (configure/activate)
arrow(ax, 0.6, 0.43, 1.5, -0.35, color=C_CTRL_EDGE,
      label_side="right")

# topic_tools -> kafka_sink (transformed topics, dashed)
arrow(ax, 3.3, 0.48, -0.8, -0.35, color=C_TOOL_EDGE, style="dashed",
     label_side="left")

# topic_tools -> mosquitto_sink (transformed topics, dashed)
arrow(ax, 3.7, 0.48, 2.2, -0.35, color=C_TOOL_EDGE, style="dashed")

# kafka_sink -> Kafka
arrow(ax, -1.8, -1.25, -1.8, -1.95, color=C_BROKER_EDGE, label="produce")

# mosquitto_sink -> MQTT
arrow(ax, 1.8, -1.25, 1.8, -1.95, color=C_BROKER_EDGE, label="publish")

# Kafka -> Downstream
arrow(ax, -1.8, -2.85, -0.5, -3.52, color=C_GRAY, label="consume")

# MQTT -> Downstream
arrow(ax, 1.8, -2.85, 0.5, -3.52, color=C_GRAY, label="consume")

# subscriptions_yaml label
# ax.text(0, 0.15, "subscriptions_yaml", fontsize=14, color=C_CTRL_EDGE,
#         ha="center", va="center", fontstyle="italic",
#         bbox=dict(boxstyle="round,pad=0.15", fc=C_CTRL, ec=C_CTRL_EDGE, lw=0.6))

# ── Legend ──
legend_items = [
    (C_ROS2, C_ROS2_EDGE, "ROS 2 nodes"),
    (C_CTRL, C_CTRL_EDGE, "Control plane"),
    (C_SINK, C_SINK_EDGE, "Sink nodes"),
    (C_BROKER, C_BROKER_EDGE, "Message brokers"),
    (C_TOOL, C_TOOL_EDGE, "Plugins"),
]
cols = 3
for i, (fc, ec, label) in enumerate(legend_items):
    col = i % cols
    row = i // cols
    x = -4.0 + col * 3.2
    y = -5.0 - row * 0.45
    box = FancyBboxPatch(
        (x, y - 0.14), 0.35, 0.28,
        boxstyle="round,pad=0.03", fc=fc, ec=ec, lw=1.2, zorder=2,
    )
    ax.add_patch(box)
    ax.text(x + 0.5, y, label, fontsize=13, va="center", zorder=3)

plt.tight_layout()
_dir = os.path.dirname(os.path.abspath(__file__))
out = os.path.join(_dir, "architecture_figure.pdf")
fig.savefig(out, dpi=600, bbox_inches="tight")
print(f"Saved {out}")

# Also save PNG for quick preview
out_png = os.path.join(_dir, "architecture_figure.png")
fig.savefig(out_png, dpi=400, bbox_inches="tight")
print(f"Saved {out_png}")
