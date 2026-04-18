#!/usr/bin/env python3
"""Generate the data flow diagram: ROS 2 message → serialization → broker → consumer."""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

# Colors — reuse palette from architecture figure
C_ROS2 = "#E3F2FD"
C_ROS2_EDGE = "#2196F3"
C_SINK = "#FFF3E0"
C_SINK_EDGE = "#FF9800"
C_BROKER = "#FFEBEE"
C_BROKER_EDGE = "#F44336"
C_LIB = "#E8F5E9"
C_LIB_EDGE = "#4CAF50"
C_GRAY = "#455A64"
C_WHITE = "#FFFFFF"
C_MQTT = "#E0F7FA"
C_MQTT_EDGE = "#00897B"

TITLE_FONT_SIZE = 20
LABEL_FONT_SIZE = 18
SUBLABEL_FONT_SIZE = 16


def rounded_box(ax, x, y, w, h, label, sublabel=None, fc="#fff", ec="#000", lw=1.4):
    box = FancyBboxPatch(
        (x - w / 2, y - h / 2), w, h,
        boxstyle="round,pad=0.15", fc=fc, ec=ec, lw=lw, zorder=2,
    )
    ax.add_patch(box)
    if sublabel:
        ax.text(x, y + 0.20, label, ha="center", va="center",
                fontsize=LABEL_FONT_SIZE, fontweight="bold", zorder=3)
        ax.text(x, y - 0.20, sublabel, ha="center", va="center",
                fontsize=SUBLABEL_FONT_SIZE, fontstyle="italic", color=C_GRAY, zorder=3)
    else:
        ax.text(x, y, label, ha="center", va="center",
                fontsize=LABEL_FONT_SIZE, fontweight="bold", zorder=3)
    return box


def arrow(ax, x0, y0, x1, y1, color=C_GRAY, style="-", lw=1.4, label=None,
          label_side="right"):
    ls = "--" if style == "dashed" else "-"
    ax.annotate(
        "", xy=(x1, y1), xytext=(x0, y0),
        arrowprops=dict(
            arrowstyle="-|>", color=color, lw=lw, linestyle=ls,
            shrinkA=2, shrinkB=2,
        ),
        zorder=1,
    )
    if label:
        mx, my = (x0 + x1) / 2, (y0 + y1) / 2
        offset = 0.15 if label_side == "right" else -0.15
        ax.text(mx + offset, my, label, fontsize=LABEL_FONT_SIZE, color=color,
                ha="left" if label_side == "right" else "right",
                va="center", fontstyle="italic", zorder=3)


fig, axes = plt.subplots(1, 2, figsize=(16, 28))
fig.subplots_adjust(wspace=0.5)

# =========================================================
# LEFT COLUMN: ROS 2 → Kafka
# =========================================================
ax = axes[0]
ax.set_xlim(-6.0, 6.0)
ax.set_ylim(-11.5, 7.0)
ax.set_aspect("equal")
ax.axis("off")

ax.text(0, 6.2, "ROS 2 → Kafka", ha="center", va="center",
        fontsize=TITLE_FONT_SIZE, fontweight="bold", color=C_GRAY)

# Nodes
y_positions = [5.0, 2.5, 0.0, -2.5, -5.0, -7.5, -10.0]

rounded_box(ax, 0, y_positions[0], 9.0, 1.5,
            "ROS 2 Publisher",
            "raw message (DDS)",
            fc=C_ROS2, ec=C_ROS2_EDGE)

rounded_box(ax, 0, y_positions[1], 9.0, 1.5,
            "kafka_sink callback",
            "GenericSubscription receives message",
            fc=C_SINK, ec=C_SINK_EDGE)

rounded_box(ax, 0, y_positions[2], 9.0, 1.5,
            "Serialization",
            "rosidl introspection → CDR or JSON",
            fc=C_SINK, ec=C_SINK_EDGE)

rounded_box(ax, 0, y_positions[3], 9.0, 1.5,
            "KafkaProducer",
            "enqueue msg + headers to bounded queue",
            fc=C_LIB, ec=C_LIB_EDGE)

rounded_box(ax, 0, y_positions[4], 9.0, 1.5,
            "librdkafka poll thread",
            "batch and send to broker",
            fc=C_LIB, ec=C_LIB_EDGE)

rounded_box(ax, 0, y_positions[5], 9.0, 1.5,
            "Apache Kafka partition",
            "stored, replicated",
            fc=C_BROKER, ec=C_BROKER_EDGE)

rounded_box(ax, 0, y_positions[6], 9.0, 1.5,
            "Downstream consumers",
            "analytics, ML pipelines, GIS",
            fc=C_WHITE, ec=C_GRAY)

# Arrows
for i in range(len(y_positions) - 1):
    arrow(ax, 0, y_positions[i] - 0.78, 0, y_positions[i + 1] + 0.78,
          color=C_GRAY)

# =========================================================
# RIGHT COLUMN: ROS 2 → MQTT
# =========================================================
ax = axes[1]
ax.set_xlim(-6.0, 6.0)
ax.set_ylim(-11.5, 7.0)
ax.set_aspect("equal")
ax.axis("off")

ax.text(0, 6.2, "ROS 2 → MQTT", ha="center", va="center",
        fontsize=TITLE_FONT_SIZE, fontweight="bold", color=C_GRAY)

y_mqtt = [5.0, 2.5, 0.0, -2.5, -5.0, -7.5]

rounded_box(ax, 0, y_mqtt[0], 9.0, 1.5,
            "ROS 2 Publisher",
            "raw message (DDS)",
            fc=C_ROS2, ec=C_ROS2_EDGE)

rounded_box(ax, 0, y_mqtt[1], 9.0, 1.5,
            "mosquitto_sink callback",
            "GenericSubscription receives message",
            fc=C_SINK, ec=C_SINK_EDGE)

rounded_box(ax, 0, y_mqtt[2], 9.0, 1.5,
            "Serialization",
            "rosidl introspection → CDR or JSON",
            fc=C_SINK, ec=C_SINK_EDGE)

rounded_box(ax, 0, y_mqtt[3], 9.0, 1.5,
            "Paho MQTT client",
            "publish to MQTT topic (QoS 0/1/2)",
            fc=C_MQTT, ec=C_MQTT_EDGE)

rounded_box(ax, 0, y_mqtt[4], 9.0, 1.5,
            "MQTT Broker",
            "Mosquitto — stored, forwarded",
            fc=C_BROKER, ec=C_BROKER_EDGE)

rounded_box(ax, 0, y_mqtt[5], 9.0, 1.5,
            "Downstream subscribers",
            "analytics, ML pipelines, GIS",
            fc=C_WHITE, ec=C_GRAY)

for i in range(len(y_mqtt) - 1):
    arrow(ax, 0, y_mqtt[i] - 0.78, 0, y_mqtt[i + 1] + 0.78,
          color=C_GRAY)

plt.tight_layout()

_dir = os.path.dirname(os.path.abspath(__file__))
out = os.path.join(_dir, "dataflow_figure.pdf")
fig.savefig(out, dpi=600, bbox_inches="tight")
print(f"Saved {out}")

out_png = os.path.join(_dir, "dataflow_figure.png")
fig.savefig(out_png, dpi=400, bbox_inches="tight")
print(f"Saved {out_png}")
