#!/usr/bin/env python3
"""Replay a NavSatFix bag with per-robot origin shift and live re-stamping.

Container entrypoint: derive ROBOT_ID from the hostname suffix, open the
bag at $BAG_PATH, loop forever, publish at $RATE_HZ. The published
message's header.stamp encodes time.time_ns() at publish time, which
the e2e consumer subtracts to compute latency.
"""
from __future__ import annotations

from sensor_msgs.msg import NavSatFix

LAT_OFFSET_DEG_PER_ID = 0.0001  # ~11 m at the equator; close enough
LON_OFFSET_DEG_PER_ID = 0.0001


def shift_navsatfix(msg: NavSatFix, robot_id: int) -> None:
    """Apply a deterministic per-robot offset to lat/lon. In-place."""
    msg.latitude += LAT_OFFSET_DEG_PER_ID * robot_id
    msg.longitude += LON_OFFSET_DEG_PER_ID * robot_id


def restamp_ns(msg: NavSatFix, t_ns: int) -> None:
    """Set header.stamp to a wall-clock ns timestamp. In-place."""
    msg.header.stamp.sec = t_ns // 1_000_000_000
    msg.header.stamp.nanosec = t_ns % 1_000_000_000
