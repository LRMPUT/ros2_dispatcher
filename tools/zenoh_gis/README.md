# Zenoh GIS baseline benchmark

`run_baseline.sh` measures the **Zenoh-native continuous-geofence path** so its
numbers can be placed directly alongside the SIGSPATIAL Kafka/MQTT scaling tables.

## What it measures

For each fleet size `N`, it launches one `zenoh_gis_query` node and `N`
`zenoh_sink` instances (each a Zenoh `peer` with a liveliness token
`gis/live/robot_K`), drives every `/robot_K/gps/fix` with a synthetic NavSatFix
that toggles across the **P12** plot boundary at `RATE_HZ`, and captures the
resulting `gis/alert/geofence/**` crossing alerts with a Zenoh subscriber. Alert
latency is `receive_time − header.stamp` (single host → one clock).

## Run

```bash
tools/zenoh_gis/run_baseline.sh                 # default sweep: 1 5 10 25 50
tools/zenoh_gis/run_baseline.sh 1               # single fleet size (quick)
DURATION_S=20 RATE_HZ=10 tools/zenoh_gis/run_baseline.sh 10 25
```

Requires Docker. It builds the `rkd:gis` runtime image
(`docker build -f docker/Dockerfile -t rkd:gis .`) if it is not already present.
Results are written to `tools/zenoh_gis/results/zenoh_baseline_N<n>.csv`.

## CSV columns → SIGSPATIAL scaling-table cells

| CSV column | SIGSPATIAL table cell |
| --- | --- |
| `n_robots` | the `N` row (1, 5, 10, 25, 50) |
| `latency_avg_ms` | average end-to-end latency |
| `latency_p95_ms` | P95 latency |
| `latency_p99_ms` | P99 latency |
| `alerts_per_sec` | throughput (events/s) |
| `drop` | dropped events (generated crossings − captured alerts) |

`drop` uses the publisher's known crossing count (`N × toggles`) minus the alerts
captured by the subscriber; `latency_*` are computed from the per-alert samples
with avg and sorted-sample P95/P99 (an approximation adequate for fleet-scale
comparison).

## Data source: synthetic vs. INRAE rosbag

The dry-run uses a **deterministic synthetic NavSatFix publisher** so the
benchmark is hermetic and reproducible. For production/paper runs, replay the
INRAE field-robot data (`rorbots_follower_leader_parcelle_1MONT.bag` at the repo
root). That file is a **ROS 1** bag, so it must be converted to ROS 2 first
(e.g. with the `rosbags` toolkit: `rosbags-convert rorbots_*.bag`) and the
trajectory topics remapped to `/robot_K/gps/fix`; swap the synthetic publisher in
`run_baseline.sh` for `ros2 bag play` of the converted bag.

## Scope / status

This is the **Phase-1 baseline** hook (clean-network numbers, single host). The
continuous-alert path it exercises is verified end-to-end by the `zenoh_gis_query`
package work (see `.superpowers/sdd/` reports). Phase 2 adds the distributed
edge/cloud topology and `tc netem` fault injection
(`docs/superpowers/specs/2026-06-26-zenoh-data-pipeline-design.md`).
