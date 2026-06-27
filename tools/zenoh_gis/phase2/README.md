# Zenoh Phase 2a — distributed resilience experiment

Runs the Phase-1 Zenoh pipeline across an emulated **edge ↔ WAN ↔ cloud** topology
and measures how delivery, latency, and membership behave under injected network
faults (`tc netem`). This produces the **resilience** result the Kafka/MQTT
papers (measured on an ideal LAN) lack.

## Topology

- **cloud_query** — `zenoh_source` as a Zenoh *peer* that **listens** on
  `tcp/0.0.0.0:7447` and republishes `ros2/**` → `/zenoh_decoded/**`, so delivery
  can be measured with plain ROS tools (the image ships no python-zenoh client).
- **edge_robot_K** — `zenoh_sink` (peer, **connects** to `tcp/cloud_query:7447`,
  declares liveliness `gis/live/robot_K`) + a synthetic NavSatFix publisher.
- `ROS_LOCALHOST_ONLY=1` so only **Zenoh (TCP)** crosses containers, not ROS/DDS.
- **No `zenohd` router / storage plugins needed** — direct peer connections reuse
  the Phase-1 `zenoh.connect`/`zenoh.listen` params.

## Run

```bash
# builds rkd:gis-netem (= rkd:gis + iproute2) on first run; needs the rkd:gis
# image from Phase 1 (docker build -f docker/Dockerfile -t rkd:gis .)
STEP_S=10 tools/zenoh_gis/phase2/run_resilience.sh
```

It brings up the stack, runs a fault timeline (**clean → 50% loss → partition →
recover**) via `netem.sh`, and prints the cloud-side received-rate time series
annotated by phase. `netem.sh` can also apply `loss_<pct>`, `delay_<ms>`,
`partition`, `clear` to any service for custom sweeps.

## Headline result (1 edge, 5 Hz, STEP_S=10)

```
phase       recv/s        latency        membership
clean       5.0 (=pub)    ~1.3 ms        robot present
loss_50     ~2-7 erratic  40-700 ms      degraded (TCP retransmit)
partition   0             -              robot drops off
recover     5.0 by ~3-4s  ~1.2 ms        auto-reconnect, no reconfig
```

Interpretation: the brokerless mesh **degrades gracefully** under loss, **tolerates
partition** cleanly, and **recovers automatically** ~3–4 s after the link heals —
with no broker/topic reconfiguration. This is the dynamic-membership /
intermittent-source behavior the design targets.

## Files

| File | Role |
| --- | --- |
| `docker-compose.phase2.yml` | cloud + edge services (1-edge reference; scale via more `edge_robot_*`) |
| `Dockerfile.netem` | `rkd:gis` + `iproute2` (keeps `tc` out of the production image) |
| `cloud_entrypoint.sh` / `edge_entrypoint.sh` | per-tier node launch + lifecycle |
| `workload_pub.py` | synthetic NavSatFix toggling the P12 boundary |
| `metrics_collector.py` | cloud-side received-rate + latency per interval → CSV |
| `netem.sh` | apply/clear `tc netem` (loss/delay/partition) on a service |
| `run_resilience.sh` | orchestrates the fault timeline + prints the annotated series |
| `results/timeline.csv` | last run's time series |

## Map to the paper

`recv/s` ↔ throughput/drop column; `latency_avg_ms` ↔ latency column; the
partition→recover segment gives a **recovery-time** result with no analogue in the
broker-centric comparisons. Extend `run_resilience.sh` for the full sweep
(loss 5/20/50, delay 50/200, N∈{1,5,10,25,50}, churn) — same harness.

## Scope

Phase 2a = the resilience experiment (this directory). **Phase 2b** (separate):
a real zenoh router + `zenoh-plugin-storage-manager` + InfluxDB backend + Grafana
dashboards — gated on obtaining the zenoh plugins (not in the ROS vendor packages).
