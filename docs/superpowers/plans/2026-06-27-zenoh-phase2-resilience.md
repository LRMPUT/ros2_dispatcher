# Zenoh Phase 2a — Distributed Resilience Experiment

- **Date:** 2026-06-27
- **Status:** In progress (controller-authored infra/scripts; not subagent-driven)
- **Builds on:** Phase 1 (`zenoh_sink`, `zenoh_gis_query`) — no new C++.
- **Design:** `docs/superpowers/specs/2026-06-26-zenoh-data-pipeline-design.md` §6/§7 (Phase 2).

## Goal

Demonstrate the contribution's thesis — Zenoh's resilience under intermittent /
geo-distributed networks — by running the Phase-1 query pipeline across an
emulated edge/fog/cloud topology with injected network faults, and measuring
**query completeness, QoD fidelity, alert latency, and membership recovery time**
as functions of the fault level. These are exactly the results the
Kafka/MQTT-on-ideal-LAN papers lack.

## Key design decisions

1. **No router daemon / no storage plugins.** The edge sinks connect **directly**
   to the cloud query node via Zenoh **peer** mode + `zenoh.connect`/`zenoh.listen`
   (built in Phase 1). This sidesteps the Task-12 blocker (`zenohd` +
   storage-manager + backends are not in the ROS vendor packages). Storage/Grafana
   are a separate follow-on (Phase 2b).
2. **Topology = star over an emulated WAN.** `cloud_query` (peer, listens
   `tcp/0.0.0.0:7447`) is the hub; `edge_robot_K` (peer, connects
   `tcp/cloud_query:7447`, declares liveliness `gis/live/robot_K`) are spokes.
   `tc netem` is applied on the edge↔cloud path to inject loss/latency/partition.
3. **The QoD already implemented is the resilience signal.** `completeness =
   contributed/expected` and `fidelity` (staleness) come straight from the
   queryable replies; no new measurement code in the nodes.
4. **docker-compose** (the repo already uses it) with `NET_ADMIN` for `tc`.

## Topology

```
 ┌ edge_robot_1 ┐   ┌ edge_robot_2 ┐  ...  (peers; sink + synthetic NavSatFix pub;
 │ zenoh_sink   │   │ zenoh_sink   │        liveliness gis/live/robot_K;
 │ + publisher  │   │ + publisher  │        zenoh.connect tcp/cloud_query:7447)
 └──────┬───────┘   └──────┬───────┘
        │  tc netem (loss/latency/partition) on each edge iface
        └─────────┬────────┘
            docker network "wan"
                  │
          ┌───────┴────────┐
          │  cloud_query    │  zenoh_gis_query (peer, listen tcp/0.0.0.0:7447)
          │  + workload     │  queryables gis/query/** + gis/alert/**
          └─────────────────┘
```

## Components (all new, all infra/scripts — no C++)

| File | Role |
| --- | --- |
| `tools/zenoh_gis/phase2/docker-compose.phase2.yml` | edge_robot_K × N + cloud_query services on a `wan` network; `NET_ADMIN`; uses `rkd:gis` image |
| `tools/zenoh_gis/phase2/cloud_query.yaml` | `zenoh_gis_query` params: peer, `zenoh.listen=["tcp/0.0.0.0:7447"]`, key_expr `ros2/**` |
| `tools/zenoh_gis/phase2/edge_sink.yaml.tmpl` | `zenoh_sink` params template: peer, `zenoh.connect=["tcp/cloud_query:7447"]`, `gis.liveliness_key=gis/live/robot_K` |
| `tools/zenoh_gis/phase2/netem.sh` | apply/clear `tc qdisc netem` (loss %, delay ms, 100% loss = partition) on a container's iface |
| `tools/zenoh_gis/phase2/run_resilience.sh` | orchestrator: for each scenario, bring up the stack, run the workload, collect metrics → CSV, tear down |
| `tools/zenoh_gis/phase2/workload.py` | per-edge synthetic NavSatFix publisher (toggles P12 boundary, stamp=now) + cloud-side periodic `gis/query/*` poller + `gis/alert/**` latency subscriber |
| `tools/zenoh_gis/phase2/README.md` | how to run; scenario list; CSV→results mapping |

## Fault scenarios (the sweep)

| Scenario | netem | Expected Zenoh behavior |
| --- | --- | --- |
| `clean` | none | completeness≈1.0, low latency (baseline) |
| `loss_5` / `loss_20` / `loss_50` | `loss 5%/20%/50%` | completeness/fidelity degrade gracefully; quantify |
| `delay_50` / `delay_200` | `delay 50ms/200ms` | alert latency rises ~linearly; completeness stays high |
| `partition_heal` | `loss 100%` for T, then clear | affected robots drop from live set (liveliness), then **recover**; measure recovery time |
| `churn` | kill+restart an edge container | membership drop+rejoin; measure rejoin time |

## Metrics → results table

Per scenario/fault-level CSV row:
`scenario,n_robots,fault,completeness_avg,fidelity_avg,alert_latency_p50_ms,alert_latency_p99_ms,recovery_s`
- `completeness/fidelity`: averaged over periodic `gis/query/geofence` QoD replies during the window.
- `alert_latency`: from `gis/alert/**` (recv − sample stamp).
- `recovery_s`: for partition/churn — time from fault-clear to completeness returning to ≥0.9.

This populates a **resilience** column the prior papers do not have.

## Task breakdown (controller-authored, verified incrementally)

1. compose + tier configs; **verify**: `docker compose config` valid + a clean-network smoke (cloud + 1 edge → geofence query returns robot_1).
2. `netem.sh` + `workload.py`; **verify**: apply 20% loss on the edge, confirm completeness drops vs clean.
3. `run_resilience.sh` orchestrator + CSV; **verify**: a 2-scenario mini-sweep (clean + loss_20) produces a CSV with sensible rows.
4. `partition_heal` + `churn` recovery measurement; **verify**: a partition run shows a robot dropping then recovering with a measured recovery_s.
5. README + results aggregation note.

## Out of scope (Phase 2b, separate)

Zenoh router + storage-manager + InfluxDB backend + Grafana dashboards (the
storage/history/visualization stack — gated on obtaining the zenoh plugins).
