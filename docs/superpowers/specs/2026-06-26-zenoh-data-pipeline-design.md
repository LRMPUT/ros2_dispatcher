# Zenoh-Native Data Pipeline for Robotic GIS — Design

- **Date:** 2026-06-26
- **Status:** Approved (design); implementation plan pending
- **Branch context:** built on the new `zenoh_sink` / `zenoh_source` packages

## 1. Motivation & contribution

The GIS4IoRT line of work (DARLI-AP @ EDBT/ICDT workshops, ICRA AgriFood, SIGSPATIAL)
compares data-integration designs that all sit on **centralized brokers** (Apache Kafka,
MQTT) feeding **centralized analytics** (ksqlDB, Flink/GeoFlink, NebulaStream). All three
papers instantiate the "middleware-agnostic ROS 2 dispatcher" on only **two transports —
Kafka and MQTT**. SIGSPATIAL names Zenoh in related work ("Zenoh-based integration layers
and `rmw_zenoh` … address communication across unreliable or geographically distributed
networks") but never builds or measures it.

This project builds a **Zenoh-native data pipeline** as a *fourth, brokerless paradigm*.
It operationalizes the three open problems the prior papers list as future work, which the
broker model structurally cannot provide:

1. **Dynamically including robotic devices** → Zenoh gossip/scouting + liveliness, no broker reconfig.
2. **Querying intermittent robotic data sources** → Zenoh `queryable`/`get` over currently-live
   peers + edge storage.
3. **Quality of Service / Quality of Data (QoS/QoD)** → native per-publisher congestion
   control + priority (QoS) and sample attachments carrying completeness/fidelity (QoD).

Distinction from prior art: the papers cite `rmw_zenoh` (Zenoh as a ROS *middleware*
replacement for ROS↔ROS comms). This work uses Zenoh as the **GIS data-integration
transport** (the Kafka/MQTT role) with Zenoh's **storage + queryable** as a brokerless
*analytics* substrate — an unexamined use of Zenoh in this program and, to our knowledge,
the literature.

## 2. Goals & non-goals

**Goals**
- A working Zenoh-native pipeline: ROS robots → Zenoh mesh → storage → queryable GIS analytics.
- The three GIS queries from the papers: geofencing, collision detection, sensor proximity.
- A benchmark harness comparable to the papers (same INRAE data, N = 1..50 robots) producing
  baseline numbers and, in Phase 2, resilience results under injected network faults.
- Staged maturity: a research PoC/benchmark that hardens into a deployable production path.

**Non-goals (YAGNI)**
- Zenoh-Flow (explicitly out of scope — queryable-centric compute chosen).
- Replacing the existing Kafka/MQTT pipelines (this runs in parallel for comparison).
- InfluxDB v3 / SQL (the Zenoh InfluxDB backend targets v1/v2).
- Heavy GIS libraries (point-in-polygon and haversine are implemented directly).

## 3. Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| Purpose | Both, staged (PoC/benchmark → production) | Validate the contribution, then harden one architecture |
| Compute layer | **Queryable-centric** | Showcases Zenoh's built-in store+query+compute; directly demonstrates dynamic-membership / intermittent-source / QoD |
| Storage | **RocksDB now, InfluxDB+Grafana later** | Phase 1 stays "pure Zenoh, no external engine"; Phase 2 adds dashboards |
| Topology | **Staged single-host → edge/fog/cloud + fault injection** | Clean baseline first; distributed resilience is where the thesis is proven |
| Query node language | **C++ (zenoh-cpp)** | Reuses repo toolchain; fair latency vs JVM/native engines in the papers |

## 4. Architecture & data flow

```
 ┌─ EDGE (per robot, Zenoh peer) ───────────────┐
 │  rosbag replay → ROS /robotK/gps/fix          │   NavSatFix @10Hz (INRAE data)
 │       → zenoh_sink                            │   CDR payload + ros_type attachment
 │           • QoS: congestion_control/priority  │   + liveliness token gis/live/robotK
 │           • key ros2/robotK/gps/fix           │
 └───────────────────────┬───────────────────────┘
                         │  Zenoh mesh (peer/router)
 ┌─ FOG / GATEWAY ────────┴───────────────────────┐
 │  zenoh router + storage-manager (rocksdb)      │   subscribes ros2/** → RocksDB
 │  (Phase 2: + influxdb backend → Grafana)       │   history; answers get transparently
 └───────────────────────┬───────────────────────┘
                         │
 ┌─ CLOUD / ANALYTICS ────┴───────────────────────┐
 │  zenoh_gis_query  (NEW C++ node)               │   on get():
 │   • queryables: gis/query/{geofence,           │     1. liveliness sub → live-robot set
 │       collision, proximity}                    │     2. get(ros2/**) → live + RocksDB history
 │   • liveliness subscriber → membership         │     3. decode NavSatFix CDR (attachment type)
 │   • loads field polygons (GeoJSON/PostGIS)     │     4. spatial op (PiP / pairwise dist / proximity)
 │   • continuous mode → gis/alert/**             │     5. reply + QoD {completeness, fidelity}
 └───────────────────────┬───────────────────────┘
                         │
        client: z_get('gis/query/geofence?plot=P12')  →  results + QoD metadata
                Grafana (Phase 2)
```

### Components — build vs reuse

| Component | Status | Role |
|---|---|---|
| `zenoh_sink` | built | egress ROS→Zenoh, CDR + `ros_type` attachment, QoS knobs |
| `zenoh_source` | built | optional republish/debug; JSON-for-Grafana path |
| `dispatcher_controller` | exists | drives `zenoh_sink` lifecycle/selection |
| liveliness token | small add to `zenoh_sink` | declares `gis/live/<robot_id>` → brokerless membership |
| `zenoh_gis_query` | **NEW (core)** | hosts GIS queryables; live+stored gather; spatial compute; QoD |
| storage config | config-only | `zenoh-plugin-storage-manager` + rocksdb on `ros2/**` |
| benchmark harness | extend existing | reuse `tools/benchmark/scaling/` + INRAE rosbag; add fault injection |

**Data-flow novelty vs the papers:** data stays **CDR end-to-end** — the queryable decodes
only the fields it needs, using the type from the Zenoh attachment. The Kafka/ksqlDB path
*must* convert to JSON to be queryable. The query `get` transparently spans **live peers +
RocksDB history** in one call — no stream-vs-batch split (which the papers needed for
geofence-live vs geofence-historical).

## 5. Query layer

### Key space + selectors

```
gis/query/geofence?plot=P12&window=5s      → who is in / crossed plot P12
gis/query/collision?radius=2.0             → robot pairs closer than 2.0 m
gis/query/proximity?sensor=S3&range=10     → robots within 10 m of sensor S3
gis/alert/geofence/<robot>                 → continuous push (latency benchmark)
gis/live/<robot_id>                        → liveliness token = brokerless membership
```

### Two execution modes (the node runs both)

- **Pull / ad-hoc** (`z_get` on `gis/query/*`): resolve live set from liveliness → `get("ros2/**/gps/fix")`
  answered by RocksDB storage (latest + history in one call) → decode NavSatFix CDR → spatial op →
  reply with results + QoD.
- **Continuous / streaming** (latency-benchmark path): subscribe `ros2/**` → in-memory latest-position
  table → incremental compute → push `gis/alert/**`. This is timed head-to-head vs the papers'
  ksqlDB/GeoFlink/NebulaStream geofence latency.

### The three queries

| Query | Compute | Static data | Note |
|---|---|---|---|
| Geofence | point-in-polygon (ray-cast) | plot polygons (GeoJSON / PostGIS) | live alert + historical window in one queryable |
| Collision | pairwise haversine < radius among live robots | — | architectural win (below) |
| Proximity | robot ↔ static sensor distance < range | sensor locations | spatial join |

**Collision win to claim:** SIGSPATIAL had to offload collision to *external Python* because
Kafka partitioning by `robot_id` destroyed the spatial co-locality pairwise distance needs.
The Zenoh query node naturally holds **all** robots' positions (no partitioning), so collision
is a direct in-node computation — an advantage of the brokerless model, not a workaround.

### Dynamic membership & QoD

- **Membership:** each `zenoh_sink` declares liveliness token `gis/live/<robot_id>`; the query
  node subscribes to liveliness and always knows who is *currently* alive → routes/aggregates only
  over live robots. A robot leaving the mesh is auto-excluded, no broker reconfig.
- **QoD on every reply:** `completeness` = (robots returning a fresh sample) / (live ∪ registered
  set); `fidelity` = max sample staleness + estimated drop (from sink metrics / sequence gaps).
  Carried as a reply attachment.
- **QoS:** `zenoh_sink` `congestion_control` / `priority` / `express` are the per-stream delivery
  controls the benchmark sweeps — a tunable the broker path cannot express per-topic.

This turns the three SIGSPATIAL future-work items (dynamic inclusion, intermittent-source
querying, QoS/QoD) into **measured features** rather than prose.

## 6. Phasing

**Phase 1 — PoC + baseline (single host)**
- Build: liveliness token in `zenoh_sink`; `zenoh_gis_query` node (geofence → collision → proximity);
  RocksDB storage config.
- Reuse: INRAE rosbag (`rorbots_follower_leader_parcelle_1MONT.bag`) + `tools/benchmark/scaling/`,
  replayed at N = 1, 5, 10, 25, 50, NavSatFix @10 Hz — same data/scale as SIGSPATIAL.
- Measure on a clean network: end-to-end latency (continuous alert path), throughput, drop,
  per-query latency, correctness vs ground truth (target 100%).

**Phase 2 — distributed + resilience (the differentiated result)**
- Distribute: edge peers / fog router+storage / cloud queryable; `tc netem` between tiers.
- Inject: link loss %, latency/jitter, peer kill+rejoin (membership churn), network partition.
- Measure: query completeness/fidelity under faults, recovery time on rejoin, latency under
  degradation, `drop` vs `block` congestion-control behavior.
- Add: InfluxDB backend + Grafana.

## 7. Evaluation plan

Metrics mapped to the papers' axes so Zenoh slots in as a new column/paradigm:

| Axis | Metric | Extends |
|---|---|---|
| Transport | latency avg/P95/P99, throughput, drop vs N; CDR kept end-to-end | ICRA / SIGSPATIAL scaling |
| Serialization | CDR-vs-JSON size/latency, with a Zenoh row | ICRA Table 1 |
| Query | per-query latency + correctness (geofence/collision/proximity) | DARLI-AP |
| Resilience (NEW) | completeness & fidelity vs loss/churn; recovery time; partition behavior | none prior |
| QoS sweep | drop-vs-block × priority → latency/loss curves | novel knob |

Baseline comparison reuses the exact INRAE rosbag + query definitions from the papers so the
Zenoh numbers populate the existing tables directly.

## 8. Risks & assumptions

1. **RocksDB-backend time-range `get`** may be limited — mitigate by serving *live* from the
   node's in-memory position table and *history* from storage; verify range-selector support
   early in Phase 1.
2. **NavSatFix CDR decode** in the query node reuses the `zenoh_source` type-support approach.
3. Phase-1 single-host numbers are a *baseline*, not the thesis. The contribution rests on
   Phase-2 resilience results; Phase 1 must be built so the harness transfers directly to Phase 2.
4. Zenoh-Flow out of scope keeps the build tractable.

## 9. Deliverables

- **Phase 1:** `zenoh_gis_query` package; liveliness token in `zenoh_sink`; storage-manager
  config (rocksdb); baseline results + comparison table vs Kafka/MQTT papers.
- **Phase 2:** distributed compose/launch; fault-injection harness; resilience results;
  InfluxDB backend + Grafana dashboards.
