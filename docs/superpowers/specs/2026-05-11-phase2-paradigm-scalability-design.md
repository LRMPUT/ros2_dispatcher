# Phase 2: Dispatcher Scalability under Two End-to-End Pipelines

**Date:** 2026-05-11
**Target paper:** SIGSPATIAL 2026 — **continuation of ICRA AgriFood 2026**.
**Scope:** scalability of the ROS 2 Dispatcher under fleet load, validated through two distinct downstream paradigm pipelines. **NOT** a reproduction of DARLI-AP 2026 (which compared engines at N=1-2 robots).

## Positioning

ICRA AgriFood 2026 introduced the Dispatcher as a middleware-agnostic bridge with two backends (Kafka, MQTT) and reported latency at N=1 robot. It explicitly does not test multi-robot fleets. The SIGSPATIAL paper closes that gap by asking:

> *"Does the Dispatcher's middleware-agnostic design hold under fleet load (N=1..50 robots, 10 Hz NavSatFix)?"*

**Subject of the experiment: the Dispatcher.**
The two paradigm engines (ksqlDB on the Kafka path, NebulaStream on the MQTT path) act as downstream validators that prove end-to-end functionality, not as competitors to be ranked against each other.

## Goal

Add two end-to-end pipeline measurements to the SIGSPATIAL scalability
story so the paper can answer:

> *"How do these two streaming-query pipelines compare under increasing
> fleet size?"*

The pipelines are:

- **Pipeline A:** `ROS → Kafka → ksqlDB`
- **Pipeline B:** `ROS → MQTT → NebulaStream`

Each pipeline is measured at **one** point: the paradigm engine's
output topic (one tap per pipeline). The metric is end-to-end latency
from the publisher's wall-clock to that output topic.

This is **not a DARLI-AP reproduction**. We deliberately diverge from
DARLI-AP on several methodological choices:

| Aspect | DARLI-AP 2026 | This experiment |
|---|---|---|
| t0 | "data ingestion timestamp" (ambiguous; likely broker produce) | Publisher wall-clock embedded in `header.stamp` (deterministic) |
| Windowing | Event-at-a-time (1-s tumbling disabled) | Same |
| Outlier rule | "Extreme outliers excluded" — criterion unstated | Explicit: skip first 30 messages (warm-up), report all remaining |
| Number of robots | 2 (leader + follower) | 5 sweep points: N ∈ {1, 5, 10, 25, 50} |
| Throughput/drop measurement | Not reported | Reported per cell |
| UDF complexity | Custom `INGEOFENCE` UDF (JTS-based) | **Minimal portable query** (see §"Query semantics") |
| Engines | All three: ksqlDB + Flink + NES | Two: ksqlDB + NES (paired with their natural transport) |

The minimum-viable goal is one clean scalability plot showing how
each pipeline's latency, throughput, and drop rate evolve from
$N{=}1$ to $N{=}50$.

## Brief alignment (verbatim)

| Brief requirement | This experiment |
|---|---|
| "Scalability with increasing number of robots" | ✓ N ∈ {1, 5, 10, 25, 50} |
| "Topics per robot — position/GNSS" | ✓ `sensor_msgs/msg/NavSatFix` only, 10 Hz |
| "Query: geofencing or proximity" | ✓ Bounding-box geofence, real (not pass-through) |
| "Throughput, avg latency, P95/P99, drops" | ✓ Five metrics per cell |
| "Replay existing trajectories with shifted IDs+timestamps" | ✓ Reused from Phase 1 |
| "Two architectures (Kafka, MQTT)" | ✓ Kafka under ksqlDB; MQTT under NebulaStream |
| "Run the same pipeline" | Two *different* paradigm pipelines, each as a single unit under test |

## Architecture (per pipeline)

```
   N robot containers (one Compose-defined service each)
         │   /robot_<i>/gnss   NavSatFix CDR, header.stamp = t0_ns
         ▼
   ROS 2 DDS (host loopback, ipc:host)
         │
         ▼
   sink container (kafka_sink  or  mosquitto_sink)
         │   serializes to CDR, produces to broker
         ▼
   broker (Confluent Kafka  or  Eclipse Mosquitto)
         │
         ▼
   paradigm bridge container
   (CDR → JSON for ksqlDB;
    MQTT → NES source for NebulaStream)
         │
         ▼
   paradigm engine container
   (ksqlDB or NebulaStream)
   evaluates geofence query
         │
         ▼
   paradigm output topic
   (Kafka topic 'robot_geofence_alerts' for ksqlDB;
    MQTT topic 'nes_geofence_alerts' for NebulaStream)
         │
         ▼
   measurement consumer
   (records t1 = time.time_ns(),
    extracts t0_ns from forwarded payload,
    writes JSONL row)
```

Single measurement tap per pipeline. `t0_ns` is preserved end-to-end
because the paradigm bridge copies it from the CDR header into the
JSON payload, and the paradigm query passes it through to the output
record. The consumer subtracts: `latency_ns = t1_ns - t0_ns`.

## Query semantics — minimal portable geofence

To keep the comparison apples-to-apples and avoid UDF entanglement,
both pipelines run the same logical query expressed in their native
DSL:

> *"For each incoming NavSatFix, emit an alert if the point falls
> outside a fixed bounding box (lat ∈ [LO_LAT, HI_LAT], lon ∈
> [LO_LON, HI_LON]). The bounding box is small enough that all real
> messages from the bag fall **inside** it (so no alert) OR all fall
> **outside** it (so every message emits an alert). The choice
> determines the output rate of the paradigm engine — we pick
> 'all outside' so every input row produces an output row,
> matching the transport-layer rate exactly and isolating
> processing overhead from filtering ratio."*

### ksqlDB realisation

```sql
CREATE STREAM robot_geofence_alerts WITH (
  KAFKA_TOPIC='robot_geofence_alerts',
  VALUE_FORMAT='JSON',
  PARTITIONS=1
) AS
SELECT
  robot_id, latitude, longitude, altitude,
  timestamp, t0_ns,
  'OUTSIDE' AS msg
FROM ros_gps_fix_stream
WHERE latitude < 30.0 OR latitude > 60.0
   OR longitude < -10.0 OR longitude > 30.0
EMIT CHANGES;
```

Real GNSS records from the bag are around lat ≈ 46.34, lon ≈ 3.43
(Massif Central). The clauses are chosen so the bounding box covers
all of metropolitan France except a hole around the bag's trajectory —
i.e.\ all messages fall *outside* and trigger the alert. This makes
the output rate equal to the input rate and isolates **paradigm
processing time** from selectivity.

### NebulaStream realisation

Same WHERE clause expressed as a NebulaStream filter operator
(NES query DSL). The MQTT-source step in the bridge container reads
`ros2/robot_<i>/gnss` MQTT topics, deserializes CDR, and pushes
records into the NebulaStream Coordinator's ingestion REST endpoint.
The NES query result is written to an MQTT output topic
`nes_geofence_alerts`. Final consumer reads that topic.

The exact NebulaStream query syntax depends on the version of
NebulaStream in the existing Docker image
(`nebulastream/nes-executable-image:latest`) and will be confirmed in
the 2b spike (see §Phasing).

## Methodology

### Run parameters

| Parameter | Value |
|---|---|
| N (robots) | {1, 5, 10, 25, 50} |
| Brokers/paradigms paired | Kafka+ksqlDB; MQTT+NebulaStream |
| Per-robot rate | 10 Hz |
| Reps per cell | 3 |
| Duration | 10 s warm-up (discarded) + 60 s measurement |
| Bag | `rorbots_follower_leader_parcelle_1MONT` |
| Spatial shift per robot | $\Delta\mathrm{lat}=\Delta\mathrm{lon}=10^{-4}\cdot\mathrm{id}$ |
| Outlier handling | First 30 messages of each cell skipped in analysis (warm-up tail) |

### Per-cell artifacts

```
results/N=<n>_pipeline=<kafka_ksqldb|mqtt_nes>_run=<r>/
├── consumer.jsonl     # paradigm tap (the single measurement)
└── sink_metrics.csv   # sink-side diagnostics (delta_dropped, serialize p95)
```

### Five metrics per cell (matching the brief)

Computed from `consumer.jsonl`:

- **Throughput** = `received_count / duration_s`
- **Avg latency** = `mean(latency_ns)` (across all rows after warm-up skip)
- **P95 latency** = nearest-rank P95 of `latency_ns`
- **P99 latency** = nearest-rank P99 of `latency_ns`
- **Drop rate** = `1 - received_count / (N × 10 × duration_s)`

## What we already have (reused from Phase 1 / pass6)

- `tools/benchmark/scaling/robot_replay.py` — N publishers, replay-and-shift
- `tools/benchmark/scaling/gen_robots_compose.sh` — N-container generator
- `tools/benchmark/scaling/sink_entrypoint.sh` — kafka_sink / mosquitto_sink launcher
- `tools/benchmark/scaling/compose.kafka.yml`, `compose.mqtt.yml` — broker + sink containers
- `tools/benchmark/scaling/compose.ksqldb.yml` — ksqldb-server + bridge + consumer overlay (smoke at N=1 ✓)
- `tools/benchmark/scaling/ksqldb/ksqldb_bridge.py` — CDR → JSON fan-in
- `tools/benchmark/scaling/ksqldb/ksqldb_consumer.py` — JSON output consumer
- `tools/benchmark/scaling/ksqldb/ksql-setup.sql` — currently pass-through; to be replaced with the bounding-box query
- `tools/benchmark/scaling/run_ksqldb_smoke.sh` — single-cell smoke; to be generalized into matrix orchestrator
- `tools/benchmark/scaling/analyze_scaling.py` — produces 5 metrics from JSONL + summary table

## What we need to build

### For Pipeline A (Kafka + ksqlDB)

1. **Replace ksql-setup.sql** to use the bounding-box `WHERE` clause (§Query semantics).
2. **Generalize `run_ksqldb_smoke.sh`** into `run_ksqldb_matrix.sh` that loops over N ∈ {1, 5, 10, 25, 50} × 3 reps.
3. **Smoke gate** at N=10 (consistent with Phase 1 pattern).
4. **Adjust `analyze_scaling.py`** to recognize `broker=ksqldb` cell-dir naming (it already does — confirmed in pass6).

Estimated effort: **2-3 hours**, ~1 hour runtime for the full N matrix.

### For Pipeline B (MQTT + NebulaStream)

1. **Spike**: confirm `nebulastream/nes-executable-image` boots as a Coordinator and accepts a Worker registration. Write a smoke script that submits a minimal query via REST.
2. **MQTT → NebulaStream source bridge**: a Python (or Java) container that subscribes to `ros2/robot_+/gnss`, deserializes CDR, and pushes records into a NES ingestion endpoint or a NES MQTT source if one exists.
3. **NebulaStream query** equivalent to the bounding-box ksqlDB SQL.
4. **NebulaStream output bridge / consumer**: reads `nes_geofence_alerts` MQTT topic, computes `latency = t1 - t0_ns`, writes JSONL.
5. **`compose.nebulastream.yml`** with Coordinator + Worker + bridges + consumer.
6. **`run_nes_matrix.sh`** orchestrator.

Estimated effort: **8-16 hours** for the integration (image not yet exercised), ~1 hour runtime for the matrix once it works.

**Critical unknowns** (resolve in spike):
- Does the NES public image have an MQTT source connector built in?
- Java vs DSL/REST submission?
- Java UDF requirement, or can the bounding-box filter be expressed without UDF?
- Output sink format and topic naming.

## Phasing

| Phase | Deliverable | Effort | Runtime |
|---|---|---|---|
| 2a | Pipeline A: real-geofence ksqlDB query + N matrix | 2-3 h | ~1 h |
| 2b | Pipeline B: NebulaStream spike (boot + minimal query) | 3-4 h | 0 |
| 2c | Pipeline B: full bridge + matrix | 5-12 h | ~1 h |
| 2d | Cross-pipeline plot + 5-metric tables | 1 h | 0 |
| 2e | Section 5 paragraph for both pipelines | 1 h | 0 |

Total: ~12-22 hours of engineering + ~2 hours runtime, with 2b as the
risk-bearing critical step.

## Acceptance criteria

1. Both pipelines produce non-empty `consumer.jsonl` for every cell.
2. At $N{=}1$, paradigm-only latency overhead (subtract Phase 1 transport
   latency from these numbers) is positive — i.e.\ the paradigm engine
   adds measurable cost.
3. `analyze_scaling.py` emits a clean side-by-side table:
   pipeline × N × {Tput, Avg, P95, P99, Drop}.
4. Smoke gate at $N{=}10$ passes for each pipeline before the full
   matrix runs.
5. No new dependency on JTS/INGEOFENCE UDF; the bounding-box query
   runs in native ksqlDB SQL and native NebulaStream filter.

## Out of scope

- DARLI-AP reproduction (3 engines, single-robot, INGEOFENCE UDF).
- Dual-tap per-hop breakdown (transport-only vs paradigm-overhead) —
  we have the transport-only numbers from Phase 1 and can subtract.
- Apache Flink + GeoFlink pipeline — possible future Phase 3.
- Payload-size sweep through the paradigm engines — keep NavSatFix only.
- Multi-host or production-grade tuning of ksqlDB / NebulaStream
  cluster parameters; both engines run in their image-default single-node configuration.
