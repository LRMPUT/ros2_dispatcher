# Scalability benchmark — reproduction guide

This directory contains the artefact for the **SIGSPATIAL 2026** scalability
experiments that extend the ICRA AgriFood 2026 paper *"Middleware Matters:
ROS 2 Dispatcher for Scalable Data Pipelines in Agri Robotics"* with a
multi-robot fleet study.

The benchmark sweeps the number of simulated robots
**N ∈ {1, 5, 10, 25, 50}** through several pipelines, measures the five
brief-mandated metrics — throughput, average latency, P95 / P99 latency,
drop rate — and emits paper-ready CSV / Markdown tables.

---

## 1. Prerequisites

| Tool | Version |
|---|---|
| Linux host with Docker Engine | ≥ 24.x with Compose v2 |
| ROS 2 Humble | required only inside containers (the helper images bring it in) |
| Python 3 venv | for one-time bag conversion (`rosbags`) |
| Free RAM | ≥ 16 GB recommended for N=50 cells |
| Free disk | ~ 4 GB for the converted bag + ~ 2 GB per archived pass |

The source recording is a **ROS 1** bag of a CHIST-ERA leader/follower
agricultural campaign at INRAE Clermont-Ferrand. Place it at the repo root
as `rorbots_follower_leader_parcelle_1MONT.bag` (≈ 345 MB).

### One-time bag conversion (ROS 1 → ROS 2)

```bash
python3 -m venv /tmp/rosbags_venv
/tmp/rosbags_venv/bin/pip install rosbags
/tmp/rosbags_venv/bin/rosbags-convert \
    --src $(git rev-parse --show-toplevel)/rorbots_follower_leader_parcelle_1MONT.bag \
    --dst /tmp/scaling_bags/source_bag_ros2 \
    --dst-version 8 --dst-typestore ros2_humble
```

Set `BAG_PATH=/tmp/scaling_bags/source_bag_ros2` before running any matrix.

### Build the Docker images (one-time)

From the **scaling directory** (`tools/benchmark/scaling/`):

```bash
# Project Dispatcher image — has the full colcon workspace with kafka_sink / mosquitto_sink
docker build -f $(git rev-parse --show-toplevel)/docker/Dockerfile \
    --build-arg ROS_DISTRO=humble \
    -t ros2-kafka-dispatcher:scaling \
    $(git rev-parse --show-toplevel)

# Standalone robot publisher (small image)
docker build -f Dockerfile.robot       -t ros2-scaling-robot:local           .

# End-to-end consumer
docker build -f Dockerfile.consumer    -t ros2-scaling-consumer:local        .

# ksqlDB CDR→JSON bridge (paradigm pipeline only)
docker build -f ksqldb/Dockerfile.bridge   -t ros2-scaling-ksqldb-bridge:local   ksqldb
docker build -f ksqldb/Dockerfile.consumer -t ros2-scaling-ksqldb-consumer:local ksqldb
```

Pulled from Docker Hub at first matrix run:

```
confluentinc/cp-kafka:latest        # Kafka broker (KRaft, no Zookeeper)
confluentinc/ksqldb-server:0.29.0   # ksqlDB engine (paradigm pipeline)
confluentinc/ksqldb-cli:0.29.0      # one-shot init container
eclipse-mosquitto:2.0               # MQTT broker
```

### Verify

```bash
docker images | grep -E "kafka-dispatcher:scaling|scaling-robot|scaling-consumer|cp-kafka|ksqldb|mosquitto"
```

Five locally-built images plus three Confluent / Eclipse images should be
present.

---

## 2. Quick smoke (≈ 3 min)

Validates the whole stack at the smallest scale.

```bash
export BAG_PATH=/tmp/scaling_bags/source_bag_ros2
export KAFKA_FAIR_LATENCY=1     # acks=1, linger.ms=0
./run_scaling_matrix.sh \
    --reps 1 --robots 1 --brokers kafka --duration 20 --warmup 5
```

Expected result: `results/N=1_broker=kafka_run=1/consumer.jsonl` with
≈ 200 lines, each one record with a sub-10-ms `latency_ns`.

---

## 3. Five benchmark configurations

Each script below runs in the foreground and writes to `results/`. After
each one we archive the contents (`mv results results_archive/<label>`)
and start the next.

### 3.1 Transport scalability — *gateway* topology, single message type

```bash
export BAG_PATH=/tmp/scaling_bags/source_bag_ros2

# Pass 2 — NavSatFix, fair-tuned Kafka, fleet mode (one container hosts N publishers)
export KAFKA_FAIR_LATENCY=1
./run_scaling_matrix.sh   # default: N ∈ {1,5,10,25,50}, kafka+mqtt, 3 reps × 60 s

mv results results_archive/pass2_fair_fleet
mkdir -p results
```

Variants:

```bash
# Default Kafka tuning (acks=all, linger.ms=5) — the contrast pass
unset KAFKA_FAIR_LATENCY
./run_scaling_matrix.sh

mv results results_archive/pass1_defaults_fleet

# Per-container fleet (one container per simulated robot)
export KAFKA_FAIR_LATENCY=1
TOPOLOGY=gateway ./run_scaling_matrix.sh \
    # gateway is the default; pass3 was already per-container
mv results results_archive/pass3_fair_per_container
```

### 3.2 Transport scalability — multiple message types

```bash
export BAG_PATH=/tmp/scaling_bags/source_bag_ros2
export KAFKA_FAIR_LATENCY=1

MSG_TYPE=odometry    ./run_scaling_matrix.sh --robots 1,10,50
mv results results_archive/pass4_fair_fleet_odometry

MSG_TYPE=pointcloud2 ./run_scaling_matrix.sh --robots 1,10,50
mv results results_archive/pass5_fair_fleet_pointcloud2
```

### 3.3 Paradigm pipeline — Kafka → ksqlDB

Adds an actual streaming query (bounding-box geofence) over Kafka.

```bash
export BAG_PATH=/tmp/scaling_bags/source_bag_ros2
./run_ksqldb_matrix.sh   # default: N ∈ {1,5,10,25,50}, 3 reps × 60 s
mv results results_archive/pass7_ksqldb_matrix
```

### 3.4 Edge topology — per-robot sink

Each robot container runs its own private `kafka_sink` / `mosquitto_sink`
(no central Dispatcher). Multi-topic by default: every robot emits
NavSatFix @ 10 Hz, Odometry @ 20 Hz, LaserScan @ 50 Hz, PointCloud2 @ 12.5 Hz
simultaneously.

```bash
export BAG_PATH=/tmp/scaling_bags/source_bag_ros2
export MSG_TYPE=multi

./run_edge_matrix.sh \
    --reps 3 --warmup 10 --duration 60 \
    --brokers kafka,mqtt

mv results results_archive/pass8_edge_multi_qos1
```

For the MQTT-QoS-0 (fire-and-forget) variant:

```bash
MQTT_QOS=0 ./run_edge_matrix.sh \
    --brokers mqtt --reps 3 --duration 60
mv results results_archive/pass9_edge_multi_mqtt_qos0
```

### 3.5 ksqlDB smoke (paradigm-only validation)

```bash
N=1 DURATION_S=30 WARMUP_S=5 ./run_ksqldb_smoke.sh
mv results results_archive/pass6_ksqldb_smoke_N1
```

---

## 4. What lands in `results/`

```
results/N=<n>_broker=<broker>_run=<rep>/
├── consumer.jsonl       # one row per delivered message
│                        # {robot_id, topic, t0_ns, t1_ns, latency_ns, bytes}
└── sink_metrics.csv     # per-second sink-side counters (gateway only)
```

Edge cells additionally include the `topology=edge` token in the dir name.

The four message types are distinguished by the **Kafka topic suffix**:

| Suffix | Type | Source topic in bag |
|---|---|---|
| `.gnss` | `sensor_msgs/msg/NavSatFix` | `/follower/gps/fix` + `/leader/gps/fix` |
| `.odom` | `nav_msgs/msg/Odometry` | `/follower/localisation/filtered_odom` + 3 others |
| `.scan` | `sensor_msgs/msg/LaserScan` | `/leader/lidar/scan` |
| `.points` | `sensor_msgs/msg/PointCloud2` | `/follower/lidar2/points` |

`t0_ns` is wall clock at the publisher's tick (`time.time_ns()`), embedded
in `header.stamp` and propagated through CDR (transport pipelines) or
JSON (paradigm pipelines).
`t1_ns` is wall clock at the consumer's callback. Single-host kernel
clock guarantees sub-millisecond resolution.

---

## 5. Analysis

After each pass:

```bash
./analyze_scaling.py \
    --input  results_archive/<pass_label> \
    --output results_archive/<pass_label> \
    --duration-s 60
```

Generates:

- `summary.csv`           — one row per `(N, broker, rep)` with all 5 metrics
- `summary_table.md`      — paper-ready Markdown table (mean across reps)
- `plots/latency_vs_n.png`, `throughput_vs_n.png`, `drop_rate_vs_n.png`

Cross-pass comparison (after multiple passes are archived):

```bash
./compare_passes.py
# Reads results_archive/pass{1,2,3}/summary.csv and emits
# results_archive/comparison.md + comparison_latency.png + comparison_drop_rate.png
```

---

## 6. Topology cheat-sheet

| Mode | Robot container | Central sink? | Containers per cell |
|---|---|---|---|
| **Gateway (default)** | publisher only | yes (one for all robots) | broker + sink + consumer + N×robot = N + 3 |
| **Edge** | publisher + own sink | no | broker + consumer + N×robot = N + 2 |
| **ksqlDB** | gateway robots + ksqldb server + bridge | yes + extra paradigm engine | broker + sink + ksqldb-server + ksqldb-init + ksqldb-bridge + consumer + N×robot = N + 6 |

`gen_robots_compose.sh <N> <output.yml>` writes the per-cell Compose
fragment. `TOPOLOGY=edge` (env) switches it to edge mode.

---

## 7. Methodology checkpoints

- **Reps:** 3 per cell unless overridden via `--reps`.
- **Warm-up:** 10 s discarded before measurement.
- **Measurement:** 60 s per cell unless overridden via `--duration`.
- **Outlier handling:** the analyser drops the first 30 rows of each cell
  before computing percentiles (covers cold-start tail).
- **Fair Kafka:** `acks=1, linger.ms=0` matches MQTT QoS 1 semantics. To
  switch off, `unset KAFKA_FAIR_LATENCY` (or set to `0`).
- **Per-robot lat/lon shift:** `Δlat = Δlon = 1e-4 × robot_id` (~ 11 m
  grid). Applied only to NavSatFix; other types use raw bag content.
- **`t0_ns`:** rewritten on every publish with `time.time_ns()` to keep
  the latency reference deterministic and host-clock-synchronous.

---

## 8. Expected runtimes (single Linux laptop)

| Matrix | Cells | Wall time |
|---|---|---|
| Quick smoke (Sec 2) | 1 | 3 min |
| Pass 2 (transport, fleet, NavSatFix) | 32 | 45 min |
| Pass 3 (transport, per-container) | 32 | 55 min |
| Pass 4 / 5 (odometry / pointcloud2) | 20 each | 30 min each |
| Pass 7 (ksqlDB matrix) | 16 | 60 min |
| Pass 8 (edge multi-topic) | 32 | 50 min |
| Pass 9 (edge MQTT QoS 0) | 16 | 30 min |

Total: roughly **5 h** of measurement plus the one-time image builds.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Consumer JSONL is empty | Bag mount missing or wrong type | `ls $BAG_PATH/metadata.yaml` should list `sensor_msgs/msg/NavSatFix` |
| `bitnami/kafka` pull error | Bitnami removed public Kafka images | `compose.kafka.yml` already uses `confluentinc/cp-kafka:latest` |
| ksqlDB exits with `Is a directory` | Old image with `KSQL_KSQL_QUERIES_FILE=""` | Already fixed; pull current `compose.ksqldb.yml` |
| Kafka topic regex fails to match | librdkafka uses POSIX regex (no `\d`) | Pattern uses `[0-9]+`; do not use `\d+` |
| MQTT consumer crashes on `ros2/robot_+/gnss` | `+` is whole-level only | Default pattern is `ros2/+/gnss` |
| Cross-container DDS sees no messages | Missing `ipc: host` (shared `/dev/shm`) | Both `network_mode: host` and `ipc: host` are set in compose |
| Per-replica hostname collisions | Compose v2 + `network_mode:host` ignores hostname | Use `gen_robots_compose.sh` (one explicit service per robot) |

---

## 10. Citing the artefact

If you reproduce these numbers, please cite the SIGSPATIAL 2026 paper
*"A Unified Architecture for Spatio-Temporal Data Pipelines in Robotic
GIS Systems"* and the ICRA AgriFood 2026 predecessor.

The full design rationale and decision log for these experiments lives in:

- `docs/superpowers/specs/2026-05-11-scalability-experiment-design.md` (Phase 1)
- `docs/superpowers/specs/2026-05-11-phase2-paradigm-scalability-design.md` (Phase 2)

The implementation plan that drove the build:

- `docs/superpowers/plans/2026-05-11-scalability-experiment.md`
