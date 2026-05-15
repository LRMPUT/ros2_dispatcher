# Scalability Experiment: ROS 2 → Kafka / MQTT under Increasing Fleet Size

**Date:** 2026-05-11
**Branch:** feature/cdr-vs-json-benchmark (parent for new branch `feature/scalability-experiment`)
**Target paper:** SIGSPATIAL (follow-on to the ICRA workshop paper that established correctness/latency for geofencing).

## Goal

Produce at least one defensible scalability plot showing how the ROS 2 →
broker dispatcher pipeline behaves as the number of simultaneous robots
grows from 1 to 50. The same matrix is run against two architectures
(Kafka and MQTT) using identical synthetic load, so the result is a
side-by-side comparison rather than two isolated curves.

## Scope

- Single-host experiment (one Docker host runs the whole stack per run).
- Per robot: one topic, `sensor_msgs/msg/NavSatFix`, published at 10 Hz.
- Latency definition: **end-to-end**, from publisher's wall-clock at
  publish time (`t0_ns`) to the external broker consumer's wall-clock at
  receive time (`t1_ns`).
- Query layer is framing only — the consumer is passive (deserialize +
  log). The paper text frames the measured latency as the budget
  available to a geofencing/proximity query.
- Serialization: CDR only. JSON scaling is a separate axis already
  covered by the CDR-vs-JSON benchmark in `tools/benchmark/`.

## Decisions locked during brainstorming

| Decision | Value | Rationale |
|---|---|---|
| Latency scope | End-to-end (publish → broker consumer) | Most defensible for "scalability of the query pipeline" claim. |
| Per-robot topics | NavSatFix only, 10 Hz | Matches "position/GNSS" framing; lightest load that still scales meaningfully. |
| Query | Passive consumer; geofencing as framing | Keeps Kafka and MQTT comparable; avoids ksqlDB-vs-MQTT asymmetry. |
| Process model | One Docker container per robot | Strongest fleet-realism story; the user explicitly chose this. |
| Trajectory data | Replay user-supplied bag, shifted per robot | Per-robot lat/lon offset by `robot_id`; bag mounted at runtime via `BAG_PATH`. |
| Orchestration | Docker Compose + `--scale robot=N` | Minimal code, mirrors existing `tools/benchmark/run_full_matrix.sh` pattern. |
| Brokers | Kafka and MQTT, identical matrix | Per the requirement to test two architectures. |

## Architecture

```
Host network (ROS_DOMAIN_ID=42, FastDDS multicast over loopback)

  robot_1 … robot_N      sink                       consumer
  (rclpy publisher)      (kafka_sink or             (confluent-kafka
  NavSatFix @10 Hz       mosquitto_sink             or paho-mqtt;
  on /robot_<i>/gnss     LifecycleNode;             deserialize CDR,
  header.stamp = t0_ns)  subscriptions_yaml         compute t1 - t0)
                         lists N topics)
        │                       │                         ▲
        └────── DDS ────────────┘                         │
                                                          │
Bridge network (per-broker)                               │
  broker  ── localhost:9092 (Kafka) ── port-forwarded ────┘
            or localhost:1883 (MQTT)
```

All ROS containers (`robot`, `sink`, `consumer`) run with
`network_mode: host` so DDS discovery just works for `N` up to at least
50 publishers + 1 sink + 1 consumer. The broker stays in a bridge
network and is reached via the forwarded port.

### Single-host clock

Because all containers share the host kernel, `time.time_ns()` is
monotonic and consistent across publisher, sink, and consumer. No NTP /
PTP setup needed. This is essential for sub-millisecond e2e latency
measurement.

## Components

### 2.1 `robot_replay` (new)

- **Image:** `tools/benchmark/scaling/Dockerfile.robot` — ROS Humble base
  + rosbag2_py + rclpy + sensor_msgs.
- **Script:** `tools/benchmark/scaling/robot_replay.py`
- **Behavior:**
  1. Open `BAG_PATH` (read-only mount) once at startup.
  2. Iterate over `sensor_msgs/msg/NavSatFix` messages; loop back to
     start on EOF.
  3. For each message at the next 10 Hz tick:
     - Re-stamp `header.stamp` to the current wall clock
       (`Time.from_msg` of `time.time_ns()`). This is `t0_ns`.
     - Shift latitude and longitude by a deterministic offset derived
       from `ROBOT_ID` (~10 m grid, e.g.
       `lat += 0.0001 * ROBOT_ID`, `lon += 0.0001 * ROBOT_ID`).
     - Publish on `/robot_<ROBOT_ID>/gnss`.
- **Env:** `ROBOT_ID`, `BAG_PATH`, `RATE_HZ` (default 10),
  `ROS_DOMAIN_ID` (default 42).
- **Note on `--scale`:** With `docker compose up --scale robot=N`,
  Compose injects an ordinal as the container's hostname suffix; the
  entrypoint derives `ROBOT_ID` from it (e.g.,
  `hostname | rev | cut -d- -f1 | rev`).

### 2.2 `sink` (reuse)

- **Image:** existing `docker/Dockerfile` (built once,
  `ros2-kafka-dispatcher:scaling`).
- **Behavior:** entrypoint reads `NUM_ROBOTS` and generates the
  `subscriptions_yaml` value as a list of `N` entries:
  ```yaml
  - topic_name: /robot_1/gnss
    msg_type: sensor_msgs/msg/NavSatFix
  - topic_name: /robot_2/gnss
    msg_type: sensor_msgs/msg/NavSatFix
  ...
  ```
  Then launches `kafka_sink_node_exe` or `mosquitto_sink_node_exe` with
  `metrics.enabled=true`, `metrics.interval_ms=1000`,
  `kafka.payload_format=cdr` (or the MQTT equivalent), and
  `kafka.drop_when_full=true`. The same entrypoint launches
  `tools/benchmark/metrics_recorder.py` in the same container, pointed
  at the sink's `/<sink_node>/metrics` topic, writing
  `/artifacts/sink_metrics.csv` (bind-mounted from the host).
- **Lifecycle:** the orchestrator drives `configure → activate` via the
  standard `lifecycle_msgs/srv/ChangeState` service, then waits for
  `get_state = ACTIVE` before starting robots.

### 2.3 `consumer` (new)

- **Image:** `tools/benchmark/scaling/Dockerfile.consumer` — Python +
  `confluent-kafka` + `paho-mqtt` +
  `rosidl_runtime_py` (for CDR deserialization).
- **Script:** `tools/benchmark/scaling/e2e_consumer.py`
- **Behavior:**
  1. Connect to broker (`--broker kafka` or `--broker mqtt`).
  2. Subscribe to a pattern that matches all robot topics:
     - Kafka: regex `^ros2\.robot_.*\.gnss$`.
     - MQTT: `ros2/robot_+/gnss`.
  3. For each delivered Kafka record / MQTT message:
     - Record `t1_ns = time.time_ns()` immediately.
     - Deserialize CDR payload → NavSatFix.
     - Extract `t0_ns = stamp.sec * 1_000_000_000 + stamp.nanosec`.
     - Extract `robot_id` from topic name.
     - Append a JSONL row to `--output` :
       ```json
       {"robot_id": 7, "topic": "ros2.robot_7.gnss",
        "t0_ns": 1234567890123456789, "t1_ns": 1234567890123987654,
        "latency_ns": 531198, "bytes": 104}
       ```
  4. Honor `--warmup` (skip rows during the first W seconds) and
     `--duration` (stop after D seconds of measurement).

### 2.4 broker (reuse)

Reuses the existing Compose snippets:

- Kafka: `kafka_bridge/kafka_brocker/docker-compose.yml` (single-broker
  Bitnami Kafka or whatever is currently there).
- MQTT: `mosquitto_bridge/mosquitto_brocker/docker-compose.yml`.

Both are included into the scaling Compose via `extends` or
`include` (whichever Compose v2 supports cleanly in this layout).

### 2.5 Orchestrator (new)

- **Script:** `tools/benchmark/scaling/run_scaling_matrix.sh`
- **Flow per cell `(N, broker, rep)`:**
  1. `docker compose -f compose.<broker>.yml up -d --scale robot=0 broker sink`
     — bring up broker and sink first.
  2. Wait for sink lifecycle `ACTIVE` (poll
     `ros2 service call /<sink_node>/get_state ...`, max 30 s).
  3. `docker compose -f compose.<broker>.yml up -d consumer`
     — start the consumer in warmup mode.
  4. `docker compose -f compose.<broker>.yml up -d --scale robot=<N> robot`
     — scale up robots.
  5. Sleep `WARMUP_S + DURATION_S` (default 10 + 60).
  6. Tear down: `docker compose -f compose.<broker>.yml down -v`.
  7. Move `consumer.jsonl` and `sink_metrics.csv` artifacts from a
     bind-mounted host directory into
     `results/N=<N>_broker=<broker>_run=<rep>/`.
- **Outer loop:** `N ∈ {1, 5, 10, 25, 50} × broker ∈ {kafka, mqtt} × rep ∈ {1, 2, 3}`.
- **Smoke gate:** before the full matrix, run a single `N=10` cell on
  each broker. If `received / expected < 0.9` or the orchestrator
  failed to reach `ACTIVE`, abort and report.

### 2.6 Analysis (new)

- **Script:** `tools/benchmark/scaling/analyze_scaling.py`
- **Input:** the `results/` tree.
- **Per `(N, broker)`:** aggregate across the 3 reps:
  - Throughput: total received / measurement duration.
  - Drop rate: `1 - received / (N × RATE_HZ × DURATION_S)`.
  - Latency stats: avg, P50, P95, P99 over all rows from all 3 reps.
- **Output:**
  - `results/summary.csv` (one row per `(N, broker)`).
  - PNG plots: latency vs N, throughput vs N, drop rate vs N — each
    with two curves (Kafka, MQTT) and error bars across reps.
  - A short Markdown table for direct paste into the paper.

## Data flow (one message)

```
robot_i container                  : t0_ns = time.time_ns()
                                   : NavSatFix.header.stamp ← t0_ns
                                   : publish /robot_i/gnss
DDS over host loopback             :
sink container                     : on_message: serialize CDR
                                   : produce to broker
broker container                   : persist + deliver
consumer container                 : on_delivery: t1_ns = time.time_ns()
                                   : deserialize CDR
                                   : latency_ns = t1_ns - t0_ns
                                   : write JSONL row
```

## Run matrix

| Axis | Values |
|---|---|
| N | 1, 5, 10, 25, 50 |
| Broker | kafka, mqtt |
| Reps | 3 |
| Warmup | 10 s |
| Duration | 60 s |

Total runs: 5 × 2 × 3 = **30 runs**. Per-run wall clock ≈ 90 s including
teardown; total ≈ 45 minutes for the full matrix. Quick pass
(`--reps 1`) ≈ 15 minutes.

## Metrics

### Consumer-side (primary, from JSONL)

- Throughput: messages received per second per `(N, broker)`.
- Latency: avg, P50, P95, P99 (nanoseconds, reported in milliseconds).
- Drop rate: `1 - received / (N × 10 × DURATION_S)`.

### Sink-side (diagnostic, from `metrics_recorder.py` CSV)

- `delta_dropped` per second — non-zero means the librdkafka / Paho
  producer queue overflowed.
- `serialize_p95`, `send_p95` — used to attribute e2e latency spikes
  between sink CPU and the broker hop.

## Artifact layout

```
tools/benchmark/scaling/
├── Dockerfile.robot
├── Dockerfile.consumer
├── compose.kafka.yml
├── compose.mqtt.yml
├── robot_replay.py
├── e2e_consumer.py
├── run_scaling_matrix.sh
├── analyze_scaling.py
└── results/
    └── N=<n>_broker=<b>_run=<r>/
        ├── consumer.jsonl
        └── sink_metrics.csv
results/
└── summary.csv
└── plots/
    ├── latency_vs_n.png
    ├── throughput_vs_n.png
    └── drop_rate_vs_n.png
```

## Risks and mitigations

1. **DDS discovery at N=50 over loopback.** Typically fine, but bears
   verification. *Mitigation:* the smoke gate at N=10 before the full
   matrix; if discovery time exceeds 5 s or any robot fails to be
   discovered by the sink (visible via `ros2 topic list | grep gnss`),
   abort and switch to ROS_LOCALHOST_ONLY=0 with explicit Fast DDS
   discovery server.
2. **Sink-not-ready race.** If robots publish before the sink has
   subscribed, those early messages are silently dropped at the DDS
   layer (not counted in `delta_dropped`). *Mitigation:* orchestrator
   blocks on sink lifecycle = ACTIVE before scaling up robots; warmup
   period absorbs the first seconds anyway.
3. **`header.stamp` re-use for `t0_ns`.** Downstream code that expects
   `header.stamp` to be the GNSS fix time will misbehave if reused
   later. *Mitigation:* document this in `robot_replay.py` and
   `e2e_consumer.py`. The consumer is the only downstream; outside the
   scaling experiment, the replay container is not used.
4. **Single-host loopback bandwidth is huge.** Won't saturate the
   network for 50 × 10 Hz × ~100 B (≈ 50 KB/s). The scaling cost
   surfaces in CPU and per-topic DDS / broker overhead — which is the
   intended target.
5. **librdkafka producer queue.** 500 msgs/s at default queue size of
   ~100k is nowhere near full. Drops at the sink would indicate a real
   pathology, not configuration. Sink-side `delta_dropped > 0` is a red
   flag.
6. **Compose `--scale` and lifecycle services.** Compose's `--scale`
   creates indistinct hostnames (`robot-1`, `robot-2`, …); deriving
   `ROBOT_ID` from hostname requires Compose v2. Documented in
   `robot_replay.py`.

## Out of scope

- JSON payload format scaling (covered by existing CDR-vs-JSON
  benchmark).
- Multi-host or multi-region brokers.
- Real geofence / proximity query computation (framing only).
- Long-running stability / endurance runs.
- Higher per-robot rates (1, 50, 100 Hz sweeps).
- Larger payloads (Odometry, PointCloud2).

## Acceptance criteria

The experiment is complete when:

1. All 30 cells produce a non-empty `consumer.jsonl` and
   `sink_metrics.csv`.
2. `analyze_scaling.py` runs to completion and emits
   `summary.csv`, three plots, and a Markdown table.
3. At N=1 (single-robot baseline), e2e P95 latency for both Kafka and
   MQTT is within 2× of the single-topic latency numbers from
   `tools/latency/` (sanity check — the new pipeline should not be
   drastically slower than the existing single-topic tooling on the
   same host).
4. The smoke gate at N=10 passes on both brokers before the full matrix
   is started.

## Next step

Once this spec is approved, hand off to the `writing-plans` skill to
produce an executable implementation plan with task-by-task ordering
and verification steps.
