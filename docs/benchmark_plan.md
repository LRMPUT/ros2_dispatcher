# Benchmark Plan

This document defines the test matrix, tooling, procedure, and result schema for benchmarking the ROS 2 → Kafka → ROS 2 pipeline.

For metric definitions and measurement rules, see [`performance_metrics.md`](performance_metrics.md).
For clock synchronization and telemetry setup, see [`latency_measurement.md`](latency_measurement.md).

---

## 1. Test matrix

### 1.1 Payload sizes

Use a logarithmic spread that covers small control messages through large sensor payloads:

| Tier | Size | Typical ROS 2 equivalent |
|------|------|--------------------------|
| XS   | 256 B | `std_msgs/String`, `geometry_msgs/Twist` |
| S    | 1 KB  | `sensor_msgs/Imu`, `nav_msgs/Odometry` |
| M    | 10 KB | `sensor_msgs/LaserScan` (mid-range) |
| L    | 100 KB | `sensor_msgs/PointCloud2` (sparse) |
| XL   | 1 MB  | `sensor_msgs/Image` (uncompressed, stress only) |

Run XL only to find the saturation ceiling — it is not expected to be sustainable at high rates.

### 1.2 Message rates

| Label | Rate | Purpose |
|-------|------|---------|
| Low   | 100 msg/s | Baseline, establishes minimum overhead |
| Mid   | 1 000 msg/s | Moderate load |
| High  | 5 000 msg/s | Near-saturation for most payload sizes |
| Max   | 10 000 msg/s | Include only if the pipeline sustains it without drops |

Compact grid (if time is limited): **100 / 1k / 10k**.

### 1.3 Message schemas

#### Flat (simple primitives)

```
uint64 id
float64 ts
float32[] values   # sized to hit payload target
```

#### Nested (structured)

```
header { uint64 id, float64 ts }
pose   { float32[3] pos, float32[4] quat }
meta   { uint32 flags, uint32[] tags }
```

Use both **fixed-size** (`float32[64]`) and **variable-size** (`float32[]`) arrays for each payload tier. Adjust array lengths to hit the target byte count.

### 1.4 Full grid

Each cell = (payload size) × (rate) × (schema) × (array variant). With the compact rate set and both schemas:

| | 100 msg/s | 1k msg/s | 10k msg/s |
|---|---|---|---|
| 256 B flat fixed | ✓ | ✓ | ✓ |
| 256 B nested var | ✓ | ✓ | ✓ |
| 1 KB flat fixed | ✓ | ✓ | ✓ |
| … | … | … | … |
| 100 KB nested var | ✓ | ✓ | (if feasible) |

---

## 2. Fixed variables (held constant across all runs)

### Kafka producer

| Parameter | Value |
|-----------|-------|
| `kafka.acks` | `all` |
| `kafka.linger_ms` | `5` |
| `kafka.batch_size` | `1048576` (1 MB) |
| `kafka.max_queue_messages` | `4096` |
| `kafka.payload_format` | `cdr` |
| Partitions | 1 (single-partition topic) |
| Replication factor | 1 (local broker) |
| Compression | none (add as a separate variable if needed) |

### ROS 2 QoS

| Setting | Value |
|---------|-------|
| Reliability | `reliable` |
| Durability | `volatile` |
| History | `keep_last` |
| Depth | `50` |

### Environment

- Same host machine and kernel version for all runs.
- CPU governor set to `performance`: `sudo cpupower frequency-set -g performance`
- No other significant workloads running.
- Kafka and the ROS 2 process on the same machine (eliminates network variable).

---

## 3. Procedure

### 3.1 Start brokers

```bash
cd kafka_bridge/kafka_brocker && docker compose up -d
```

Verify:

```bash
kcat -b localhost:9092 -L
```

### 3.2 Configure kafka_sink

Enable telemetry in your `kafka_sink.param.yaml`:

```yaml
kafka_sink:
  ros__parameters:
    kafka:
      bootstrap_servers: "localhost:9092"
      payload_format: "cdr"
      acks: "all"
      linger_ms: 5
      batch_size: 1048576
      max_queue_messages: 4096
    telemetry:
      enabled: true
      log_every_n: 1
    metrics:
      enabled: true
      interval_ms: 1000
```

### 3.3 Launch the pipeline

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:=/path/to/bench_topics.yaml \
  kafka_sink_param_file:=/path/to/kafka_sink.param.yaml \
  enable_mosquitto_sink:=false
```

### 3.4 Run a single benchmark cell

Use the latency runner from `tools/latency/`:

```bash
./tools/latency/run_latency_capture.sh \
  --count 6000 \
  --rate 1000 \
  --payload-bytes 10240 \
  --output-dir ./results/2025-04-01__size-10240__rate-1000__schema-flat__array-var__run-01
```

Flags:

| Flag | Description |
|------|-------------|
| `--count` | Total messages to send (use `rate × (warmup + measurement)` seconds) |
| `--rate` | Target publish rate in msg/s |
| `--payload-bytes` | Serialized payload target size |
| `--output-dir` | Directory for artifacts (created automatically) |

### 3.5 Artifacts per run

| File | Contents |
|------|----------|
| `publisher.jsonl` | `msg_id`, `t0_ns`, payload size, topic |
| `consumer.jsonl` | `msg_id`, `t0_ns`, `t1_ns`, `latency_ns`, payload size |
| `kafka_sink.log` | Per-message telemetry (serialize time, payload bytes) |
| `kafka_source.log` | Consumer-side log |

---

## 4. Duration and repetitions

| Phase | Duration |
|-------|---------|
| Warm-up (excluded from analysis) | 30 s |
| Measurement window | 3 min (180 s) |

- **5 runs minimum** per grid cell.
- Increase to **10 runs** if p99 latency variance exceeds 20% across runs.
- Discard the first run if the broker JVM needs warming up.

---

## 5. Metrics to record per cell

From `consumer.jsonl` (drop the warm-up period messages before aggregating):

| Metric | Aggregation |
|--------|-------------|
| End-to-end latency | mean, p50, p95, p99 (ms) |
| Throughput | sustained mean msg/s and MB/s |
| Drop rate | `(sent - received) / sent` |
| Payload size | mean bytes (verify against target) |

From system monitoring during the run:

| Metric | Tool |
|--------|------|
| CPU (per process) | `pidstat -p <pid> 1` |
| RSS memory | `ps -o pid,rss -p <pid>` |
| Kafka broker lag | `kcat -b localhost:9092 -G <group> <topic>` |

---

## 6. File naming convention

```
<date>__size-<bytes>__rate-<msgs>__schema-<flat|nested>__array-<fixed|var>__run-<n>
```

Example:

```
2025-04-01__size-10240__rate-1000__schema-nested__array-var__run-03
```

Store all runs for a grid cell in the same parent directory, one subdirectory per run.

---

## 7. Analysis checklist

- [ ] Strip warm-up messages (first 30 s of `t0_ns` timestamps).
- [ ] Compute latency percentiles per run, then average across runs.
- [ ] Plot latency CDF for each (size, rate) pair.
- [ ] Flag any run with drop rate > 0.1% as a saturated data point.
- [ ] Compare `payload_format: cdr` vs `json` for selected cells if JSON support is needed.
- [ ] Note CPU and memory peaks — these reveal whether the bottleneck is the bridge or the broker.

---

## 8. Reporting template

For each grid cell, record:

```
date:            2025-04-01
payload_bytes:   10240
rate_target:     1000
schema:          nested
array_type:      var
runs:            5
warmup_s:        30
measurement_s:   180

latency_mean_ms: 3.2
latency_p50_ms:  2.9
latency_p95_ms:  7.1
latency_p99_ms:  14.3

throughput_msgs_s: 998.4
throughput_mb_s:   9.75

drop_rate_pct:   0.00

cpu_bridge_avg_pct:  12.3
cpu_bridge_max_pct:  18.7
rss_bridge_mb_avg:   142
rss_bridge_mb_max:   156
```
