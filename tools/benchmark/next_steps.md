# CDR vs JSON Benchmark — Experiment Plan

Branch: `feature/cdr-vs-json-benchmark`

## Phase 1 — Tooling (DONE)

Created in `tools/benchmark/`:
- `synthetic_publisher.py` — publishes NavSatFix / Odometry / PointCloud2 at controlled rates
- `metrics_recorder.py` — records `kafka_sink/metrics` to CSV (warmup + duration)
- `run_benchmark.sh` — orchestrates a single run (sink + publisher + recorder)
- `run_full_matrix.sh` — runs full 72-configuration matrix

## Phase 2 — Single-type validation (NavSatFix, verify tooling)

Prerequisites:
```bash
source /opt/ros/humble/setup.bash
source ~/colcon_ws/install/setup.bash
cd ~/Github/ros2_kafka_dispatcher/kafka_bridge/kafka_brocker && docker compose up -d
```

Run:
```bash
cd ~/Github/ros2_kafka_dispatcher/tools/benchmark

# CDR baseline
./run_benchmark.sh --msg-type navsatfix --format cdr --rate 10 --duration 15 --warmup 3 --run-id 1

# JSON comparison
./run_benchmark.sh --msg-type navsatfix --format json --rate 10 --duration 15 --warmup 3 --run-id 1
```

Verify:
- [ ] CSV files created with data rows (not just headers)
- [ ] `msg_size_avg_bytes`: JSON > CDR
- [ ] `serialize_avg_ns`: JSON > CDR
- [ ] `delta_dropped` is 0 at 10 Hz

## Phase 3 — Full matrix (all types, all rates)

Test matrix: 3 msg types × 2 formats × 4 rates × 3 reps = 72 runs

| Message Type | Format | Rates (Hz) |
|---|---|---|
| NavSatFix (~100B) | cdr, json | 10, 50, 100, 500 |
| Odometry (~700B) | cdr, json | 10, 50, 100, 500 |
| PointCloud2 (~160KB) | cdr, json | 10, 50, 100, 500 |

```bash
cd ~/Github/ros2_kafka_dispatcher/tools/benchmark

# Quick pass first (1 rep, 30s) — ~30 min
./run_full_matrix.sh --reps 1 --duration 30

# Full run (3 reps, 60s) — ~90 min
./run_full_matrix.sh --reps 3 --duration 60
```

Results saved to `tools/benchmark/results/`.

## Phase 4 — Analysis and paper figures

Create `tools/benchmark/analyze_results.py`:

1. Read all CSVs from `results/`
2. Compute summary statistics per configuration (mean ± std across repetitions)
3. Generate figures:
   - (a) Serialization latency bar chart: CDR vs JSON grouped by message type
   - (b) Message size ratio table: CDR bytes vs JSON bytes per type
   - (c) Throughput vs publish rate line plot (CDR vs JSON, per type)
   - (d) p95/p99 tail latency comparison
4. Generate LaTeX table with key numbers for the paper

Output: `tools/benchmark/figures/` (PDF + PNG)

## Phase 5 — Rosbag experiments (deferred)

Replace synthetic publisher with real agricultural robot data:
```bash
ros2 bag play <rosbag_path> --rate 1.0
```

- Re-run metrics collection with same recorder
- Compare real-world message mix against synthetic baselines
- Report message type distribution and size variance from real data

## Key metrics to report in paper

| Metric | Source field | Unit |
|---|---|---|
| Serialization latency (avg) | `serialize_avg_ns` | µs |
| Serialization latency (p99) | `serialize_p99_ns` | µs |
| Send latency (avg) | `send_avg_ns` | µs |
| Message size | `msg_size_avg_bytes` | bytes |
| Size ratio | JSON size / CDR size | × |
| Throughput | `serialize_mb_per_sec` | MB/s |
| Drop rate | `delta_dropped / delta_received` | % |
