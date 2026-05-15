# Scalability Experiment

Implements the spec at
`docs/superpowers/specs/2026-05-11-scalability-experiment-design.md`.

## Quick run

```bash
export BAG_PATH=/absolute/path/to/single_robot_navsatfix_bag
./tools/benchmark/scaling/run_scaling_matrix.sh --reps 1
./tools/benchmark/scaling/analyze_scaling.py \
    --input  tools/benchmark/scaling/results \
    --output tools/benchmark/scaling/results
```

## Smoke pass

```bash
./tools/benchmark/scaling/run_scaling_matrix.sh \
    --reps 1 --robots 1,10 --duration 30
```

## Inputs

- `BAG_PATH` env var: a directory containing a rosbag2 (`metadata.yaml`
  + `*.db3` or `*.mcap`) with at least one `sensor_msgs/msg/NavSatFix`
  topic. The replay loops it automatically.

## Outputs

- `results/N=<n>_broker=<b>_run=<r>/consumer.jsonl` — one line per
  delivered message: `{robot_id, topic, t0_ns, t1_ns, latency_ns, bytes}`.
- `results/N=<n>_broker=<b>_run=<r>/sink_metrics.csv` — per-second
  sink-side stats from `metrics_recorder.py`.
- `results/summary.csv` — aggregated per `(N, broker)`.
- `results/plots/*.png` — latency / throughput / drop rate vs N.
