# ROS2 ↔ Kafka Dispatcher Benchmark Plan

## 1) Message payload sizes
Use a logarithmic spread that exercises small control messages through large sensor payloads:

- 256 B
- 1 KB
- 10 KB
- 100 KB
- Optional stress tier: 1 MB (only if you want upper bounds or fragmentation behavior)

## 2) Message rates (feasible tiers)
Pick rates that cover under‑saturation, near‑saturation, and saturation.

- 100 msgs/s (baseline)
- 1,000 msgs/s (moderate load)
- 5,000 msgs/s (high load, but usually feasible)
- 10,000 msgs/s (include only if your pipeline can sustain it without drops)

If you need a compact grid, use 100, 1k, 10k.

## 3) Message schemas
Define canonical schemas that are easy to reason about and repeat across sizes.

### 3.1 Simple primitives (flat)
Example:

```
{
  uint64 id,
  float64 ts,
  float32[?] values
}
```

### 3.2 Nested structure
Example:

```
{
  header { uint64 id, float64 ts },
  pose   { float32[3] pos, float32[4] quat },
  meta   { uint32 flags, uint32[?] tags }
}
```

### 3.3 Array shape variants
Use both variants for each payload size tier:

- Fixed‑size arrays (e.g., `float32[64]`)
- Variable‑size arrays (e.g., `float32[]` sized to hit payload target)

Adjust array lengths to hit the target payload size for each tier.

## 4) Non‑serialization variables (fixed across all runs)

### Kafka
- Same broker, same topic(s), same partitions
- Same `acks`, `batch.size`, `linger.ms`, `compression.type`

### ROS2
- Same QoS policy (reliability, history, depth, durability)
- Same publishers/subscribers and executor configuration

### Environment
- Same host, same CPU governor (performance), pinned affinity if possible
- Same container/image versions

## 5) Duration & warm‑up
- Warm‑up: 30–60 seconds
- Measurement window: 2–5 minutes

## 6) Repetitions
- 5 runs per cell (minimum)
- Increase to 7–10 if variability is high

## 7) Naming convention
Use a structured, parseable scheme for later attribution:

```
<date>__size-<bytes>__rate-<msgs>__schema-<flat|nested>__array-<fixed|var>__run-<n>
```

Example:

```
2025-03-08__size-10240__rate-1000__schema-nested__array-var__run-03
```
