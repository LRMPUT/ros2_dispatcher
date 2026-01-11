# Latency measurement and telemetry workflow

This guide documents the end-to-end data path, telemetry hooks, and a runnable script to
collect structured latency logs for the ROS 2 → Kafka → ROS 2 pipeline.

## 1) Data path for measurement

**Publisher → kafka_sink → Kafka → kafka_source → Consumer**

1. **Publisher** injects a unique `msg_id` and a **t0** timestamp into the payload.
2. **kafka_sink** serializes the ROS 2 message and forwards it to Kafka.
   - When telemetry is enabled, it logs per-message serialization time and payload size.
3. **kafka_source** reads the Kafka payload, deserializes it, and republishes on a ROS 2 topic.
4. **Consumer** records **t1** on receive and computes `latency_ns = t1_ns - t0_ns`.

## 2) Logging hooks / telemetry

### kafka_sink telemetry

Enable the structured telemetry logs (per-message) by setting:

```yaml
telemetry.enabled: true
telemetry.log_every_n: 1
```

Each log line is JSON and includes:

- `msg_id`
- `receive_time_ns` (time at the sink callback)
- `kafka_timestamp_ms`
- `payload_bytes`
- `serialize_time_ns`
- `ros_topic` / `kafka_topic`

### Producer / consumer logs

The helper scripts below log JSON lines:

- `publisher.jsonl`: `msg_id`, `t0_ns`, payload size, topic
- `consumer.jsonl`: `msg_id`, `t0_ns`, `t1_ns`, `latency_ns`, payload size

## 3) System monitoring tools

Recommended commands during a run (adjust PIDs from the runner output):

- **CPU**
  - `pidstat -p <pid> 1`
  - `top -p <pid>`
  - `htop` (interactive)
  - `perf stat -p <pid>`
- **RSS / memory**
  - `ps -o pid,rss,command -p <pid>`
  - `cat /proc/<pid>/status | rg -n "VmRSS|VmHWM"`

## 4) One-shot runner script

Use the provided runner to start:

- Kafka sink (ROS 2 → Kafka)
- Kafka source (Kafka → ROS 2)
- Latency publisher
- Latency consumer

```bash
./tools/latency/run_latency_capture.sh \
  --count 200 \
  --rate 10 \
  --payload-bytes 256 \
  --output-dir ./latency_artifacts
```

Artifacts are written as structured JSON lines:

- `publisher.jsonl`
- `consumer.jsonl`
- `kafka_sink.log`
- `kafka_source.log`

## 5) Clock synchronization

For accurate end-to-end latency:

- **Preferred:** run publisher, sink, source, and consumer on the same machine.
- **Distributed:** synchronize clocks using NTP or PTP. Example NTP check:
  - `timedatectl status` (systemd)
  - `chronyc tracking` (chrony)

