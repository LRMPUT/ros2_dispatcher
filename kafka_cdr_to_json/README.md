# Kafka CDR-to-JSON Bridge

`kafka_cdr_to_json` is a ROS 2 lifecycle node that consumes CDR-encoded messages from Kafka,
loads ROS 2 type support dynamically, converts payloads to human-readable JSON, and publishes
JSON back to Kafka while emitting per-topic metrics.

## Parameters

| Name | Type | Default | Description |
| ---- | ---- | ------- | ----------- |
| `kafka.bootstrap_servers` | string | `localhost:9092` | Kafka broker list. |
| `kafka.group_id` | string | `kafka-cdr-to-json` | Kafka consumer group id. |
| `kafka.input_topic_pattern` | string | `^ros2\..*` | Regex for input topics. |
| `kafka.output_topic_prefix` | string | `ros2_json` | Prefix for JSON output topics. |
| `kafka.offset_reset` | string | `latest` | `latest` or `earliest`. |
| `json.include_ros_type` | bool | `true` | Add `__ros_type` field to JSON. |
| `json.include_timestamp` | bool | `true` | Add `__kafka_timestamp_ms` field to JSON. |
| `metrics.enabled` | bool | `true` | Enable metrics publication. |
| `metrics.interval_ms` | int | `1000` | Metrics publish interval in ms. |
| `metrics.topic` | string | `kafka_cdr2json/metrics` | ROS 2 topic for metrics JSON. |
| `topic_mappings` | string | empty | Comma-separated `input=output` overrides. |

## Metrics Output

Metrics are published as JSON (string) on the configured ROS 2 topic. Each entry contains
per-input-topic counters, deltas, throughput rates, latency percentiles, JSON sizes, and
type metadata.
