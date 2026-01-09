# kafka_cdr_to_json

Lifecycle-based Kafka consumer/producer that bridges ROS 2 CDR payloads into JSON.
It subscribes to Kafka topics (regex pattern), deserializes raw CDR bytes using ROS 2
introspection type support, converts the message to JSON, and publishes the JSON to
output Kafka topics. Per-topic metrics are emitted as JSON on a ROS 2 topic.

## Parameters

- `kafka.bootstrap_servers` (string, default: `localhost:9092`)
- `kafka.group_id` (string, default: `ros2-cdr-to-json`)
- `kafka.input_topic_pattern` (string, default: `^ros2\..*`)
- `kafka.output_topic_prefix` (string, default: `ros2_json`)
- `kafka.offset_reset` (string, `earliest` or `latest`)
- `json.include_ros_type` (bool, default: `true`)
- `json.include_timestamp` (bool, default: `true`)
- `metrics.enabled` (bool, default: `true`)
- `metrics.interval_ms` (int, default: `1000`)
- `metrics.topic` (string, default: `kafka_cdr2json/metrics`)
- `topic_mappings` (string, optional overrides: `input_topic=output_topic,...`)

## Metrics

Metrics are published as JSON on the topic configured by `metrics.topic`. Each entry includes
counters for received/converted/failed messages, deltas, throughput, latency percentiles, JSON
payload sizes, and topic/type metadata.
