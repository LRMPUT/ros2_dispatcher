# kafka_source

Lifecycle-based Kafka consumer that subscribes to Kafka topics carrying raw ROS 2 CDR payloads and republishes them as ROS 2 topics with a configurable prefix.

## Overview

`kafka_source` mirrors the Python `ros2_kafka_cdr_to_ros.py` workflow in C++:

- Subscribes to Kafka topics matching a regex pattern (default: `^ros2\..*`).
- Reads raw CDR payloads from Kafka messages.
- Looks up the ROS 2 type from the `ros_type` Kafka header.
- Dynamically loads the ROS 2 type support via `rosbag2_cpp`.
- Deserializes the payload with `rmw_deserialize` and republishes the message.
- Publishes per-topic JSON metrics to a ROS 2 topic.

## Parameters

All parameters are configurable while the node is inactive:

- `kafka.bootstrap_servers` (string, default: `localhost:9092`): Kafka broker address.
- `kafka.group_id` (string, default: `ros2-kafka-source`): Kafka consumer group id.
- `kafka.topic_pattern` (string, default: `^ros2\..*`): Kafka subscription regex.
- `kafka.offset_reset` (string, default: `latest`): `earliest` or `latest`.
- `ros_topic_prefix` (string, default: `/kafka_decoded`): Output ROS topic prefix.
- `qos_depth` (int, default: `10`): ROS 2 publisher QoS depth (KeepLast).
- `metrics.enabled` (bool, default: `true`): Enable metrics publishing.
- `metrics.interval_ms` (int, default: `1000`): Metrics publish interval.
- `metrics.topic` (string, default: `kafka_source/metrics`): Metrics topic name.
- `topic_mappings` (string, default: empty): Optional comma-separated `kafka_topic=ros_topic` overrides.

## Topic mapping

Kafka topic names are converted to ROS 2 topics by removing the `ros2.` prefix (if present) and replacing dots with slashes.

Example:

- Kafka: `ros2.sensing.gnss.gnss_transforms.rear_axis_pose`
- ROS: `/kafka_decoded/sensing/gnss/gnss_transforms/rear_axis_pose`

## Metrics

Metrics are published as JSON on the topic configured by `metrics.topic`. Each entry includes totals, deltas, throughput, and latency percentiles.

**Note:** The `metrics.topic` parameter specifies a relative topic name (default: `kafka_source/metrics`). ROS 2 automatically prepends the node's namespace, resulting in an absolute topic name (e.g., `/kafka_source/metrics` in the default namespace).

Example subscription:

```bash
ros2 topic echo /kafka_source/metrics
```

## Example usage

```bash
ros2 run kafka_source kafka_source_node_exe
```

Verify output topic:

```bash
ros2 topic echo /kafka_decoded/sensing/gnss/gnss_transforms/rear_axis_pose
```
