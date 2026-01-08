# kafka_sink

Lifecycle-based Kafka sink node that dynamically subscribes to topics, forwards serialized payloads
into Kafka, and emits lightweight per-topic telemetry.

## Build

```bash
rosdep install --from-paths src --ignore-src -y
colcon build --symlink-install --cmake-args -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=On --packages-up-to kafka_sink
source install/setup.bash
```

## Launch as a component

Start the node inside a component container:

```bash
ros2 launch kafka_sink kafka_sink_container.launch.py
```

The launch file loads the `kafka_sink::KafkaSinkNode` into `component_container_mt` with parameters
from `config/kafka_sink.param.yaml` by default.

## Lifecycle control

Configure and activate the node:

```bash
ros2 lifecycle set /kafka_sink configure
ros2 lifecycle set /kafka_sink activate
```

Deactivate to stop all subscriptions and DDS traffic (the Kafka producer is stopped as well):

```bash
ros2 lifecycle set /kafka_sink deactivate
```

## Parameters

- `subscriptions_yaml` (string): YAML sequence describing subscriptions. Example:
  ```yaml
  - topic_name: /demo/chatter
    msg_type: std_msgs/msg/String
    kafka_name: chatter_json
  - topic_name: /demo/number
    msg_type: std_msgs/msg/Int32
  ```
  - `kafka_name` (string, optional): override the Kafka topic mapping input name for this
    subscription (defaults to `topic_name`).
- `qos_depth` (int, default 10): Depth for the QoS profile (KeepLast).
- Kafka producer parameters (modifiable only while inactive):
  - `kafka.bootstrap_servers` (string, default `localhost:9092`)
  - `kafka.client_id` (string, default `kafka_sink`)
  - `kafka.acks` (string, default `all`)
  - `kafka.topic_prefix` (string, default `ros2`)
  - `kafka.topic_mapping_mode` (string): `prefix_ros_topic` (default) maps ROS topics to `{prefix}.{ros_topic with '/' -> '.'}`; `fixed` sends everything to `kafka.fixed_topic`.
  - `kafka.fixed_topic` (string, default `ros2.raw`)
  - `kafka.strict_startup` (bool, default `false`): fail activation when true if Kafka is unreachable; otherwise the producer retries in the background.
  - `kafka.max_queue_messages` (int, default `1024`): bounded local retry buffer when Kafka back-pressure occurs.
  - `kafka.drop_when_full` (bool, default `true`): drop instead of buffering when queues are full.
  - `kafka.linger_ms` / `kafka.batch_size` (ints, default `-1` meaning unset): forwarded to librdkafka for batching.
  - `kafka.payload_format` (string, default `cdr`): `cdr` publishes raw ROS 2 serialized bytes; `json` converts to JSON before sending.

You can update `subscriptions_yaml` while the node is inactive. When the node is active the update
is rejected with the message `deactivate first`. The same rule applies to the `kafka.*` parameters.

Set parameters at runtime:

```bash
ros2 param set /kafka_sink subscriptions_yaml "[{'topic_name': '/foo', 'msg_type': 'std_msgs/msg/String'}]"
```

## Notes

- Subscriptions are created on activation and destroyed on deactivation/cleanup/shutdown. Kafka
  producer startup and shutdown follow the same lifecycle transitions.
- For every message, `kafka_sink` sends either the raw serialized CDR payload or a JSON
  representation (controlled by `kafka.payload_format`) to Kafka with headers for `ros_topic`,
  `ros_type`, `kafka_topic`, `msg_type`, `stamp_ms`, and `payload_format`. The Kafka topic is
  derived from the ROS topic according to the configured mapping rules.
- Each received serialized message is logged at most once per second per topic with topic name,
  byte size, and aggregate send statistics (sent/queued drops/errors).
