# kafka_sink

Lifecycle-based Kafka sink node that dynamically subscribes to topics and pushes serialized payloads
(CDR) into Kafka along with ROS metadata headers. The sink remains transport-agnostic and forwards
raw bytes without schema translation.

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

Deactivate to stop all subscriptions and DDS traffic:

```bash
ros2 lifecycle set /kafka_sink deactivate
```

## Parameters

- `subscriptions_yaml` (string): YAML sequence describing subscriptions. Example:
  ```yaml
  - topic_name: /demo/chatter
    msg_type: std_msgs/msg/String
  - topic_name: /demo/number
    msg_type: std_msgs/msg/Int32
  ```
- `qos_depth` (int, default 10): Depth for the QoS profile (KeepLast).
- Kafka configuration (immutable after configure):
  - `kafka.bootstrap_servers` (string, required)
  - `kafka.client_id` (string, default `kafka_sink`)
  - `kafka.acks` (string, default `all`)
  - `kafka.start_mode` (`strict`|`tolerant`, default `strict`)
  - `kafka.enable_idempotence` (bool, default `true`)
  - `kafka.linger_ms`, `kafka.batch_size`, `kafka.queue_buffering_max_kbytes`,
    `kafka.max_in_flight`, `kafka.retries`, `kafka.retry_backoff_ms` (ints, negative to skip)
  - `kafka.max_pending_messages` (int, limit for librdkafka internal queue; 0 keeps default)
  - `kafka.topic_prefix` (string, optional prefix applied to resolved Kafka topics)

You can update `subscriptions_yaml` while the node is inactive. When the node is active the update
is rejected with the message `deactivate first`.

Set parameters at runtime:

```bash
ros2 param set /kafka_sink subscriptions_yaml "[{'topic_name': '/foo', 'msg_type': 'std_msgs/msg/String'}]"
```

## Notes

- Subscriptions are created on activation and destroyed on deactivation/cleanup/shutdown.
- Each received serialized message is logged at most once per second per topic with topic name and
  byte size.
- Kafka send attempts are non-blocking; queue backpressure is handled by dropping with counters and
  throttled warnings. Metadata headers include ROS topic, type, encoding (`cdr`), and a timestamp in
  nanoseconds.
