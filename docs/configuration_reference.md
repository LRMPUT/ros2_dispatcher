# Configuration Reference

Complete parameter reference for all nodes in `ros2_kafka_dispatcher`.

---

## dispatcher_controller

Declared in `dispatcher_controller/src/dispatcher_controller_node.cpp`.

### Selection

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `selection_mode` | string | `file` | Topic selection mode: `gui`, `file`, or `all`. |
| `selection_file_path` | string | `""` | Path to the YAML selection file (required in `file` mode). |
| `auto_apply_on_mode_change` | bool | `true` | Automatically apply selection when mode changes. |
| `validate_topics` | bool | `false` | Verify topics exist in the live graph before activating sinks. |

### Sink management

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `kafka_sink_node_name` | string | `/kafka_sink` | Fully qualified name of the Kafka sink lifecycle node. |
| `mosquitto_sink_node_name` | string | `/mosquitto_sink` | Fully qualified name of the Mosquitto sink lifecycle node. |
| `allow_missing_sinks` | bool | `true` | Continue if one or both sinks are absent. |
| `component_container_name` | string | `/ros2_kafka_dispatcher_container` | Container used for topic_tools plugin nodes. |

### Introspection

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `introspection_service_name` | string | (derived) | Service name for `~/get_topics`. |
| `introspection_node_name` | string | (derived) | Node name of the introspection manager. |
| `disable_introspection_after_apply` | bool | `true` | Stop polling the graph after selection is applied to save CPU. |

### All-mode

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `all_mode_max_topics` | int | `200` | Maximum number of topics to select in `all` mode. |
| `all_mode_allowlist` | string[] | `[]` | Only include topics matching these patterns (empty = allow all). |
| `all_mode_denylist` | string[] | `[]` | Exclude topics matching these patterns. |
| `all_mode_hide_rosout` | bool | `true` | Exclude `/rosout` from auto-selected topics. |

### Selection file format

```yaml
# dispatcher_controller/config/topics.yaml
- topic_name: /camera/image_raw
  msg_type: sensor_msgs/msg/Image

- topic_name: /cmd_vel
  msg_type: geometry_msgs/msg/Twist
  topic_tools:
    plugin: topic_tools::ThrottleNode
    output_topic: /throttle/cmd_vel
    parameters:
      period: 0.05          # seconds between forwarded messages
```

`msg_type` can be omitted when `validate_topics:=false` and the controller can infer it from the running graph.

---

## introspection_manager

Defaults in `introspection_manager/config/introspection_manager.param.yaml`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `publisher_queue_depth` | int | `1` | Queue depth for `~/topics_info` publisher. |
| `publisher_reliability` | string | `reliable` | QoS reliability: `reliable` or `best_effort`. |
| `publisher_durability` | string | `volatile` | QoS durability: `volatile` or `transient_local`. |
| `publish_on_change` | bool | `true` | Publish `topics_info` only when the topic list changes. |
| `filter_hidden` | bool | `true` | Exclude topics whose name segments start with `_`. |
| `introspection_enabled` | bool | `true` | Enable background graph monitoring thread. |

---

## kafka_sink

Defaults in `kafka_bridge/kafka_sink/config/kafka_sink.param.yaml`.

### Subscriptions / QoS

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `subscriptions_yaml` | string | `""` | YAML-encoded list of topic subscriptions (set by controller). |
| `qos_depth` | int | `10` | History queue depth for ROS 2 subscriptions. |

### Kafka producer

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `kafka.bootstrap_servers` | string | `localhost:9092` | Comma-separated Kafka broker addresses. |
| `kafka.client_id` | string | `kafka_sink` | Producer client identifier. |
| `kafka.acks` | string | `all` | Producer acknowledgement level (`0`, `1`, or `all`). |
| `kafka.topic_prefix` | string | `ros2` | Prefix prepended to ROS topic name for Kafka topic (in `prefix_ros_topic` mode). |
| `kafka.topic_mapping_mode` | string | `prefix_ros_topic` | `prefix_ros_topic` maps `/a/b` → `<prefix>.a.b`; `fixed` sends everything to `kafka.fixed_topic`. |
| `kafka.fixed_topic` | string | `ros2.raw` | Kafka topic used when `topic_mapping_mode` is `fixed`. |
| `kafka.strict_startup` | bool | `false` | Fail `on_configure` if the broker is unreachable. |
| `kafka.max_queue_messages` | int | `1024` | Maximum messages in the in-process send queue. |
| `kafka.drop_when_full` | bool | `true` | Drop new messages when queue is full (vs. blocking). |
| `kafka.linger_ms` | int | (librdkafka default) | Linger time before sending a batch (ms). |
| `kafka.batch_size` | int | (librdkafka default) | Maximum bytes per batch. |
| `kafka.payload_format` | string | `cdr` | Serialization format: `cdr` (binary) or `json`. |

### Metrics

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `metrics.enabled` | bool | `true` | Publish per-topic metrics to ROS 2. |
| `metrics.interval_ms` | int | `1000` | Metrics publish interval in milliseconds. |
| `metrics.topic` | string | `kafka_sink/metrics` | ROS 2 topic name for metrics messages. |

### Telemetry

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `telemetry.enabled` | bool | `false` | Log per-message structured telemetry to the ROS 2 logger. |
| `telemetry.log_every_n` | int | `1` | Log every Nth message (1 = every message). |

### Topic subscription YAML format

```yaml
- topic_name: /demo/chatter
  msg_type: std_msgs/msg/String

- topic_name: /camera/image_raw
  msg_type: sensor_msgs/msg/Image
  kafka_name: camera_raw          # optional: override Kafka topic name
```

### Kafka headers added to every record

| Header key | Value |
|------------|-------|
| `ros_topic` | Original ROS 2 topic name |
| `ros_type` | ROS 2 message type (e.g. `std_msgs/msg/String`) |
| `kafka_topic` | Kafka topic the record was produced to |
| `stamp_ms` | Timestamp in milliseconds |
| `payload_format` | `cdr` or `json` |

---

## mosquitto_sink

Defaults in `mosquitto_bridge/mosquitto_sink/config/mosquitto_sink.param.yaml`.

### Subscriptions / QoS

Same as `kafka_sink` (`subscriptions_yaml`, `qos_depth`).

### MQTT broker

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `mqtt.broker_host` | string | `localhost` | MQTT broker hostname or IP. |
| `mqtt.broker_port` | int | `1883` | MQTT broker port. |
| `mqtt.client_id` | string | `mosquitto_sink` | MQTT client identifier. |
| `mqtt.username` | string | `""` | MQTT username (leave empty to skip authentication). |
| `mqtt.password` | string | `""` | MQTT password. |
| `mqtt.qos` | int | `1` | MQTT QoS level: `0`, `1`, or `2`. |
| `mqtt.retain` | bool | `false` | Set the MQTT retain flag on published messages. |
| `mqtt.keep_alive_seconds` | int | `60` | MQTT keep-alive interval in seconds. |

### Topic mapping

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `mqtt.topic_prefix` | string | `ros2` | Prefix for MQTT topic names (in `prefix_ros_topic` mode). |
| `mqtt.topic_mapping_mode` | string | `prefix_ros_topic` | Same semantics as `kafka_sink` (`prefix_ros_topic` or `fixed`). |
| `mqtt.fixed_topic` | string | `ros2/raw` | MQTT topic used when mode is `fixed`. |
| `mqtt.payload_format` | string | `cdr` | `cdr` or `json`. |

### TLS / SSL

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `mqtt.use_tls` | bool | `false` | Enable TLS encryption. |
| `mqtt.ca_cert_path` | string | `""` | Path to CA certificate file. |

### Last Will and Testament (LWT)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `mqtt.lwt_topic` | string | `""` | LWT topic (empty disables LWT). |
| `mqtt.lwt_payload` | string | `mosquitto_sink disconnected` | LWT message payload. |
| `mqtt.lwt_qos` | int | `1` | LWT QoS level. |
| `mqtt.lwt_retain` | bool | `false` | Set retain flag on LWT message. |

### Metrics

Same as `kafka_sink` (`metrics.enabled`, `metrics.interval_ms`, `metrics.topic`).

---

## kafka_source

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `kafka.bootstrap_servers` | string | `localhost:9092` | Broker address. |
| `kafka.group_id` | string | `ros2-kafka-source` | Consumer group ID. |
| `kafka.topic_pattern` | string | `^ros2\\..*` | Regex pattern for Kafka topics to consume. |
| `kafka.offset_reset` | string | `latest` | Offset policy: `earliest` or `latest`. |

---

## System-level launch arguments

Exposed by `ros2_kafka_dispatcher_bringup/launch/system_minimal.launch.py`.

| Argument | Default | Description |
|----------|---------|-------------|
| `selection_mode` | `file` | Passed to `dispatcher_controller`. |
| `selection_file_path` | `""` | Path to selection YAML. |
| `kafka_sink_node_name` | `/kafka_sink` | Node name for Kafka sink. |
| `mosquitto_sink_node_name` | `/mosquitto_sink` | Node name for Mosquitto sink. |
| `validate_topics` | `false` | Enable topic validation. |
| `subscriptions_yaml` | `""` | Direct subscription override (bypasses controller). |
| `qos_depth` | `10` | Subscription queue depth. |
| `enable_kafka_sink` | `true` | Include Kafka sink in launch. |
| `enable_mosquitto_sink` | `true` | Include Mosquitto sink in launch. |
| `controller_log_level` | `debug` | Log level for `dispatcher_controller`. |
| `kafka_sink_log_level` | `info` | Log level for `kafka_sink`. |
| `mosquitto_sink_log_level` | `info` | Log level for `mosquitto_sink`. |
| `introspection_manager_log_level` | `DEBUG` | Log level for `introspection_manager`. |
| `kafka_sink_param_file` | `""` | Override parameter file for Kafka sink. |
| `mosquitto_sink_param_file` | `""` | Override parameter file for Mosquitto sink. |
| `introspection_manager_param_file` | `""` | Override parameter file for introspection manager. |
