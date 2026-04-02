# API Reference

ROS 2 service, topic, and message-type reference for `ros2_kafka_dispatcher`.

---

## Custom message types

### introspection_manager_msgs/msg/TopicInfo

```
string name    # ROS 2 topic name, e.g. /camera/image_raw
string type    # ROS 2 message type, e.g. sensor_msgs/msg/Image
```

### introspection_manager_msgs/msg/TopicsInfo

```
TopicInfo[] topics    # list of all discovered topics
```

---

## Services

### dispatcher_controller

All services are relative to the controller node namespace (default `/dispatcher_controller`).

#### apply_selection

`dispatcher_controller/srv/ApplySelection`

Applies a GUI-provided topic list. Only valid when `selection_mode` is `gui`; ignored or rejected in other modes.

```
# Request
introspection_manager_msgs/TopicInfo[] topics    # topics to stream

---
# Response
bool   success
string message
```

#### reload_selection

`dispatcher_controller/srv/ReloadSelection`

Reload and apply the selection from the current mode: in `file` mode re-reads the YAML file from `selection_file_path`; in `all` mode re-runs topic auto-discovery via `introspection_manager`; in `gui` mode re-applies the last GUI-provided selection if present.

```
# Request
string selection_file_path   # optional override path (file mode only)
bool   apply_now             # immediately apply after reload

---
# Response
bool   success
string message
```

#### stop_streaming

`dispatcher_controller/srv/StopStreaming`

Deactivate all sinks immediately; no messages will be forwarded until the next `apply_selection` or `reload_selection`.

```
# Request
bool reset_cached   # also clear cached selections if true

---
# Response
bool   success
string message
```

#### get_status

`dispatcher_controller/srv/GetStatus`

Query the current state of the controller and managed sinks.

```
# Request
(empty)

---
# Response
bool                                        success
string                                      message
string                                      selection_mode         # gui | file | all
bool                                        streaming_active
string                                      kafka_sink_state       # lifecycle state of kafka_sink
string                                      mosquitto_sink_state   # lifecycle state of mosquitto_sink
introspection_manager_msgs/TopicInfo[]      applied_topics         # topics currently streaming
uint32                                      gui_selection_count
uint32                                      file_selection_count
uint32                                      all_selection_count
string                                      last_error
builtin_interfaces/Time                     last_error_stamp
bool                                        reconciling
```

#### set_selection_mode

`dispatcher_controller/srv/SetSelectionMode`

Switch selection mode at runtime.

```
# Request
string selection_mode        # gui | file | all
string selection_file_path   # path to selection YAML (only for file mode)
bool   apply_now             # immediately apply after mode change

---
# Response
bool   success
string message
```

### introspection_manager

Relative to the introspection manager node namespace (default `/introspection_manager`).

#### get_topics

`introspection_manager_msgs/srv/GetTopics`

Return the current snapshot of all discovered topics.

```
# Request
(empty)

---
# Response
introspection_manager_msgs/TopicInfo[] topics
```

#### apply_pipeline

`introspection_manager_msgs/srv/ApplyPipeline`

Apply a desired pipeline specification to the dispatcher_controller.

```
# Request
introspection_manager_msgs/TopicInfo[]      subscriptions       # topics to subscribe to
introspection_manager_msgs/PluginInstance[] plugins             # plugin instances to apply
string                                      kafka_mapping_yaml  # reserved for future kafka mapping config (YAML)

---
# Response
bool   success
string message
```

#### get_pipeline_status

`introspection_manager_msgs/srv/GetPipelineStatus`

Return the current pipeline status.

```
# Request
(empty)

---
# Response
bool                                        success
string                                      message
string                                      kafka_sink_state       # lifecycle state of kafka_sink
introspection_manager_msgs/TopicInfo[]      active_subscriptions   # currently active subscriptions
introspection_manager_msgs/PluginInstance[] active_plugins         # currently active plugins
string                                      last_error
bool                                        reconciling
```

#### stop_pipeline

`introspection_manager_msgs/srv/StopPipeline`

Stop the current pipeline.

```
# Request
(empty)

---
# Response
bool   success
string message
```

---

## Published topics

### introspection_manager

| Topic | Type | QoS |
|-------|------|-----|
| `~/topics_info` | `introspection_manager_msgs/msg/TopicsInfo` | Configurable (default: reliable, volatile) |

Published whenever the topic list changes (if `publish_on_change:=true`) or at every poll cycle.

### kafka_sink / mosquitto_sink

| Topic | Type | Description |
|-------|------|-------------|
| `<metrics.topic>` | `std_msgs/msg/String` (JSON) | Per-topic metrics published at `metrics.interval_ms` intervals. |

#### Metrics JSON schema

```json
{
  "ros_topic": "/camera/image_raw",
  "kafka_topic": "ros2.camera.image_raw",
  "msg_count": 1234,
  "msg_rate_hz": 30.1,
  "bytes_total": 56789012,
  "bytes_rate_bps": 1234567,
  "latency_p50_ms": 2.3,
  "latency_p95_ms": 5.1,
  "latency_p99_ms": 9.8,
  "avg_payload_bytes": 46000
}
```

---

## Telemetry log format (kafka_sink)

When `telemetry.enabled: true`, the sink logs one JSON line per message (every `telemetry.log_every_n` messages) at `INFO` level via the ROS 2 logger.

```json
{
  "msg_id": "...",
  "ros_topic": "/camera/image_raw",
  "kafka_topic": "ros2.camera.image_raw",
  "receive_time_ns": 1711900000000000000,
  "kafka_timestamp_ms": 1711900000000,
  "payload_bytes": 46080,
  "serialize_time_ns": 12345
}
```

---

## Kafka record format

Each Kafka record produced by `kafka_sink` has:

- **Value:** CDR bytes (binary) or JSON string, depending on `kafka.payload_format`.
- **Headers:**

| Key | Example value |
|-----|--------------|
| `ros_type` | `sensor_msgs/msg/Image` |

> **Note:** `ros_topic`, `kafka_topic`, `msg_type`, `stamp_ms`, and `payload_format` are **not**
> Kafka record headers. `ros_topic`, `kafka_topic`, and `payload_format` appear in the telemetry
> log; `msg_type` appears in the metrics log. The message timestamp is carried as the Kafka record
> timestamp (set via `timestamp_ms` in `produce()`), not as a header.

---

## Topic naming conventions

### Kafka (prefix_ros_topic mode)

ROS topic `/a/b/c` with `kafka.topic_prefix = ros2` becomes Kafka topic `ros2.a.b.c` (slashes replaced with dots, leading slash stripped).

### MQTT (prefix_ros_topic mode)

ROS topic `/a/b/c` with `mqtt.topic_prefix = ros2` becomes MQTT topic `ros2/a/b/c` (leading slash stripped, slashes preserved as MQTT hierarchy).

### Fixed mode

All messages go to the single configured topic regardless of ROS topic name.
