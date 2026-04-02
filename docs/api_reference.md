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

### kafka_sink

| Topic | Type | Description |
|-------|------|-------------|
| `<metrics.topic>` | `std_msgs/msg/String` (JSON) | Per-topic metrics published at `metrics.interval_ms` intervals. |

#### kafka_sink metrics JSON schema

The payload is a JSON **array**. Each element corresponds to one active ROS topic subscription.

```json
[
  {
    "ros_topic": "/camera/image_raw",
    "kafka_topic": "ros2.camera.image_raw",
    "msg_type": "sensor_msgs/msg/Image",
    "payload_format": "cdr",
    "interval_ms": 1000,
    "delta": {
      "received": 30,
      "sent_ok": 30,
      "dropped": 0,
      "errors": 0,
      "bytes": 1382400
    },
    "rates": {
      "received_per_sec": 30.0,
      "sent_per_sec": 30.0
    },
    "message_size": {
      "avg_bytes": 46080.0,
      "min_bytes": 46080,
      "max_bytes": 46080
    },
    "latency_ns": {
      "serialize_avg": 12345,
      "serialize_p95": 15000,
      "serialize_p99": 18000,
      "serialize_max": 20000,
      "send_avg": 5000,
      "send_p95": 7000,
      "send_p99": 9000,
      "send_max": 11000
    },
    "throughput": {
      "serialize_mb_per_sec": 120.5,
      "send_mb_per_sec": 300.0
    },
    "cpu_efficiency": {
      "ns_per_byte": 0.27,
      "bytes_per_cpu_ms": 3700000.0
    },
    "totals": {
      "received": 1234,
      "sent_ok": 1230,
      "dropped": 2,
      "errors": 2,
      "bytes": 56789012
    }
  }
]
```

### kafka_cdr_to_json

| Topic | Type | Description |
|-------|------|-------------|
| `<metrics.topic>` | `std_msgs/msg/String` (JSON) | Per-topic metrics published at `metrics.interval_ms` intervals. |

#### kafka_cdr_to_json metrics JSON schema

The payload is a JSON **object** with a top-level `topics` array. Each element corresponds to one
Kafka input topic being consumed and converted.

```json
{
  "timestamp_ns": 1711900000000000000,
  "topics": [
    {
      "input_topic": "ros2.camera.image_raw",
      "output_topic": "ros2.camera.image_raw.json",
      "ros_type": "sensor_msgs/msg/Image",
      "received": 1234,
      "converted": 1230,
      "failed": 4,
      "delta_received": 30,
      "delta_converted": 30,
      "delta_failed": 0,
      "bytes": 56789012,
      "rate_msgs_per_s": 30.0,
      "rate_bytes_per_s": 1382400.0,
      "latency_ns_p50": 2300000,
      "latency_ns_p95": 5100000,
      "latency_ns_p99": 9800000,
      "latency_ns_max": 15000000,
      "json_size_min": 4096,
      "json_size_max": 49152,
      "json_size_avg": 46000.0
    }
  ]
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
