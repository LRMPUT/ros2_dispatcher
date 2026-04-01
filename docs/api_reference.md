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

Apply a topic selection provided in the request. Used by GUI clients or external automation.

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

Reload and apply the selection from the current mode (re-reads file in `file` mode, re-discovers in `all` mode).

```
# Request
(empty)

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
(empty)

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
string selection_mode            # gui | file | all
string phase                     # IDLE | BUSY | ERROR
string last_error                # last error message, empty if none
int64  applied_topics_count      # number of topics currently streaming
string lifecycle_state_kafka     # lifecycle state of kafka_sink
string lifecycle_state_mosquitto # lifecycle state of mosquitto_sink
```

#### set_selection_mode

`dispatcher_controller/srv/SetSelectionMode`

Switch selection mode at runtime.

```
# Request
string mode        # gui | file | all
string file_path   # path to selection YAML (only for file mode)
bool   auto_apply  # immediately apply after mode change

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
| `ros_topic` | `/camera/image_raw` |
| `ros_type` | `sensor_msgs/msg/Image` |
| `kafka_topic` | `ros2.camera.image_raw` |
| `msg_type` | `sensor_msgs/msg/Image` |
| `stamp_ms` | `1711900000000` |
| `payload_format` | `cdr` |

---

## Topic naming conventions

### Kafka (prefix_ros_topic mode)

ROS topic `/a/b/c` with `kafka.topic_prefix = ros2` becomes Kafka topic `ros2.a.b.c` (slashes replaced with dots, leading slash stripped).

### MQTT (prefix_ros_topic mode)

ROS topic `/a/b/c` with `mqtt.topic_prefix = ros2` becomes MQTT topic `ros2/a/b/c` (leading slash stripped, slashes preserved as MQTT hierarchy).

### Fixed mode

All messages go to the single configured topic regardless of ROS topic name.
