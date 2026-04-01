# Getting Started

This guide walks through building the workspace, starting local brokers, launching a minimal pipeline, and verifying that messages are flowing.

---

## Prerequisites

- ROS 2 (Humble or later) installed and sourced.
- `colcon`, `rosdep`, and common ROS 2 build tools available.
- Docker + Docker Compose for running local Kafka / MQTT brokers.
- `kcat` (formerly `kafkacat`) for quick Kafka verification — install with `sudo apt install kafkacat` or `brew install kcat`.

---

## 1. Build the workspace

Clone the repository into your ROS 2 workspace `src/` directory, then:

```bash
# From the workspace root (parent of src/)
rosdep install --from-paths src --ignore-src -y
colcon build --symlink-install \
             --cmake-args -DCMAKE_BUILD_TYPE=Release \
                          -DCMAKE_EXPORT_COMPILE_COMMANDS=On
source install/setup.bash
```

Rebuilding only the dispatcher packages during iteration:

```bash
colcon build --packages-up-to dispatcher_controller kafka_sink introspection_manager
source install/setup.bash
```

---

## 2. Start local brokers

### Kafka

```bash
cd kafka_bridge/kafka_brocker
docker compose up -d
```

Verify the broker is up:

```bash
kcat -b localhost:9092 -L
```

### MQTT (optional)

```bash
cd mosquitto_bridge/mosquitto_brocker
docker compose up -d
```

---

## 3. Create a topic selection file

Create a YAML file listing the ROS 2 topics you want to forward. Example:

```yaml
# ~/topics.yaml
- topic_name: /chatter
  msg_type: std_msgs/msg/String

- topic_name: /odom
  msg_type: nav_msgs/msg/Odometry
```

You can omit `msg_type` when `validate_topics:=false` (the default) and the topics are already publishing — the introspection manager will infer the type.

An example file is provided at:

```
ros2_kafka_dispatcher_bringup/config/selection_example.yaml
```

---

## 4. Launch the minimal pipeline

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:=/absolute/path/to/topics.yaml
```

This starts four nodes:

| Node | Role |
|------|------|
| `introspection_manager` | Monitors the ROS 2 graph, infers message types |
| `dispatcher_controller` | Loads the selection file, drives sink lifecycles |
| `kafka_sink` | Forwards selected topics to Kafka |
| `mosquitto_sink` | Forwards selected topics to MQTT |

To disable the MQTT sink if you only need Kafka:

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:=/absolute/path/to/topics.yaml \
  enable_mosquitto_sink:=false
```

---

## 5. Publish a test message

In a separate terminal (source your workspace first):

```bash
ros2 topic pub /chatter std_msgs/msg/String "data: 'hello kafka'" --once
```

---

## 6. Verify the pipeline

### Check controller status

```bash
ros2 service call /dispatcher_controller/get_status \
  dispatcher_controller/srv/GetStatus "{}"
```

Expected response when streaming is active:

```
phase: IDLE
selection_mode: file
lifecycle_state_kafka: active
applied_topics_count: 2
last_error: ''
```

### Check topic discovery

```bash
ros2 topic echo /introspection_manager/topics_info
```

### Confirm messages arrived in Kafka

```bash
kcat -b localhost:9092 -t ros2.chatter -C -e
```

The Kafka topic name follows the pattern `<topic_prefix>.<ros_topic_without_leading_slash_dots>`. With the default prefix `ros2`, `/chatter` becomes `ros2.chatter`.

---

## 7. Common adjustments

### Switch to all-topics mode

Automatically forward every topic in the graph:

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=all \
  enable_mosquitto_sink:=false
```

Set `all_mode_max_topics` to a safe limit in your parameter file when using this mode in a dense graph.

### Enable topic validation

Verify that each topic in the selection file is actively publishing before activating sinks:

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:=/path/to/topics.yaml \
  validate_topics:=true
```

### Reload the selection at runtime

After editing `topics.yaml`, apply the changes without restarting:

```bash
ros2 service call /dispatcher_controller/reload_selection \
  dispatcher_controller/srv/ReloadSelection "{}"
```

### Stop streaming

```bash
ros2 service call /dispatcher_controller/stop_streaming \
  dispatcher_controller/srv/StopStreaming "{}"
```

---

## 8. Troubleshooting

| Symptom | What to check |
|---------|--------------|
| `kafka_sink` stays `inactive` | Broker unreachable — run `kcat -b localhost:9092 -L` and check `kafka.bootstrap_servers`. |
| `get_status` shows `ERROR` | Inspect `last_error` in the response. |
| Topics missing from `topics_info` | Ensure the publisher is running and `filter_hidden:=true` isn't hiding it (topics with `_`-prefixed segments are hidden by default). |
| Kafka topic not created | The topic is created on first produce — check that a message was actually published on the ROS 2 side. |
| `msg_type` required error | Set `validate_topics:=true` only when the introspection manager can see the topic, or provide `msg_type` explicitly in the selection file. |

---

## 9. Where to go next

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | How the components interact and where data flows. |
| [Configuration Reference](configuration_reference.md) | Every parameter for every node. |
| [API Reference](api_reference.md) | Services, published topics, Kafka record format. |
| [Deployment](deployment.md) | Docker build, production parameter files, CI/CD. |
| [Developer Guide](developer_guide.md) | Adding sinks, plugins, contributing. |
| [Latency Measurement](latency_measurement.md) | Measuring end-to-end latency with the built-in tools. |
| [Benchmark Plan](benchmark_plan.md) | Structured test matrix and procedure. |
