# Developer Guide

How to build, test, extend, and contribute to `ros2_kafka_dispatcher`.

---

## Repository layout

```
ros2_kafka_dispatcher/
├── dispatcher_controller/          # Control-plane orchestration node
├── introspection_manager/          # Graph monitoring node
├── introspection_manager_msgs/     # Custom message and service definitions
├── kafka_bridge/
│   ├── kafka_client/               # C++ librdkafka wrapper (library)
│   ├── kafka_sink/                 # ROS 2 → Kafka lifecycle node
│   ├── kafka_source/               # Kafka → ROS 2 lifecycle node
│   ├── kafka_cdr_to_json/          # CDR → JSON converter node
│   ├── kafka_brocker/              # Docker Compose for local broker
│   └── kafka_mock_server/          # Python mock broker for testing
├── mosquitto_bridge/
│   ├── mosquitto_sink/             # ROS 2 → MQTT lifecycle node
│   └── mosquitto_brocker/          # Docker Compose for local broker
├── plugin_interfaces/              # Plugin interface definitions (stub)
├── plugin_loader/                  # Plugin loader (stub)
├── processing_plugins/             # Example plugin packages (placeholders)
├── ros2_kafka_dispatcher_bringup/  # System-level launch files and configs
├── tools/
│   └── latency/                    # Latency measurement scripts
├── docker/                         # Multi-stage Dockerfile
└── docs/                           # Documentation (this directory)
```

---

## Build

```bash
# From your ROS 2 workspace root
rosdep install --from-paths src --ignore-src -y
colcon build --symlink-install \
             --cmake-args -DCMAKE_BUILD_TYPE=Release \
                          -DCMAKE_EXPORT_COMPILE_COMMANDS=On
source install/setup.bash
```

### Incremental rebuild (faster iteration)

```bash
colcon build --packages-select dispatcher_controller
source install/setup.bash
```

### Debug build

```bash
colcon build --cmake-args -DCMAKE_BUILD_TYPE=Debug
```

---

## Run tests

```bash
colcon test --packages-select introspection_manager kafka_sink mosquitto_sink
colcon test-result --verbose
```

Key test files:

| Package | Test file |
|---------|-----------|
| `introspection_manager` | `test/test_introspection_manager.cpp` |
| `kafka_sink` | `test/test_kafka_sink.cpp` |
| `mosquitto_sink` | `test/test_mosquitto_sink.cpp` |
| `gui_client` | `test/` (flake8 / pep257) |

---

## Package dependencies overview

| Package | Key dependencies |
|---------|-----------------|
| `dispatcher_controller` | rclcpp, rclcpp_lifecycle, rclcpp_components, lifecycle_msgs, introspection_manager_msgs, yaml-cpp |
| `introspection_manager` | rclcpp, rclcpp_components, introspection_manager_msgs, rcpputils |
| `kafka_sink` | rclcpp_lifecycle, rclcpp_components, kafka_client, yaml-cpp, rosbag2_cpp |
| `kafka_client` | librdkafka |
| `kafka_source` | rclcpp_lifecycle, kafka_client, rosbag2_cpp |
| `mosquitto_sink` | rclcpp_lifecycle, rclcpp_components, libpaho-mqttpp3, yaml-cpp |

---

## Adding a new selection mode

1. Add the mode string to the validation in `dispatcher_controller/src/dispatcher_controller_node.cpp` where `selection_mode` is declared.
2. Add a branch in the mode-switching logic (see `set_selection_mode` service handler).
3. Implement a method that produces a `std::vector<TopicInfo>` and call the existing `applySelection(topics)` helper.
4. Add a launch argument in `ros2_kafka_dispatcher_bringup/launch/system_minimal.launch.py` if the mode needs its own parameters.

---

## Adding a new sink

A sink must be a `rclcpp_lifecycle::LifecycleNode` that:

1. Accepts a `subscriptions_yaml` string parameter listing `{topic_name, msg_type}` pairs.
2. On `on_configure`: parses the YAML and sets up the transport layer.
3. On `on_activate`: creates `rclcpp::GenericSubscription` for each topic and starts sending.
4. On `on_deactivate`: destroys subscriptions and flushes/closes the transport.

Register the new node's fully qualified name as a controller parameter (similar to `kafka_sink_node_name`) and add it to the lifecycle orchestration in `dispatcher_controller_node.cpp`.

---

## Processing plugins (topic_tools)

The controller can inject `topic_tools` plugins between a ROS 2 topic and a sink. Each plugin is loaded as a component into the shared container (`component_container_name`).

### Supported topic_tools plugins (ROS 2 Humble+)

| Plugin | Purpose |
|--------|---------|
| `topic_tools::ThrottleNode` | Rate-limit messages. |
| `topic_tools::DropNode` | Drop every Nth message. |
| `topic_tools::DelayNode` | Introduce a fixed delay. |

### Selection YAML example

```yaml
- topic_name: /camera/image_raw
  msg_type: sensor_msgs/msg/Image
  topic_tools:
    plugin: topic_tools::ThrottleNode
    output_topic: /throttled/image_raw
    parameters:
      period: 0.1    # forward at most 10 Hz
```

The controller wires `/camera/image_raw` → `ThrottleNode` → `/throttled/image_raw`, then subscribes the sink to `/throttled/image_raw`.

---

## CDR ↔ JSON conversion

### kafka_cdr_to_json node

Reads CDR-encoded records from Kafka and republishes as JSON to a new topic (default: appends `.json`).

### ros2_kafka_json_sidecar.py

A Python script that does the same conversion as an external consumer:

```bash
python3 ros2_kafka_json_sidecar.py \
  --bootstrap-servers localhost:9092 \
  --topic-pattern '^ros2\..*' \
  --output-suffix .json \
  --group-id ros2-json-sidecar \
  --log-level INFO
```

The script reads the `ros_type` Kafka record header to determine the ROS 2 type, deserializes with `rclpy.serialization.deserialize_message`, and publishes JSON.

---

## Latency measurement workflow

See [`latency_measurement.md`](latency_measurement.md) for a full walkthrough. The quick version:

```bash
# Start the full pipeline
./tools/latency/run_latency_capture.sh \
  --count 1000 \
  --rate 50 \
  --payload-bytes 256 \
  --output-dir ./latency_run_1

# Artifacts
# latency_run_1/publisher.jsonl   - per-message t0 timestamps
# latency_run_1/consumer.jsonl    - per-message t0, t1, latency_ns
# latency_run_1/kafka_sink.log    - per-message telemetry from the sink
```

---

## Code style

- C++: follow the ROS 2 coding style (clang-format, ament_lint).
- Python: PEP 8 (enforced via flake8 and pep257 tests in `gui_client`).
- Run linters before opening a PR:

```bash
ament_flake8 gui_client
ament_pep257 gui_client
ament_cpplint dispatcher_controller
```

---

## Common pitfalls

| Symptom | Likely cause |
|---------|-------------|
| Sink stays `INACTIVE` after apply | Broker unreachable; check `kafka.bootstrap_servers` / `mqtt.broker_host`. |
| `get_status` shows `ERROR` phase | Call `get_status` and inspect `last_error`. |
| Topics missing from introspection | `introspection_manager` not running, or `filter_hidden:=true` hiding the topic. |
| Messages dropping at high rate | `kafka.max_queue_messages` too low or broker too slow; increase queue or enable batching. |
| Unknown type during serialization | Ensure the message package is on `AMENT_PREFIX_PATH` and the node can load the type library. |
