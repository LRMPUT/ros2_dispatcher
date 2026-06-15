# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

```bash
# From workspace root (parent of this repo, i.e. the colcon workspace src/)
rosdep install --from-paths src --ignore-src -y
colcon build --symlink-install --cmake-args -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=On
source install/setup.bash

# Single package rebuild
colcon build --packages-select <package_name>
source install/setup.bash

# Debug build
colcon build --cmake-args -DCMAKE_BUILD_TYPE=Debug
```

## Tests

```bash
# Run tests for the packages that have them
colcon test --packages-select introspection_manager kafka_sink mosquitto_sink gui_client
colcon test-result --verbose

# Single test binary (after build)
./build/<package>/test_<name>
```

## Linting

No `.clang-format` exists; CI runs clang-format only when that file is present. Python linting:

```bash
ament_flake8 gui_client
ament_pep257 gui_client
ament_cpplint dispatcher_controller
```

## Launch

```bash
# Minimal system (controller + kafka_sink + introspection_manager)
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file selection_file_path:=/path/to/topics.yaml

# Composed system (component container with all nodes)
ros2 launch ros2_kafka_dispatcher_bringup system_composed.launch.py

# Individual nodes
ros2 launch ros2_kafka_dispatcher_bringup kafka_sink.launch.py
ros2 launch ros2_kafka_dispatcher_bringup mosquitto_sink.launch.py
```

Config files live in `ros2_kafka_dispatcher_bringup/config/`. Example topic selection file: `selection_example.yaml`.

### Selection modes and payload formats

- `selection_mode` (dispatcher_controller): `file` (default; reads YAML at `selection_file_path`) | `gui` (waits for an `apply_selection` service call) | `all` (auto-discovers via `introspection_manager`).
- `kafka.payload_format` (kafka_sink): `cdr` (default, binary, smallest) | `json` (introspection-based, larger but consumer-friendly).

## Local brokers

```bash
# Kafka
cd kafka_bridge/kafka_brocker && docker compose up -d

# MQTT
cd mosquitto_bridge/mosquitto_brocker && docker compose up -d
```

## Docker

```bash
docker build -f docker/Dockerfile --build-arg ROS_DISTRO=humble -t ros2-kafka-dispatcher:local .
```

## Architecture

### Control flow

`dispatcher_controller` (plain `rclcpp::Node`) is the control plane. It does not have lifecycle states itself, but it drives the lifecycle of the sink nodes. On startup it:

1. Reads a topic selection (from YAML file, GUI service call, or `introspection_manager` auto-discovery, depending on `selection_mode`).
2. Calls `configure` → `activate` on `kafka_sink` and/or `mosquitto_sink` via `lifecycle_msgs/srv/ChangeState`.
3. Sets `subscriptions_yaml` on each sink via `rcl_interfaces/srv/SetParameters` before activating.
4. Optionally loads `topic_tools` plugins (ThrottleNode, DropNode, DelayNode) as components into a shared container via `composition_interfaces/srv/LoadNode`, wiring them between the source topic and the sink.

The controller exposes five services: `apply_selection`, `reload_selection`, `stop_streaming`, `get_status`, `set_selection_mode`.

### Sink nodes (kafka_sink, mosquitto_sink)

Both are `rclcpp_lifecycle::LifecycleNode`. On `on_configure` they parse `subscriptions_yaml` and create the transport (KafkaProducer / MQTT client). On `on_activate` they create `rclcpp::GenericSubscription` for each topic — no hardcoded message types, all handled via `rosidl` type introspection at runtime. On `on_deactivate` they destroy subscriptions and flush.

`kafka_sink` can serialize in CDR (binary, default) or JSON (via `rosidl_typesupport_introspection_cpp`). It adds Kafka record headers: `ros_topic`, `ros_type`, `kafka_topic`, `stamp_ms`, `payload_format`.

Topic naming for Kafka: `/a/b/c` with prefix `ros2` → `ros2.a.b.c` (leading slash stripped, remaining slashes replaced with dots). MQTT uses `/` as separator instead.

### kafka_client library

Wraps `librdkafka`. Provides async bounded queue with background poll thread and delivery callbacks. Supports `strict` startup mode (fail if broker unreachable) or `tolerant` (warn and continue). Used by both `kafka_sink` and `kafka_cdr_to_json`.

### kafka_source / kafka_cdr_to_json

`kafka_source` consumes Kafka topics matching a regex, deserializes CDR payloads using `rosbag2_cpp`, and republishes as ROS 2 topics (prefixed `/kafka_decoded` by default).

`kafka_cdr_to_json` is a separate lifecycle node that reads CDR records from Kafka and republishes them as JSON to new Kafka topics (default: appends `.json` suffix). It dynamically loads type support libraries at runtime using `rosbag2_cpp::get_typesupport_library`.

### introspection_manager

Plain `rclcpp::Node`. Monitors the ROS 2 graph, filters hidden topics (names with segments starting with `_`), publishes `~/topics_info` as `introspection_manager_msgs/msg/TopicsInfo`, and serves `~/get_topics`.

### Key parameter: subscriptions_yaml

The YAML passed to sinks is a list of `{topic_name, msg_type}` entries with an optional `kafka_name` override and an optional `topic_tools` block:

```yaml
- topic_name: /robot/odom
  msg_type: nav_msgs/msg/Odometry
  kafka_name: robot_odom          # optional Kafka topic name override
  topic_tools:
    plugin: topic_tools::ThrottleNode
    output_topic: /throttled/odom
    parameters:
      period: 0.1
```

### kafka_mock_server

Python-based mock Kafka broker (`kafka_bridge/kafka_mock_server/`) for testing sink nodes without a real Kafka cluster. Launched via its own launch file.

### Known stubs

`plugin_loader`, `plugin_interfaces`, and `processing_plugins/*` are placeholder packages without runnable implementations.

### Benchmark tooling

`tools/benchmark/` runs a CDR vs JSON serialization matrix (message types × formats × rates × repetitions).
- `synthetic_publisher.py` — publishes one of `navsatfix`, `odometry`, `odometry_fullcov`, `pointcloud2` at a fixed rate; `run_benchmark.sh` only wires three of these (no `odometry_fullcov`).
- `metrics_recorder.py` — subscribes to `kafka_sink/metrics` (a JSON-on-`std_msgs/String` topic) and flattens it into a per-second CSV. The same metrics format is published by `mosquitto_sink/metrics`.
- `run_full_matrix.sh` — full matrix; `--reps 1` for a quick pass
- `run_benchmark_docker.sh` — single run in Docker (requires `ros2-kafka-benchmark:run` image)
- `analyze_results.py` — aggregates YAML results into plots/tables

### Latency tooling

`tools/latency/` measures end-to-end ROS→Kafka→ROS latency using `std_msgs/String` messages with a JSON payload that embeds `t0_ns` (publish wall clock). `latency_consumer.py` subtracts `t1_ns - t0_ns` and logs JSONL. Note: tests only the `std_msgs/String` path, not native ROS message types — the scalability work in `docs/superpowers/specs/2026-05-11-scalability-experiment-design.md` extends this to NavSatFix via `header.stamp`.

### Prior reviews (read before large changes)

Four `REVIEW-*.md` files at the repo root (`REVIEW-error-handling.md`, `REVIEW-performance.md`, `REVIEW-security.md`, `REVIEW-test-coverage.md`) capture earlier audits with concrete findings. Consult them before refactoring sink internals, touching the producer queue, or claiming "good test coverage" — they enumerate known gaps.

## Docs

Reference docs live in `docs/` (Markdown). See `README.md` for the docs index; `docs/configuration_reference.md` enumerates every node parameter. Two LaTeX-based PDFs are built from `docs/main.tex` (two-column CEUR-ART format):
- `docs/documentation.pdf` — all docs except node_description
- `docs/node_description.pdf` — node_description only

A validation script `tools/validate_docs.py` checks that every parameter name claimed in the docs exists in a `declare_parameter` call in the corresponding C++ source. Run it after editing docs or parameters.

The repo also has latency measurement tooling in `tools/latency/`.
