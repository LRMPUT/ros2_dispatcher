# ros2_kafka_dispatcher

## Project Overview
- ROS 2 packages for discovering active topics, selecting subsets, and forwarding serialized messages to Apache Kafka.
- Includes lifecycle-managed Kafka sink, dispatcher/controller logic, introspection tooling, and bringup launch files.

## Architecture / Key Components
- **introspection_manager** (`introspection_manager/`): Monitors the ROS 2 graph and exposes topic/type listings via the `~/topics_info` publisher and `~/get_topics` service. Configurable defaults in `introspection_manager/config/introspection_manager.param.yaml`. Launch file: `introspection_manager/launch/introspection_manager.launch.py`.
- **dispatcher_controller** (`dispatcher_controller/`): Control-plane node that selects topics and manages the Kafka sink lifecycle. Supports selection modes `gui` | `file` | `all`, validates topics (optional), and exposes services `apply_selection`, `reload_selection`, `stop_streaming`, `get_status`, and `set_selection_mode`. Default parameters set in `dispatcher_controller/src/dispatcher_controller_node.cpp` and `dispatcher_controller/launch/dispatcher_controller.launch.py`. Example selection YAML at `dispatcher_controller/config/topics.yaml`.
- **kafka_sink** (`kafka_bridge/kafka_sink/`): Lifecycle node that subscribes to configured topics and forwards payloads to Kafka using `kafka_client`. Parameters (e.g., `subscriptions_yaml`, `kafka.bootstrap_servers`, QoS depth) are defined in `kafka_bridge/kafka_sink/config/kafka_sink.param.yaml`. Launch file: `kafka_bridge/kafka_sink/launch/kafka_sink_container.launch.py`.

## Consuming CDR payloads

When `kafka_sink` runs with `kafka.payload_format:=cdr` (default), Kafka message values are raw ROS 2 CDR bytes.

- Convert to JSON and publish back to Kafka: `ros2_kafka_json_sidecar.py`
- Republish back into ROS 2 (deserialize CDR and publish messages): `ros2_kafka_cdr_to_ros.py`
- **ros2_kafka_dispatcher_bringup** (`ros2_kafka_dispatcher_bringup/`): Launches the dispatcher controller, Kafka sink, and introspection manager together. Minimal setup in `ros2_kafka_dispatcher_bringup/launch/system_minimal.launch.py`; composed setup in `ros2_kafka_dispatcher_bringup/launch/system_composed.launch.py`.
- **processing_plugins/** (`processing_plugins/`): Contains placeholder plugin packages (`gps_velocity_estimator`, `moving_average_filter`) without documented behavior in this repository.
- **plugin_loader/** and **plugin_interfaces/**: Package stubs with manifests; runtime behavior is not defined in this repository.

## Build & Dependencies
- Build with `colcon` in a ROS 2 workspace after resolving dependencies via `rosdep`:
  ```bash
  rosdep install --from-paths src --ignore-src -y
  colcon build --symlink-install --cmake-args -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=On
  ```
- Key package dependencies (from `package.xml` files):
  - `dispatcher_controller`: `rclcpp`, `rclcpp_lifecycle`, `rclcpp_components`, `lifecycle_msgs`, `rcl_interfaces`, `introspection_manager_msgs`, `yaml-cpp`.
  - `kafka_sink`: `rclcpp`, `rclcpp_lifecycle`, `rclcpp_components`, `lifecycle_msgs`, `yaml-cpp`, `launch_ros`, `kafka_client`.
  - `introspection_manager`: `rclcpp`, `rclcpp_components`, `introspection_manager_msgs`, `rcpputils`, `launch_ros`.

## Usage
- Bring up introspection manager, dispatcher controller, and Kafka sink:
  ```bash
  ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
    selection_mode:=file \
    selection_file_path:="" \
    kafka_sink_node_name:=/kafka_sink \
    validate_topics:=false \
    subscriptions_yaml:="" \
    qos_depth:=10 \
    controller_log_level:=debug \
    kafka_sink_log_level:=info \
    introspection_manager_param_file:="" \
    introspection_manager_log_level:=DEBUG
  ```
- Run only the Kafka sink as a component container (uses `kafka_sink.config.kafka_sink.param.yaml` by default):
  ```bash
  ros2 launch kafka_sink kafka_sink_container.launch.py
  ```
- Run only the introspection manager with optional parameter override:
  ```bash
  ros2 launch introspection_manager introspection_manager.launch.py \
    introspection_manager_param_file:=/path/to/introspection_manager.param.yaml
  ```
- Dispatcher controller can be launched standalone:
  ```bash
  ros2 launch dispatcher_controller dispatcher_controller.launch.py
  ```

## Configuration
- **Dispatcher controller parameters** (declare defaults in `dispatcher_controller/src/dispatcher_controller_node.cpp`):
  - `selection_mode` (`gui`|`file`|`all`), `selection_file_path`, `auto_apply_on_mode_change`, `validate_topics`, `kafka_sink_node_name`, `introspection_service_name`, `introspection_node_name`, `disable_introspection_after_apply`, `all_mode_max_topics`, `all_mode_allowlist`, `all_mode_denylist`, `all_mode_hide_rosout`.
  - Services exposed: `apply_selection`, `reload_selection`, `stop_streaming`, `get_status`, `set_selection_mode`.
- **Kafka sink parameters** (`kafka_bridge/kafka_sink/config/kafka_sink.param.yaml`):
  - Subscription/QoS: `qos_depth`, `subscriptions_yaml`.
  - Kafka client: `kafka.bootstrap_servers`, `kafka.client_id`, `kafka.acks`, `kafka.topic_prefix`, `kafka.topic_mapping_mode`, `kafka.fixed_topic`, `kafka.strict_startup`, `kafka.max_queue_messages`, `kafka.drop_when_full`, `kafka.linger_ms`, `kafka.batch_size`.
- **Introspection manager parameters** (`introspection_manager/config/introspection_manager.param.yaml`):
  - `publisher_queue_depth`, `publisher_reliability`, `publisher_durability`, `publish_on_change`, `filter_hidden`, `introspection_enabled`.
- **Bringup launch arguments** (`ros2_kafka_dispatcher_bringup/launch/system_minimal.launch.py`):
  - `selection_mode`, `selection_file_path`, `kafka_sink_node_name`, `validate_topics`, `subscriptions_yaml`, `qos_depth`, `controller_log_level`, `kafka_sink_log_level`, `introspection_manager_param_file`, `introspection_manager_log_level`.

## Limitations / Known Gaps
- No GUI client is included in this repository; selection mode `gui` requires an external client.
- `plugin_loader`, `plugin_interfaces`, and `processing_plugins/*` contain minimal or placeholder content without runnable examples.
- Kafka broker provisioning is not part of this codebase; `kafka.bootstrap_servers` must point to an existing cluster.

## Kafka JSON sidecar
Use `ros2_kafka_json_sidecar.py` to convert ROS 2 CDR payloads coming from the Kafka bridge into JSON:

```bash
python3 ros2_kafka_json_sidecar.py \
  --bootstrap-servers localhost:9092 \
  --topic-pattern '^ros2\\..*' \
  --output-suffix .json \
  --group-id ros2-json-sidecar \
  --log-level INFO
```

Behavior:
- Subscribes to a single topic (`--topic`) or a regex pattern (`--topic-pattern`, default `^ros2\\..*`).
- Reads Kafka headers `ros_topic`, `ros_type`, and `stamp_ms` to determine how to deserialize the CDR payload.
- Dynamically imports ROS 2 message classes using `rosidl_runtime_py.utilities.get_message`.
- Deserializes messages with `rclpy.serialization.deserialize_message` and publishes JSON to a mirrored topic (input topic name + `--output-suffix`, default `.json`).
- Logs and skips messages when the ROS type is unknown or the payload cannot be deserialized.

## Docker & CI/CD
The repository ships with a multi-stage Docker build and a GitHub Actions workflow that builds, smoke-tests, and (on releases) publishes images to GHCR.

### Docker build
```bash
docker build \
  -f docker/Dockerfile \
  --build-arg ROS_DISTRO=humble \
  --build-arg GIT_SHA="$(git rev-parse HEAD)" \
  --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --build-arg IMAGE_SOURCE="https://github.com/<org>/<repo>" \
  -t ros2-kafka-dispatcher:local \
  .
```
The base image is `ros:<distro>-ros-base`, controlled by the `ROS_DISTRO` build arg (default: `humble`).

### Docker run
```bash
docker run --rm -it ros2-kafka-dispatcher:local bash
```

### Health check / smoke test
The container health check validates the ROS 2 environment by listing packages. You can run the same command manually:
```bash
docker run --rm ros2-kafka-dispatcher:local \
  bash -lc "source /opt/ros/humble/setup.bash && source /ws/install/setup.bash && ros2 pkg list >/dev/null"
```

### CI/CD behavior
- Pull requests and pushes build the image and run the smoke test.
- Tags matching `v*.*.*` publish the image to GitHub Container Registry and Docker Hub.
- `latest` is only applied on builds from `main`; release tags publish semantic-version and git-SHA tags.
The smoke test runs inside the builder stage so it exercises the same dependency set used for compilation while keeping the runtime image minimal.

### Publishing to Docker Hub (professional setup)
1. Create a Docker Hub repository (e.g., `your-org/ros2-kafka-dispatcher`).
2. Create a Docker Hub access token (Account Settings → Security) and store it in GitHub secrets:
   - `DOCKERHUB_USERNAME`: your Docker Hub username or org bot account.
   - `DOCKERHUB_TOKEN`: the access token.
   - `DOCKERHUB_REPOSITORY`: the full image name, e.g. `your-org/ros2-kafka-dispatcher`.
3. Push to `main` for `latest` and SHA tags, or create a tag `vX.Y.Z` for release tags.

Example tag publish:
```bash
git tag v1.2.3
git push origin v1.2.3
```
The workflow will publish:
- `your-org/ros2-kafka-dispatcher:1.2.3`
- `your-org/ros2-kafka-dispatcher:sha-<shortsha>`
