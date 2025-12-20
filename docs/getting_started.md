# Getting started (beginner friendly)

This guide walks through a minimal setup to explore `ros2_kafka_dispatcher`, from installing dependencies to streaming a couple of ROS 2 topics into Kafka.

## Prerequisites
- ROS 2 workspace with `colcon` and common build tools.
- Kafka broker reachable on your network (local or remote). For local testing, `docker run -p 9092:9092 ...` with a single-broker image is sufficient.
- Python `pip`/`rosdep` available to install dependencies when needed.

## Build the workspace
1. From the workspace root (where `src/ros2_kafka_dispatcher` is located):
   ```bash
   rosdep install --from-paths src --ignore-src -y
   colcon build --symlink-install --cmake-args -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=On
   source install/setup.bash
   ```
2. (Optional) Rebuild only the dispatcher packages while iterating:
   ```bash
   colcon build --packages-up-to dispatcher_controller kafka_bridge introspection_manager
   source install/setup.bash
   ```

## Launch a minimal pipeline
Use the bringup package to start the dispatcher controller and Kafka sink with a simple file-based selection.

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:=\"$(ros2 pkg prefix ros2_kafka_dispatcher_bringup)/share/ros2_kafka_dispatcher_bringup/config/selection_example.yaml\"
```

What this does:
- Spins up the `dispatcher_controller` and `kafka_sink` nodes.
- Loads a small selection of topics from the provided YAML file.
- Activates the Kafka sink lifecycle so messages start flowing to Kafka.

## Verify the pipeline
- Check controller status:
  ```bash
  ros2 service call /dispatcher_controller/get_status dispatcher_controller/srv/GetStatus "{}"
  ```
- Inspect Kafka output with your preferred consumer (e.g., `kcat -b localhost:9092 -t <topic>`).
- Confirm topic discovery is working:
  ```bash
  ros2 topic echo /introspection_manager/topics_info
  ```

## Common tweaks
- **Switch to GUI mode:** Change `selection_mode:=gui` when launching (or via the `set_selection_mode` service) to drive selections from your UI.
- **Validate topics before streaming:** Set the controller parameter `validate_topics:=true` to ensure topics exist and types match before activating the sink.
- **Disable hidden topics:** In the introspection manager, set `filter_hidden:=true` (default) to ignore topics with name segments starting with `_`.

## Troubleshooting tips
- If `kafka_sink` stays inactive, call `get_status` to inspect lifecycle state and last error messages.
- When types are missing from your YAML selection, ensure the `introspection_manager` node is running so the controller can infer message types.
- Use `selection_mode:=all` cautiously; set `all_mode_max_topics` to protect the sink from excessive subscriptions in dense graphs.

## Where to go next
- Architecture overview: see the top-level [`README.md`](../README.md).
- Package-specific docs:
  - [`dispatcher_controller`](../dispatcher_controller/README.md)
  - [`introspection_manager`](../introspection_manager/README.md)
  - [`introspection_manager_msgs`](../introspection_manager_msgs/README.md)
  - [`ros2_kafka_dispatcher_bringup`](../ros2_kafka_dispatcher_bringup/README.md)
