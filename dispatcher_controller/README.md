# dispatcher_controller

Control-plane node that orchestrates ROS 2 to Kafka streaming pipelines by managing the lifecycle of the `kafka_sink` component and coordinating pipeline selections from multiple sources (GUI/file/introspection).

## Build

```bash
colcon build --packages-select dispatcher_controller
source install/setup.bash
```

## Parameters

- `kafka_sink_node_name` (string, default `/kafka_sink`): Target kafka_sink node name.
- `selection_mode` (string, default `gui`): `gui` | `file` | `all`.
- `selection_file_path` (string): Path to YAML selection when in `file` mode.
- `auto_apply_on_mode_change` (bool, default `true`): Apply immediately on mode switch.
- `validate_topics` (bool, default `false`): Validate topics via introspection before applying.
- `introspection_service_name` (string, default `/introspection_manager/get_topics`): Service used for topic validation/discovery.
- `disable_introspection_after_apply` (bool, default `true`): Optionally disable introspection after applying a selection.
- `all_mode_max_topics` (int, default `200`): Safety limit for `all` mode discovery.

## Services

- `apply_selection` (`dispatcher_controller/srv/ApplySelection`): GUI-driven selection; valid only in `gui` mode.
- `reload_selection` (`dispatcher_controller/srv/ReloadSelection`): Reloads current mode source (file/all/gui cache) and optionally applies.
- `stop_streaming` (`dispatcher_controller/srv/StopStreaming`): Deactivates `kafka_sink`; optional cache reset.
- `get_status` (`dispatcher_controller/srv/GetStatus`): Reports mode, kafka_sink lifecycle, active selection, cached counts, and last error.
- `set_selection_mode` (`dispatcher_controller/srv/SetSelectionMode`): Switches between `gui` | `file` | `all`, optionally applying immediately.

## kafka_sink lifecycle interactions

The controller uses lifecycle services:
- `/<kafka_sink_name>/change_state` (`lifecycle_msgs/srv/ChangeState`)
- `/<kafka_sink_name>/get_state` (`lifecycle_msgs/srv/GetState`)
and sets parameter `subscriptions_yaml` via `/<kafka_sink_name>/set_parameters` (`rcl_interfaces/srv/SetParameters`).

## Usage examples

- **Start in file mode**: set parameters `selection_mode:=file selection_file_path:=/path/to/selection.yaml auto_apply_on_mode_change:=true` and launch; the controller loads the file, infers missing types via introspection, applies to `kafka_sink`, and (optionally) disables introspection.
- **Switch to gui mode at runtime**: call `set_selection_mode` with `selection_mode=gui apply_now=true`. If a cached GUI selection exists it is re-applied; otherwise the controller waits for `apply_selection`.
- **Switch to all mode**: call `set_selection_mode` with `selection_mode=all apply_now=true` to discover topics from `introspection_manager` (subject to filters/limits) and apply immediately. Use `reload_selection` later to refresh from discovery.

### File mode selection format

`selection_file_path` should point to a YAML sequence of maps containing `topic_name` and optional `msg_type` keys, e.g.:

```yaml
- topic_name: /camera/image_raw
  msg_type: sensor_msgs/msg/Image
- topic_name: /cmd_vel
  msg_type: geometry_msgs/msg/Twist
```
