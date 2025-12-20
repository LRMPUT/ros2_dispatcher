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

## How the controller manages kafka_sink lifecycle

When you apply a selection (from any source), the dispatcher_controller orchestrates kafka_sink through these transitions:

1. **Check current state** → Query `/kafka_sink/get_state`
2. **Deactivate if active** → Send `TRANSITION_DEACTIVATE` if currently in `ACTIVE` state
3. **Configure if unconfigured** → Send `TRANSITION_CONFIGURE` if in `UNCONFIGURED` state
4. **Set subscriptions** → Update kafka_sink parameter `subscriptions_yaml` with the topic list (name + type)
5. **Activate** → Send `TRANSITION_ACTIVATE` to start streaming
6. **Optionally disable introspection** → If `disable_introspection_after_apply=true`, disable the introspection_manager

### Type inference and introspection

The controller infers missing topic message types in two scenarios:

- **File mode without `msg_type`**: If your YAML only has `topic_name` keys, the controller calls `infer_missing_types()`, which queries the `introspection_manager/get_topics` service to look up types.
- **GUI mode with missing types**: When you call `apply_selection` without providing types, introspection fills them in.

**With full `msg_type` in YAML**: No introspection dependency; types are read directly from the file.

**Without `msg_type` in YAML**: Requires `introspection_manager` running and reachable at the configured `introspection_service_name`.

## Usage examples

### File mode (recommended for deterministic setups)

1. **Create a topics.yaml file** with topic names and types:
   ```yaml
   - topic_name: /demo/chatter
     msg_type: std_msgs/msg/String
   - topic_name: /demo/number
     msg_type: std_msgs/msg/Int32
   ```

2. **Set parameters** to use file mode:
   ```bash
   ros2 param set /dispatcher_controller selection_mode file
   ros2 param set /dispatcher_controller selection_file_path /absolute/path/to/topics.yaml
   ros2 param set /dispatcher_controller auto_apply_on_mode_change true
   ```

3. **Apply via service** (if not auto-applied):
   ```bash
   ros2 service call /set_selection_mode dispatcher_controller/srv/SetSelectionMode \
     "{selection_mode: 'file', selection_file_path: '/absolute/path/to/topics.yaml', apply_now: true}"
   ```

4. **Verify** the streaming is active:
   ```bash
   ros2 service call /get_status dispatcher_controller/srv/GetStatus "{}"
   ```
   Expect `kafka_sink_state: active` and `applied_topics` matching your YAML.

### File mode without introspection dependency

Use explicit `msg_type` keys to avoid needing introspection_manager:

```yaml
- topic_name: /camera/image_raw
  msg_type: sensor_msgs/msg/Image
- topic_name: /cmd_vel
  msg_type: geometry_msgs/msg/Twist
```

### File mode with type inference

If you only specify topic names and introspection is available:

```yaml
- topic_name: /camera/image_raw
- topic_name: /cmd_vel
```

The controller will query introspection_manager to discover types. Ensure `/introspection_manager/get_topics` service is available before applying.

### GUI mode workflow

1. Start in GUI mode (default):
   ```bash
   ros2 param set /dispatcher_controller selection_mode gui
   ```

2. Apply a selection via service:
   ```bash
   ros2 service call /apply_selection dispatcher_controller/srv/ApplySelection \
     "{topics: [
       {name: '/demo/chatter', type: 'std_msgs/msg/String'},
       {name: '/demo/number', type: 'std_msgs/msg/Int32'}
     ]}"
   ```

3. Check status:
   ```bash
   ros2 service call /get_status dispatcher_controller/srv/GetStatus "{}"
   ```

### Switching modes at runtime

Switch from GUI to file mode and apply immediately:

```bash
ros2 service call /set_selection_mode dispatcher_controller/srv/SetSelectionMode \
  "{selection_mode: 'file', selection_file_path: '/path/to/topics.yaml', apply_now: true}"
```

**Note**: `apply_now: true` triggers the full pipeline (load → infer types → configure kafka_sink → activate).

Switch to all mode to discover all topics:

```bash
ros2 service call /set_selection_mode dispatcher_controller/srv/SetSelectionMode \
  "{selection_mode: 'all', apply_now: true}"
```

### Reload selection without switching modes

If you update your topics.yaml, reload and re-apply:

```bash
ros2 service call /reload_selection dispatcher_controller/srv/ReloadSelection \
  "{selection_file_path: '/path/to/topics.yaml', apply_now: true}"
```

### Stop streaming and reset state

Deactivate kafka_sink and clear cached selections:

```bash
ros2 service call /stop_streaming dispatcher_controller/srv/StopStreaming \
  "{reset_cached: true}"
```

### File mode selection format

`selection_file_path` should point to a YAML sequence of maps. Each entry must have `topic_name` (or `name`) and optionally `msg_type` (or `type`):

```yaml
# Accepted keys: topic_name or name
# Accepted keys: msg_type or type

- topic_name: /camera/image_raw
  msg_type: sensor_msgs/msg/Image
  
- name: /cmd_vel
  type: geometry_msgs/msg/Twist
```

**Key mapping**:
- Topic field: accepts `topic_name` or `name`
- Type field: accepts `msg_type` or `type`
- Missing type fields trigger introspection fallback (if available)
