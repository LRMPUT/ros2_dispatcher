# ros2_kafka_dispatcher_bringup

Launcher and configuration package for bringing up the ros2_kafka_dispatcher system. It provides launch files and default parameter YAMLs for starting the dispatcher controller and kafka sink nodes in either standalone or composed setups.

## Build

```bash
rosdep install --from-paths src --ignore-src -y
colcon build --symlink-install --packages-up-to ros2_kafka_dispatcher_bringup
```

## Launching

Minimal two-node bringup:

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py
```

Start in file-selection mode with an explicit selection file:

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file selection_file_path:=/path/to/selection.yaml
```

Composable container bringup (single process):

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_composed.launch.py
```

GUI mode can be enabled later by switching `selection_mode:=gui` on the dispatcher_controller; the GUI itself is not launched by this package.

## Configuration

Default parameter YAMLs live in `config/`. `config/selection_example.yaml` contains a dispatcher-compatible file-mode selection:

```yaml
- topic_name: /demo/chatter
  msg_type: std_msgs/msg/String
- topic_name: /demo/number
  msg_type: std_msgs/msg/Int32
```

When launching in file mode, point `selection_file_path` at this example (or your own file) to start streaming immediately:

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:=\"$(ros2 pkg prefix ros2_kafka_dispatcher_bringup)/share/ros2_kafka_dispatcher_bringup/config/selection_example.yaml\"
```

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file selection_file_path:=/path/to/selection.yaml
```

Composable container bringup (single process):

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_composed.launch.py
```

GUI mode can be enabled later by switching `selection_mode:=gui` on the dispatcher_controller; the GUI itself is not launched by this package.
