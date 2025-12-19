# ros2_kafka_dispatcher_bringup Tutorial

This tutorial walks through running the minimal dispatcher + Kafka sink system, explains key parameters, and includes troubleshooting tips.

## Prerequisites
- Build the workspace (including dependencies) and source the overlay.
- Ensure Kafka sink dependencies are satisfied (Kafka brokers reachable, etc.).
- If you plan to enable topic validation or `all` mode discovery, have `introspection_manager` available.

## 1. Launch the minimal system (file mode)
Use the provided selection example:
```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:="$(ros2 pkg prefix ros2_kafka_dispatcher_bringup)/share/ros2_kafka_dispatcher_bringup/config/selection_example.yaml"
```

What starts:
- `dispatcher_controller` (manages selections and the Kafka sink lifecycle)
- `kafka_sink` (lifecycle node that streams topics to Kafka)

## 2. Common overrides
- Change selection file:
  ```bash
  selection_file_path:=/path/to/your_selection.yaml
  ```
- Enable topic validation/inference (requires introspection):
  ```bash
  validate_topics:=true
  ```
- Adjust QoS depth for sink subscriptions:
  ```bash
  qos_depth:=50
  ```
- Increase logging verbosity:
  ```bash
  controller_log_level:=debug kafka_sink_log_level:=debug
  ```

## 3. Selection file format
`selection_mode:=file` expects a YAML list of maps:
```yaml
- topic_name: /demo/chatter
  msg_type: std_msgs/msg/String
- topic_name: /demo/number
  msg_type: std_msgs/msg/Int32
```
If `validate_topics` is `true`, missing `msg_type` entries are inferred via introspection.

## 4. Using GUI mode
Launch with:
```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py selection_mode:=gui
```
The controller waits for GUI-driven selections (`apply_selection` service).

## 5. Using discovery (“all”) mode
```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py selection_mode:=all validate_topics:=true
```
Requires `introspection_manager` to discover topics. Use `reload_selection` to refresh.

## 6. Composed (single-process) bringup
Run everything in one component container:
```bash
ros2 launch ros2_kafka_dispatcher_bringup system_composed.launch.py \
  selection_mode:=file \
  selection_file_path:=/path/to/selection.yaml
```
Override log levels as needed:
```bash
controller_log_level:=debug kafka_sink_log_level:=debug
```

## 7. Troubleshooting checklist
- **No Kafka messages**: Verify `kafka_sink` lifecycle state is `active` (`ros2 lifecycle get /kafka_sink`), ensure brokers/topics reachable, and check `subscriptions_yaml` is populated (via `ros2 param get /kafka_sink subscriptions_yaml`).
- **Selection not applied**: Confirm `dispatcher_controller` logs (set `controller_log_level:=debug`), ensure `selection_file_path` exists and is readable, and that `selection_mode` matches your intent (`file|gui|all`).
- **Validation failures**: Make sure `introspection_manager` is running when `validate_topics:=true` or when using `all` mode. Check `introspection_service_name` parameter if using a non-default service name.
- **Composition issues**: If using `system_composed`, ensure the container process is alive and that `rclcpp_components` is in the environment. Try standalone mode (`system_minimal`) to compare behavior.

## 8. Useful commands
- Check node states: `ros2 node list`, `ros2 lifecycle get /kafka_sink`
- Inspect parameters: `ros2 param list /dispatcher_controller`, `ros2 param get /dispatcher_controller selection_mode`
- Watch logs with higher verbosity: set `controller_log_level:=debug kafka_sink_log_level:=debug`
