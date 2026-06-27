# zenoh_sink

`zenoh_sink` is a lifecycle-based ROS 2 node that streams configured ROS 2 topics to a
[Zenoh](https://zenoh.io/) network. It mirrors `kafka_sink` / `mosquitto_sink`: it creates a
`rclcpp::GenericSubscription` per configured topic (no hardcoded message types — everything is
handled via `rosidl` type introspection at runtime) and forwards each message to a Zenoh key
expression.

It is a `rclcpp_lifecycle::LifecycleNode`:

- `on_configure` parses `subscriptions_yaml` and validates the `zenoh.*` parameters.
- `on_activate` opens the Zenoh session, declares one publisher per key expression, and creates the
  subscriptions.
- `on_deactivate` destroys the subscriptions and closes the session.

## Payload formats

`zenoh.payload_format`:

- `cdr` (default) — the raw serialized ROS message (CDR), smallest payload.
- `json` — introspection-based JSON, consumer-friendly but larger.

## Key expression mapping

`zenoh.topic_mapping_mode`:

- `prefix_ros_topic` (default) — `/a/b/c` with prefix `ros2` → `ros2/a/b/c` (leading slash stripped,
  `key_prefix` prepended). Per-subscription `zenoh_name` overrides the ROS topic name before
  prefixing.
- `fixed` — every message is published to `zenoh.fixed_keyexpr`.

## Session / transport options

| Parameter | Default | Meaning |
| --- | --- | --- |
| `zenoh.mode` | `peer` | `peer` (gossip discovery), `client` (connect to a router), or `router`. |
| `zenoh.connect` | `[]` | Endpoints to connect to, e.g. `["tcp/localhost:7447"]`. Required in `client` mode. |
| `zenoh.listen` | `[]` | Endpoints to listen on, e.g. `["tcp/0.0.0.0:7447"]`. |
| `zenoh.config_path` | `""` | Optional full zenoh JSON5 config file; `mode`/`connect`/`listen` are applied on top of it. |
| `zenoh.key_prefix` | `ros2` | Prefix prepended in `prefix_ros_topic` mode. |
| `zenoh.fixed_keyexpr` | `ros2/raw` | Key expression used in `fixed` mode. |
| `zenoh.congestion_control` | `drop` | `drop` (never block the ROS callback) or `block`. |
| `zenoh.priority` | `5` | Zenoh priority 1 (real-time) .. 7 (background); 5 == DATA. |
| `zenoh.express` | `false` | Bypass batching for lower latency at the cost of bandwidth. |
| `zenoh.message_key` | `""` | When set, overrides `header.frame_id` in published messages (nebula parsing). |

## subscriptions_yaml

A list of `{topic_name, msg_type}` entries with an optional `zenoh_name` key-expression override:

```yaml
- topic_name: /robot/odom
  msg_type: nav_msgs/msg/Odometry
  zenoh_name: robot/odom        # optional key-expression override (before prefixing)
- topic_name: /demo/chatter
  msg_type: std_msgs/msg/String
```

## Metrics

When `metrics.enabled` is true the node publishes per-subscription throughput/latency stats as JSON
on `std_msgs/String` at `metrics.topic` (default `zenoh_sink/metrics`), matching the format used by
`kafka_sink` / `mosquitto_sink` (the `zenoh_keyexpr` and `zenoh_session` fields replace the
MQTT-specific ones).

## Build dependency

This package depends on `zenoh_cpp_vendor` (the ROS vendor package that provides the header-only
zenoh-cpp wrapper over the zenoh-c backend). CMake links the imported target `zenohcxx::zenohc`,
which also defines `ZENOHCXX_ZENOHC` so `<zenoh.hxx>` selects the zenoh-c backend.

## Run

```bash
ros2 launch zenoh_sink zenoh_sink_container.launch.py
# or via the bringup package, which also wires it into dispatcher_controller:
ros2 launch ros2_kafka_dispatcher_bringup zenoh_sink.launch.py
```
