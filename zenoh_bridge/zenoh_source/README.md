# zenoh_source

`zenoh_source` is the consumer counterpart of `zenoh_sink`. It is a
`rclcpp_lifecycle::LifecycleNode` that subscribes to a Zenoh key expression,
deserializes the CDR payloads, and republishes them as ROS 2 topics — the inverse
of `zenoh_sink`, mirroring how `kafka_source` consumes what `kafka_sink` produces.

## How it works

- On `on_activate` it opens a Zenoh session and declares a subscriber on
  `zenoh.key_expr` (default `ros2/**`). Zenoh delivers samples on its own threads.
- Each sample carries the ROS message type as a Zenoh **attachment** (set by
  `zenoh_sink`). The node reads it, validates it, maps the key expression back to a
  ROS topic, and republishes the CDR payload through a `rclcpp::GenericPublisher`
  — no message types are hardcoded.
- A sample with no attachment, or a type not permitted by `zenoh.allowed_types`, is
  skipped (with a throttled warning).

It expects **CDR** payloads (the `zenoh_sink` default). JSON payloads are not
republishable as native ROS messages.

## Key expression → ROS topic mapping

`zenoh.key_prefix` is stripped from the received key expression and
`ros_topic_prefix` is prepended:

```
key "ros2/robot/odom"  (key_prefix "ros2", ros_topic_prefix "/zenoh_decoded")
  -> ROS topic "/zenoh_decoded/robot/odom"
```

## Security

The ROS type name arrives from the (untrusted) Zenoh network and is used to load
type-support libraries. `zenoh.allowed_types` is an allowlist gate applied
**before** any library is loaded: an empty list allows any *syntactically valid*
type name (and logs a warning); a non-empty list restricts to exactly those types.
This mirrors `kafka_source`'s `kafka.allowed_types`.

## Parameters

| Parameter | Default | Meaning |
| --- | --- | --- |
| `qos_depth` | `10` | KeepLast depth of the republished topics. |
| `ros_topic_prefix` | `/zenoh_decoded` | Prefix for republished ROS topics. |
| `zenoh.mode` | `peer` | `peer`, `client` (connect to a router), or `router`. |
| `zenoh.connect` | _(unset)_ | Endpoints to connect to, e.g. `["tcp/localhost:7447"]`. |
| `zenoh.listen` | _(unset)_ | Endpoints to listen on. |
| `zenoh.config_path` | `""` | Optional full zenoh JSON5 config file. |
| `zenoh.key_expr` | `ros2/**` | Key expression to subscribe to. |
| `zenoh.key_prefix` | `ros2` | Prefix stripped when deriving the ROS topic. |
| `zenoh.allowed_types` | _(unset)_ | Allowlist of ROS types (empty = any valid). |
| `metrics.enabled` | `true` | Publish per-key decode metrics. |
| `metrics.interval_ms` | `1000` | Metrics interval. |
| `metrics.topic` | `zenoh_source/metrics` | Metrics topic (`std_msgs/String` JSON). |

## Run

```bash
ros2 launch zenoh_source zenoh_source_container.launch.py
# then drive its lifecycle (or use a lifecycle manager):
ros2 lifecycle set /zenoh_source configure
ros2 lifecycle set /zenoh_source activate
```
