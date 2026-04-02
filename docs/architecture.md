# Architecture

This document describes the system design, component interactions, and data flow of `ros2_kafka_dispatcher`.

## Overview

`ros2_kafka_dispatcher` bridges ROS 2 topics to external message brokers (Apache Kafka and MQTT). It provides dynamic topic discovery, flexible selection modes, and lifecycle-managed sinks.

```
                         ┌─────────────────────────────────────────────────────┐
                         │                ROS 2 Graph                          │
                         │  /camera/image   /odom   /scan   /cmd_vel  ...      │
                         └────────────────────────┬────────────────────────────┘
                                                  │ topic discovery
                                                  ▼
                         ┌────────────────────────────────────────┐
                         │         introspection_manager          │
                         │  • monitors graph for topic changes    │
                         │  • publishes ~/topics_info             │
                         │  • serves ~/get_topics                 │
                         └───────────────┬────────────────────────┘
                                         │ TopicsInfo / GetTopics
                                         ▼
                         ┌────────────────────────────────────────┐
┌─────────┐  services    │       dispatcher_controller            │
│   GUI   │◄────────────►│  • selection modes: gui|file|all       │
│  / CLI  │              │  • validates topics                    │
└─────────┘              │  • manages topic_tools plugins         │
                         │  • controls sink lifecycles            │
                         └──────────┬──────────────┬─────────────┘
                                    │ configure     │ configure
                              subscriptions_yaml    subscriptions_yaml
                                    │               │
                         ┌──────────▼──┐    ┌───────▼────────────┐
                         │  kafka_sink │    │  mosquitto_sink     │
                         │  (Lifecycle)│    │  (Lifecycle)        │
                         │  CDR / JSON │    │  CDR / JSON         │
                         └──────────┬──┘    └───────┬────────────┘
                                    │               │
                         ┌──────────▼──┐    ┌───────▼────────────┐
                         │Apache Kafka │    │  MQTT Broker        │
                         │  cluster    │    │  (Mosquitto/other)  │
                         └─────────────┘    └────────────────────┘
```

## Components

### introspection_manager

A standard `rclcpp::Node` that continuously monitors the ROS 2 graph.

- Runs a background thread that polls for topic/type changes.
- Publishes discovered topics on `~/topics_info` (latched, transient-local by default).
- Serves `~/get_topics` for on-demand queries.
- Filters hidden topics (name segments starting with `_`) by default.

**When to use it:** It is required whenever `dispatcher_controller` needs to infer message types (e.g. `validate_topics:=true` or `selection_mode:=all`).

### dispatcher_controller

The control-plane node. It is a standard `rclcpp::Node` that orchestrates everything else.

**Selection modes:**

| Mode   | Description |
|--------|-------------|
| `file` | Reads topic list from a YAML file on disk (default). |
| `gui`  | Waits for an `apply_selection` service call from an external client (e.g. rqt GUI). |
| `all`  | Auto-discovers all topics via `introspection_manager` (subject to allowlist/denylist/max). |

**Lifecycle orchestration:**

1. Receives a topic selection (from file, GUI call, or auto-discovery).
2. Optionally validates each topic against the live graph via `introspection_manager`.
3. Serialises the selection to `subscriptions_yaml` and calls the lifecycle transitions on the Kafka and Mosquitto sinks: `configure` → `activate`.
4. On `stop_streaming`, calls `deactivate` on all sinks.

**topic_tools integration:**

For each topic in the selection, an optional `topic_tools` block can specify a plugin. The controller loads the plugin as a component into the shared container (`component_container_name`) via `composition_interfaces/srv/LoadNode`, wires the ROS topic through it (`input_topic` → plugin → `output_topic`), and registers the transformed output topic with the sink instead of the original.

The integration model is strictly **one input topic → one output topic**. Plugin names follow the `package::ClassName` convention and are passed directly to the component loader without validation, so any composable node that conforms to the single-input/single-output contract can be used.

**Available topic_tools nodes:**

| Node | Plugin name | Key parameters | Support status |
|------|-------------|----------------|----------------|
| Throttle | `topic_tools::ThrottleNode` | `period` (s between messages) | Supported — tested, config examples available |
| Drop | `topic_tools::DropNode` | `x` (drop 1-in-x messages) | Supported — listed in developer guide, no config example yet |
| Delay | `topic_tools::DelayNode` | `delay` (seconds) | Supported — listed in developer guide, no config example yet |
| Relay | `topic_tools::RelayNode` | — | Architecturally compatible (single in → single out); not tested or documented |
| RelayField | `topic_tools::RelayFieldNode` | `expression` (field path), `output_type` | Architecturally compatible; requires `output_type` in config; not tested or documented |
| Transform | `topic_tools::TransformNode` | `expression`, `output_type` | **Not supported** — Python-only node in ROS 2 Humble+; cannot be loaded as a C++ component via `LoadNode` |
| Mux | `topic_tools::MuxNode` | `mux_topics` (list) | **Not supported** — requires multiple input topics; current `TopicToolsConfig` maps exactly one `input_topic` |
| Demux | `topic_tools::DemuxNode` | `demux_topics` (list) | **Not supported** — requires multiple output topics; current `TopicToolsConfig` maps exactly one `output_topic` |

**Node descriptions:**

- **Throttle** — Forwards messages from `input_topic` to `output_topic` at most once every `period` seconds. Excess messages are silently dropped. Useful for reducing Kafka ingestion rate for high-frequency sensors (e.g. lidar at 20 Hz → 5 Hz).

- **Drop** — Forwards every x-th message and discards the rest. Unlike Throttle, Drop is count-based rather than time-based, so the output rate tracks the input rate proportionally. Useful when a fixed decimation ratio is needed regardless of timing jitter.

- **Delay** — Buffers incoming messages and republishes them after a fixed `delay` in seconds. Does not change message rate or content. Useful for time-aligning topics from sensors with different hardware latencies before sending to Kafka.

- **Relay** — Re-publishes messages from `input_topic` to `output_topic` without any modification. Primarily useful for renaming a topic or bridging across namespace boundaries before forwarding to a sink.

- **RelayField** — Extracts a single field from the incoming message and publishes it as a new message on `output_topic`. The field is selected via a Python-style `expression` (e.g. `m.linear.x`). The `output_type` must be set to the resulting message type. Useful for stripping large messages down to a single scalar or sub-message before sending to Kafka.

- **Transform** *(not supported)* — Applies an arbitrary Python expression to the incoming message and publishes the result. Implemented as a pure Python node in ROS 2 Humble+; it is not registered as a composable C++ component and therefore cannot be loaded via `composition_interfaces/srv/LoadNode`. Cannot be used with `dispatcher_controller`.

- **Mux** — Subscribes to a list of input topics and republishes the selected one to a single output topic. The active input can be switched at runtime via a service call. Not supported because the current `TopicToolsConfig` struct models exactly one `input_topic`; extending support would require a `mux_topics` list field and corresponding YAML parsing.

- **Demux** — Routes messages from a single input topic to one of several output topics, switchable at runtime. Not supported because the current model maps to exactly one `output_topic`; support would require a `demux_topics` list and the sink would need to know which output channel to subscribe to.

### kafka_sink

A `rclcpp_lifecycle::LifecycleNode` that forwards ROS 2 messages to Apache Kafka.

- On `configure`: parses `subscriptions_yaml` and creates a `KafkaProducer`.
- On `activate`: creates generic ROS 2 subscriptions (using `rosidl` type introspection) and starts the producer poll thread.
- On `deactivate`: destroys subscriptions; producer drains and stops.
- Each message is serialized (CDR or JSON) and sent to a Kafka topic derived from the ROS topic name (prefix or fixed mapping).
- Adds one Kafka record header: `ros_type` (the ROS 2 message type). Other fields (`ros_topic`, `kafka_topic`, `stamp_ms`, `payload_format`) are written to the telemetry log, not set as headers.
- Optionally publishes per-topic metrics to ROS 2 at a configurable interval.

### mosquitto_sink

Mirrors `kafka_sink` functionality for MQTT using the Eclipse Paho C++ client.

- Supports TLS/SSL, LWT (Last Will and Testament), QoS levels 0/1/2, and automatic reconnection.
- Topic naming follows the same prefix/fixed mapping as `kafka_sink`.

### kafka_client (library)

A C++ library wrapping `librdkafka` with:

- Async producer with a bounded, drop-when-full queue.
- Background poll thread for delivery callbacks.
- `STRICT` startup mode (fail if broker unreachable) or `TOLERANT` (continue without broker).
- Header support.

### kafka_source

A lifecycle node that consumes Kafka topics and republishes messages as ROS 2 topics.

- Subscribes via regex pattern.
- Deserializes CDR payloads using `rosbag2_cpp` type support.
- Used for round-trip latency testing and bridging Kafka back into ROS 2.

### kafka_cdr_to_json

A lifecycle node that reads CDR-encoded Kafka records and republishes them as JSON to new Kafka topics (adds a `.json` suffix by default).

### ros2_kafka_dispatcher_bringup

Contains system-level launch files and example configuration files. Two main launch files:

- `system_minimal.launch.py` — each node in its own process.
- `system_composed.launch.py` — all nodes as components in a single container.

## Data flow: ROS 2 → Kafka

```
ROS 2 publisher
      │  raw message (DDS)
      ▼
kafka_sink subscription callback
      │  rosidl introspection serializes to CDR (or JSON)
      ▼
KafkaProducer (kafka_client)
      │  enqueues message + headers to bounded queue
      ▼
librdkafka poll thread
      │  batches and sends to broker
      ▼
Apache Kafka partition
      │  stored, replicated
      ▼
downstream consumers (kafka_source, analytics, ML pipelines, ...)
```

## Data flow: Kafka → ROS 2 (kafka_source)

```
Apache Kafka partition
      │
      ▼
kafka_source consumer thread
      │  reads CDR bytes and the `ros_type` header
      ▼
rosbag2_cpp deserializer
      │  reconstructs typed ROS 2 message
      ▼
ROS 2 publisher → downstream subscribers
```

## Lifecycle state machine

Both `kafka_sink` and `mosquitto_sink` use the standard ROS 2 lifecycle:

```
UNCONFIGURED ──configure──► INACTIVE ──activate──► ACTIVE
                                 ▲                    │
                                 └────deactivate───────┘
                                 │
                               cleanup
                                 │
                                 ▼
                           UNCONFIGURED
```

The `dispatcher_controller` drives these transitions. It maintains its own internal phase:

| Phase   | Meaning |
|---------|---------|
| `IDLE`  | Ready to accept a new selection. |
| `BUSY`  | Currently applying or stopping a selection. |
| `ERROR` | Last operation failed; inspect `last_error`. |

## Deployment topologies

### Single-machine (development)

All nodes on one machine, Kafka running in Docker. Latency dominated by local loopback.

### Edge device → cloud Kafka

Robot / edge device runs the full ROS 2 stack and the dispatcher. Kafka cluster lives in the cloud. Network latency adds to end-to-end measurement but CDR payload is compact.

### Composed vs. separate processes

`system_composed.launch.py` runs everything in one component container — lower IPC overhead, single process to monitor. `system_minimal.launch.py` gives each node its own process — easier to restart individual components and observe per-process CPU/memory.
