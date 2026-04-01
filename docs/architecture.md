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

For each topic in the selection, an optional `topic_tools` block can specify a plugin (e.g. `ThrottleNode`). The controller instantiates the plugin as a component in the shared container, wires the ROS topic through it, and registers the transformed output topic with the sink instead of the original.

### kafka_sink

A `rclcpp_lifecycle::LifecycleNode` that forwards ROS 2 messages to Apache Kafka.

- On `configure`: parses `subscriptions_yaml` and creates a `KafkaProducer`.
- On `activate`: creates generic ROS 2 subscriptions (using `rosidl` type introspection) and starts the producer poll thread.
- On `deactivate`: destroys subscriptions; producer drains and stops.
- Each message is serialized (CDR or JSON) and sent to a Kafka topic derived from the ROS topic name (prefix or fixed mapping).
- Adds Kafka headers: `ros_topic`, `ros_type`, `kafka_topic`, `stamp_ms`, `payload_format`.
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
      │  reads CDR bytes + headers (ros_topic, ros_type)
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
