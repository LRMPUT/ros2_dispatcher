# ros2_kafka_dispatcher

ROS 2 packages for discovering active topics, selecting subsets, and forwarding serialized messages to Apache Kafka or MQTT.

## Documentation

| | |
|---|---|
| [Getting Started](docs/getting_started.md) | Build, launch, and verify a minimal pipeline |
| [Architecture](docs/architecture.md) | System design, component interactions, data flow |
| [Configuration Reference](docs/configuration_reference.md) | All parameters for every node |
| [API Reference](docs/api_reference.md) | Services, topics, message types, Kafka record format |
| [Deployment](docs/deployment.md) | Docker, broker setup, production checklist |
| [Developer Guide](docs/developer_guide.md) | Build, test, extend, contribute |
| [Latency Measurement](docs/latency_measurement.md) | End-to-end latency tooling and workflow |
| [Performance Metrics](docs/performance_metrics.md) | Metric definitions and measurement methodology |
| [Benchmark Plan](docs/benchmark_plan.md) | Structured test matrix and procedure |

---

## Quick start

```bash
# 1. Build
rosdep install --from-paths src --ignore-src -y
colcon build --symlink-install --cmake-args -DCMAKE_BUILD_TYPE=Release
source install/setup.bash

# 2. Start a local Kafka broker
cd kafka_bridge/kafka_brocker && docker compose up -d

# 3. Launch
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:=/path/to/topics.yaml
```

See [Getting Started](docs/getting_started.md) for a full walkthrough including MQTT, topic selection files, and verification steps.

---

## Components

| Component | Type | Role |
|-----------|------|------|
| `introspection_manager` | Node | Monitors the ROS 2 graph; publishes `~/topics_info`; serves `~/get_topics` |
| `dispatcher_controller` | Node | Loads topic selections; drives sink lifecycles; exposes control services |
| `kafka_sink` | Lifecycle Node | Subscribes to selected topics and forwards payloads to Kafka |
| `mosquitto_sink` | Lifecycle Node | Subscribes to selected topics and forwards payloads to MQTT |
| `kafka_source` | Lifecycle Node | Consumes Kafka topics and republishes as ROS 2 messages |
| `kafka_client` | Library | `librdkafka` wrapper with async queue and health monitoring |

### Selection modes

| Mode | Description |
|------|-------------|
| `file` | Reads topic list from a YAML file (default) |
| `gui` | Waits for an `apply_selection` service call from an external client |
| `all` | Auto-discovers all topics via `introspection_manager` |

### Payload formats

| Format | Description |
|--------|-------------|
| `cdr` | Binary ROS 2 CDR serialization — compact, default |
| `json` | Human-readable JSON — larger, easier to consume downstream |

CDR payloads can be converted to JSON after the fact using `ros2_kafka_json_sidecar.py`:

```bash
python3 ros2_kafka_json_sidecar.py \
  --bootstrap-servers localhost:9092 \
  --topic-pattern '^ros2\..*' \
  --output-suffix .json
```

---

## Docker

```bash
docker build -f docker/Dockerfile --build-arg ROS_DISTRO=humble -t ros2-kafka-dispatcher:local .
docker run --rm -it ros2-kafka-dispatcher:local bash
```

See [Deployment](docs/deployment.md) for CI/CD, production parameter files, and Docker Hub publishing.

---

## Known limitations

- `selection_mode:=gui` requires an external GUI client (not included in this repository).
- `plugin_loader`, `plugin_interfaces`, and `processing_plugins/*` are stubs without runnable examples.
- Kafka and MQTT brokers must be provisioned separately; this repository does not manage broker infrastructure beyond Docker Compose files for local development.
