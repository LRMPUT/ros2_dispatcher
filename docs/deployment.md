# Deployment Guide

This document covers Docker-based deployment, broker setup, and production considerations.

---

## Prerequisites

- ROS 2 (Humble or later) installed on the target machine, or use the provided Docker image.
- Apache Kafka broker reachable from the ROS 2 machine.
- (Optional) MQTT broker reachable from the ROS 2 machine.

---

## Building the Docker image

The repository ships a multi-stage `Dockerfile` at `docker/Dockerfile`.

```bash
docker build \
  -f docker/Dockerfile \
  --build-arg ROS_DISTRO=humble \
  --build-arg GIT_SHA="$(git rev-parse HEAD)" \
  --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --build-arg IMAGE_SOURCE="https://github.com/<org>/ros2_kafka_dispatcher" \
  -t ros2-kafka-dispatcher:local \
  .
```

### Smoke test

```bash
docker run --rm ros2-kafka-dispatcher:local \
  bash -lc "source /opt/ros/humble/setup.bash && \
            source /ws/install/setup.bash && \
            ros2 pkg list >/dev/null && echo OK"
```

---

## Starting a local Kafka broker (development)

A `docker-compose.yml` for a single-broker Kafka cluster is provided in `kafka_bridge/kafka_brocker/`.

```bash
cd kafka_bridge/kafka_brocker
docker compose up -d
```

The broker listens on `localhost:9092` by default.

Verify connectivity:

```bash
# Install kcat (formerly kafkacat) for quick checks
kcat -b localhost:9092 -L
```

---

## Starting a local MQTT broker (development)

A `docker-compose.yml` for Mosquitto is provided in `mosquitto_bridge/mosquitto_brocker/`.

```bash
cd mosquitto_bridge/mosquitto_brocker
docker compose up -d
```

The broker listens on `localhost:1883` by default.

---

## Minimal system launch

After building and sourcing the workspace:

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:=/path/to/topics.yaml
```

This starts:
- `introspection_manager`
- `dispatcher_controller`
- `kafka_sink`
- `mosquitto_sink`

To disable either sink:

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  selection_mode:=file \
  selection_file_path:=/path/to/topics.yaml \
  enable_mosquitto_sink:=false
```

---

## Composed (single container) launch

Runs all nodes as components in one process — lower IPC overhead, easier to monitor as a single PID.

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_composed.launch.py \
  selection_mode:=file \
  selection_file_path:=/path/to/topics.yaml
```

---

## Production parameter files

For production deployments, override default parameters with YAML files instead of command-line arguments.

### kafka_sink.param.yaml (example)

```yaml
kafka_sink:
  ros__parameters:
    qos_depth: 50
    kafka:
      bootstrap_servers: "kafka-broker-1:9092,kafka-broker-2:9092"
      client_id: "robot-1-kafka-sink"
      acks: "all"
      topic_prefix: "robot1"
      topic_mapping_mode: "prefix_ros_topic"
      payload_format: "cdr"
      strict_startup: true
      max_queue_messages: 4096
      linger_ms: 5
      batch_size: 1048576
    metrics:
      enabled: true
      interval_ms: 5000
```

Pass to the launch:

```bash
ros2 launch ros2_kafka_dispatcher_bringup system_minimal.launch.py \
  kafka_sink_param_file:=/path/to/kafka_sink.param.yaml
```

### mosquitto_sink.param.yaml (example with TLS)

```yaml
mosquitto_sink:
  ros__parameters:
    mqtt:
      broker_host: "mqtt.example.com"
      broker_port: 8883
      client_id: "robot-1-mqtt-sink"
      username: "ros2"
      password: "secret"
      qos: 1
      use_tls: true
      ca_cert_path: "/etc/ssl/certs/ca-certificates.crt"
      lwt_topic: "robot1/status"
      lwt_payload: "offline"
      lwt_retain: true
```

---

## Docker Hub / GHCR publishing (CI/CD)

The GitHub Actions workflow in `.github/workflows/` builds the image and publishes it:

- **Pull requests / pushes to `main`:** Build + smoke test only.
- **Tags matching `v*.*.*`:** Publish to GHCR and Docker Hub.

Required GitHub secrets:

| Secret | Description |
|--------|-------------|
| `DOCKERHUB_USERNAME` | Docker Hub username or bot account. |
| `DOCKERHUB_TOKEN` | Docker Hub access token. |
| `DOCKERHUB_REPOSITORY` | Full image name, e.g. `your-org/ros2-kafka-dispatcher`. |

Publish a release:

```bash
git tag v1.2.3
git push origin v1.2.3
```

The workflow publishes:
- `your-org/ros2-kafka-dispatcher:1.2.3`
- `your-org/ros2-kafka-dispatcher:sha-<shortsha>`
- `your-org/ros2-kafka-dispatcher:latest` (from `main` only)

---

## Production checklist

- [ ] Set `kafka.strict_startup: true` so the node fails clearly if the broker is unreachable at startup.
- [ ] Configure `kafka.acks: all` and appropriate `kafka.max_queue_messages` for your throughput requirements.
- [ ] Use a dedicated Kafka `client_id` per robot/deployment to distinguish producers in broker metrics.
- [ ] Set `all_mode_max_topics` if using `all` mode to prevent accidentally subscribing to thousands of topics.
- [ ] Enable `metrics.enabled` and pipe metrics to your monitoring system.
- [ ] Use `system_composed.launch.py` for simpler process supervision (single PID per deployment).
- [ ] Synchronise system clocks (NTP/PTP) if you need accurate end-to-end latency measurements.
- [ ] Prefer `payload_format: cdr` for bandwidth efficiency; use `json` only when downstream consumers cannot handle CDR.
