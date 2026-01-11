#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR=""
TOPIC="/latency_test"
PAYLOAD_BYTES=256
RATE_HZ=10
COUNT=100
KAFKA_BOOTSTRAP="localhost:9092"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --output-dir <path>   Directory for logs/artifacts (default: ./latency_artifacts/<timestamp>)
  --topic <name>        ROS 2 topic to publish (default: /latency_test)
  --payload-bytes <n>   Payload size in bytes (default: 256)
  --rate <hz>           Publish rate (default: 10)
  --count <n>           Number of messages to publish/consume (default: 100)
  --bootstrap <host>    Kafka bootstrap servers (default: localhost:9092)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --topic)
      TOPIC="$2"
      shift 2
      ;;
    --payload-bytes)
      PAYLOAD_BYTES="$2"
      shift 2
      ;;
    --rate)
      RATE_HZ="$2"
      shift 2
      ;;
    --count)
      COUNT="$2"
      shift 2
      ;;
    --bootstrap)
      KAFKA_BOOTSTRAP="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="${ROOT_DIR}/latency_artifacts/$(date +%Y%m%d_%H%M%S)"
fi

mkdir -p "$OUTPUT_DIR"

SINK_PARAMS="${OUTPUT_DIR}/kafka_sink.params.yaml"
SOURCE_PARAMS="${OUTPUT_DIR}/kafka_source.params.yaml"

cat > "$SINK_PARAMS" <<EOF
/**:
  ros__parameters:
    qos_depth: 10
    subscriptions_yaml: |
      - topic_name: ${TOPIC}
        msg_type: std_msgs/msg/String
    metrics.enabled: true
    metrics.interval_ms: 1000
    metrics.topic: "kafka_sink/metrics"
    telemetry.enabled: true
    telemetry.log_every_n: 1
    kafka.bootstrap_servers: "${KAFKA_BOOTSTRAP}"
    kafka.client_id: "kafka_sink_latency"
    kafka.acks: "all"
    kafka.topic_prefix: "ros2"
    kafka.topic_mapping_mode: "prefix_ros_topic"
    kafka.strict_startup: false
    kafka.payload_format: "cdr"
EOF

cat > "$SOURCE_PARAMS" <<EOF
/**:
  ros__parameters:
    kafka.bootstrap_servers: "${KAFKA_BOOTSTRAP}"
    kafka.group_id: "ros2-kafka-source-latency"
    kafka.topic_pattern: "^ros2\\..*"
    kafka.offset_reset: "latest"
    ros_topic_prefix: "/kafka_decoded"
    qos_depth: 10
    metrics.enabled: true
    metrics.interval_ms: 1000
    metrics.topic: "kafka_source/metrics"
EOF

SINK_LOG="${OUTPUT_DIR}/kafka_sink.log"
SOURCE_LOG="${OUTPUT_DIR}/kafka_source.log"
PUBLISH_LOG="${OUTPUT_DIR}/publisher.jsonl"
CONSUME_LOG="${OUTPUT_DIR}/consumer.jsonl"

echo "Logs/output directory: ${OUTPUT_DIR}"

cleanup() {
  if [[ -n "${PUBLISH_PID:-}" ]]; then
    kill "${PUBLISH_PID}" 2>/dev/null || true
  fi
  if [[ -n "${CONSUMER_PID:-}" ]]; then
    kill "${CONSUMER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SOURCE_PID:-}" ]]; then
    kill "${SOURCE_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SINK_PID:-}" ]]; then
    kill "${SINK_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

ros2 run kafka_sink kafka_sink_node_exe --ros-args --params-file "$SINK_PARAMS" \
  >"$SINK_LOG" 2>&1 &
SINK_PID=$!

ros2 run kafka_source kafka_source_node_exe --ros-args --params-file "$SOURCE_PARAMS" \
  >"$SOURCE_LOG" 2>&1 &
SOURCE_PID=$!

sleep 2
ros2 lifecycle set /kafka_sink configure
ros2 lifecycle set /kafka_sink activate
ros2 lifecycle set /kafka_source configure
ros2 lifecycle set /kafka_source activate

CONSUME_TOPIC="/kafka_decoded${TOPIC}"

python3 "${ROOT_DIR}/tools/latency/latency_consumer.py" \
  --topic "${CONSUME_TOPIC}" \
  --count "${COUNT}" \
  --log-file "${CONSUME_LOG}" &
CONSUMER_PID=$!

python3 "${ROOT_DIR}/tools/latency/latency_publisher.py" \
  --topic "${TOPIC}" \
  --rate "${RATE_HZ}" \
  --payload-bytes "${PAYLOAD_BYTES}" \
  --count "${COUNT}" \
  --log-file "${PUBLISH_LOG}" &
PUBLISH_PID=$!

wait "${PUBLISH_PID}"
wait "${CONSUMER_PID}"

sleep 1
echo "Capture complete. Logs are in ${OUTPUT_DIR}"
