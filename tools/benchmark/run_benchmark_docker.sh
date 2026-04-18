#!/usr/bin/env bash
#
# Run a single CDR vs JSON benchmark inside Docker.
#
# Usage:
#   ./run_benchmark_docker.sh --msg-type navsatfix --format cdr --rate 10
#   ./run_benchmark_docker.sh --msg-type odometry --format json --rate 100
#
# Prerequisites:
#   - Kafka broker running (kafka_brocker_default network)
#   - Docker image ros2-kafka-benchmark:local built

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

MSG_TYPE="navsatfix"
FORMAT="cdr"
RATE=10
RUN_ID=1
DURATION=60
WARMUP=5
NUM_POINTS=10000
RESULTS_DIR="${SCRIPT_DIR}/results"
DOCKER_IMAGE="ros2-kafka-benchmark:run"
KAFKA_NETWORK="kafka_brocker_default"

declare -A TOPIC_TYPES=(
    [navsatfix]="sensor_msgs/msg/NavSatFix"
    [odometry]="nav_msgs/msg/Odometry"
    [odometry_fullcov]="nav_msgs/msg/Odometry"
    [pointcloud2]="sensor_msgs/msg/PointCloud2"
)

while [[ $# -gt 0 ]]; do
    case $1 in
        --msg-type)     MSG_TYPE="$2";    shift 2 ;;
        --format)       FORMAT="$2";      shift 2 ;;
        --rate)         RATE="$2";        shift 2 ;;
        --run-id)       RUN_ID="$2";      shift 2 ;;
        --duration)     DURATION="$2";    shift 2 ;;
        --warmup)       WARMUP="$2";      shift 2 ;;
        --num-points)   NUM_POINTS="$2";  shift 2 ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "  --msg-type    navsatfix|odometry|pointcloud2"
            echo "  --format      cdr|json"
            echo "  --rate        publish rate in Hz"
            echo "  --run-id      repetition number"
            echo "  --duration    measurement seconds"
            echo "  --warmup      warmup seconds"
            exit 0
            ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

TOPIC="/benchmark/${MSG_TYPE}"
MSG_TYPE_FULL="${TOPIC_TYPES[$MSG_TYPE]}"
OUTPUT_CSV="${RESULTS_DIR}/${MSG_TYPE}_${FORMAT}_${RATE}hz_run${RUN_ID}.csv"

mkdir -p "$RESULTS_DIR"

# Generate a params YAML file for kafka_sink (avoids multiline -p issues)
PARAMS_FILE="${RESULTS_DIR}/.kafka_sink_params_${MSG_TYPE}_${FORMAT}.yaml"
cat > "$PARAMS_FILE" <<YAML
kafka_sink:
  ros__parameters:
    subscriptions_yaml: |
      - topic_name: ${TOPIC}
        msg_type: ${MSG_TYPE_FULL}
    kafka.payload_format: "${FORMAT}"
    kafka.bootstrap_servers: "broker:29092"
    kafka.topic_prefix: "benchmark"
    kafka.strict_startup: false
    kafka.drop_when_full: true
    kafka.max_queue_messages: 4096
    kafka.linger_ms: 5
    kafka.batch_size: 65536
    metrics.enabled: true
    metrics.interval_ms: 1000
    metrics.topic: "kafka_sink/metrics"
    telemetry.enabled: false
YAML

echo "============================================="
echo "  CDR vs JSON Benchmark (Docker)"
echo "============================================="
echo "  Message type : ${MSG_TYPE} (${MSG_TYPE_FULL})"
echo "  Format       : ${FORMAT}"
echo "  Rate         : ${RATE} Hz"
echo "  Duration     : ${DURATION}s (warmup ${WARMUP}s)"
echo "  Run ID       : ${RUN_ID}"
echo "  Output       : ${OUTPUT_CSV}"
echo "============================================="

# Pointcloud2 extra args
PUB_EXTRA=""
if [[ "$MSG_TYPE" == "pointcloud2" ]]; then
    PUB_EXTRA="--num-points ${NUM_POINTS}"
fi

docker run --rm \
    --name "benchmark_${MSG_TYPE}_${FORMAT}_${RATE}" \
    --network "${KAFKA_NETWORK}" \
    --user root \
    -v "${SCRIPT_DIR}:/benchmark:ro" \
    -v "${RESULTS_DIR}:/results" \
    -v "${PARAMS_FILE}:/tmp/kafka_sink_params.yaml:ro" \
    "${DOCKER_IMAGE}" \
    bash -c "
set -e

source /opt/ros/\${ROS_DISTRO}/setup.bash
source /ws/install/setup.bash

echo '[1/3] Starting kafka_sink (format=${FORMAT})...'
ros2 run kafka_sink kafka_sink_node_exe --ros-args \
    --params-file /tmp/kafka_sink_params.yaml \
    --log-level warn &
SINK_PID=\$!
sleep 3

echo '  Transitioning kafka_sink: configure -> activate...'
ros2 lifecycle set /kafka_sink configure 2>/dev/null || echo '  WARN: configure failed'
sleep 1
ros2 lifecycle set /kafka_sink activate 2>/dev/null || echo '  WARN: activate failed'
sleep 1
echo '  kafka_sink state:' \$(ros2 lifecycle get /kafka_sink 2>/dev/null || echo 'unknown')

echo '[2/3] Starting synthetic publisher (${MSG_TYPE} @ ${RATE} Hz)...'
python3 /benchmark/synthetic_publisher.py \
    --msg-type ${MSG_TYPE} \
    --topic ${TOPIC} \
    --rate ${RATE} ${PUB_EXTRA} &
PUB_PID=\$!

echo '[3/3] Starting metrics recorder (warmup=${WARMUP}s, duration=${DURATION}s)...'
python3 /benchmark/metrics_recorder.py \
    --output /results/${MSG_TYPE}_${FORMAT}_${RATE}hz_run${RUN_ID}.csv \
    --warmup ${WARMUP} \
    --duration ${DURATION} \
    --metrics-topic kafka_sink/metrics

echo 'Recording complete. Cleaning up...'
kill \$PUB_PID 2>/dev/null || true
kill \$SINK_PID 2>/dev/null || true
sleep 1
# Force kill anything still alive
kill -9 \$PUB_PID 2>/dev/null || true
kill -9 \$SINK_PID 2>/dev/null || true
# Kill all remaining ROS/Python processes
pkill -f synthetic_publisher 2>/dev/null || true
pkill -f kafka_sink_node 2>/dev/null || true
echo 'Cleanup done.'
" 2>&1

# Report results
if [[ -f "$OUTPUT_CSV" ]]; then
    ROWS=$(($(wc -l < "$OUTPUT_CSV") - 1))
    echo ""
    echo "Result: ${OUTPUT_CSV} (${ROWS} rows)"
else
    echo ""
    echo "WARNING: Output CSV not found at ${OUTPUT_CSV}"
fi
