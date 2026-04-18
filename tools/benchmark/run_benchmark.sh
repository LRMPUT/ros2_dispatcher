#!/usr/bin/env bash
#
# Orchestrate a single CDR vs JSON benchmark run.
#
# Usage:
#   ./run_benchmark.sh --msg-type odometry --format cdr --rate 100 --run-id 1
#   ./run_benchmark.sh --msg-type navsatfix --format json --rate 50 --run-id 2
#
# Prerequisites:
#   - Kafka broker running (cd kafka_bridge/kafka_brocker && docker compose up -d)
#   - Workspace built and sourced (source install/setup.bash)
#
# The script will:
#   1. Launch kafka_sink with the specified payload_format
#   2. Launch the synthetic publisher at the specified rate
#   3. Launch the metrics recorder (with warmup + duration)
#   4. Wait for the recorder to finish
#   5. Clean up all processes
#   6. Save the CSV to results/<msg_type>_<format>_<rate>hz_run<id>.csv

set -euo pipefail

# ── Defaults ──
MSG_TYPE="navsatfix"
FORMAT="cdr"
RATE=10
RUN_ID=1
DURATION=60
WARMUP=5
NUM_POINTS=10000
RESULTS_DIR="$(dirname "$0")/results"
KAFKA_SINK_NODE="kafka_sink"

# ── Topic type mapping ──
declare -A TOPIC_TYPES=(
    [navsatfix]="sensor_msgs/msg/NavSatFix"
    [odometry]="nav_msgs/msg/Odometry"
    [pointcloud2]="sensor_msgs/msg/PointCloud2"
)

# ── Parse arguments ──
while [[ $# -gt 0 ]]; do
    case $1 in
        --msg-type)     MSG_TYPE="$2";    shift 2 ;;
        --format)       FORMAT="$2";      shift 2 ;;
        --rate)         RATE="$2";        shift 2 ;;
        --run-id)       RUN_ID="$2";      shift 2 ;;
        --duration)     DURATION="$2";    shift 2 ;;
        --warmup)       WARMUP="$2";      shift 2 ;;
        --num-points)   NUM_POINTS="$2";  shift 2 ;;
        --results-dir)  RESULTS_DIR="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "  --msg-type    navsatfix|odometry|pointcloud2 (default: navsatfix)"
            echo "  --format      cdr|json (default: cdr)"
            echo "  --rate        publish rate in Hz (default: 10)"
            echo "  --run-id      repetition number (default: 1)"
            echo "  --duration    measurement duration in seconds (default: 60)"
            echo "  --warmup      warmup seconds to skip (default: 5)"
            echo "  --num-points  points for pointcloud2 (default: 10000)"
            echo "  --results-dir output directory (default: tools/benchmark/results)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

TOPIC="/benchmark/${MSG_TYPE}"
MSG_TYPE_FULL="${TOPIC_TYPES[$MSG_TYPE]}"
OUTPUT_CSV="${RESULTS_DIR}/${MSG_TYPE}_${FORMAT}_${RATE}hz_run${RUN_ID}.csv"

mkdir -p "$RESULTS_DIR"

echo "============================================="
echo "  CDR vs JSON Benchmark"
echo "============================================="
echo "  Message type : ${MSG_TYPE} (${MSG_TYPE_FULL})"
echo "  Format       : ${FORMAT}"
echo "  Rate         : ${RATE} Hz"
echo "  Duration     : ${DURATION}s (warmup ${WARMUP}s)"
echo "  Run ID       : ${RUN_ID}"
echo "  Output       : ${OUTPUT_CSV}"
echo "============================================="

# ── Cleanup function ──
PIDS=()
cleanup() {
    echo ""
    echo "[cleanup] Stopping all processes..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    # Wait briefly for graceful shutdown
    sleep 2
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    echo "[cleanup] Done."
}
trap cleanup EXIT

# ── Build subscriptions YAML for a single topic ──
SUBS_YAML="- topic_name: ${TOPIC}
  msg_type: ${MSG_TYPE_FULL}"

# ── 1. Launch kafka_sink ──
echo "[1/3] Starting kafka_sink (format=${FORMAT})..."
ros2 run kafka_sink kafka_sink_node_exe --ros-args \
    -p "subscriptions_yaml:=${SUBS_YAML}" \
    -p "kafka.payload_format:=${FORMAT}" \
    -p "kafka.bootstrap_servers:=localhost:9092" \
    -p "kafka.topic_prefix:=benchmark" \
    -p "kafka.strict_startup:=false" \
    -p "kafka.drop_when_full:=true" \
    -p "metrics.enabled:=true" \
    -p "metrics.interval_ms:=1000" \
    -p "metrics.topic:=kafka_sink/metrics" \
    --log-level warn &
PIDS+=($!)
echo "  kafka_sink PID: ${PIDS[-1]}"

# Give the sink a moment to initialize
sleep 3

# Transition lifecycle node: configure -> activate
echo "  Transitioning kafka_sink: configure -> activate..."
ros2 lifecycle set /kafka_sink configure 2>/dev/null || echo "  WARN: configure failed"
sleep 1
ros2 lifecycle set /kafka_sink activate 2>/dev/null || echo "  WARN: activate failed"
sleep 1
echo "  kafka_sink state: $(ros2 lifecycle get /kafka_sink 2>/dev/null || echo 'unknown')"

# ── 2. Launch synthetic publisher ──
echo "[2/3] Starting synthetic publisher (${MSG_TYPE} @ ${RATE} Hz)..."
PUBLISHER_ARGS="--msg-type ${MSG_TYPE} --topic ${TOPIC} --rate ${RATE}"
if [[ "$MSG_TYPE" == "pointcloud2" ]]; then
    PUBLISHER_ARGS="${PUBLISHER_ARGS} --num-points ${NUM_POINTS}"
fi
python3 "$(dirname "$0")/synthetic_publisher.py" ${PUBLISHER_ARGS} &
PIDS+=($!)
echo "  publisher PID: ${PIDS[-1]}"

# ── 3. Launch metrics recorder ──
echo "[3/3] Starting metrics recorder (warmup=${WARMUP}s, duration=${DURATION}s)..."
python3 "$(dirname "$0")/metrics_recorder.py" \
    --output "${OUTPUT_CSV}" \
    --warmup "${WARMUP}" \
    --duration "${DURATION}" \
    --metrics-topic "kafka_sink/metrics" &
RECORDER_PID=$!
PIDS+=($RECORDER_PID)
echo "  recorder PID: ${RECORDER_PID}"

# ── Wait for recorder to finish ──
TOTAL_WAIT=$((WARMUP + DURATION + 10))
echo ""
echo "Waiting up to ${TOTAL_WAIT}s for recorder to complete..."
if wait "$RECORDER_PID" 2>/dev/null; then
    echo ""
    echo "Recording complete: ${OUTPUT_CSV}"
else
    echo ""
    echo "Recorder exited (may have completed or been interrupted)."
fi

# Count rows (excluding header)
if [[ -f "$OUTPUT_CSV" ]]; then
    ROWS=$(($(wc -l < "$OUTPUT_CSV") - 1))
    echo "  Rows recorded: ${ROWS}"
fi

echo ""
echo "Run finished: ${MSG_TYPE}_${FORMAT}_${RATE}hz_run${RUN_ID}"
