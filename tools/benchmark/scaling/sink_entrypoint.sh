#!/usr/bin/env bash
# Sink container entrypoint: template subscriptions_yaml for $NUM_ROBOTS,
# launch the sink, launch metrics_recorder.py in parallel.
set -euo pipefail

: "${NUM_ROBOTS:?NUM_ROBOTS env var is required}"
: "${SINK_KIND:?SINK_KIND must be 'kafka' or 'mqtt'}"
: "${RESULTS_DIR:=/artifacts}"
: "${METRICS_RECORDER_PATH:=/host_tools/benchmark/metrics_recorder.py}"
: "${MSG_TYPE:=navsatfix}"

mkdir -p "${RESULTS_DIR}"

# Build list of (ROS_MSG_TYPE, TOPIC_SUFFIX) pairs to subscribe to per robot.
# Single-stream modes: one pair. Multi-topic mode: four pairs.
declare -a PAIRS=()
add_pair() { PAIRS+=("$1|$2"); }
case "${MSG_TYPE}" in
    navsatfix)   add_pair "sensor_msgs/msg/NavSatFix"   "gnss"   ;;
    odometry)    add_pair "nav_msgs/msg/Odometry"       "odom"   ;;
    laserscan)   add_pair "sensor_msgs/msg/LaserScan"   "scan"   ;;
    pointcloud2) add_pair "sensor_msgs/msg/PointCloud2" "points" ;;
    multi)
        add_pair "sensor_msgs/msg/NavSatFix"   "gnss"
        add_pair "nav_msgs/msg/Odometry"       "odom"
        add_pair "sensor_msgs/msg/LaserScan"   "scan"
        add_pair "sensor_msgs/msg/PointCloud2" "points"
        ;;
    *) echo "Unknown MSG_TYPE: ${MSG_TYPE}" >&2; exit 1 ;;
esac

# ── Generate subscriptions_yaml content ──
# For multi-topic mode this is 4×NUM_ROBOTS entries.
SUBS_YAML=""
for ((i = 1; i <= NUM_ROBOTS; i++)); do
    for pair in "${PAIRS[@]}"; do
        IFS='|' read -r ros_type suffix <<< "${pair}"
        SUBS_YAML+="- topic_name: /robot_${i}/${suffix}
  msg_type: ${ros_type}
"
    done
done

# ── Source the workspace built inside the image ──
# ROS setup scripts reference unset variables (e.g. AMENT_TRACE_SETUP_FILES) which
# trip `set -u`. Disable nounset locally for the sourcing block.
set +u
source /opt/ros/humble/setup.bash
source /ws/install/setup.bash
set -u

# ── Pick exe / node / broker params ──
# Multi-line subscriptions_yaml cannot be passed via `-p name:=value` on the
# CLI; we write a params file instead. Other params go alongside.
PARAMS_FILE="/tmp/sink_params.yaml"
case "${SINK_KIND}" in
    kafka)
        EXE="kafka_sink_node_exe"
        NODE_NAME="kafka_sink"
        METRICS_TOPIC="/kafka_sink/metrics"
        # Fair-comparison mode: when KAFKA_FAIR_LATENCY=1, configure the Kafka
        # producer with semantics matching MQTT qos=1 (broker ack only) and no
        # batching, so we measure transport overhead rather than the gap between
        # default tuning choices.
        FAIR_PARAMS=""
        if [[ "${KAFKA_FAIR_LATENCY:-0}" == "1" ]]; then
            FAIR_PARAMS="    kafka.acks: \"1\"
    kafka.linger_ms: 0"
        fi
        cat > "${PARAMS_FILE}" <<EOF
${NODE_NAME}:
  ros__parameters:
    subscriptions_yaml: |
$(printf '%s\n' "${SUBS_YAML}" | sed 's/^/      /')
    kafka.payload_format: cdr
    kafka.bootstrap_servers: "${BROKER_HOST:-localhost}:9092"
    kafka.topic_prefix: ros2
    kafka.drop_when_full: true
    kafka.strict_startup: false
${FAIR_PARAMS}
    metrics.enabled: true
    metrics.interval_ms: 1000
EOF
        ;;
    mqtt)
        EXE="mosquitto_sink_node_exe"
        NODE_NAME="mosquitto_sink"
        METRICS_TOPIC="/mosquitto_sink/metrics"
        cat > "${PARAMS_FILE}" <<EOF
${NODE_NAME}:
  ros__parameters:
    subscriptions_yaml: |
$(printf '%s\n' "${SUBS_YAML}" | sed 's/^/      /')
    mqtt.broker_host: "${BROKER_HOST:-localhost}"
    mqtt.broker_port: 1883
    mqtt.topic_prefix: ros2
    metrics.enabled: true
    metrics.interval_ms: 1000
EOF
        ;;
    *)
        echo "Unknown SINK_KIND: ${SINK_KIND}" >&2
        exit 1
        ;;
esac

echo "[entrypoint] Wrote params file:"
sed 's/^/  | /' "${PARAMS_FILE}"

# ── Start metrics recorder in background (skip if file missing) ──
if [[ -f "${METRICS_RECORDER_PATH}" ]]; then
    python3 "${METRICS_RECORDER_PATH}" \
        --metrics-topic "${METRICS_TOPIC}" \
        --output "${RESULTS_DIR}/sink_metrics.csv" \
        --duration 0 &
    METRICS_PID=$!
    echo "[entrypoint] metrics_recorder started (PID ${METRICS_PID})"
else
    echo "[entrypoint] WARNING: ${METRICS_RECORDER_PATH} not found; skipping sink-side metrics" >&2
    METRICS_PID=""
fi

cleanup() {
    [[ -n "${METRICS_PID}" ]] && kill "${METRICS_PID}" 2>/dev/null || true
}
trap cleanup EXIT

# ── Start the sink ──
ros2 run "${NODE_NAME}" "${EXE}" --ros-args --params-file "${PARAMS_FILE}" &
SINK_PID=$!

# ── Wait for lifecycle service, then configure → activate ──
echo "[entrypoint] Waiting for lifecycle node /${NODE_NAME} to appear..."
if ! timeout 30 bash -c "until ros2 lifecycle get /${NODE_NAME} >/dev/null 2>&1; do sleep 0.2; done"; then
    echo "[entrypoint] ERROR: /${NODE_NAME} lifecycle service not reachable after 30 s" >&2
    exit 1
fi
ros2 lifecycle set "/${NODE_NAME}" configure
ros2 lifecycle set "/${NODE_NAME}" activate
echo "[entrypoint] Sink ACTIVE."

wait "${SINK_PID}"
