#!/usr/bin/env bash
# Sink container entrypoint: template subscriptions_yaml for $NUM_ROBOTS,
# launch the sink, launch metrics_recorder.py in parallel.
set -euo pipefail

: "${NUM_ROBOTS:?NUM_ROBOTS env var is required}"
: "${SINK_KIND:?SINK_KIND must be 'kafka' or 'mqtt'}"
: "${RESULTS_DIR:=/artifacts}"
: "${METRICS_RECORDER_PATH:=/host_tools/benchmark/metrics_recorder.py}"

mkdir -p "${RESULTS_DIR}"

# ── Generate subscriptions_yaml content ──
SUBS_YAML=""
for ((i = 1; i <= NUM_ROBOTS; i++)); do
    SUBS_YAML+="- topic_name: /robot_${i}/gnss
  msg_type: sensor_msgs/msg/NavSatFix
"
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
