#!/usr/bin/env bash
# Sink container entrypoint: template subscriptions_yaml for $NUM_ROBOTS,
# launch the sink, launch metrics_recorder.py in parallel.
set -euo pipefail

: "${NUM_ROBOTS:?NUM_ROBOTS env var is required}"
: "${SINK_KIND:?SINK_KIND must be 'kafka' or 'mqtt'}"
: "${RESULTS_DIR:=/artifacts}"

mkdir -p "${RESULTS_DIR}"

# ── Generate subscriptions_yaml ──
SUBS_YAML=""
for ((i = 1; i <= NUM_ROBOTS; i++)); do
    SUBS_YAML+="- topic_name: /robot_${i}/gnss
  msg_type: sensor_msgs/msg/NavSatFix
"
done

# ── Source the workspace built inside the image ──
source /opt/ros/humble/setup.bash
source /ws/install/setup.bash

# ── Pick exe and metrics topic ──
case "${SINK_KIND}" in
    kafka)
        EXE="kafka_sink_node_exe"
        NODE_NAME="kafka_sink"
        METRICS_TOPIC="/kafka_sink/metrics"
        FORMAT_PARAM="-p kafka.payload_format:=cdr"
        # NOTE: BROKER_HOST must be a single token; we rely on word-splitting below.
        BROKER_PARAM="-p kafka.bootstrap_servers:=${BROKER_HOST:-localhost}:9092"
        TOPIC_PREFIX_PARAM="-p kafka.topic_prefix:=ros2"
        DROP_PARAM="-p kafka.drop_when_full:=true"
        STRICT_PARAM="-p kafka.strict_startup:=false"
        ;;
    mqtt)
        EXE="mosquitto_sink_node_exe"
        NODE_NAME="mosquitto_sink"
        METRICS_TOPIC="/mosquitto_sink/metrics"
        FORMAT_PARAM=""
        # NOTE: BROKER_HOST must be a single token; we rely on word-splitting below.
        BROKER_PARAM="-p mqtt.host:=${BROKER_HOST:-localhost} -p mqtt.port:=1883"
        TOPIC_PREFIX_PARAM="-p mqtt.topic_prefix:=ros2"
        DROP_PARAM="-p mqtt.drop_when_full:=true"
        STRICT_PARAM="-p mqtt.strict_startup:=false"
        ;;
    *)
        echo "Unknown SINK_KIND: ${SINK_KIND}" >&2
        exit 1
        ;;
esac

# ── Start metrics recorder in background ──
python3 /ws/src/ros2_kafka_dispatcher/tools/benchmark/metrics_recorder.py \
    --topic "${METRICS_TOPIC}" \
    --output "${RESULTS_DIR}/sink_metrics.csv" \
    --duration 0 &
METRICS_PID=$!

cleanup() {
    kill "${METRICS_PID}" 2>/dev/null || true
}
trap cleanup EXIT

# ── Start the sink ──
ros2 run "${NODE_NAME}" "${EXE}" --ros-args \
    -p "subscriptions_yaml:=${SUBS_YAML}" \
    ${FORMAT_PARAM} \
    ${BROKER_PARAM} \
    ${TOPIC_PREFIX_PARAM} \
    ${DROP_PARAM} \
    ${STRICT_PARAM} \
    -p metrics.enabled:=true \
    -p metrics.interval_ms:=1000 &
SINK_PID=$!

# ── Wait for lifecycle service, then configure → activate ──
# At large N the sink may need >2 s to register; poll until reachable.
echo "[entrypoint] Waiting for lifecycle node /${NODE_NAME} to appear..."
if ! timeout 30 bash -c "until ros2 lifecycle get /${NODE_NAME} >/dev/null 2>&1; do sleep 0.2; done"; then
    echo "[entrypoint] ERROR: /${NODE_NAME} lifecycle service not reachable after 30 s" >&2
    exit 1
fi
ros2 lifecycle set "/${NODE_NAME}" configure
ros2 lifecycle set "/${NODE_NAME}" activate

wait "${SINK_PID}"
