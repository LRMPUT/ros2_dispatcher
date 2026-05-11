#!/usr/bin/env bash
# Edge topology entrypoint: one container runs BOTH the robot publisher AND
# its own kafka_sink (or mosquitto_sink), so each simulated robot has a
# private Dispatcher instance that talks DDS locally and produces directly
# to the broker.  Distinct from the gateway topology where one central sink
# subscribes to all robots' DDS topics.
set -euo pipefail

: "${ROBOT_ID:?ROBOT_ID env var is required}"
: "${SINK_KIND:=kafka}"          # 'kafka' or 'mqtt'
: "${BAG_PATH:?BAG_PATH env var is required}"
: "${BROKER_HOST:=localhost}"
: "${MSG_TYPE:=multi}"
: "${RATE_HZ:=10}"
: "${MQTT_QOS:=1}"               # 0 = fire-and-forget, 1 = at-least-once (paper default)

# ROS setup scripts reference unset variables; relax set -u for the source step.
set +u
source /opt/ros/humble/setup.bash
source /ws/install/setup.bash
set -u

# Build the per-robot subscription list (4 entries in multi mode).
case "${MSG_TYPE}" in
    navsatfix)   PAIRS=("sensor_msgs/msg/NavSatFix|gnss") ;;
    odometry)    PAIRS=("nav_msgs/msg/Odometry|odom") ;;
    laserscan)   PAIRS=("sensor_msgs/msg/LaserScan|scan") ;;
    pointcloud2) PAIRS=("sensor_msgs/msg/PointCloud2|points") ;;
    multi)
        PAIRS=("sensor_msgs/msg/NavSatFix|gnss"
               "nav_msgs/msg/Odometry|odom"
               "sensor_msgs/msg/LaserScan|scan"
               "sensor_msgs/msg/PointCloud2|points")
        ;;
    *) echo "Unknown MSG_TYPE=${MSG_TYPE}" >&2; exit 1 ;;
esac

SUBS_YAML=""
for p in "${PAIRS[@]}"; do
    IFS='|' read -r ros_type suffix <<< "$p"
    SUBS_YAML+="- topic_name: /robot_${ROBOT_ID}/${suffix}
  msg_type: ${ros_type}
"
done

NODE_NAME="kafka_sink_${ROBOT_ID}"
EXE="kafka_sink_node_exe"
PKG="kafka_sink"
TOPIC_PREFIX_PARAM_NAME="kafka.topic_prefix"
case "${SINK_KIND}" in
    kafka)
        PARAMS_FILE="/tmp/sink_params_${ROBOT_ID}.yaml"
        cat > "${PARAMS_FILE}" <<EOF
${NODE_NAME}:
  ros__parameters:
    subscriptions_yaml: |
$(printf '%s\n' "${SUBS_YAML}" | sed 's/^/      /')
    kafka.payload_format: cdr
    kafka.bootstrap_servers: "${BROKER_HOST}:9092"
    kafka.topic_prefix: ros2
    kafka.drop_when_full: true
    kafka.strict_startup: false
    kafka.acks: "1"
    kafka.linger_ms: 0
    metrics.enabled: false
EOF
        ;;
    mqtt)
        EXE="mosquitto_sink_node_exe"
        PKG="mosquitto_sink"
        NODE_NAME="mosquitto_sink_${ROBOT_ID}"
        PARAMS_FILE="/tmp/sink_params_${ROBOT_ID}.yaml"
        cat > "${PARAMS_FILE}" <<EOF
${NODE_NAME}:
  ros__parameters:
    subscriptions_yaml: |
$(printf '%s\n' "${SUBS_YAML}" | sed 's/^/      /')
    mqtt.broker_host: "${BROKER_HOST}"
    mqtt.broker_port: 1883
    mqtt.topic_prefix: ros2
    mqtt.qos: ${MQTT_QOS}
    metrics.enabled: false
EOF
        ;;
    *) echo "Unknown SINK_KIND=${SINK_KIND}" >&2; exit 1 ;;
esac

echo "[edge] robot_id=${ROBOT_ID} sink=${SINK_KIND} broker=${BROKER_HOST}"
echo "[edge] params:"; sed 's/^/  | /' "${PARAMS_FILE}"

# 1. Start the per-robot sink in the background with a unique node name.
ros2 run "${PKG}" "${EXE}" --ros-args \
    -r __node:="${NODE_NAME}" \
    --params-file "${PARAMS_FILE}" &
SINK_PID=$!

cleanup() {
    kill "${SINK_PID}" 2>/dev/null || true
}
trap cleanup EXIT

# 2. Wait for the sink lifecycle service, then configure + activate.
echo "[edge] waiting for /${NODE_NAME} lifecycle..."
if ! timeout 30 bash -c "until ros2 lifecycle get /${NODE_NAME} >/dev/null 2>&1; do sleep 0.2; done"; then
    echo "[edge] ERROR: /${NODE_NAME} lifecycle never appeared" >&2
    exit 1
fi
ros2 lifecycle set "/${NODE_NAME}" configure
ros2 lifecycle set "/${NODE_NAME}" activate
echo "[edge] /${NODE_NAME} ACTIVE."

# 3. Run the publisher in the foreground. When it exits, cleanup() kills the sink.
exec python3 /app/robot_replay.py
