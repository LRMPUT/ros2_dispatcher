#!/usr/bin/env bash
# Edge tier: one robot. Runs a zenoh_sink (peer, CONNECTS to the cloud hub,
# declares a liveliness token) and a synthetic NavSatFix publisher that toggles
# the P12 boundary. ROBOT_ID is injected by compose (1..N).
set -eo pipefail
source /opt/ros/humble/setup.bash
source /ws/install/setup.bash

RID="${ROBOT_ID:-1}"
ROBOT="robot_${RID}"
RATE_HZ="${RATE_HZ:-5}"
CLOUD="${CLOUD_ENDPOINT:-tcp/cloud_query:7447}"

cat > /tmp/sink.yaml <<EOF
/**:
  ros__parameters:
    subscriptions_yaml: |
      - topic_name: /${ROBOT}/gps/fix
        msg_type: sensor_msgs/msg/NavSatFix
    zenoh.mode: "peer"
    zenoh.connect: ["${CLOUD}"]
    gis.liveliness_key: "gis/live/${ROBOT}"
    metrics.enabled: false
EOF

ros2 run zenoh_sink zenoh_sink_node_exe \
  --ros-args --params-file /tmp/sink.yaml -r __node:=zenoh_sink &
SPID=$!
sleep 8   # allow the peer to connect to the cloud hub before activating
ros2 lifecycle set /zenoh_sink configure
ros2 lifecycle set /zenoh_sink activate
echo "[edge ${ROBOT}] zenoh_sink active, connected to ${CLOUD}"

# Publish synthetic NavSatFix (toggles inside/outside P12, header.stamp=now).
exec python3 /phase2/workload_pub.py "${ROBOT}" "${RATE_HZ}"
