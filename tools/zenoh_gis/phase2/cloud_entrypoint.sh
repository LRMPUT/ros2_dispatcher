#!/usr/bin/env bash
# Cloud tier consumer. Runs zenoh_source as a Zenoh peer that LISTENS on
# tcp/0.0.0.0:7447 (edge peers connect to it), receives the edge position
# stream over the (netem-emulated) WAN, and republishes ros2/** -> /zenoh_decoded/**
# as ROS topics so delivery/latency/recovery can be measured with plain ROS tools
# (no external Zenoh client needed — the image ships no python-zenoh/pip).
set -eo pipefail
source /opt/ros/humble/setup.bash
source /ws/install/setup.bash

cat > /tmp/cloud.yaml <<'EOF'
/**:
  ros__parameters:
    zenoh.mode: "peer"
    zenoh.listen: ["tcp/0.0.0.0:7447"]
    zenoh.key_expr: "ros2/**"
    zenoh.key_prefix: "ros2"
    ros_topic_prefix: "/zenoh_decoded"
    zenoh.allowed_types: ["sensor_msgs/msg/NavSatFix"]
    metrics.enabled: false
EOF

ros2 run zenoh_source zenoh_source_node_exe \
  --ros-args --params-file /tmp/cloud.yaml -r __node:=zenoh_source &
SPID=$!
sleep 6
ros2 lifecycle set /zenoh_source configure
ros2 lifecycle set /zenoh_source activate
echo "[cloud] zenoh_source active, listening tcp/0.0.0.0:7447, ros2/** -> /zenoh_decoded/**"
wait "${SPID}"
