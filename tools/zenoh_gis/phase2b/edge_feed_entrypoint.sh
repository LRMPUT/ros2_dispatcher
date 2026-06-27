#!/usr/bin/env bash
# Edge feed: a zenoh_sink (JSON payload so stored values are readable) connecting
# as a Zenoh CLIENT to the router, plus the synthetic NavSatFix publisher.
set -eo pipefail
source /opt/ros/humble/setup.bash
source /ws/install/setup.bash
ROBOT="robot_${ROBOT_ID:-1}"
cat > /tmp/sink.yaml <<YAML
/**:
  ros__parameters:
    subscriptions_yaml: |
      - topic_name: /${ROBOT}/gps/fix
        msg_type: sensor_msgs/msg/NavSatFix
    zenoh.mode: "client"
    zenoh.connect: ["tcp/zenoh_router:7447"]
    zenoh.payload_format: "json"
    metrics.enabled: false
YAML
ros2 run zenoh_sink zenoh_sink_node_exe --ros-args --params-file /tmp/sink.yaml -r __node:=zenoh_sink &
sleep 8
ros2 lifecycle set /zenoh_sink configure
ros2 lifecycle set /zenoh_sink activate
echo "[edge_feed] zenoh_sink(json) active -> router"
exec python3 /zg/phase2/workload_pub.py "${ROBOT}" "${RATE_HZ:-2}"
