#!/usr/bin/env bash
# Builds and runs kafka_sink + gis_health_node + mock_leader_publisher inside the
# ros2-kafka-benchmark:run image, on host network, talking to the GIS4IoRT-ksqlDB
# Kafka broker exposed at localhost:9092.
#
# Run from the repo root:
#   ./tools/e2e_gis/run_dispatcher_in_docker.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="ros2-kafka-benchmark:run"
CONTAINER="gis_e2e_dispatcher"

# Stop any leftover from previous runs
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true

docker run -d --rm \
  --name "${CONTAINER}" \
  --network host \
  -v "${REPO_ROOT}:/work:ro" \
  -v "/tmp/${CONTAINER}_ws:/ws" \
  -e ROS_DOMAIN_ID=42 \
  "${IMAGE}" \
  bash -c 'tail -f /dev/null'

docker exec "${CONTAINER}" bash -c '
set -e
mkdir -p /ws/src
[ -e /ws/src/ros2_kafka_dispatcher ] || ln -s /work /ws/src/ros2_kafka_dispatcher
cd /ws
source /opt/ros/humble/setup.bash
colcon build --packages-select kafka_client kafka_sink gis_health_node --cmake-args -DCMAKE_BUILD_TYPE=Release 2>&1 | tail -20
'

cat <<EOF
[run_dispatcher_in_docker] container '${CONTAINER}' is up with workspace at /ws.
Use:
  docker exec -i ${CONTAINER} bash -lc "source /opt/ros/humble/setup.bash && source /ws/install/setup.bash && <CMD>"
to run ros2 commands inside it.
EOF
