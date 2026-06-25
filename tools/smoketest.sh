#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

: "${DISPATCHER_IMAGE:=ros2-kafka-dispatcher:local}"
: "${ROS_DISTRO:=humble}"

cleanup() {
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

export DISPATCHER_IMAGE ROS_DISTRO

docker compose up -d broker dispatcher

echo "[smoketest] waiting for dispatcher service..."
for _ in $(seq 1 30); do
  STATUS="$(
    docker compose exec -T dispatcher bash -lc \
      "source /opt/ros/${ROS_DISTRO}/setup.bash && \
       source /ws/install/setup.bash && \
       ros2 service list" \
      2>/dev/null || true
  )"
  if grep -q "^/dispatcher_controller/get_status$" <<<"${STATUS}"; then
    break
  fi
  sleep 2
done

if ! grep -q "^/dispatcher_controller/get_status$" <<<"${STATUS:-}"; then
  echo "[smoketest] dispatcher service never appeared" >&2
  docker compose logs dispatcher >&2 || true
  exit 1
fi

# get_status appears at controller construction, before the startup timer has
# configured/activated kafka_sink and created its subscription on /demo/number.
# Publish with -w 1 (wait for one matching subscription) instead of --once so the
# message cannot race DDS discovery and get dropped; bound the wait with timeout
# so a sink that never activates fails loudly instead of hanging.
docker compose exec -T dispatcher bash -lc \
  "source /opt/ros/${ROS_DISTRO}/setup.bash && \
   source /ws/install/setup.bash && \
   timeout 30 ros2 topic pub -w 1 --times 5 --rate 2 \
     /demo/number std_msgs/msg/Int32 '{data: 42}'"

CONSUMED="$(
  docker compose exec -T broker bash -lc \
    "kafka-console-consumer --bootstrap-server broker:29092 \
     --topic ros2.demo.number \
     --from-beginning \
     --timeout-ms 10000 \
     --max-messages 1" 2>/dev/null || true
)"

if ! grep -Eq '"data"[[:space:]]*:[[:space:]]*42' <<<"${CONSUMED}"; then
  echo "[smoketest] failed to observe expected Kafka payload" >&2
  echo "[smoketest] consumer output: ${CONSUMED}" >&2
  docker compose logs broker dispatcher >&2 || true
  exit 1
fi

echo "[smoketest] OK"
