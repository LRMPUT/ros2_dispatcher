#!/usr/bin/env bash
# Smoke test for the edge topology: each robot container runs its own
# private kafka_sink alongside the publisher.  The central `sink` service
# in compose.kafka.yml is NOT brought up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

N="${N:-1}"
WARMUP_S="${WARMUP_S:-5}"
DURATION_S="${DURATION_S:-20}"
MSG_TYPE="${MSG_TYPE:-multi}"
SINK_KIND="${SINK_KIND:-kafka}"

: "${BAG_PATH:?BAG_PATH env var required}"
export BAG_PATH MSG_TYPE

CELL_DIR="${SCRIPT_DIR}/results/N=${N}_topology=edge_broker=${SINK_KIND}_run=smoke"
mkdir -p "${CELL_DIR}"

ROBOTS_COMPOSE="/tmp/scaling_edge_robots.yml"
TOPOLOGY=edge SINK_KIND="${SINK_KIND}" "${SCRIPT_DIR}/gen_robots_compose.sh" "${N}" "${ROBOTS_COMPOSE}"

COMPOSE_CMD=(docker compose
  -f "${SCRIPT_DIR}/compose.${SINK_KIND}.yml"
  -f "${ROBOTS_COMPOSE}")

echo "===================================================="
echo "  Edge smoke: N=${N}, SINK_KIND=${SINK_KIND}, MSG_TYPE=${MSG_TYPE}"
echo "===================================================="

cleanup() {
    echo "[edge-smoke] tearing down..."
    NUM_ROBOTS="${N}" "${COMPOSE_CMD[@]}" down -v --remove-orphans 2>&1 | tail -5 || true
    rm -f "${ROBOTS_COMPOSE}"
}
trap cleanup EXIT

# 1. Bring up broker first.
NUM_ROBOTS="${N}" "${COMPOSE_CMD[@]}" up -d broker
sleep 6

# 2. Bring up consumer WITHOUT its dependencies (the central `sink` service
#    would otherwise be implicitly started by depends_on).
NUM_ROBOTS="${N}" "${COMPOSE_CMD[@]}" up -d --no-deps consumer
sleep 2

# 3. Bring up the edge robots (each will boot its own sink inside).
#    --no-deps so the central sink is NOT implicitly brought up.
NUM_ROBOTS="${N}" "${COMPOSE_CMD[@]}" up -d --no-deps $(for ((i=1;i<=N;i++)); do echo -n "robot_${i} "; done)

# 3. Wait warmup + duration + per-robot startup pad.
bringup_pad=$(( 8 + N / 5 ))
sleep $(( WARMUP_S + DURATION_S + bringup_pad ))

# 4. Stop consumer cleanly so its buffer flushes.
NUM_ROBOTS="${N}" "${COMPOSE_CMD[@]}" stop consumer 2>&1 | tail -3 || true

# 5. Move artifacts.
if [[ -f "${SCRIPT_DIR}/results/consumer.jsonl" ]]; then
    mv "${SCRIPT_DIR}/results/consumer.jsonl" "${CELL_DIR}/consumer.jsonl"
    LINES=$(wc -l < "${CELL_DIR}/consumer.jsonl")
    echo "[edge-smoke] consumer.jsonl: ${LINES} lines"
    [[ ${LINES} -gt 0 ]] && head -1 "${CELL_DIR}/consumer.jsonl"
else
    echo "[edge-smoke] WARNING: consumer.jsonl missing"
fi

echo "[edge-smoke] artifacts → ${CELL_DIR}"
