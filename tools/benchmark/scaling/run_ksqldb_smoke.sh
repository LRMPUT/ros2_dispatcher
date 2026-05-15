#!/usr/bin/env bash
# Smoke test for the ksqlDB paradigm-latency stack.
# Brings up: broker + sink + ksqlDB + bridge + consumer, plus N robot
# containers via gen_robots_compose.sh, then runs for a single cell.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

N="${N:-1}"
WARMUP_S="${WARMUP_S:-10}"
DURATION_S="${DURATION_S:-30}"
KSQLDB_BOOT_TIMEOUT="${KSQLDB_BOOT_TIMEOUT:-90}"

: "${BAG_PATH:?BAG_PATH env var required}"
export BAG_PATH

CELL_DIR="${SCRIPT_DIR}/results/N=${N}_broker=ksqldb_run=smoke"
mkdir -p "${CELL_DIR}"

ROBOTS_COMPOSE="/tmp/scaling_ksqldb_robots.yml"
"${SCRIPT_DIR}/gen_robots_compose.sh" "${N}" "${ROBOTS_COMPOSE}"

COMPOSE_CMD=(docker compose
  -f "${SCRIPT_DIR}/compose.kafka.yml"
  -f "${SCRIPT_DIR}/compose.ksqldb.yml"
  -f "${ROBOTS_COMPOSE}")

echo "===================================================="
echo "  ksqlDB smoke: N=${N}, duration=${DURATION_S}s"
echo "===================================================="

cleanup() {
  echo "[smoke] tearing down..."
  NUM_ROBOTS="${N}" "${COMPOSE_CMD[@]}" down -v --remove-orphans 2>&1 | tail -5 || true
  rm -f "${ROBOTS_COMPOSE}"
}
trap cleanup EXIT

# 1. broker + sink + ksqlDB + bridge + consumer (no robots yet)
NUM_ROBOTS="${N}" "${COMPOSE_CMD[@]}" up -d broker sink ksqldb-server ksqldb-init ksqldb-bridge consumer

# 2. Wait for ksqlDB to be ready and the init script to finish
echo "[smoke] waiting for ksqlDB readiness (up to ${KSQLDB_BOOT_TIMEOUT}s)..."
if ! timeout "${KSQLDB_BOOT_TIMEOUT}" bash -c '
  until curl -sf http://localhost:8088/info > /dev/null; do sleep 1; done
'; then
  echo "[smoke] ksqlDB never became reachable" >&2
  "${COMPOSE_CMD[@]}" logs ksqldb-server | tail -40 >&2
  exit 1
fi
echo "[smoke] ksqlDB ready."

# Give ksql-setup.sql a moment to land the stream definitions
sleep 5
echo "[smoke] streams after init:"
curl -sf -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql":"SHOW STREAMS;"}' | head -c 400
echo

# 3. Bring up the robot fleet
NUM_ROBOTS="${N}" "${COMPOSE_CMD[@]}" up -d

# 4. Wait warmup + duration + small bringup pad
sleep $((WARMUP_S + DURATION_S + 8 + N / 5))

# 5. Stop the consumer cleanly so its output buffer flushes (cleanup trap handles the rest)
NUM_ROBOTS="${N}" "${COMPOSE_CMD[@]}" stop consumer 2>&1 | tail -3

# 6. Move artifacts
if [[ -f "${SCRIPT_DIR}/results/consumer.jsonl" ]]; then
    mv "${SCRIPT_DIR}/results/consumer.jsonl" "${CELL_DIR}/consumer.jsonl"
    LINES=$(wc -l < "${CELL_DIR}/consumer.jsonl")
    echo "[smoke] consumer.jsonl: ${LINES} lines"
    [[ ${LINES} -gt 0 ]] && head -1 "${CELL_DIR}/consumer.jsonl"
else
    echo "[smoke] WARNING: consumer.jsonl missing"
fi
if [[ -f "${SCRIPT_DIR}/results/sink_metrics.csv" ]]; then
    mv "${SCRIPT_DIR}/results/sink_metrics.csv" "${CELL_DIR}/sink_metrics.csv"
fi

echo "[smoke] artifacts → ${CELL_DIR}"
