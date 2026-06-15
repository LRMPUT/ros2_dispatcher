#!/usr/bin/env bash
# Phase 2a: Kafka -> ksqlDB scalability matrix.
# For each cell:
#   - bring up broker + sink + ksqldb-server + ksqldb-init + ksqldb-bridge + consumer
#   - wait for ksqlDB streams to be ready
#   - bring up N robot containers (per-container fleet via gen_robots_compose.sh)
#   - let it run for WARMUP + DURATION + pad seconds
#   - tear down
#   - move consumer.jsonl + sink_metrics.csv into cell directory
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

REPS=3
WARMUP_S=10
DURATION_S=60
ROBOTS_CSV="1,5,10,25,50"
KSQLDB_BOOT_TIMEOUT=120

usage() {
    cat <<EOF
Usage: $0 [options]
  --reps <n>         Repetitions per cell (default: 3)
  --warmup <s>       Warmup seconds (default: 10)
  --duration <s>     Measurement seconds (default: 60)
  --robots <csv>     Robot counts (default: 1,5,10,25,50)
  --help

Env required: BAG_PATH=/path/to/rosbag2_dir
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --reps)     REPS="$2";       shift 2 ;;
        --warmup)   WARMUP_S="$2";   shift 2 ;;
        --duration) DURATION_S="$2"; shift 2 ;;
        --robots)   ROBOTS_CSV="$2"; shift 2 ;;
        --help|-h)  usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 2 ;;
    esac
done

: "${BAG_PATH:?BAG_PATH env var required}"
if [[ ! -d "${BAG_PATH}" ]]; then
    echo "BAG_PATH (${BAG_PATH}) is not a directory" >&2
    exit 2
fi
export BAG_PATH

IFS=',' read -r -a ROBOTS <<< "${ROBOTS_CSV}"

ABS_RESULTS="${SCRIPT_DIR}/results"
mkdir -p "${ABS_RESULTS}"

run_cell() {
    local n="$1" rep="$2"
    local cell_dir="${ABS_RESULTS}/N=${n}_broker=ksqldb_run=${rep}"
    local robots_compose="/tmp/scaling_ksqldb_robots_${n}_${rep}.yml"

    echo "================================================="
    echo "  cell: N=${n} broker=ksqldb rep=${rep}"
    echo "================================================="

    mkdir -p "${cell_dir}"
    "${SCRIPT_DIR}/gen_robots_compose.sh" "${n}" "${robots_compose}"

    local COMPOSE_ARGS=(
        -f "${SCRIPT_DIR}/compose.kafka.yml"
        -f "${SCRIPT_DIR}/compose.ksqldb.yml"
        -f "${robots_compose}"
    )

    # 1. Bring up everything except robots
    NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" \
        up -d broker sink ksqldb-server ksqldb-init ksqldb-bridge consumer

    # 2. Wait for ksqlDB streams to be created by ksqldb-init
    echo "[matrix] waiting for ksqlDB streams (up to ${KSQLDB_BOOT_TIMEOUT}s)..."
    local i=0
    until curl -sf -X POST http://localhost:8088/ksql \
        -H "Content-Type: application/vnd.ksql.v1+json" \
        -d '{"ksql":"SHOW STREAMS;"}' 2>/dev/null \
        | grep -q "ROS_GPS_FIX_STREAM"; do
        i=$((i+1))
        if (( i > KSQLDB_BOOT_TIMEOUT )); then
            echo "[matrix] ksqlDB streams never appeared after ${KSQLDB_BOOT_TIMEOUT}s" >&2
            NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" logs ksqldb-server ksqldb-init | tail -50 >&2
            NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" down -v --remove-orphans
            rm -f "${robots_compose}"
            exit 1
        fi
        sleep 1
    done
    echo "[matrix] ksqlDB streams ready after ${i}s"

    # 3. Bring up the robot fleet
    NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" up -d

    # 4. Wait warmup + duration + pad
    local bringup_pad=$(( 5 + n / 5 ))
    sleep $(( WARMUP_S + DURATION_S + bringup_pad ))

    # 5. Stop the consumer cleanly so its buffer flushes, then tear down
    NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" stop consumer 2>&1 | tail -3 || true
    NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" down -v --remove-orphans 2>&1 | tail -3
    rm -f "${robots_compose}"

    # 6. Move artifacts into cell directory
    if [[ -f "${ABS_RESULTS}/consumer.jsonl" ]]; then
        mv "${ABS_RESULTS}/consumer.jsonl" "${cell_dir}/consumer.jsonl"
    fi
    if [[ -f "${ABS_RESULTS}/sink_metrics.csv" ]]; then
        mv "${ABS_RESULTS}/sink_metrics.csv" "${cell_dir}/sink_metrics.csv"
    fi
    echo "  -> ${cell_dir}"
}

# ── Smoke gate at N=10 ──
echo "[smoke] Running N=10 ksqlDB gate."
SMOKE_REP="smoke"
run_cell 10 "${SMOKE_REP}"
SMOKE_JSONL="${ABS_RESULTS}/N=10_broker=ksqldb_run=${SMOKE_REP}/consumer.jsonl"
LINES=$(wc -l < "${SMOKE_JSONL}" 2>/dev/null || echo 0)
# At N=10 × 10 Hz × 60 s = 6000, with ksqlDB overhead and warmup we expect
# at least ~30% to make it through.
MIN_EXPECTED=$(( 10 * 10 * DURATION_S * 30 / 100 ))
if (( LINES < MIN_EXPECTED )); then
    echo "[smoke] ksqlDB gate FAILED: got ${LINES} rows, expected >=${MIN_EXPECTED}. ABORTING." >&2
    exit 1
fi
echo "[smoke] ksqlDB gate OK (${LINES} rows)"

# ── Full matrix ──
for n in "${ROBOTS[@]}"; do
    for rep in $(seq 1 "${REPS}"); do
        run_cell "${n}" "${rep}"
    done
done

echo "Matrix complete. Results under ${ABS_RESULTS}/."
