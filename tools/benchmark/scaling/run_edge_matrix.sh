#!/usr/bin/env bash
# Edge-topology scalability matrix: each robot container runs its own
# private kafka_sink (or mosquitto_sink), so there is NO central sink.
# Loops over N × broker × reps; smoke gates at N=10 per broker.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

REPS=3
WARMUP_S=10
DURATION_S=60
ROBOTS_CSV="1,5,10,25,50"
BROKERS_CSV="kafka,mqtt"
MSG_TYPE="${MSG_TYPE:-multi}"

usage() {
    cat <<EOF
Usage: $0 [options]
  --reps <n>         Repetitions per cell (default: 3)
  --warmup <s>       Warmup seconds (default: 10)
  --duration <s>     Measurement seconds (default: 60)
  --robots <csv>     Robot counts (default: 1,5,10,25,50)
  --brokers <csv>    Brokers to test (default: kafka,mqtt)
  --help

Env required: BAG_PATH=/path/to/rosbag2_dir
Env optional: MSG_TYPE (default: multi)
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --reps)     REPS="$2";       shift 2 ;;
        --warmup)   WARMUP_S="$2";   shift 2 ;;
        --duration) DURATION_S="$2"; shift 2 ;;
        --robots)   ROBOTS_CSV="$2"; shift 2 ;;
        --brokers)  BROKERS_CSV="$2"; shift 2 ;;
        --help|-h)  usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 2 ;;
    esac
done

: "${BAG_PATH:?BAG_PATH env var required}"
if [[ ! -d "${BAG_PATH}" ]]; then
    echo "BAG_PATH (${BAG_PATH}) is not a directory" >&2
    exit 2
fi
export BAG_PATH MSG_TYPE

IFS=',' read -r -a ROBOTS <<< "${ROBOTS_CSV}"
IFS=',' read -r -a BROKERS <<< "${BROKERS_CSV}"

ABS_RESULTS="${SCRIPT_DIR}/results"
mkdir -p "${ABS_RESULTS}"

run_cell() {
    local broker="$1" n="$2" rep="$3"
    local cell_dir="${ABS_RESULTS}/N=${n}_topology=edge_broker=${broker}_run=${rep}"
    local robots_compose="/tmp/scaling_edge_robots_${broker}_${n}_${rep}.yml"

    echo "================================================="
    echo "  cell: topology=edge N=${n} broker=${broker} rep=${rep}"
    echo "================================================="

    mkdir -p "${cell_dir}"
    TOPOLOGY=edge SINK_KIND="${broker}" "${SCRIPT_DIR}/gen_robots_compose.sh" "${n}" "${robots_compose}"

    local COMPOSE_ARGS=(
        -f "${SCRIPT_DIR}/compose.${broker}.yml"
        -f "${robots_compose}"
    )

    # 1. Broker only.
    NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" up -d broker
    sleep 6

    # 2. Consumer without dependencies (else central sink is implicitly created).
    NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" up -d --no-deps consumer
    sleep 2

    # 3. Edge robots — each starts its own private sink.
    local robot_services=""
    for ((i=1;i<=n;i++)); do robot_services+="robot_${i} "; done
    NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" up -d --no-deps ${robot_services}

    # 4. Wait warmup + duration + per-robot bringup pad.
    local bringup_pad=$(( 10 + n / 3 ))
    sleep $(( WARMUP_S + DURATION_S + bringup_pad ))

    # 5. Stop consumer cleanly, tear down everything.
    NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" stop consumer 2>&1 | tail -3 || true
    NUM_ROBOTS="${n}" docker compose "${COMPOSE_ARGS[@]}" down -v --remove-orphans 2>&1 | tail -3
    rm -f "${robots_compose}"

    # 6. Move artifacts.
    if [[ -f "${ABS_RESULTS}/consumer.jsonl" ]]; then
        mv "${ABS_RESULTS}/consumer.jsonl" "${cell_dir}/consumer.jsonl"
    fi
    echo "  → ${cell_dir}"
}

# ── Smoke gate per broker at N=10 ──
echo "[smoke] Running edge N=10 gate per broker."
for broker in "${BROKERS[@]}"; do
    run_cell "${broker}" 10 "smoke"
    SMOKE_JSONL="${ABS_RESULTS}/N=10_topology=edge_broker=${broker}_run=smoke/consumer.jsonl"
    LINES=$(wc -l < "${SMOKE_JSONL}" 2>/dev/null || echo 0)
    # Expected: 10 robots × (10+20+50+12.5)Hz × DURATION_S = 925 msgs/s × 60s = 55500 (multi).
    # For single types, much less.  Require >= 30% of multi expectation in multi mode.
    if [[ "${MSG_TYPE}" == "multi" ]]; then
        MIN_EXPECTED=$(( 10 * 92 * DURATION_S * 30 / 100 ))
    else
        MIN_EXPECTED=$(( 10 * 10 * DURATION_S * 30 / 100 ))
    fi
    if (( LINES < MIN_EXPECTED )); then
        echo "[smoke] edge ${broker} gate FAILED: ${LINES} rows < ${MIN_EXPECTED}. ABORTING." >&2
        exit 1
    fi
    echo "[smoke] edge ${broker} gate OK (${LINES} rows)"
done

# ── Full matrix ──
for broker in "${BROKERS[@]}"; do
    for n in "${ROBOTS[@]}"; do
        for rep in $(seq 1 "${REPS}"); do
            run_cell "${broker}" "${n}" "${rep}"
        done
    done
done

echo "Edge matrix complete. Results under ${ABS_RESULTS}/."
