#!/usr/bin/env bash
# Orchestrator for the scalability matrix.
# Loops over N × broker × reps; each cell brings the stack up, waits
# warmup+duration, tears down, and moves artifacts to results/.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# ── Defaults ──
REPS=3
WARMUP_S=10
DURATION_S=60
ROBOTS_CSV="1,5,10,25,50"
BROKERS_CSV="kafka,mqtt"

usage() {
    cat <<EOF
Usage: $0 [options]
  --reps <n>         Repetitions per cell (default: 3)
  --warmup <s>       Warmup seconds (default: 10)
  --duration <s>     Measurement seconds (default: 60)
  --robots <csv>     Robot counts to test (default: 1,5,10,25,50)
  --brokers <csv>    Brokers to test (default: kafka,mqtt)
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
export BAG_PATH

IFS=',' read -r -a ROBOTS <<< "${ROBOTS_CSV}"
IFS=',' read -r -a BROKERS <<< "${BROKERS_CSV}"

ABS_RESULTS="${SCRIPT_DIR}/results"
mkdir -p "${ABS_RESULTS}"

run_cell() {
    local broker="$1" n="$2" rep="$3"
    local compose="compose.${broker}.yml"
    local cell_dir="${ABS_RESULTS}/N=${n}_broker=${broker}_run=${rep}"

    echo "==============================================="
    echo "  cell: N=${n} broker=${broker} rep=${rep}"
    echo "==============================================="

    mkdir -p "${cell_dir}"

    # 1. Bring up broker + sink first (no robots yet)
    NUM_ROBOTS="${n}" docker compose -f "${compose}" up -d broker sink consumer
    # 2. Give the sink time to reach ACTIVE
    sleep 8
    # 3. Scale robots up
    NUM_ROBOTS="${n}" docker compose -f "${compose}" up -d --scale robot="${n}" robot
    # 4. Wait warmup + duration (the consumer enforces its own window
    #    independently; we sleep slightly longer to ensure clean exit)
    sleep $(( WARMUP_S + DURATION_S + 5 ))
    # 5. Tear down
    NUM_ROBOTS="${n}" docker compose -f "${compose}" down -v --remove-orphans

    # 6. Move artifacts
    if [[ -f "${ABS_RESULTS}/consumer.jsonl" ]]; then
        mv "${ABS_RESULTS}/consumer.jsonl" "${cell_dir}/consumer.jsonl"
    fi
    if [[ -f "${ABS_RESULTS}/sink_metrics.csv" ]]; then
        mv "${ABS_RESULTS}/sink_metrics.csv" "${cell_dir}/sink_metrics.csv"
    fi

    echo "  → ${cell_dir}"
}

# ── Smoke gate (N=10, single rep) before the full matrix ──
echo "[smoke] Running N=10 on each broker as a smoke gate."
for broker in "${BROKERS[@]}"; do
    run_cell "${broker}" 10 "smoke"
    LINES=$(wc -l < "${ABS_RESULTS}/N=10_broker=${broker}_run=smoke/consumer.jsonl" 2>/dev/null || echo 0)
    EXPECTED=$(( 10 * 10 * DURATION_S ))
    # Require ≥80 % of expected messages
    if (( LINES * 10 < EXPECTED * 8 )); then
        echo "[smoke] broker=${broker}: got ${LINES} rows, expected ~${EXPECTED}. ABORTING." >&2
        exit 1
    fi
    echo "[smoke] broker=${broker}: OK (${LINES} rows)"
done

# ── Full matrix ──
for broker in "${BROKERS[@]}"; do
    for n in "${ROBOTS[@]}"; do
        for rep in $(seq 1 "${REPS}"); do
            run_cell "${broker}" "${n}" "${rep}"
        done
    done
done

echo "Matrix complete. Results under ${ABS_RESULTS}/."
