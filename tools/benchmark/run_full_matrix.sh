#!/usr/bin/env bash
#
# Run the full CDR vs JSON benchmark matrix.
#
# Test matrix:
#   3 message types  x  2 formats  x  4 rates  x  3 repetitions = 72 runs
#
# Usage:
#   ./run_full_matrix.sh                    # run all 72
#   ./run_full_matrix.sh --reps 1           # quick pass (24 runs)
#   ./run_full_matrix.sh --duration 30      # shorter runs
#
# Prerequisites:
#   - Kafka broker running (docker compose up -d)
#   - Docker image ros2-kafka-benchmark:run built

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DURATION=60
WARMUP=5
REPS=3
NUM_POINTS=10000

while [[ $# -gt 0 ]]; do
    case $1 in
        --duration)     DURATION="$2";    shift 2 ;;
        --warmup)       WARMUP="$2";      shift 2 ;;
        --reps)         REPS="$2";        shift 2 ;;
        --num-points)   NUM_POINTS="$2";  shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--duration 60] [--warmup 5] [--reps 3] [--num-points 10000]"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

MSG_TYPES=(navsatfix odometry pointcloud2)
FORMATS=(cdr json)
RATES=(10 50 100 500)

TOTAL=$((${#MSG_TYPES[@]} * ${#FORMATS[@]} * ${#RATES[@]} * REPS))
RUN=0

echo "============================================="
echo "  Full Benchmark Matrix"
echo "============================================="
echo "  Message types : ${MSG_TYPES[*]}"
echo "  Formats       : ${FORMATS[*]}"
echo "  Rates         : ${RATES[*]} Hz"
echo "  Repetitions   : ${REPS}"
echo "  Duration      : ${DURATION}s + ${WARMUP}s warmup"
echo "  Total runs    : ${TOTAL}"
echo "  Est. time     : ~$((TOTAL * (DURATION + WARMUP + 15) / 60)) min"
echo "============================================="
echo ""

START_TIME=$(date +%s)

for msg_type in "${MSG_TYPES[@]}"; do
    for format in "${FORMATS[@]}"; do
        for rate in "${RATES[@]}"; do
            for rep in $(seq 1 "$REPS"); do
                RUN=$((RUN + 1))
                echo ""
                echo ">>> Run ${RUN}/${TOTAL}: ${msg_type} ${format} ${rate}Hz rep${rep}"
                echo "    $(date '+%Y-%m-%d %H:%M:%S')"
                echo ""

                "${SCRIPT_DIR}/run_benchmark_docker.sh" \
                    --msg-type "$msg_type" \
                    --format "$format" \
                    --rate "$rate" \
                    --run-id "$rep" \
                    --duration "$DURATION" \
                    --warmup "$WARMUP" \
                    --num-points "$NUM_POINTS"

                # Brief pause between runs for cleanup
                sleep 3
            done
        done
    done
done

END_TIME=$(date +%s)
ELAPSED=$(( END_TIME - START_TIME ))

echo ""
echo "============================================="
echo "  Matrix complete!"
echo "  Total time: $((ELAPSED / 60))m $((ELAPSED % 60))s"
echo "  Results in: ${SCRIPT_DIR}/results/"
echo "  Total CSV files: $(ls ${SCRIPT_DIR}/results/*.csv 2>/dev/null | wc -l)"
echo "============================================="
