#!/usr/bin/env bash
# Phase 2a resilience demonstration: brings up the cloud<-edge Zenoh topology,
# collects the cloud-side received-position rate over a fault timeline
# (clean -> 50% loss -> partition -> recover) applied with tc netem, and prints
# the annotated time series. The rate dropping under loss/partition and
# returning after clear is the resilience result.
#
# Usage: run_resilience.sh            # default timeline, 1 edge
# Env: STEP_S (seconds/phase, default 12), RKD_GIS_IMAGE
set -eo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CF="${HERE}/docker-compose.phase2.yml"
OUT="${HERE}/results"; mkdir -p "${OUT}"
IMG="${RKD_GIS_IMAGE:-rkd:gis-netem}"
STEP="${STEP_S:-12}"

docker image inspect "${IMG}" >/dev/null 2>&1 || \
  docker build -f "${HERE}/Dockerfile.netem" -t "${IMG}" "${HERE}"

cleanup() { docker compose -f "${CF}" down >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "[run] up"; docker compose -f "${CF}" up -d >/dev/null 2>&1
echo "[run] warmup 22s (activate + peer connect)"; sleep 22
DUR=$((STEP * 4 + 4))
echo "[run] collecting ${DUR}s while driving netem timeline -> ${OUT}/timeline.csv"
docker compose -f "${CF}" exec -T cloud_query bash -lc \
  "source /opt/ros/humble/setup.bash && source /ws/install/setup.bash && python3 /phase2/metrics_collector.py 1 1 ${DUR}" \
  > "${OUT}/timeline.csv" 2>/dev/null &
COL=$!
sleep "${STEP}"                                                       # phase 1: clean
bash "${HERE}/netem.sh" "${CF}" edge_robot_1 loss_50   >/dev/null;  sleep "${STEP}"   # phase 2
bash "${HERE}/netem.sh" "${CF}" edge_robot_1 partition >/dev/null;  sleep "${STEP}"   # phase 3
bash "${HERE}/netem.sh" "${CF}" edge_robot_1 clear     >/dev/null;  sleep "${STEP}"   # phase 4: recover
wait "${COL}" || true

echo "=== timeline (recv rate vs fault phase) ==="
awk -F, -v s="${STEP}" '
  NR==1 { print $0",phase"; next }
  { p="clean"; if($1+0>=s) p="loss_50"; if($1+0>=2*s) p="partition"; if($1+0>=3*s) p="recover";
    print $0","p }' "${OUT}/timeline.csv"
