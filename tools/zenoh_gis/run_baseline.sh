#!/usr/bin/env bash
# Baseline benchmark for the Zenoh-native continuous-geofence path.
#
# Drives N synthetic robots (NavSatFix toggling across the P12 boundary) through
# N zenoh_sink instances + one zenoh_gis_query node, captures the resulting
# gis/alert/geofence/** crossing alerts over Zenoh, and records per-fleet-size
# alert latency / throughput / drop to CSV — for head-to-head comparison against
# the SIGSPATIAL Kafka/MQTT scaling tables (see README.md for the column map).
#
# Data source: a deterministic SYNTHETIC NavSatFix publisher (no rosbag needed).
# Production runs should replay the INRAE `rorbots_*.bag` after ROS1->ROS2
# conversion (see README.md) — the synthetic source keeps the dry-run hermetic.
#
# Usage:
#   tools/zenoh_gis/run_baseline.sh [N ...]      # default sweep: 1 5 10 25 50
#   DURATION_S=20 RATE_HZ=10 tools/zenoh_gis/run_baseline.sh 1
#
# Env:
#   RKD_GIS_IMAGE  runtime image tag (default rkd:gis; built if missing)
#   DURATION_S     measurement window per N (default 15)
#   RATE_HZ        publish rate per robot (default 10)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUT_DIR="${SCRIPT_DIR}/results"
IMAGE="${RKD_GIS_IMAGE:-rkd:gis}"
DURATION_S="${DURATION_S:-15}"
RATE_HZ="${RATE_HZ:-10}"
NS=("${@:-1 5 10 25 50}")
# shellcheck disable=SC2206
NS=(${NS[*]})

mkdir -p "${OUT_DIR}"

# Build the runtime image if it is not already present.
if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  echo "[run_baseline] building ${IMAGE} ..."
  docker build -f "${REPO_DIR}/docker/Dockerfile" --build-arg ROS_DISTRO=humble -t "${IMAGE}" "${REPO_DIR}"
fi

# The in-container benchmark for a single fleet size. Prints one CSV data row:
#   n_robots,alerts,alerts_per_sec,latency_avg_ms,latency_p95_ms,latency_p99_ms,drop
read -r -d '' INNER <<'INNER_EOF' || true
set -euo pipefail
source /opt/ros/humble/setup.bash && source /ws/install/setup.bash
N="${BENCH_N}"; DUR="${BENCH_DURATION}"; RATE="${BENCH_RATE}"
QF=$(find /ws/install -name zenoh_gis_query.param.yaml | head -1)

# 1) query node (peer)
ros2 run zenoh_gis_query zenoh_gis_query_node_exe --ros-args --params-file "${QF}" \
  -r __node:=zenoh_gis_query >/tmp/q.log 2>&1 &
# 2) N sinks, one per robot, each with a liveliness token
for k in $(seq 1 "${N}"); do
  cat > "/tmp/sink_${k}.yaml" <<EOF
/**:
  ros__parameters:
    subscriptions_yaml: |
      - topic_name: /robot_${k}/gps/fix
        msg_type: sensor_msgs/msg/NavSatFix
    zenoh.mode: "peer"
    gis.liveliness_key: "gis/live/robot_${k}"
    metrics.enabled: false
EOF
  ros2 run zenoh_sink zenoh_sink_node_exe --ros-args --params-file "/tmp/sink_${k}.yaml" \
    -r __node:="zenoh_sink_${k}" >/tmp/s_${k}.log 2>&1 &
done
sleep 6
ros2 lifecycle set /zenoh_gis_query configure >/dev/null && ros2 lifecycle set /zenoh_gis_query activate >/dev/null
for k in $(seq 1 "${N}"); do
  ros2 lifecycle set "/zenoh_sink_${k}" configure >/dev/null && ros2 lifecycle set "/zenoh_sink_${k}" activate >/dev/null
done
sleep 2

# 3) Zenoh subscriber: capture gis/alert/geofence/** and record (recv_ns - stamp_ns).
python3 - "${DUR}" > /tmp/lat.txt 2>/tmp/sub.log <<'PY' &
import sys, time, json, zenoh
dur = float(sys.argv[1]) + 4.0
lats = []
def on_alert(sample):
    try:
        payload = bytes(sample.payload) if hasattr(sample, "payload") else sample.value.payload
        ev = json.loads(payload.decode())
        st = int(ev.get("stamp_ns", 0))
        if st > 0:
            lats.append(time.time_ns() - st)
    except Exception:
        pass
cfg = zenoh.Config()
s = zenoh.open(cfg)
sub = s.declare_subscriber("gis/alert/geofence/**", on_alert)
time.sleep(dur)
try: sub.undeclare()
except Exception: pass
s.close()
for v in lats:
    print(v)
PY
SUB_PID=$!
sleep 2

# 4) Synthetic publishers: each robot toggles inside<->outside P12 (lat 48.05 vs
#    47.9, lon 3.05) at RATE Hz, header.stamp = now. Each toggle is one crossing.
python3 - "${N}" "${DUR}" "${RATE}" >/tmp/pub.log 2>&1 <<'PY'
import sys, time, rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix
N = int(sys.argv[1]); dur = float(sys.argv[2]); rate = float(sys.argv[3])
rclpy.init()
node = Node("bench_pub")
pubs = [node.create_publisher(NavSatFix, f"/robot_{k}/gps/fix", 10) for k in range(1, N + 1)]
period = 1.0 / rate
inside = (48.05, 3.05); outside = (47.9, 3.05)
crossings = 0
t_end = time.time() + dur
state = False
while time.time() < t_end:
    state = not state
    lat, lon = (inside if state else outside)
    now = node.get_clock().now().to_msg()
    for p in pubs:
        m = NavSatFix()
        m.header.frame_id = "bench"
        m.header.stamp = now
        m.latitude = lat; m.longitude = lon
        p.publish(m)
    crossings += N      # each published toggle is a boundary crossing per robot
    time.sleep(period)
print(f"CROSSINGS {crossings}")
node.destroy_node(); rclpy.shutdown()
PY
GEN=$(grep -oE 'CROSSINGS [0-9]+' /tmp/pub.log | awk '{print $2}'); GEN="${GEN:-0}"

wait "${SUB_PID}" 2>/dev/null || true

# 5) Compute stats from /tmp/lat.txt (one latency_ns per line).
awk -v n="${N}" -v dur="${DUR}" -v gen="${GEN}" '
  { v[NR]=$1; sum+=$1 } END {
    c=NR;
    if (c==0) { printf "%d,0,0,0,0,0,%d\n", n, gen; exit }
    # sort
    for (i=1;i<=c;i++) for (j=i+1;j<=c;j++) if (v[j]<v[i]){t=v[i];v[i]=v[j];v[j]=t}
    avg=sum/c/1e6;
    p95=v[int(0.95*(c-1))+1]/1e6; p99=v[int(0.99*(c-1))+1]/1e6;
    rate=c/dur;
    drop=gen-c; if (drop<0) drop=0;
    printf "%d,%d,%.2f,%.3f,%.3f,%.3f,%d\n", n, c, rate, avg, p95, p99, drop
  }' /tmp/lat.txt
INNER_EOF

HEADER="n_robots,alerts,alerts_per_sec,latency_avg_ms,latency_p95_ms,latency_p99_ms,drop"
for N in "${NS[@]}"; do
  echo "[run_baseline] N=${N} (duration=${DURATION_S}s rate=${RATE_HZ}Hz) ..."
  CSV="${OUT_DIR}/zenoh_baseline_N${N}.csv"
  echo "${HEADER}" > "${CSV}"
  ROW="$(docker run --rm \
      -e BENCH_N="${N}" -e BENCH_DURATION="${DURATION_S}" -e BENCH_RATE="${RATE_HZ}" \
      "${IMAGE}" bash -lc "${INNER}" | tail -1)"
  echo "${ROW}" | tee -a "${CSV}"
done

echo "[run_baseline] done. CSVs in ${OUT_DIR}/"
