#!/usr/bin/env bash
# Apply / clear `tc netem` on a compose service's eth0 to emulate WAN faults on
# the edge<->cloud path. Requires the rkd:gis-netem image (iproute2) + NET_ADMIN.
#
# Usage: netem.sh <compose-file> <service> <clear|loss_<pct>|delay_<ms>|partition>
set -eo pipefail
CF="$1"; SVC="$2"; ACT="$3"
tcx() { docker compose -f "$CF" exec -T "$SVC" tc "$@"; }
case "$ACT" in
  clear)     tcx qdisc del dev eth0 root 2>/dev/null || true ;;
  partition) tcx qdisc replace dev eth0 root netem loss 100% ;;
  loss_*)    tcx qdisc replace dev eth0 root netem loss "${ACT#loss_}%" ;;
  delay_*)   tcx qdisc replace dev eth0 root netem delay "${ACT#delay_}ms" ;;
  *) echo "netem.sh: unknown action '$ACT'" >&2; exit 2 ;;
esac
echo "[netem] ${SVC} <- ${ACT}"
