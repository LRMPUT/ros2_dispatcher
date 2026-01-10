#!/usr/bin/env bash
set -euo pipefail

# watch_topics.sh
# Usage:
#   ./watch_topics.sh [HOST] [PORT] [TOPIC] [TIMEOUT]
# Examples:
#   ./watch_topics.sh                 # localhost, MOSQUITTO_PORT or 1884, topic '#', no timeout
#   ./watch_topics.sh localhost 1884  # same as above
#   ./watch_topics.sh localhost 1883 '#' 10s   # run for 10 seconds then exit

HOST="${1:-localhost}"
PORT="${2:-${MOSQUITTO_PORT:-1884}}"
TOPIC="${3:-#}"
TIMEOUT_ARG="${4:-}"

if ! command -v mosquitto_sub >/dev/null 2>&1; then
  echo "Error: mosquitto_sub not found. Install mosquitto-clients (e.g. apt install mosquitto-clients)."
  exit 2
fi

CMD=(mosquitto_sub -h "${HOST}" -p "${PORT}" -t "${TOPIC}" -v)

if [ -n "${TIMEOUT_ARG}" ]; then
  if command -v timeout >/dev/null 2>&1; then
    exec timeout "${TIMEOUT_ARG}" "${CMD[@]}"
  else
    echo "Warning: 'timeout' command not available; running without timeout."
  fi
fi

exec "${CMD[@]}"
