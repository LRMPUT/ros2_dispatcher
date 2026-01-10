#!/usr/bin/env bash
set -euo pipefail

image_name="ros2-mosquitto-broker"
container_name="ros2_mosquitto_broker"
host_port="${MOSQUITTO_PORT:-1884}"

docker build -t "${image_name}" "$(dirname "$0")"

if docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
  docker rm -f "${container_name}" >/dev/null
fi

if command -v lsof >/dev/null 2>&1; then
  if lsof -PiTCP:"${host_port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port ${host_port} is already in use. Set MOSQUITTO_PORT to another value or stop the existing service."
    exit 1
  fi
elif command -v ss >/dev/null 2>&1; then
  if ss -ltn "( sport = :${host_port} )" | grep -q ":${host_port}"; then
    echo "Port ${host_port} is already in use. Set MOSQUITTO_PORT to another value or stop the existing service."
    exit 1
  fi
fi

docker run -d --name "${container_name}" -p "${host_port}":1883 "${image_name}"

echo "Mosquitto broker running on localhost:${host_port} (container: ${container_name})."
