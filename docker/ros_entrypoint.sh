#!/bin/bash
set -e

if [ -f "/opt/ros/${ROS_DISTRO}/setup.bash" ]; then
  # shellcheck source=/dev/null
  source "/opt/ros/${ROS_DISTRO}/setup.bash"
fi

if [ -f "/ws/install/setup.bash" ]; then
  # shellcheck source=/dev/null
  source "/ws/install/setup.bash"
fi

exec "$@"
