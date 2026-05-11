# Robot replay container — one per simulated robot.
ARG ROS_DISTRO=humble
FROM ros:${ROS_DISTRO}-ros-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    ros-${ROS_DISTRO}-rosbag2-py \
    ros-${ROS_DISTRO}-rosbag2-storage-default-plugins \
    ros-${ROS_DISTRO}-rosbag2-storage-mcap \
    ros-${ROS_DISTRO}-sensor-msgs \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY robot_replay.py /app/

ENV PYTHONUNBUFFERED=1
ENV ROS_DOMAIN_ID=42
ENV RATE_HZ=10

ENTRYPOINT ["bash", "-lc", "source /opt/ros/${ROS_DISTRO}/setup.bash && exec python3 /app/robot_replay.py"]
