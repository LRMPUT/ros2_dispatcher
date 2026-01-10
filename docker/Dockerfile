# syntax=docker/dockerfile:1.5
ARG ROS_DISTRO=humble
ARG IMAGE_SOURCE="https://github.com/ros2-kafka/ros2_kafka_dispatcher"

FROM ros:${ROS_DISTRO}-ros-base AS builder
ARG ROS_DISTRO
ARG BUILD_DATE
ARG GIT_SHA
ARG IMAGE_SOURCE

LABEL org.opencontainers.image.created=${BUILD_DATE} \
      org.opencontainers.image.revision=${GIT_SHA} \
      org.opencontainers.image.source=${IMAGE_SOURCE}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      build-essential \
      cmake \
      git \
      python3-colcon-common-extensions \
      python3-rosdep \
      python3-vcstool \
    && rm -rf /var/lib/apt/lists/*

RUN rosdep init || true \
    && rosdep update

WORKDIR /ws
COPY . /ws/src

RUN . /opt/ros/${ROS_DISTRO}/setup.sh \
    && rosdep install --from-paths src --ignore-src -y --rosdistro ${ROS_DISTRO}

RUN . /opt/ros/${ROS_DISTRO}/setup.sh \
    && colcon build --merge-install --cmake-args -DCMAKE_BUILD_TYPE=Release

FROM ros:${ROS_DISTRO}-ros-base AS runtime
ARG ROS_DISTRO
ARG BUILD_DATE
ARG GIT_SHA
ARG IMAGE_SOURCE

LABEL org.opencontainers.image.created=${BUILD_DATE} \
      org.opencontainers.image.revision=${GIT_SHA} \
      org.opencontainers.image.source=${IMAGE_SOURCE}

ENV ROS_DISTRO=${ROS_DISTRO}

RUN useradd -m -u 10001 ros \
    && mkdir -p /ws \
    && chown -R ros:ros /ws

COPY --from=builder /ws/install /ws/install
COPY docker/ros_entrypoint.sh /ros_entrypoint.sh

RUN chmod +x /ros_entrypoint.sh

USER ros
WORKDIR /ws

ENTRYPOINT ["/ros_entrypoint.sh"]
CMD ["bash"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["bash","-lc","source /opt/ros/${ROS_DISTRO}/setup.bash && source /ws/install/setup.bash && ros2 pkg list >/dev/null"]
