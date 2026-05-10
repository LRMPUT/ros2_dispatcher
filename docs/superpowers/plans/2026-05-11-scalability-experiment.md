# Scalability Experiment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the SIGSPATIAL scalability experiment described in
`docs/superpowers/specs/2026-05-11-scalability-experiment-design.md`:
publish from 1, 5, 10, 25, 50 simulated robots in Docker containers
through a kafka_sink/mosquitto_sink → broker → external consumer
pipeline, measuring end-to-end latency, throughput, and drop rate.

**Architecture:** One Docker container per robot, replaying a user-supplied
NavSatFix bag with per-robot origin shift. ROS containers run with
`network_mode: host` and a fixed `ROS_DOMAIN_ID`; broker stays in a
bridge network with port forwarding. End-to-end latency is computed
from `NavSatFix.header.stamp` (the publisher encodes
`time.time_ns()` there) against `time.time_ns()` at the broker
consumer.

**Tech Stack:** Python 3.10 (rclpy, rosbag2_py, rosidl_runtime_py for
CDR deserialize), `confluent-kafka` for Kafka consumer, `paho-mqtt` for
MQTT consumer, Docker Compose v2 (`--scale` for replica count),
Bash for orchestration.

---

## Prerequisites (do once before starting)

1. **Bag file:** export the path to your single-robot NavSatFix bag,
   e.g. `export BAG_PATH=/data/my_gnss_bag`. The bag must contain at
   least one topic of type `sensor_msgs/msg/NavSatFix` at ≥1 Hz. The
   robot_replay container will loop it automatically.
2. **GPG signing:** the user's git config has `commit.gpgsign=true` but
   the gpg secret key is missing for the configured identity. Before
   running the commit steps in this plan, either install the secret
   key or run `git config commit.gpgsign false` on the working branch
   (and revert at the end). Do NOT bake `--no-gpg-sign` into the
   commits without permission.
3. **Workspace built:** `colcon build --symlink-install` and
   `source install/setup.bash` from the parent workspace.
4. **Branch:** `git checkout -b feature/scalability-experiment` off the
   current branch (`feature/cdr-vs-json-benchmark`).

---

## File Structure (everything under `tools/benchmark/scaling/`)

```
tools/benchmark/scaling/
├── Dockerfile.robot
├── Dockerfile.consumer
├── compose.kafka.yml
├── compose.mqtt.yml
├── robot_replay.py
├── sink_entrypoint.sh
├── e2e_consumer.py
├── run_scaling_matrix.sh
├── analyze_scaling.py
├── README.md
├── test_data/
│   └── generate_test_bag.py
├── tests/
│   ├── test_robot_replay.py
│   ├── test_e2e_consumer.py
│   └── test_analyze_scaling.py
└── results/
    └── .gitkeep
```

`tools/benchmark/scaling/` is self-contained. No existing files outside
this directory are modified except:

- `.gitignore` — add `tools/benchmark/scaling/results/*` (except `.gitkeep`).
- `README.md` (project root) — optional one-line pointer (not required).

---

## Task 1: Repo skeleton, gitignore, README

**Files:**
- Create: `tools/benchmark/scaling/README.md`
- Create: `tools/benchmark/scaling/results/.gitkeep`
- Modify: `.gitignore` (append one block)

- [ ] **Step 1: Create the scaling directory and placeholder result tree**

```bash
mkdir -p tools/benchmark/scaling/tests
mkdir -p tools/benchmark/scaling/test_data
mkdir -p tools/benchmark/scaling/results
touch tools/benchmark/scaling/results/.gitkeep
```

- [ ] **Step 2: Write the README**

Write `tools/benchmark/scaling/README.md`:

```markdown
# Scalability Experiment

Implements the spec at
`docs/superpowers/specs/2026-05-11-scalability-experiment-design.md`.

## Quick run

```bash
export BAG_PATH=/absolute/path/to/single_robot_navsatfix_bag
./tools/benchmark/scaling/run_scaling_matrix.sh --reps 1
./tools/benchmark/scaling/analyze_scaling.py \
    --input  tools/benchmark/scaling/results \
    --output tools/benchmark/scaling/results
```

## Smoke pass

```bash
./tools/benchmark/scaling/run_scaling_matrix.sh \
    --reps 1 --robots 1,10 --duration 30
```

## Inputs

- `BAG_PATH` env var: a directory containing a rosbag2 (`metadata.yaml`
  + `*.db3` or `*.mcap`) with at least one `sensor_msgs/msg/NavSatFix`
  topic. The replay loops it automatically.

## Outputs

- `results/N=<n>_broker=<b>_run=<r>/consumer.jsonl` — one line per
  delivered message: `{robot_id, topic, t0_ns, t1_ns, latency_ns, bytes}`.
- `results/N=<n>_broker=<b>_run=<r>/sink_metrics.csv` — per-second
  sink-side stats from `metrics_recorder.py`.
- `results/summary.csv` — aggregated per `(N, broker)`.
- `results/plots/*.png` — latency / throughput / drop rate vs N.
```

- [ ] **Step 3: Update `.gitignore`**

Append to `/home/maciej/Github/ros2_kafka_dispatcher/.gitignore`:

```
# Scalability experiment outputs
tools/benchmark/scaling/results/*
!tools/benchmark/scaling/results/.gitkeep
```

- [ ] **Step 4: Commit**

```bash
git add tools/benchmark/scaling/README.md \
        tools/benchmark/scaling/results/.gitkeep \
        .gitignore
git commit -m "scaling: scaffold tools/benchmark/scaling directory"
```

---

## Task 2: Test bag fixture generator

A tiny script that produces a 5-second NavSatFix bag for unit/integration tests.

**Files:**
- Create: `tools/benchmark/scaling/test_data/generate_test_bag.py`

- [ ] **Step 1: Write the script**

`tools/benchmark/scaling/test_data/generate_test_bag.py`:

```python
#!/usr/bin/env python3
"""Generate a tiny NavSatFix bag for tests.

Run once:
    cd tools/benchmark/scaling/test_data
    python3 generate_test_bag.py --output ./test_bag
"""
import argparse
import os
import shutil

import rclpy
from rclpy.serialization import serialize_message
from sensor_msgs.msg import NavSatFix, NavSatStatus
from std_msgs.msg import Header

import rosbag2_py


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, help="output bag directory")
    parser.add_argument("--rate-hz", type=float, default=10.0)
    parser.add_argument("--duration-s", type=float, default=5.0)
    parser.add_argument("--topic", default="/source/gnss")
    args = parser.parse_args()

    if os.path.exists(args.output):
        shutil.rmtree(args.output)

    storage_options = rosbag2_py.StorageOptions(uri=args.output, storage_id="sqlite3")
    converter_options = rosbag2_py.ConverterOptions("", "")
    writer = rosbag2_py.SequentialWriter()
    writer.open(storage_options, converter_options)
    writer.create_topic(
        rosbag2_py.TopicMetadata(
            name=args.topic,
            type="sensor_msgs/msg/NavSatFix",
            serialization_format="cdr",
        )
    )

    n_msgs = int(args.rate_hz * args.duration_s)
    period_ns = int(1e9 / args.rate_hz)
    t0_ns = 0
    for i in range(n_msgs):
        msg = NavSatFix()
        msg.header = Header()
        msg.header.frame_id = "gps_link"
        ts_ns = t0_ns + i * period_ns
        msg.header.stamp.sec = ts_ns // 1_000_000_000
        msg.header.stamp.nanosec = ts_ns % 1_000_000_000
        msg.status.status = NavSatStatus.STATUS_FIX
        msg.status.service = NavSatStatus.SERVICE_GPS
        msg.latitude = 51.1079 + 0.00001 * i
        msg.longitude = 17.0385 + 0.00001 * i
        msg.altitude = 120.0
        writer.write(args.topic, serialize_message(msg), ts_ns)

    del writer
    print(f"Wrote {n_msgs} messages to {args.output}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Generate the fixture bag**

```bash
cd tools/benchmark/scaling/test_data
python3 generate_test_bag.py --output ./test_bag
```

Expected output: `Wrote 50 messages to ./test_bag`. The directory
`./test_bag/` should contain `metadata.yaml` and a `.db3` file.

- [ ] **Step 3: Commit**

```bash
git add tools/benchmark/scaling/test_data/generate_test_bag.py
git commit -m "scaling: add test bag fixture generator"
```

Note: do not commit the generated `test_bag/` directory itself — add it
to gitignore if needed. The fixture is regenerable from the script.

- [ ] **Step 4: Add the generated bag dir to .gitignore**

Append to `.gitignore`:

```
tools/benchmark/scaling/test_data/test_bag/
```

Then `git add .gitignore && git commit -m "scaling: ignore generated test_bag"`.

---

## Task 3: `robot_replay.py` — pure shift/restamp helpers (TDD)

**Files:**
- Create: `tools/benchmark/scaling/robot_replay.py`
- Create: `tools/benchmark/scaling/tests/test_robot_replay.py`
- Create: `tools/benchmark/scaling/tests/__init__.py` (empty)

- [ ] **Step 1: Write failing tests for `shift_navsatfix` and `restamp_ns`**

`tools/benchmark/scaling/tests/test_robot_replay.py`:

```python
"""Unit tests for robot_replay helpers."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sensor_msgs.msg import NavSatFix

from robot_replay import shift_navsatfix, restamp_ns


def _make_fix(lat: float, lon: float, alt: float) -> NavSatFix:
    msg = NavSatFix()
    msg.latitude = lat
    msg.longitude = lon
    msg.altitude = alt
    return msg


def test_shift_navsatfix_robot_0_is_identity():
    msg = _make_fix(51.1, 17.0, 120.0)
    shift_navsatfix(msg, robot_id=0)
    assert msg.latitude == 51.1
    assert msg.longitude == 17.0
    assert msg.altitude == 120.0


def test_shift_navsatfix_offset_scales_with_robot_id():
    msg = _make_fix(51.1, 17.0, 120.0)
    shift_navsatfix(msg, robot_id=5)
    assert abs(msg.latitude - (51.1 + 0.0001 * 5)) < 1e-12
    assert abs(msg.longitude - (17.0 + 0.0001 * 5)) < 1e-12
    # altitude unchanged
    assert msg.altitude == 120.0


def test_restamp_ns_writes_sec_and_nanosec_correctly():
    msg = _make_fix(0.0, 0.0, 0.0)
    restamp_ns(msg, t_ns=1_234_567_890_123_456_789)
    assert msg.header.stamp.sec == 1_234_567_890
    assert msg.header.stamp.nanosec == 123_456_789


def test_restamp_ns_zero():
    msg = _make_fix(0.0, 0.0, 0.0)
    restamp_ns(msg, t_ns=0)
    assert msg.header.stamp.sec == 0
    assert msg.header.stamp.nanosec == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd tools/benchmark/scaling
python3 -m pytest tests/test_robot_replay.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'robot_replay'`.

- [ ] **Step 3: Write the minimal implementation**

`tools/benchmark/scaling/robot_replay.py`:

```python
#!/usr/bin/env python3
"""Replay a NavSatFix bag with per-robot origin shift and live re-stamping.

Container entrypoint: derive ROBOT_ID from the hostname suffix, open the
bag at $BAG_PATH, loop forever, publish at $RATE_HZ. The published
message's header.stamp encodes time.time_ns() at publish time, which
the e2e consumer subtracts to compute latency.
"""
from __future__ import annotations

from sensor_msgs.msg import NavSatFix

LAT_OFFSET_DEG_PER_ID = 0.0001  # ~11 m at the equator; close enough
LON_OFFSET_DEG_PER_ID = 0.0001


def shift_navsatfix(msg: NavSatFix, robot_id: int) -> None:
    """Apply a deterministic per-robot offset to lat/lon. In-place."""
    msg.latitude += LAT_OFFSET_DEG_PER_ID * robot_id
    msg.longitude += LON_OFFSET_DEG_PER_ID * robot_id


def restamp_ns(msg: NavSatFix, t_ns: int) -> None:
    """Set header.stamp to a wall-clock ns timestamp. In-place."""
    msg.header.stamp.sec = t_ns // 1_000_000_000
    msg.header.stamp.nanosec = t_ns % 1_000_000_000
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd tools/benchmark/scaling
python3 -m pytest tests/test_robot_replay.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add tools/benchmark/scaling/robot_replay.py \
        tools/benchmark/scaling/tests/test_robot_replay.py \
        tools/benchmark/scaling/tests/__init__.py
git commit -m "scaling: add robot_replay shift/restamp helpers with tests"
```

---

## Task 4: `robot_replay.py` — bag iterator with looping (TDD)

**Files:**
- Modify: `tools/benchmark/scaling/robot_replay.py` (add `BagLooper` class)
- Modify: `tools/benchmark/scaling/tests/test_robot_replay.py` (add tests)

- [ ] **Step 1: Add failing test for `BagLooper`**

Append to `tools/benchmark/scaling/tests/test_robot_replay.py`:

```python
import os

import pytest

from robot_replay import BagLooper


TEST_BAG = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "test_data",
    "test_bag",
)


@pytest.mark.skipif(not os.path.exists(TEST_BAG), reason="test bag not generated")
def test_bag_looper_yields_navsatfix_messages():
    looper = BagLooper(TEST_BAG, topic_type="sensor_msgs/msg/NavSatFix")
    msg = next(looper)
    assert hasattr(msg, "latitude")
    assert hasattr(msg, "longitude")


@pytest.mark.skipif(not os.path.exists(TEST_BAG), reason="test bag not generated")
def test_bag_looper_loops_past_end():
    looper = BagLooper(TEST_BAG, topic_type="sensor_msgs/msg/NavSatFix")
    # The fixture bag has 50 messages — iterate 75 and confirm no StopIteration
    msgs = []
    for _ in range(75):
        msgs.append(next(looper))
    assert len(msgs) == 75
    # Looped → first and 51st messages should have identical latitude
    assert msgs[0].latitude == msgs[50].latitude
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd tools/benchmark/scaling
python3 -m pytest tests/test_robot_replay.py -v
```

Expected: 2 new tests FAIL with `ImportError: cannot import name 'BagLooper'`.

- [ ] **Step 3: Implement `BagLooper`**

Append to `tools/benchmark/scaling/robot_replay.py`:

```python
import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message


class BagLooper:
    """Iterate messages from a rosbag2 bag, looping forever.

    Filters to a single topic type. Reuses the underlying reader; on EOF,
    closes and re-opens it.
    """

    def __init__(self, bag_path: str, topic_type: str) -> None:
        self._bag_path = bag_path
        self._topic_type_str = topic_type
        self._msg_class = get_message(topic_type)
        self._reader = None
        self._open_reader()

    def _open_reader(self) -> None:
        if self._reader is not None:
            del self._reader
        self._reader = rosbag2_py.SequentialReader()
        storage_options = rosbag2_py.StorageOptions(uri=self._bag_path, storage_id="sqlite3")
        converter_options = rosbag2_py.ConverterOptions("", "")
        self._reader.open(storage_options, converter_options)
        # Filter to topics matching our type (the bag may contain other types)
        topics_meta = self._reader.get_all_topics_and_types()
        wanted = [m.name for m in topics_meta if m.type == self._topic_type_str]
        if not wanted:
            raise RuntimeError(
                f"No topic of type {self._topic_type_str} found in {self._bag_path}"
            )
        self._reader.set_filter(rosbag2_py.StorageFilter(topics=wanted))

    def __iter__(self):
        return self

    def __next__(self):
        if not self._reader.has_next():
            self._open_reader()
        _topic, data, _t = self._reader.read_next()
        return deserialize_message(data, self._msg_class)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
source /opt/ros/humble/setup.bash
cd tools/benchmark/scaling
python3 -m pytest tests/test_robot_replay.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add tools/benchmark/scaling/robot_replay.py \
        tools/benchmark/scaling/tests/test_robot_replay.py
git commit -m "scaling: add BagLooper with looping playback"
```

---

## Task 5: `robot_replay.py` — main ROS node

**Files:**
- Modify: `tools/benchmark/scaling/robot_replay.py` (append `RobotReplay` node + `main`)

- [ ] **Step 1: Append the node and main function**

Append to `tools/benchmark/scaling/robot_replay.py`:

```python
import argparse
import os
import socket
import time

import rclpy
from rclpy.node import Node


def derive_robot_id_from_hostname() -> int:
    """Compose --scale assigns hostnames like '<project>-robot-3'. Take the trailing int."""
    host = socket.gethostname()
    parts = host.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return int(parts[1])
    return 0


class RobotReplay(Node):
    def __init__(self, robot_id: int, bag_path: str, rate_hz: float) -> None:
        super().__init__(f"robot_replay_{robot_id}")
        self._robot_id = robot_id
        self._rate_hz = rate_hz
        self._looper = BagLooper(bag_path, topic_type="sensor_msgs/msg/NavSatFix")
        self._pub = self.create_publisher(NavSatFix, f"/robot_{robot_id}/gnss", 10)
        self._timer = self.create_timer(1.0 / rate_hz, self._tick)
        self.get_logger().info(
            f"Replaying bag={bag_path} as robot_id={robot_id} at {rate_hz} Hz"
        )

    def _tick(self) -> None:
        msg = next(self._looper)
        shift_navsatfix(msg, self._robot_id)
        restamp_ns(msg, time.time_ns())
        self._pub.publish(msg)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--robot-id",
        type=int,
        default=int(os.environ.get("ROBOT_ID", "-1")),
        help="If <0, derived from hostname",
    )
    parser.add_argument(
        "--bag-path",
        default=os.environ.get("BAG_PATH"),
        help="Path to a rosbag2 directory containing NavSatFix messages",
    )
    parser.add_argument(
        "--rate-hz",
        type=float,
        default=float(os.environ.get("RATE_HZ", "10")),
    )
    args = parser.parse_args()

    if not args.bag_path:
        raise SystemExit("BAG_PATH (env or --bag-path) is required")
    robot_id = args.robot_id if args.robot_id >= 0 else derive_robot_id_from_hostname()

    rclpy.init()
    node = RobotReplay(robot_id, args.bag_path, args.rate_hz)
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Manual smoke test against the fixture bag**

In one terminal:

```bash
source /opt/ros/humble/setup.bash
source ~/colcon_ws/install/setup.bash  # adjust to your workspace
cd tools/benchmark/scaling
python3 robot_replay.py --robot-id 3 \
    --bag-path test_data/test_bag --rate-hz 10
```

Expected: `Replaying bag=test_data/test_bag as robot_id=3 at 10.0 Hz`.

In another terminal:

```bash
source /opt/ros/humble/setup.bash
ros2 topic hz /robot_3/gnss
ros2 topic echo /robot_3/gnss --once
```

Expected:
- `ros2 topic hz` reports ~10 Hz steady.
- `ros2 topic echo` shows `latitude` near `51.1 + 0.0001 * 3 = 51.1003`
  and a recent `header.stamp`.

Stop the replay with Ctrl-C.

- [ ] **Step 3: Commit**

```bash
git add tools/benchmark/scaling/robot_replay.py
git commit -m "scaling: add RobotReplay ROS node with bag looping"
```

---

## Task 6: `Dockerfile.robot`

**Files:**
- Create: `tools/benchmark/scaling/Dockerfile.robot`

- [ ] **Step 1: Write the Dockerfile**

`tools/benchmark/scaling/Dockerfile.robot`:

```dockerfile
# Robot replay container — one per simulated robot.
ARG ROS_DISTRO=humble
FROM ros:${ROS_DISTRO}-ros-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-rosbag2-py \
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
```

- [ ] **Step 2: Build the image**

```bash
cd tools/benchmark/scaling
docker build -f Dockerfile.robot -t ros2-scaling-robot:local .
```

Expected: build succeeds; image size ~1 GB (ROS base layer is large).

- [ ] **Step 3: Verify the image runs (single-robot smoke)**

```bash
docker run --rm --network host \
    -e ROBOT_ID=7 \
    -e BAG_PATH=/data/test_bag \
    -e ROS_DOMAIN_ID=42 \
    -v "$(pwd)/test_data/test_bag:/data/test_bag:ro" \
    ros2-scaling-robot:local
```

In another terminal: `ros2 topic hz /robot_7/gnss` → expect ~10 Hz.
Ctrl-C to stop.

- [ ] **Step 4: Commit**

```bash
git add tools/benchmark/scaling/Dockerfile.robot
git commit -m "scaling: add Dockerfile.robot"
```

---

## Task 7: `e2e_consumer.py` — CDR deserialize + JSONL writer (TDD)

**Files:**
- Create: `tools/benchmark/scaling/e2e_consumer.py`
- Create: `tools/benchmark/scaling/tests/test_e2e_consumer.py`

- [ ] **Step 1: Write failing tests for `extract_t0_ns` and `format_row`**

`tools/benchmark/scaling/tests/test_e2e_consumer.py`:

```python
"""Unit tests for e2e_consumer helpers."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rclpy.serialization import serialize_message
from sensor_msgs.msg import NavSatFix

from e2e_consumer import deserialize_navsatfix, extract_t0_ns, format_row, robot_id_from_kafka_topic, robot_id_from_mqtt_topic


def _make_fix_with_stamp(t_ns: int) -> bytes:
    msg = NavSatFix()
    msg.header.stamp.sec = t_ns // 1_000_000_000
    msg.header.stamp.nanosec = t_ns % 1_000_000_000
    msg.latitude = 51.1
    msg.longitude = 17.0
    return serialize_message(msg)


def test_deserialize_navsatfix_roundtrip():
    payload = _make_fix_with_stamp(1_234_567_890_123_456_789)
    msg = deserialize_navsatfix(payload)
    assert abs(msg.latitude - 51.1) < 1e-12


def test_extract_t0_ns_combines_sec_and_nanosec():
    payload = _make_fix_with_stamp(1_234_567_890_123_456_789)
    msg = deserialize_navsatfix(payload)
    assert extract_t0_ns(msg) == 1_234_567_890_123_456_789


def test_extract_t0_ns_zero():
    payload = _make_fix_with_stamp(0)
    msg = deserialize_navsatfix(payload)
    assert extract_t0_ns(msg) == 0


def test_robot_id_from_kafka_topic():
    assert robot_id_from_kafka_topic("ros2.robot_7.gnss") == 7
    assert robot_id_from_kafka_topic("ros2.robot_50.gnss") == 50


def test_robot_id_from_mqtt_topic():
    assert robot_id_from_mqtt_topic("ros2/robot_7/gnss") == 7
    assert robot_id_from_mqtt_topic("ros2/robot_50/gnss") == 50


def test_format_row_jsonl():
    row = format_row(robot_id=3, topic="ros2.robot_3.gnss", t0_ns=100, t1_ns=250, bytes_=104)
    parsed = json.loads(row)
    assert parsed == {
        "robot_id": 3,
        "topic": "ros2.robot_3.gnss",
        "t0_ns": 100,
        "t1_ns": 250,
        "latency_ns": 150,
        "bytes": 104,
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd tools/benchmark/scaling
python3 -m pytest tests/test_e2e_consumer.py -v
```

Expected: 6 tests FAIL with `ModuleNotFoundError: No module named 'e2e_consumer'`.

- [ ] **Step 3: Implement the helpers**

`tools/benchmark/scaling/e2e_consumer.py`:

```python
#!/usr/bin/env python3
"""Consume Kafka or MQTT records carrying CDR-serialized NavSatFix.

Subscribes to all `robot_*` topics, deserializes CDR, extracts the
publish-time t0_ns from header.stamp, captures t1_ns at receive, and
writes one JSONL row per delivered message.
"""
from __future__ import annotations

import argparse
import json
import re
import signal
import time

from rclpy.serialization import deserialize_message
from sensor_msgs.msg import NavSatFix


_KAFKA_ROBOT_RE = re.compile(r"^ros2\.robot_(\d+)\.gnss$")
_MQTT_ROBOT_RE = re.compile(r"^ros2/robot_(\d+)/gnss$")


def deserialize_navsatfix(data: bytes) -> NavSatFix:
    return deserialize_message(data, NavSatFix)


def extract_t0_ns(msg: NavSatFix) -> int:
    return msg.header.stamp.sec * 1_000_000_000 + msg.header.stamp.nanosec


def robot_id_from_kafka_topic(topic: str) -> int:
    m = _KAFKA_ROBOT_RE.match(topic)
    if not m:
        raise ValueError(f"Unrecognised Kafka topic: {topic}")
    return int(m.group(1))


def robot_id_from_mqtt_topic(topic: str) -> int:
    m = _MQTT_ROBOT_RE.match(topic)
    if not m:
        raise ValueError(f"Unrecognised MQTT topic: {topic}")
    return int(m.group(1))


def format_row(robot_id: int, topic: str, t0_ns: int, t1_ns: int, bytes_: int) -> str:
    return json.dumps(
        {
            "robot_id": robot_id,
            "topic": topic,
            "t0_ns": t0_ns,
            "t1_ns": t1_ns,
            "latency_ns": t1_ns - t0_ns,
            "bytes": bytes_,
        }
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
source /opt/ros/humble/setup.bash
cd tools/benchmark/scaling
python3 -m pytest tests/test_e2e_consumer.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add tools/benchmark/scaling/e2e_consumer.py \
        tools/benchmark/scaling/tests/test_e2e_consumer.py
git commit -m "scaling: add e2e_consumer CDR+latency helpers with tests"
```

---

## Task 8: `e2e_consumer.py` — Kafka and MQTT client wiring

**Files:**
- Modify: `tools/benchmark/scaling/e2e_consumer.py` (add broker clients + main)

- [ ] **Step 1: Append Kafka + MQTT runners and main**

Append to `tools/benchmark/scaling/e2e_consumer.py`:

```python
from typing import IO, Callable


def _now_ns() -> int:
    return time.time_ns()


def _consume_kafka(
    bootstrap: str,
    pattern: str,
    warmup_s: float,
    duration_s: float,
    out: IO[str],
) -> None:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"e2e-consumer-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([], on_assign=None)
    consumer.subscribe([pattern])  # confluent-kafka supports regex with `^` prefix

    t_start = time.monotonic()
    while True:
        elapsed = time.monotonic() - t_start
        if elapsed >= warmup_s + duration_s:
            break
        msg = consumer.poll(timeout=0.5)
        if msg is None or msg.error():
            continue
        t1_ns = _now_ns()
        if elapsed < warmup_s:
            continue  # skip warmup window
        try:
            nav = deserialize_navsatfix(msg.value())
            robot_id = robot_id_from_kafka_topic(msg.topic())
        except (ValueError, Exception):  # noqa: BLE001
            continue
        t0_ns = extract_t0_ns(nav)
        out.write(format_row(robot_id, msg.topic(), t0_ns, t1_ns, len(msg.value())) + "\n")
    consumer.close()


def _consume_mqtt(
    host: str,
    port: int,
    pattern: str,
    warmup_s: float,
    duration_s: float,
    out: IO[str],
) -> None:
    import paho.mqtt.client as mqtt

    t_start = time.monotonic()

    def _on_message(_client, _userdata, msg):
        elapsed = time.monotonic() - t_start
        if elapsed < warmup_s:
            return
        t1_ns = _now_ns()
        try:
            nav = deserialize_navsatfix(msg.payload)
            robot_id = robot_id_from_mqtt_topic(msg.topic)
        except (ValueError, Exception):  # noqa: BLE001
            return
        t0_ns = extract_t0_ns(nav)
        out.write(format_row(robot_id, msg.topic, t0_ns, t1_ns, len(msg.payload)) + "\n")

    client = mqtt.Client()
    client.on_message = _on_message
    client.connect(host, port, keepalive=60)
    client.subscribe(pattern)
    client.loop_start()
    while time.monotonic() - t_start < warmup_s + duration_s:
        time.sleep(0.5)
    client.loop_stop()
    client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", choices=["kafka", "mqtt"], required=True)
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--kafka-pattern", default="^ros2\\.robot_.*\\.gnss$")
    parser.add_argument("--mqtt-pattern", default="ros2/robot_+/gnss")
    parser.add_argument("--warmup", type=float, default=10.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--output", required=True, help="JSONL output path")
    args = parser.parse_args()

    # Graceful shutdown so files flush
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    with open(args.output, "w", buffering=1) as out:
        if args.broker == "kafka":
            _consume_kafka(args.bootstrap, args.kafka_pattern, args.warmup, args.duration, out)
        else:
            _consume_mqtt(args.mqtt_host, args.mqtt_port, args.mqtt_pattern, args.warmup, args.duration, out)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Install deps locally for the manual smoke**

```bash
pip install --user confluent-kafka paho-mqtt
```

- [ ] **Step 3: Manual smoke — Kafka path**

Bring up Kafka and a sink that publishes one topic:

```bash
cd kafka_bridge/kafka_brocker && docker compose up -d && cd -
# In another terminal, start an existing single-topic benchmark publisher + sink
tools/benchmark/run_benchmark.sh --msg-type navsatfix --format cdr \
    --rate 10 --duration 30 --warmup 2 --run-id smoke &
SINK_PID=$!

# In a third terminal:
python3 tools/benchmark/scaling/e2e_consumer.py \
    --broker kafka --bootstrap localhost:9092 \
    --kafka-pattern '^benchmark\.benchmark\.navsatfix$' \
    --warmup 0 --duration 10 --output /tmp/smoke.jsonl

wait $SINK_PID
head /tmp/smoke.jsonl
```

Expected: `/tmp/smoke.jsonl` contains JSON lines with `latency_ns` values
in the low millisecond range (single-host loopback).

Stop containers: `cd kafka_bridge/kafka_brocker && docker compose down`.

- [ ] **Step 4: Commit**

```bash
git add tools/benchmark/scaling/e2e_consumer.py
git commit -m "scaling: add Kafka/MQTT consumer wiring with warmup window"
```

---

## Task 9: `Dockerfile.consumer`

**Files:**
- Create: `tools/benchmark/scaling/Dockerfile.consumer`

- [ ] **Step 1: Write the Dockerfile**

`tools/benchmark/scaling/Dockerfile.consumer`:

```dockerfile
ARG ROS_DISTRO=humble
FROM ros:${ROS_DISTRO}-ros-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    ros-${ROS_DISTRO}-sensor-msgs \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir confluent-kafka paho-mqtt

WORKDIR /app
COPY e2e_consumer.py /app/

ENV PYTHONUNBUFFERED=1
ENV ROS_DOMAIN_ID=42

ENTRYPOINT ["bash", "-lc", "source /opt/ros/${ROS_DISTRO}/setup.bash && exec python3 /app/e2e_consumer.py \"$@\"", "--"]
```

- [ ] **Step 2: Build the image**

```bash
cd tools/benchmark/scaling
docker build -f Dockerfile.consumer -t ros2-scaling-consumer:local .
```

- [ ] **Step 3: Commit**

```bash
git add tools/benchmark/scaling/Dockerfile.consumer
git commit -m "scaling: add Dockerfile.consumer"
```

---

## Task 10: `sink_entrypoint.sh`

**Files:**
- Create: `tools/benchmark/scaling/sink_entrypoint.sh`

- [ ] **Step 1: Write the script**

`tools/benchmark/scaling/sink_entrypoint.sh`:

```bash
#!/usr/bin/env bash
# Sink container entrypoint: template subscriptions_yaml for $NUM_ROBOTS,
# launch the sink, launch metrics_recorder.py in parallel.
set -euo pipefail

: "${NUM_ROBOTS:?NUM_ROBOTS env var is required}"
: "${SINK_KIND:?SINK_KIND must be 'kafka' or 'mqtt'}"
: "${RESULTS_DIR:=/artifacts}"

mkdir -p "${RESULTS_DIR}"

# ── Generate subscriptions_yaml ──
SUBS_YAML=""
for ((i = 1; i <= NUM_ROBOTS; i++)); do
    SUBS_YAML+="- topic_name: /robot_${i}/gnss
  msg_type: sensor_msgs/msg/NavSatFix
"
done

# ── Source the workspace built inside the image ──
source /opt/ros/humble/setup.bash
source /ws/install/setup.bash

# ── Pick exe and metrics topic ──
case "${SINK_KIND}" in
    kafka)
        EXE="kafka_sink_node_exe"
        NODE_NAME="kafka_sink"
        METRICS_TOPIC="/kafka_sink/metrics"
        FORMAT_PARAM="-p kafka.payload_format:=cdr"
        BROKER_PARAM="-p kafka.bootstrap_servers:=${BROKER_HOST:-localhost}:9092"
        TOPIC_PREFIX_PARAM="-p kafka.topic_prefix:=ros2"
        DROP_PARAM="-p kafka.drop_when_full:=true"
        STRICT_PARAM="-p kafka.strict_startup:=false"
        ;;
    mqtt)
        EXE="mosquitto_sink_node_exe"
        NODE_NAME="mosquitto_sink"
        METRICS_TOPIC="/mosquitto_sink/metrics"
        FORMAT_PARAM=""
        BROKER_PARAM="-p mqtt.host:=${BROKER_HOST:-localhost} -p mqtt.port:=1883"
        TOPIC_PREFIX_PARAM="-p mqtt.topic_prefix:=ros2"
        DROP_PARAM="-p mqtt.drop_when_full:=true"
        STRICT_PARAM="-p mqtt.strict_startup:=false"
        ;;
    *)
        echo "Unknown SINK_KIND: ${SINK_KIND}" >&2
        exit 1
        ;;
esac

# ── Start metrics recorder in background ──
python3 /ws/src/ros2_kafka_dispatcher/tools/benchmark/metrics_recorder.py \
    --topic "${METRICS_TOPIC}" \
    --output "${RESULTS_DIR}/sink_metrics.csv" \
    --duration 0 &
METRICS_PID=$!

cleanup() {
    kill "${METRICS_PID}" 2>/dev/null || true
}
trap cleanup EXIT

# ── Start the sink ──
ros2 run "${NODE_NAME}" "${EXE}" --ros-args \
    -p "subscriptions_yaml:=${SUBS_YAML}" \
    ${FORMAT_PARAM} \
    ${BROKER_PARAM} \
    ${TOPIC_PREFIX_PARAM} \
    ${DROP_PARAM} \
    ${STRICT_PARAM} \
    -p metrics.enabled:=true \
    -p metrics.interval_ms:=1000 &
SINK_PID=$!

# ── Lifecycle: configure → activate ──
sleep 2
ros2 lifecycle set "/${NODE_NAME}" configure
ros2 lifecycle set "/${NODE_NAME}" activate

wait "${SINK_PID}"
```

- [ ] **Step 2: Make it executable**

```bash
chmod +x tools/benchmark/scaling/sink_entrypoint.sh
```

- [ ] **Step 3: Commit**

```bash
git add tools/benchmark/scaling/sink_entrypoint.sh
git commit -m "scaling: add sink entrypoint that templates subscriptions_yaml"
```

Note: this entrypoint runs **inside the project's existing
`ros2-kafka-dispatcher` image**. We do not build a separate sink image;
we mount this script and the workspace into the existing image (see
Task 11).

---

## Task 11: Compose files

**Files:**
- Create: `tools/benchmark/scaling/compose.kafka.yml`
- Create: `tools/benchmark/scaling/compose.mqtt.yml`

- [ ] **Step 1: Pre-build the project image once**

```bash
cd /home/maciej/Github/ros2_kafka_dispatcher
docker build -f docker/Dockerfile --build-arg ROS_DISTRO=humble \
    -t ros2-kafka-dispatcher:scaling .
```

- [ ] **Step 2: Write `compose.kafka.yml`**

`tools/benchmark/scaling/compose.kafka.yml`:

```yaml
# Kafka stack for the scalability matrix. Bring up with:
#   BAG_PATH=/path/to/bag NUM_ROBOTS=10 docker compose \
#     -f compose.kafka.yml up -d --scale robot=10
services:

  broker:
    image: bitnami/kafka:3.7
    environment:
      KAFKA_CFG_NODE_ID: "0"
      KAFKA_CFG_PROCESS_ROLES: "controller,broker"
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "0@broker:9093"
      ALLOW_PLAINTEXT_LISTENER: "yes"
    ports:
      - "9092:9092"

  sink:
    image: ros2-kafka-dispatcher:scaling
    network_mode: host
    environment:
      NUM_ROBOTS: "${NUM_ROBOTS:?NUM_ROBOTS is required}"
      SINK_KIND: "kafka"
      BROKER_HOST: "localhost"
      ROS_DOMAIN_ID: "42"
    volumes:
      - ./sink_entrypoint.sh:/usr/local/bin/sink_entrypoint.sh:ro
      - ./results:/artifacts
    entrypoint: ["/usr/local/bin/sink_entrypoint.sh"]
    depends_on:
      - broker

  consumer:
    image: ros2-scaling-consumer:local
    network_mode: host
    command:
      - "--broker=kafka"
      - "--bootstrap=localhost:9092"
      - "--warmup=10"
      - "--duration=60"
      - "--output=/artifacts/consumer.jsonl"
    volumes:
      - ./results:/artifacts
    depends_on:
      - broker
      - sink

  robot:
    image: ros2-scaling-robot:local
    network_mode: host
    environment:
      BAG_PATH: "/data/bag"
      RATE_HZ: "10"
      ROS_DOMAIN_ID: "42"
    volumes:
      - "${BAG_PATH:?BAG_PATH is required}:/data/bag:ro"
    depends_on:
      - sink
```

- [ ] **Step 3: Write `compose.mqtt.yml`**

`tools/benchmark/scaling/compose.mqtt.yml`:

```yaml
# MQTT stack. Bring up with:
#   BAG_PATH=/path/to/bag NUM_ROBOTS=10 docker compose \
#     -f compose.mqtt.yml up -d --scale robot=10
services:

  broker:
    image: eclipse-mosquitto:2.0
    ports:
      - "1883:1883"
    volumes:
      - ../../../mosquitto_bridge/mosquitto_brocker/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro

  sink:
    image: ros2-kafka-dispatcher:scaling
    network_mode: host
    environment:
      NUM_ROBOTS: "${NUM_ROBOTS:?NUM_ROBOTS is required}"
      SINK_KIND: "mqtt"
      BROKER_HOST: "localhost"
      ROS_DOMAIN_ID: "42"
    volumes:
      - ./sink_entrypoint.sh:/usr/local/bin/sink_entrypoint.sh:ro
      - ./results:/artifacts
    entrypoint: ["/usr/local/bin/sink_entrypoint.sh"]
    depends_on:
      - broker

  consumer:
    image: ros2-scaling-consumer:local
    network_mode: host
    command:
      - "--broker=mqtt"
      - "--mqtt-host=localhost"
      - "--mqtt-port=1883"
      - "--warmup=10"
      - "--duration=60"
      - "--output=/artifacts/consumer.jsonl"
    volumes:
      - ./results:/artifacts
    depends_on:
      - broker
      - sink

  robot:
    image: ros2-scaling-robot:local
    network_mode: host
    environment:
      BAG_PATH: "/data/bag"
      RATE_HZ: "10"
      ROS_DOMAIN_ID: "42"
    volumes:
      - "${BAG_PATH:?BAG_PATH is required}:/data/bag:ro"
    depends_on:
      - sink
```

- [ ] **Step 4: Verify Compose files parse**

```bash
cd tools/benchmark/scaling
BAG_PATH=/tmp NUM_ROBOTS=1 docker compose -f compose.kafka.yml config > /dev/null
BAG_PATH=/tmp NUM_ROBOTS=1 docker compose -f compose.mqtt.yml config > /dev/null
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add tools/benchmark/scaling/compose.kafka.yml \
        tools/benchmark/scaling/compose.mqtt.yml
git commit -m "scaling: add Compose stacks for Kafka and MQTT"
```

---

## Task 12: `run_scaling_matrix.sh`

**Files:**
- Create: `tools/benchmark/scaling/run_scaling_matrix.sh`

- [ ] **Step 1: Write the script**

`tools/benchmark/scaling/run_scaling_matrix.sh`:

```bash
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
    SMOKE_DIR="${ABS_RESULTS}/SMOKE_${broker}"
    rm -rf "${SMOKE_DIR}"
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
```

- [ ] **Step 2: Make it executable and dry-run**

```bash
chmod +x tools/benchmark/scaling/run_scaling_matrix.sh
tools/benchmark/scaling/run_scaling_matrix.sh --help
```

Expected: usage output.

- [ ] **Step 3: Commit**

```bash
git add tools/benchmark/scaling/run_scaling_matrix.sh
git commit -m "scaling: add matrix orchestrator with smoke gate"
```

---

## Task 13: `analyze_scaling.py` — aggregation math (TDD)

**Files:**
- Create: `tools/benchmark/scaling/analyze_scaling.py`
- Create: `tools/benchmark/scaling/tests/test_analyze_scaling.py`

- [ ] **Step 1: Write failing tests**

`tools/benchmark/scaling/tests/test_analyze_scaling.py`:

```python
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyze_scaling import (
    aggregate_cell,
    expected_messages,
    load_jsonl,
    percentile,
)


def test_percentile_basic():
    samples = list(range(1, 101))  # 1..100
    assert percentile(samples, 0.5) == 50
    assert percentile(samples, 0.95) == 95
    assert percentile(samples, 0.99) == 99


def test_percentile_empty_returns_zero():
    assert percentile([], 0.95) == 0


def test_expected_messages():
    assert expected_messages(n=10, rate_hz=10, duration_s=60) == 6000


def test_load_jsonl_round_trip(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text('{"latency_ns": 100}\n{"latency_ns": 200}\n')
    rows = load_jsonl(str(p))
    assert [r["latency_ns"] for r in rows] == [100, 200]


def test_aggregate_cell_basic(tmp_path):
    cell_dir = tmp_path / "N=5_broker=kafka_run=1"
    cell_dir.mkdir()
    rows = [{"latency_ns": v} for v in [100, 200, 300, 400, 500]]
    with open(cell_dir / "consumer.jsonl", "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    summary = aggregate_cell(str(cell_dir), n=5, rate_hz=10, duration_s=10)
    assert summary["received"] == 5
    assert summary["latency_avg_ns"] == 300
    # drop_rate = 1 - 5/(5*10*10) = 1 - 5/500 = 0.99
    assert abs(summary["drop_rate"] - 0.99) < 1e-9
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd tools/benchmark/scaling
python3 -m pytest tests/test_analyze_scaling.py -v
```

Expected: 5 tests FAIL with `ModuleNotFoundError: No module named 'analyze_scaling'`.

- [ ] **Step 3: Implement the math**

`tools/benchmark/scaling/analyze_scaling.py`:

```python
#!/usr/bin/env python3
"""Aggregate per-cell consumer JSONL into summary.csv + plots."""
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import os
import re
import statistics
from typing import Dict, List


def percentile(samples: List[float], q: float) -> float:
    if not samples:
        return 0
    s = sorted(samples)
    # Nearest-rank method, 1-indexed
    k = max(1, math.ceil(q * len(s)))
    return s[k - 1]


def expected_messages(n: int, rate_hz: float, duration_s: float) -> int:
    return int(n * rate_hz * duration_s)


def load_jsonl(path: str) -> List[dict]:
    out: List[dict] = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def aggregate_cell(cell_dir: str, n: int, rate_hz: float, duration_s: float) -> dict:
    jsonl_path = os.path.join(cell_dir, "consumer.jsonl")
    rows = load_jsonl(jsonl_path)
    latencies = [r["latency_ns"] for r in rows]
    received = len(rows)
    expected = expected_messages(n, rate_hz, duration_s)
    drop_rate = 1.0 - (received / expected) if expected > 0 else 0.0

    return {
        "received": received,
        "expected": expected,
        "drop_rate": drop_rate,
        "throughput_msgs_per_s": received / duration_s if duration_s > 0 else 0,
        "latency_avg_ns": int(statistics.mean(latencies)) if latencies else 0,
        "latency_p50_ns": percentile(latencies, 0.50),
        "latency_p95_ns": percentile(latencies, 0.95),
        "latency_p99_ns": percentile(latencies, 0.99),
    }


_CELL_RE = re.compile(r"^N=(\d+)_broker=(\w+)_run=([\w]+)$")


def discover_cells(results_dir: str) -> List[Dict]:
    out = []
    for entry in sorted(os.listdir(results_dir)):
        m = _CELL_RE.match(entry)
        if not m:
            continue
        out.append(
            {
                "dir": os.path.join(results_dir, entry),
                "n": int(m.group(1)),
                "broker": m.group(2),
                "run": m.group(3),
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="results directory")
    parser.add_argument("--output", required=True, help="output directory (summary.csv + plots/)")
    parser.add_argument("--rate-hz", type=float, default=10.0)
    parser.add_argument("--duration-s", type=float, default=60.0)
    args = parser.parse_args()

    os.makedirs(os.path.join(args.output, "plots"), exist_ok=True)
    cells = discover_cells(args.input)

    rows = []
    for cell in cells:
        if cell["run"] == "smoke":
            continue  # exclude smoke gate runs
        try:
            agg = aggregate_cell(cell["dir"], cell["n"], args.rate_hz, args.duration_s)
        except FileNotFoundError:
            continue
        rows.append({"n": cell["n"], "broker": cell["broker"], "run": cell["run"], **agg})

    csv_path = os.path.join(args.output, "summary.csv")
    if rows:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {csv_path}")
    else:
        print("No cells with data found.")
        return

    _plot_summary(rows, os.path.join(args.output, "plots"))


def _plot_summary(rows: List[dict], plot_dir: str) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:  # noqa: BLE001
        print(f"matplotlib not available; skipping plots ({exc})")
        return

    # Group by broker → list of (n, mean_p95_ns) across runs
    by_broker: Dict[str, Dict[int, List[float]]] = {}
    by_broker_drop: Dict[str, Dict[int, List[float]]] = {}
    by_broker_tput: Dict[str, Dict[int, List[float]]] = {}
    for r in rows:
        by_broker.setdefault(r["broker"], {}).setdefault(r["n"], []).append(r["latency_p95_ns"])
        by_broker_drop.setdefault(r["broker"], {}).setdefault(r["n"], []).append(r["drop_rate"])
        by_broker_tput.setdefault(r["broker"], {}).setdefault(r["n"], []).append(r["throughput_msgs_per_s"])

    def _plot(data: Dict[str, Dict[int, List[float]]], ylabel: str, fname: str, scale: float = 1.0) -> None:
        plt.figure()
        for broker, points in data.items():
            xs = sorted(points.keys())
            ys = [statistics.mean(points[x]) * scale for x in xs]
            errs = [statistics.pstdev(points[x]) * scale for x in xs]
            plt.errorbar(xs, ys, yerr=errs, marker="o", label=broker)
        plt.xlabel("Number of robots")
        plt.ylabel(ylabel)
        plt.legend()
        plt.grid(True, alpha=0.3)
        out = os.path.join(plot_dir, fname)
        plt.savefig(out, dpi=140, bbox_inches="tight")
        print(f"Wrote {out}")

    _plot(by_broker, "P95 e2e latency (ms)", "latency_vs_n.png", scale=1e-6)
    _plot(by_broker_tput, "Throughput (msgs/s)", "throughput_vs_n.png")
    _plot(by_broker_drop, "Drop rate", "drop_rate_vs_n.png")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd tools/benchmark/scaling
python3 -m pytest tests/test_analyze_scaling.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Make executable and commit**

```bash
chmod +x tools/benchmark/scaling/analyze_scaling.py
git add tools/benchmark/scaling/analyze_scaling.py \
        tools/benchmark/scaling/tests/test_analyze_scaling.py
git commit -m "scaling: add analyze_scaling aggregator with tests"
```

---

## Task 14: End-to-end smoke at N=1

The simplest integration test against your real bag, before running the full matrix.

- [ ] **Step 1: Set BAG_PATH and quick-run a single cell**

```bash
export BAG_PATH=/absolute/path/to/your/single_robot_bag
cd tools/benchmark/scaling
./run_scaling_matrix.sh --reps 1 --robots 1 --brokers kafka --duration 20 --warmup 5
```

Expected:
- Smoke gate runs N=10 on kafka (may take ~90 s).
- Then the N=1 cell runs.
- `results/N=1_broker=kafka_run=1/consumer.jsonl` exists and has ~200 lines
  (`20 s * 10 Hz` minus warmup).
- `results/N=1_broker=kafka_run=1/sink_metrics.csv` exists and has data.

- [ ] **Step 2: Inspect a row**

```bash
head -1 results/N=1_broker=kafka_run=1/consumer.jsonl | python3 -m json.tool
```

Expected: keys `robot_id, topic, t0_ns, t1_ns, latency_ns, bytes` with
`latency_ns` in the low millisecond range (1e6–1e7).

- [ ] **Step 3: Run the analyzer**

```bash
./analyze_scaling.py --input results --output results --duration-s 20
```

Expected: `results/summary.csv` and `results/plots/*.png` written.

- [ ] **Step 4: Commit any fixes**

If the smoke run revealed bugs, fix and commit. Otherwise no commit.

---

## Task 15: Smoke MQTT path

Same as Task 14 but on MQTT, to verify the parallel path works.

- [ ] **Step 1: Run a single MQTT cell**

```bash
cd tools/benchmark/scaling
./run_scaling_matrix.sh --reps 1 --robots 1 --brokers mqtt --duration 20 --warmup 5
```

Expected: `results/N=1_broker=mqtt_run=1/consumer.jsonl` populated; MQTT
latencies should be in the same order of magnitude as Kafka. If
significantly worse, check `sink_metrics.csv` for `delta_dropped > 0`.

- [ ] **Step 2: Fix any MQTT-specific issues** (likely topic-prefix mapping)

Common issue: `mosquitto_sink` may use `.` vs `/` differently. If the
consumer matches no messages, run `mosquitto_sub -h localhost -t '#' -v`
to see the real topic names, and adjust `--mqtt-pattern` defaults in
`e2e_consumer.py`.

If fixes needed: commit.

```bash
git commit -m "scaling: fix MQTT pattern after smoke run" -a
```

---

## Task 16: Full matrix run

- [ ] **Step 1: Run the full matrix**

```bash
export BAG_PATH=/absolute/path/to/your/bag
cd tools/benchmark/scaling
./run_scaling_matrix.sh
```

Expected wall time: ~45 minutes (smoke + 30 cells × ~90 s).
Output: `results/N=<n>_broker=<b>_run=<r>/` for all 30 combinations.

- [ ] **Step 2: Aggregate and plot**

```bash
./analyze_scaling.py --input results --output results
cat results/summary.csv
ls results/plots/
```

Expected files:
- `results/summary.csv` (30 rows + header)
- `results/plots/latency_vs_n.png`
- `results/plots/throughput_vs_n.png`
- `results/plots/drop_rate_vs_n.png`

- [ ] **Step 3: Commit the analysis script outputs to a separate data branch (optional)**

The `results/` directory is gitignored. If you want to preserve the raw
JSONLs, either commit them on a dedicated `data/scalability-2026-05` branch
or archive them outside git:

```bash
tar -czf scalability_results_$(date +%Y%m%d).tar.gz results/
```

- [ ] **Step 4: Verify acceptance criteria from the spec**

From `docs/superpowers/specs/2026-05-11-scalability-experiment-design.md`
§ Acceptance criteria:

1. All 30 cells produced non-empty `consumer.jsonl` and `sink_metrics.csv`?
2. `analyze_scaling.py` ran to completion, produced summary.csv + 3 plots?
3. At N=1, P95 latency is within 2× of `tools/latency/` numbers on the same host?
4. Smoke gate passed at N=10 on both brokers?

If all four are yes, the experiment is complete.

---

## Self-Review

- **Spec coverage** — every section in the spec maps to a task:
  - Topology / network mode → Tasks 6, 9, 11
  - robot_replay → Tasks 3, 4, 5, 6
  - sink (templated subscriptions, metrics_recorder co-located) → Task 10
  - e2e_consumer (CDR deserialize, Kafka + MQTT, warmup window, JSONL) → Tasks 7, 8, 9
  - Broker reuse → Task 11 (extends existing compose snippets)
  - Orchestrator (smoke gate, full matrix, artifact moves) → Task 12
  - Analysis (percentile, throughput, drop rate, plots) → Task 13
  - Single-host clock assumption → enforced by `network_mode: host` (Tasks 6, 9, 11)
  - Acceptance criteria → Task 16 Step 4
- **Placeholders** — none ("TBD"/"TODO"/"appropriate error handling"
  scanned and absent).
- **Type consistency** — `BagLooper`, `RobotReplay`, `shift_navsatfix`,
  `restamp_ns`, `deserialize_navsatfix`, `extract_t0_ns`,
  `robot_id_from_kafka_topic`, `robot_id_from_mqtt_topic`,
  `format_row`, `percentile`, `aggregate_cell`, `expected_messages`,
  `load_jsonl`, `discover_cells` — names consistent between definition
  and use in tests + main code.
- **Known gap** — the spec's "Risks 1: DDS at N=50" is addressed by the
  smoke gate at N=10; it does not test N=50 specifically before the
  full matrix. If discovery breaks between 10 and 25 robots, the
  matrix's first N=25 cell will fail and the operator must intervene.
  This is acceptable given the cost of pre-flighting at N=50.
