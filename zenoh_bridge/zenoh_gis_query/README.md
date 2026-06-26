# zenoh_gis_query

A ROS 2 lifecycle node that subscribes to NavSatFix position streams over Zenoh,
tracks robot membership via a Zenoh liveliness subscriber, and maintains a
latest-position table for QoD-aware GIS field-plot selection.

## Overview

`ZenohGisQueryNode` opens a Zenoh session in `on_activate` and:

- Declares a **liveliness subscriber** on `<zenoh.live_key_prefix>/*` (default
  `gis/live/*`) to track which robots are online. A `PUT` event inserts the
  robot id into `live_robots_`; a `DELETE` removes it.
- Declares a **position subscriber** on `zenoh.key_expr` (default `ros2/**`) and
  decodes each CDR `sensor_msgs/msg/NavSatFix` payload into a `FixSample`, stored
  in `latest_` keyed by robot id (the path segment after `ros2/`).
- Declares three **pull queryables**:
  - `gis/query/geofence?plot=<id>` — which live robots are inside the named plot.
  - `gis/query/collision?radius=<m>` — pairs of robots within `radius` metres.
  - `gis/query/proximity?sensor=<id>&range=<m>` — robots within `range` of a sensor.

### History-aware storage fallback

When a live robot is tracked in `live_robots_` but its position is **not yet
present** in the in-memory `latest_` map (e.g. the robot just reconnected or the
node just activated), each pull queryable issues a bounded
`Session::get("ros2/<robot>/gps/fix")` with a **200 ms timeout** to pull the
last stored value from a Zenoh storage (see *Running with RocksDB storage* below).
The fetched fix is merged into the per-query snapshot and its staleness feeds the
QoD fidelity metric, so callers can detect degraded confidence via `qod.fidelity`.

**Concurrency note:** the `get` is issued only after `state_mutex_` is released.
`rt_` (the session handle) is snapshotted into a local `shared_ptr` while the
lock is held; the actual `get` call never holds the state lock.

`on_configure` loads field-plot polygons and fixed-sensor positions from GeoJSON /
JSON files. Empty `gis.plots_path` / `gis.sensors_path` parameters auto-resolve to
the installed `config/plots.geojson` / `config/sensors.json`.

Pure functions in the companion headers (`geometry.hpp`, `selector.hpp`, `qod.hpp`,
`navsatfix_decode.hpp`, `plots.hpp`) are all unit-tested independently.

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `zenoh.mode` | `peer` | Session role: `peer`, `client`, or `router` |
| `zenoh.connect` | `[]` | Endpoints to connect to in client mode |
| `zenoh.key_expr` | `ros2/**` | Key expression for position subscriber |
| `zenoh.live_key_prefix` | `gis/live` | Liveliness key prefix |
| `gis.plots_path` | `""` | Path to GeoJSON file with field-plot polygons |
| `gis.sensors_path` | `""` | Path to JSON file with sensor positions |
| `gis.fidelity_horizon_ms` | `2000` | Max fix age (ms) before QoD fidelity drops to 0 |

## Launch

```bash
ros2 launch zenoh_gis_query zenoh_gis_query.launch.py
```

Or with a custom param file:

```bash
ros2 launch zenoh_gis_query zenoh_gis_query.launch.py \
  zenoh_gis_query_param_file:=/path/to/my_params.yaml
```

## Running with RocksDB storage (optional but recommended)

The history-aware fallback in the pull queryables works only when a Zenoh router
running the **storage-manager plugin** with a **RocksDB backend** is reachable.
A ready-to-use router config is installed at:

```
<package_share>/config/zenoh_storage_rocksdb.json5
```

It configures:
- A `rocksdb` volume backed by `/ws/zenoh_storage` (created on first run).
- A storage `ros2_history` covering `ros2/**` — the same key space used by
  `zenoh_sink` and `zenoh_source`.

### Obtaining the plugins

`zenoh-plugin-storage-manager` and `zenoh-backend-rocksdb` are **standalone Zenoh
daemon plugins**. They are **NOT** shipped by `ros-humble-zenoh-cpp-vendor` (which
provides only the client C library). Obtain them from the
[Zenoh GitHub releases](https://github.com/eclipse-zenoh/zenoh/releases) matching
the zenoh-c version installed in your image, or build from source:

```bash
# Check installed zenoh-c version
dpkg -l | grep zenoh   # or: find /opt/ros -name 'zenoh-c' -type d
```

Typical installation (adjust the zenoh release tag to match):

```bash
# Install zenohd router + storage-manager plugin + rocksdb backend
# from the zenoh GitHub releases page for your architecture.
# Example (version must match the zenoh-c version in the ROS image):
curl -L https://github.com/eclipse-zenoh/zenoh/releases/download/<VERSION>/zenohd-<ARCH>.deb -o zenohd.deb
dpkg -i zenohd.deb
# zenoh-plugin-storage-manager and zenoh-backend-rocksdb follow the same pattern.
```

### Running the router

```bash
zenohd --config "$(ros2 pkg prefix zenoh_gis_query)/share/zenoh_gis_query/config/zenoh_storage_rocksdb.json5"
```

With the router running, `ZenohGisQueryNode` should be started in `client` mode
pointing at the router:

```yaml
# zenoh_gis_query.param.yaml
zenoh_gis_query:
  ros__parameters:
    zenoh.mode: client
    zenoh.connect: ["tcp/localhost:7447"]
```

### Availability in the Docker image

As of 2026-06-26 the `rkd:builder` / `rkd:gis` Docker images do **not** include
`zenohd`, `zenoh-plugin-storage-manager`, or `zenoh-backend-rocksdb`. The
storage fallback therefore returns empty results in the containerised integration
tests. The config file and the client-side `Session::get` code path are fully
implemented; end-to-end storage verification requires a separately provisioned
router or an image built with the zenoh daemon packages.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
