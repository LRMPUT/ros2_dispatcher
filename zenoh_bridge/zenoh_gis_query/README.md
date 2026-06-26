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

## License

Apache License 2.0 — see [LICENSE](LICENSE).
