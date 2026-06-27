# Zenoh GIS Query Pipeline (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Zenoh-native query layer — a `zenoh_gis_query` node that hosts geofence/collision/proximity queryables over live + RocksDB-stored robot positions, with brokerless membership (liveliness) and QoD annotations — plus the baseline benchmark hook.

**Architecture:** Robots publish NavSatFix as CDR via the existing `zenoh_sink` (key `ros2/<robot>/gps/fix`, `ros_type` attachment, QoS knobs) and each declares a liveliness token `gis/live/<robot>`. A `zenoh-plugin-storage-manager` (RocksDB backend) persists `ros2/**`. The new `zenoh_gis_query` lifecycle node opens a Zenoh session, tracks live robots via a liveliness subscriber, and answers `gis/query/{geofence,collision,proximity}` by gathering live + stored positions, decoding the CDR, running pure spatial functions, and replying with results + QoD; it also runs a continuous mode pushing `gis/alert/**`.

**Tech Stack:** C++17, ROS 2 Humble (ament_cmake, rclcpp_lifecycle), zenoh-cpp 1.x (`zenohcxx::zenohc` via `zenoh_cpp_vendor`), `sensor_msgs` (NavSatFix), `nlohmann_json`, gtest. Build/test via the repo Docker images (`docker/Dockerfile`, builder stage).

## Global Constraints

- ROS distro: **humble**; build only via `docker/Dockerfile` (no local ROS). Copied verbatim from spec: builds and tests run in Docker.
- Zenoh binding: **zenoh-cpp 1.x** over the zenoh-c backend; CMake must `find_package(zenoh_cpp_vendor REQUIRED)` **before** `find_package(zenohcxx REQUIRED)`, then link `zenohcxx::zenohc`.
- Empty YAML arrays (`[]`) are rejected by ROS 2 static typing — never put empty arrays in param files; leave unset for "none".
- Spatial scope: NavSatFix lat/lon only; **no heavy GIS library** — point-in-polygon (ray-cast) and haversine are implemented in-repo.
- License header: Apache-2.0 header block (copy from `zenoh_bridge/zenoh_sink/src/zenoh_sink_node.cpp`) on every new source file.
- Lint: code must pass `ament_uncrustify`/`ament_cpplint` (run `ament_uncrustify --reformat` before committing C++).
- Out of scope (Phase 2, not this plan): distributed deployment, `tc netem` fault injection, InfluxDB backend, Grafana.

---

## File structure

New package `zenoh_bridge/zenoh_gis_query/`:
- `package.xml`, `CMakeLists.txt`, `include/zenoh_gis_query/visibility_control.hpp`
- `include/zenoh_gis_query/geometry.hpp` — pure: `point_in_polygon`, `haversine_m` (unit-tested)
- `include/zenoh_gis_query/selector.hpp` — pure: parse `?k=v&k=v` selector params (unit-tested)
- `include/zenoh_gis_query/qod.hpp` — pure: `QoD{completeness, fidelity}` + computation (unit-tested)
- `include/zenoh_gis_query/navsatfix_decode.hpp` — decode CDR → `{lat, lon, alt, stamp_ns, frame_id}` (unit-tested)
- `include/zenoh_gis_query/plots.hpp` — load GeoJSON polygons + sensors YAML (unit-tested parse)
- `include/zenoh_gis_query/zenoh_gis_query_node.hpp` / `src/zenoh_gis_query_node.cpp` — the lifecycle node + Zenoh session/queryables/liveliness
- `config/zenoh_gis_query.param.yaml`, `config/plots.geojson`, `config/sensors.yaml`
- `config/zenoh_storage_rocksdb.json5` — router config for storage-manager
- `launch/zenoh_gis_query.launch.py`
- `test/test_geometry.cpp`, `test/test_selector.cpp`, `test/test_qod.cpp`, `test/test_navsatfix_decode.cpp`, `test/test_plots.cpp`
- `README.md`, `LICENSE`

Modified:
- `zenoh_bridge/zenoh_sink/include/.../zenoh_sink_node.hpp` + `src/zenoh_sink_node.cpp` — declare `gis.liveliness_key` param and a liveliness token on activate.

Helper used throughout (define once, reuse): build+test a package in the builder image.
```bash
# build builder image once (cached after first run)
docker build -f docker/Dockerfile --target builder --build-arg ROS_DISTRO=humble -t rkd:builder .
# run colcon build+test for a package with BUILD_TESTING
run_pkg_test() { docker run --rm rkd:builder bash -lc "
  source /opt/ros/humble/setup.bash && cd /ws &&
  colcon build --merge-install --packages-select $1 --cmake-args -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON 2>&1 | tail -5 &&
  source install/setup.bash &&
  colcon test --merge-install --packages-select $1 >/dev/null 2>&1 ;
  colcon test-result --all | grep -iE '$1|Summary'"; }
```
Note: the builder image COPYs the repo at build time, so **rebuild `rkd:builder` after editing files** before `run_pkg_test`. (Each "run tests" step below assumes a fresh `docker build ... --target builder -t rkd:builder .` first.)

---

### Task 1: Package skeleton + geometry unit (point-in-polygon, haversine)

**Files:**
- Create: `zenoh_bridge/zenoh_gis_query/package.xml`, `CMakeLists.txt`, `include/zenoh_gis_query/visibility_control.hpp`, `include/zenoh_gis_query/geometry.hpp`
- Test: `zenoh_bridge/zenoh_gis_query/test/test_geometry.cpp`

**Interfaces:**
- Produces: `namespace zenoh_gis_query { struct LatLon{double lat; double lon;}; bool point_in_polygon(const LatLon& p, const std::vector<LatLon>& poly); double haversine_m(const LatLon& a, const LatLon& b); }`

- [ ] **Step 1: Write the failing test** — `test/test_geometry.cpp`

```cpp
// Apache-2.0 header (copy from zenoh_sink)
#include <vector>
#include "gtest/gtest.h"
#include "zenoh_gis_query/geometry.hpp"

using zenoh_gis_query::LatLon;
using zenoh_gis_query::point_in_polygon;
using zenoh_gis_query::haversine_m;

TEST(Geometry, PointInsideSquare) {
  std::vector<LatLon> sq{{0, 0}, {0, 1}, {1, 1}, {1, 0}};
  EXPECT_TRUE(point_in_polygon({0.5, 0.5}, sq));
  EXPECT_FALSE(point_in_polygon({1.5, 0.5}, sq));
  EXPECT_FALSE(point_in_polygon({-0.1, 0.5}, sq));
}

TEST(Geometry, HaversineKnownDistance) {
  // ~111.19 km per degree of latitude at the equator
  double d = haversine_m({0.0, 0.0}, {1.0, 0.0});
  EXPECT_NEAR(d, 111195.0, 500.0);
}

TEST(Geometry, HaversineZero) {
  EXPECT_NEAR(haversine_m({48.5, 3.0}, {48.5, 3.0}), 0.0, 1e-6);
}
```

- [ ] **Step 2: Create package files**

`package.xml` (mirror `zenoh_bridge/zenoh_source/package.xml`; deps: `rclcpp`, `rclcpp_lifecycle`, `rclcpp_components`, `lifecycle_msgs`, `launch_ros`, `std_msgs`, `sensor_msgs`, `zenoh_cpp_vendor`; test_depend `ament_cmake_ros`, `ament_lint_auto`, `ament_lint_common`).

`CMakeLists.txt`:
```cmake
cmake_minimum_required(VERSION 3.14)
project(zenoh_gis_query)
find_package(ament_cmake_auto REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(zenoh_cpp_vendor REQUIRED)
find_package(zenohcxx REQUIRED)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME} SHARED
  include/zenoh_gis_query/zenoh_gis_query_node.hpp
  include/zenoh_gis_query/visibility_control.hpp
  src/zenoh_gis_query_node.cpp)
target_link_libraries(${PROJECT_NAME} nlohmann_json::nlohmann_json zenohcxx::zenohc)
rclcpp_components_register_node(${PROJECT_NAME}
  PLUGIN "zenoh_gis_query::ZenohGisQueryNode" EXECUTABLE ${PROJECT_NAME}_node_exe)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()
  foreach(t geometry selector qod navsatfix_decode plots)
    ament_add_ros_isolated_gtest(test_${t} test/test_${t}.cpp)
    ament_target_dependencies(test_${t} ${AMENT_DEPENDENCIES})
    target_link_libraries(test_${t} ${PROJECT_NAME} nlohmann_json::nlohmann_json)
  endforeach()
endif()
ament_auto_package(USE_SCOPED_HEADER_INSTALL_DIR INSTALL_TO_SHARE launch config)
```
`visibility_control.hpp`: copy `zenoh_bridge/zenoh_source/include/zenoh_source/visibility_control.hpp`, replacing `ZENOH_SOURCE` → `ZENOH_GIS_QUERY`.

> Note: the node source (`zenoh_gis_query_node.{hpp,cpp}`) is created in Task 6. Until then, comment the `ament_auto_add_library` `src/...cpp` line is NOT needed — instead create a minimal stub in this task so the library links: `src/zenoh_gis_query_node.cpp` containing only the Apache header + `namespace zenoh_gis_query {}`. Replace it fully in Task 6.

- [ ] **Step 3: Write minimal implementation** — `include/zenoh_gis_query/geometry.hpp`

```cpp
// Apache-2.0 header
#ifndef ZENOH_GIS_QUERY__GEOMETRY_HPP_
#define ZENOH_GIS_QUERY__GEOMETRY_HPP_
#include <cmath>
#include <vector>
namespace zenoh_gis_query
{
struct LatLon { double lat; double lon; };

// Ray-casting point-in-polygon. Polygon is an ordered ring (lat=y, lon=x);
// closing edge implied. Good enough for field-plot scale (planar approx).
inline bool point_in_polygon(const LatLon & p, const std::vector<LatLon> & poly)
{
  bool inside = false;
  const size_t n = poly.size();
  for (size_t i = 0, j = n - 1; i < n; j = i++) {
    const double yi = poly[i].lat, xi = poly[i].lon;
    const double yj = poly[j].lat, xj = poly[j].lon;
    const bool intersect = ((yi > p.lat) != (yj > p.lat)) &&
      (p.lon < (xj - xi) * (p.lat - yi) / (yj - yi) + xi);
    if (intersect) {inside = !inside;}
  }
  return inside;
}

inline double haversine_m(const LatLon & a, const LatLon & b)
{
  constexpr double R = 6371000.0;
  constexpr double kDeg2Rad = M_PI / 180.0;
  const double dlat = (b.lat - a.lat) * kDeg2Rad;
  const double dlon = (b.lon - a.lon) * kDeg2Rad;
  const double s = std::sin(dlat / 2) * std::sin(dlat / 2) +
    std::cos(a.lat * kDeg2Rad) * std::cos(b.lat * kDeg2Rad) *
    std::sin(dlon / 2) * std::sin(dlon / 2);
  return 2 * R * std::asin(std::min(1.0, std::sqrt(s)));
}
}  // namespace zenoh_gis_query
#endif  // ZENOH_GIS_QUERY__GEOMETRY_HPP_
```

- [ ] **Step 4: Run tests** — rebuild builder, then `run_pkg_test zenoh_gis_query`. Expected: `test_geometry.gtest.xml: 3 tests, 0 failures`.

- [ ] **Step 5: Commit**
```bash
git add zenoh_bridge/zenoh_gis_query
git commit -m "feat(zenoh_gis_query): package skeleton + geometry (PiP, haversine)"
```

---

### Task 2: Selector parsing unit

**Files:**
- Create: `include/zenoh_gis_query/selector.hpp`
- Test: `test/test_selector.cpp`

**Interfaces:**
- Produces: `std::unordered_map<std::string,std::string> parse_selector_params(const std::string& key_selector);` — splits the part after `?` into k=v pairs.

- [ ] **Step 1: Write the failing test**
```cpp
// Apache-2.0 header
#include "gtest/gtest.h"
#include "zenoh_gis_query/selector.hpp"
using zenoh_gis_query::parse_selector_params;

TEST(Selector, ParsesParams) {
  auto m = parse_selector_params("gis/query/geofence?plot=P12&window=5s");
  EXPECT_EQ(m["plot"], "P12");
  EXPECT_EQ(m["window"], "5s");
}
TEST(Selector, NoParamsEmpty) {
  EXPECT_TRUE(parse_selector_params("gis/query/collision").empty());
}
TEST(Selector, MissingValueIsEmptyString) {
  auto m = parse_selector_params("k?flag&x=1");
  EXPECT_EQ(m.count("flag"), 1u);
  EXPECT_EQ(m["flag"], "");
  EXPECT_EQ(m["x"], "1");
}
```
- [ ] **Step 2: Run tests, verify `test_selector` FAILS to compile** (header missing). Run: rebuild builder + `run_pkg_test zenoh_gis_query`. Expected: build error "selector.hpp not found".
- [ ] **Step 3: Implement** — `include/zenoh_gis_query/selector.hpp`
```cpp
// Apache-2.0 header
#ifndef ZENOH_GIS_QUERY__SELECTOR_HPP_
#define ZENOH_GIS_QUERY__SELECTOR_HPP_
#include <string>
#include <unordered_map>
namespace zenoh_gis_query
{
inline std::unordered_map<std::string, std::string> parse_selector_params(
  const std::string & key_selector)
{
  std::unordered_map<std::string, std::string> out;
  const auto q = key_selector.find('?');
  if (q == std::string::npos) {return out;}
  std::string rest = key_selector.substr(q + 1);
  size_t pos = 0;
  while (pos < rest.size()) {
    size_t amp = rest.find('&', pos);
    std::string pair = rest.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
    size_t eq = pair.find('=');
    if (eq == std::string::npos) {
      out[pair] = "";
    } else {
      out[pair.substr(0, eq)] = pair.substr(eq + 1);
    }
    if (amp == std::string::npos) {break;}
    pos = amp + 1;
  }
  return out;
}
}  // namespace zenoh_gis_query
#endif  // ZENOH_GIS_QUERY__SELECTOR_HPP_
```
- [ ] **Step 4: Run tests** — rebuild + `run_pkg_test zenoh_gis_query`. Expected: `test_selector.gtest.xml: 3 tests, 0 failures`.
- [ ] **Step 5: Commit** — `git commit -am "feat(zenoh_gis_query): selector param parsing"`

---

### Task 3: QoD computation unit

**Files:**
- Create: `include/zenoh_gis_query/qod.hpp`
- Test: `test/test_qod.cpp`

**Interfaces:**
- Produces: `struct QoD{double completeness; double fidelity; uint64_t contributed; uint64_t expected; int64_t max_staleness_ns;}; QoD compute_qod(uint64_t contributed, uint64_t expected, int64_t max_staleness_ns, int64_t fidelity_horizon_ns);`
- `completeness = contributed/expected` (0 if expected==0). `fidelity = clamp(1 - max_staleness/horizon, 0..1)`.

- [ ] **Step 1: Write the failing test**
```cpp
// Apache-2.0 header
#include "gtest/gtest.h"
#include "zenoh_gis_query/qod.hpp"
using zenoh_gis_query::compute_qod;

TEST(QoD, CompletenessRatio) {
  auto q = compute_qod(3, 4, 0, 1'000'000'000);
  EXPECT_DOUBLE_EQ(q.completeness, 0.75);
}
TEST(QoD, ExpectedZeroIsZeroCompleteness) {
  EXPECT_DOUBLE_EQ(compute_qod(0, 0, 0, 1).completeness, 0.0);
}
TEST(QoD, FidelityDecaysWithStaleness) {
  auto fresh = compute_qod(1, 1, 0, 1'000'000'000);
  auto stale = compute_qod(1, 1, 500'000'000, 1'000'000'000);
  auto dead = compute_qod(1, 1, 2'000'000'000, 1'000'000'000);
  EXPECT_DOUBLE_EQ(fresh.fidelity, 1.0);
  EXPECT_NEAR(stale.fidelity, 0.5, 1e-9);
  EXPECT_DOUBLE_EQ(dead.fidelity, 0.0);
}
```
- [ ] **Step 2: Run tests, verify FAILS** (header missing).
- [ ] **Step 3: Implement** — `include/zenoh_gis_query/qod.hpp`
```cpp
// Apache-2.0 header
#ifndef ZENOH_GIS_QUERY__QOD_HPP_
#define ZENOH_GIS_QUERY__QOD_HPP_
#include <algorithm>
#include <cstdint>
namespace zenoh_gis_query
{
struct QoD
{
  double completeness{0.0};
  double fidelity{0.0};
  uint64_t contributed{0};
  uint64_t expected{0};
  int64_t max_staleness_ns{0};
};
inline QoD compute_qod(
  uint64_t contributed, uint64_t expected, int64_t max_staleness_ns,
  int64_t fidelity_horizon_ns)
{
  QoD q;
  q.contributed = contributed;
  q.expected = expected;
  q.max_staleness_ns = max_staleness_ns;
  q.completeness = expected == 0 ? 0.0 :
    static_cast<double>(contributed) / static_cast<double>(expected);
  if (fidelity_horizon_ns <= 0) {
    q.fidelity = 0.0;
  } else {
    const double f = 1.0 - static_cast<double>(max_staleness_ns) /
      static_cast<double>(fidelity_horizon_ns);
    q.fidelity = std::clamp(f, 0.0, 1.0);
  }
  return q;
}
}  // namespace zenoh_gis_query
#endif  // ZENOH_GIS_QUERY__QOD_HPP_
```
- [ ] **Step 4: Run tests.** Expected: `test_qod.gtest.xml: 3 tests, 0 failures`.
- [ ] **Step 5: Commit** — `git commit -am "feat(zenoh_gis_query): QoD (completeness/fidelity) computation"`

---

### Task 4: NavSatFix CDR decode unit

**Files:**
- Create: `include/zenoh_gis_query/navsatfix_decode.hpp`
- Test: `test/test_navsatfix_decode.cpp`
- Modify: `package.xml` (already has `sensor_msgs`)

**Interfaces:**
- Produces: `struct FixSample{double lat; double lon; double alt; int64_t stamp_ns; std::string frame_id; bool valid;}; FixSample decode_navsatfix(const std::vector<uint8_t>& cdr);`

- [ ] **Step 1: Write the failing test** (round-trips a real NavSatFix through CDR)
```cpp
// Apache-2.0 header
#include <vector>
#include "gtest/gtest.h"
#include "rclcpp/serialization.hpp"
#include "rclcpp/serialized_message.hpp"
#include "sensor_msgs/msg/nav_sat_fix.hpp"
#include "zenoh_gis_query/navsatfix_decode.hpp"

TEST(NavSatFixDecode, RoundTrip) {
  sensor_msgs::msg::NavSatFix msg;
  msg.header.frame_id = "robot_3";
  msg.header.stamp.sec = 5;
  msg.header.stamp.nanosec = 250'000'000;
  msg.latitude = 48.5; msg.longitude = 3.2; msg.altitude = 120.0;
  rclcpp::Serialization<sensor_msgs::msg::NavSatFix> ser;
  rclcpp::SerializedMessage sm;
  ser.serialize_message(&msg, &sm);
  const auto & r = sm.get_rcl_serialized_message();
  std::vector<uint8_t> cdr(r.buffer, r.buffer + r.buffer_length);

  auto s = zenoh_gis_query::decode_navsatfix(cdr);
  ASSERT_TRUE(s.valid);
  EXPECT_DOUBLE_EQ(s.lat, 48.5);
  EXPECT_DOUBLE_EQ(s.lon, 3.2);
  EXPECT_EQ(s.frame_id, "robot_3");
  EXPECT_EQ(s.stamp_ns, 5LL * 1'000'000'000LL + 250'000'000LL);
}
TEST(NavSatFixDecode, EmptyIsInvalid) {
  EXPECT_FALSE(zenoh_gis_query::decode_navsatfix({}).valid);
}
```
- [ ] **Step 2: Run tests, verify FAILS** (header missing).
- [ ] **Step 3: Implement** — `include/zenoh_gis_query/navsatfix_decode.hpp`
```cpp
// Apache-2.0 header
#ifndef ZENOH_GIS_QUERY__NAVSATFIX_DECODE_HPP_
#define ZENOH_GIS_QUERY__NAVSATFIX_DECODE_HPP_
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include "rclcpp/serialization.hpp"
#include "rclcpp/serialized_message.hpp"
#include "sensor_msgs/msg/nav_sat_fix.hpp"
namespace zenoh_gis_query
{
struct FixSample
{
  double lat{0}; double lon{0}; double alt{0};
  int64_t stamp_ns{0}; std::string frame_id; bool valid{false};
};
inline FixSample decode_navsatfix(const std::vector<uint8_t> & cdr)
{
  FixSample s;
  if (cdr.empty()) {return s;}
  rclcpp::SerializedMessage sm(cdr.size());
  auto & r = sm.get_rcl_serialized_message();
  std::memcpy(r.buffer, cdr.data(), cdr.size());
  r.buffer_length = cdr.size();
  sensor_msgs::msg::NavSatFix msg;
  try {
    rclcpp::Serialization<sensor_msgs::msg::NavSatFix> ser;
    ser.deserialize_message(&sm, &msg);
  } catch (...) {
    return s;
  }
  s.lat = msg.latitude; s.lon = msg.longitude; s.alt = msg.altitude;
  s.frame_id = msg.header.frame_id;
  s.stamp_ns = static_cast<int64_t>(msg.header.stamp.sec) * 1'000'000'000LL +
    msg.header.stamp.nanosec;
  s.valid = true;
  return s;
}
}  // namespace zenoh_gis_query
#endif  // ZENOH_GIS_QUERY__NAVSATFIX_DECODE_HPP_
```
- [ ] **Step 4: Run tests.** Expected: `test_navsatfix_decode.gtest.xml: 2 tests, 0 failures`.
- [ ] **Step 5: Commit** — `git commit -am "feat(zenoh_gis_query): NavSatFix CDR decode"`

---

### Task 5: Plot/sensor config loading unit

**Files:**
- Create: `include/zenoh_gis_query/plots.hpp`, `config/plots.geojson`, `config/sensors.yaml`
- Test: `test/test_plots.cpp`

**Interfaces:**
- Produces: `struct Plot{std::string id; std::vector<LatLon> ring;}; struct Sensor{std::string id; LatLon pos;}; std::vector<Plot> load_plots_geojson(const std::string& json_text); std::vector<Sensor> load_sensors_json(const std::string& json_text);`
- (Sensors loaded from a small JSON array to avoid a YAML dep here; the param file points at a `.json`.)

- [ ] **Step 1: Write the failing test**
```cpp
// Apache-2.0 header
#include "gtest/gtest.h"
#include "zenoh_gis_query/plots.hpp"
using zenoh_gis_query::load_plots_geojson;
using zenoh_gis_query::load_sensors_json;

TEST(Plots, ParsesPolygonFeature) {
  const char * gj = R"({"type":"FeatureCollection","features":[
    {"type":"Feature","properties":{"id":"P12"},
     "geometry":{"type":"Polygon","coordinates":[[[3.0,48.0],[3.0,48.1],[3.1,48.1],[3.1,48.0],[3.0,48.0]]]}}]})";
  auto plots = load_plots_geojson(gj);
  ASSERT_EQ(plots.size(), 1u);
  EXPECT_EQ(plots[0].id, "P12");
  ASSERT_GE(plots[0].ring.size(), 4u);
  EXPECT_DOUBLE_EQ(plots[0].ring[0].lon, 3.0);   // GeoJSON is [lon,lat]
  EXPECT_DOUBLE_EQ(plots[0].ring[0].lat, 48.0);
}
TEST(Sensors, ParsesArray) {
  const char * js = R"([{"id":"S3","lat":48.05,"lon":3.05}])";
  auto s = load_sensors_json(js);
  ASSERT_EQ(s.size(), 1u);
  EXPECT_EQ(s[0].id, "S3");
  EXPECT_DOUBLE_EQ(s[0].pos.lat, 48.05);
}
```
- [ ] **Step 2: Run tests, verify FAILS.**
- [ ] **Step 3: Implement** — `include/zenoh_gis_query/plots.hpp`
```cpp
// Apache-2.0 header
#ifndef ZENOH_GIS_QUERY__PLOTS_HPP_
#define ZENOH_GIS_QUERY__PLOTS_HPP_
#include <string>
#include <vector>
#include "nlohmann/json.hpp"
#include "zenoh_gis_query/geometry.hpp"
namespace zenoh_gis_query
{
struct Plot { std::string id; std::vector<LatLon> ring; };
struct Sensor { std::string id; LatLon pos; };

inline std::vector<Plot> load_plots_geojson(const std::string & json_text)
{
  std::vector<Plot> plots;
  auto j = nlohmann::json::parse(json_text, nullptr, false);
  if (j.is_discarded() || !j.contains("features")) {return plots;}
  for (const auto & f : j["features"]) {
    Plot p;
    if (f.contains("properties") && f["properties"].contains("id")) {
      p.id = f["properties"]["id"].get<std::string>();
    }
    const auto & geom = f["geometry"];
    if (!geom.contains("coordinates") || geom["coordinates"].empty()) {continue;}
    for (const auto & pt : geom["coordinates"][0]) {     // outer ring; [lon,lat]
      if (pt.size() >= 2) {p.ring.push_back({pt[1].get<double>(), pt[0].get<double>()});}
    }
    plots.push_back(std::move(p));
  }
  return plots;
}
inline std::vector<Sensor> load_sensors_json(const std::string & json_text)
{
  std::vector<Sensor> out;
  auto j = nlohmann::json::parse(json_text, nullptr, false);
  if (j.is_discarded() || !j.is_array()) {return out;}
  for (const auto & s : j) {
    out.push_back({s.value("id", ""), {s.value("lat", 0.0), s.value("lon", 0.0)}});
  }
  return out;
}
}  // namespace zenoh_gis_query
#endif  // ZENOH_GIS_QUERY__PLOTS_HPP_
```
Create `config/plots.geojson` (one INRAE-plot-shaped polygon with `"id":"P12"`) and `config/sensors.json` (`[{"id":"S3","lat":48.05,"lon":3.05}]`).
- [ ] **Step 4: Run tests.** Expected: `test_plots.gtest.xml: 2 tests, 0 failures`.
- [ ] **Step 5: Commit** — `git commit -am "feat(zenoh_gis_query): GeoJSON plot + sensor loading"`

---

### Task 6: Lifecycle node + Zenoh session + liveliness membership

**Files:**
- Create: `include/zenoh_gis_query/zenoh_gis_query_node.hpp`, `src/zenoh_gis_query_node.cpp` (replace the stub), `config/zenoh_gis_query.param.yaml`, `launch/zenoh_gis_query.launch.py`, `README.md`, `LICENSE`

**Interfaces:**
- Consumes: `geometry.hpp`, `navsatfix_decode.hpp`, `selector.hpp`, `qod.hpp`, `plots.hpp`.
- Produces: `class ZenohGisQueryNode : public rclcpp_lifecycle::LifecycleNode` with params `zenoh.mode`, `zenoh.connect`, `zenoh.key_expr` (default `ros2/**`), `zenoh.live_key_prefix` (default `gis/live`), `gis.plots_path`, `gis.sensors_path`, `gis.fidelity_horizon_ms` (default 2000). On activate: open session, subscribe to `gis/live/*` liveliness (maintain `std::set<std::string> live_robots_` under mutex), subscribe `ros2/**` to maintain `std::unordered_map<std::string, FixSample> latest_` (key = robot id parsed from `ros2/<robot>/...`).

- [ ] **Step 1: Write the node header** — `zenoh_gis_query_node.hpp` (lifecycle methods; `ZenohRuntime` PIMPL holding `zenoh::Session` + subscribers + queryables; `std::mutex state_mutex_` guarding `live_robots_` and `latest_`; helper `std::string robot_from_key(const std::string&)`).
```cpp
// Apache-2.0 header — full class declaration mirroring zenoh_source_node.hpp:
//  - CallbackReturn on_configure/activate/deactivate/cleanup/shutdown
//  - private: ZenohParameters{mode,connect,key_expr,live_key_prefix}; std::string plots_path_, sensors_path_; int fidelity_horizon_ms_{2000};
//    std::vector<Plot> plots_; std::vector<Sensor> sensors_;
//    std::mutex state_mutex_; std::set<std::string> live_robots_; std::unordered_map<std::string, FixSample> latest_;
//    struct ZenohRuntime; std::shared_ptr<ZenohRuntime> rt_;
//    void on_position_sample(const std::string& key, const std::vector<uint8_t>& payload);
//    static std::string robot_from_key(const std::string& key, const std::string& prefix="ros2");
```
- [ ] **Step 2: Implement the node .cpp** — session open (reuse the `zenoh_source` config-building block: `Config::create_default` + `insert_json5("mode"/"connect/endpoints")`), liveliness subscriber via `session.liveliness().declare_subscriber(KeyExpr(live_key_prefix + "/*"), on_liveliness, closures::none)` updating `live_robots_` (PUT→insert, DELETE→erase), and `ros2/**` subscriber decoding each sample into `latest_`. `robot_from_key("ros2/robot_3/gps/fix")` returns `"robot_3"` (segment after the `ros2/` prefix). Load plots/sensors in `on_configure` from the configured paths.
```cpp
// key callback bodies (inside the lambdas):
auto on_liveliness = [this](const zenoh::Sample & s) {
    std::string key{s.get_keyexpr().as_string_view()};      // gis/live/robot_3
    std::string robot = key.substr(key.rfind('/') + 1);
    std::lock_guard<std::mutex> lk(state_mutex_);
    if (s.get_kind() == Z_SAMPLE_KIND_PUT) {live_robots_.insert(robot);}
    else {live_robots_.erase(robot);}
  };
auto on_pos = [this](const zenoh::Sample & s) {
    std::string key{s.get_keyexpr().as_string_view()};
    auto fix = decode_navsatfix(s.get_payload().as_vector());
    if (!fix.valid) {return;}
    std::lock_guard<std::mutex> lk(state_mutex_);
    latest_[robot_from_key(key)] = fix;
  };
```
Create `config/zenoh_gis_query.param.yaml` (mode peer, key_expr `ros2/**`, plots/sensors paths default to installed `config/`), `launch/zenoh_gis_query.launch.py` (mirror `zenoh_source_container.launch.py`), `README.md`, copy `LICENSE`.
- [ ] **Step 3: Build (integration smoke)** — rebuild builder, then:
```bash
docker build -f docker/Dockerfile --build-arg ROS_DISTRO=humble -t rkd:zenoh-gis .
docker run --rm rkd:zenoh-gis bash -lc '
 source /opt/ros/humble/setup.bash && source /ws/install/setup.bash
 PF=$(find /ws/install -name zenoh_gis_query.param.yaml|head -1)
 ros2 run zenoh_gis_query zenoh_gis_query_node_exe --ros-args --params-file "$PF" -r __node:=zenoh_gis_query >/tmp/q.log 2>&1 &
 sleep 6; ros2 lifecycle set /zenoh_gis_query configure; ros2 lifecycle set /zenoh_gis_query activate
 sleep 2; ros2 lifecycle get /zenoh_gis_query; grep -iE "Configured|Activated|error" /tmp/q.log | tail -5'
```
Expected: node reaches `active [3]`, log shows plots loaded, no errors.
- [ ] **Step 4: Run unit suite** — `run_pkg_test zenoh_gis_query`. Expected: all `test_*` pass (geometry/selector/qod/navsatfix_decode/plots).
- [ ] **Step 5: Commit** — `git commit -am "feat(zenoh_gis_query): lifecycle node, Zenoh session, liveliness membership"`

---

### Task 7: Liveliness token in zenoh_sink

**Files:**
- Modify: `zenoh_bridge/zenoh_sink/include/zenoh_sink/zenoh_sink_node.hpp` (add `std::string liveliness_key;` to `ZenohParameters`), `src/zenoh_sink_node.cpp` (declare param `gis.liveliness_key` default `""`; in `ZenohClient`, after session open, if non-empty declare a liveliness token; hold it in the client so it drops on close).

**Interfaces:**
- Consumes: the existing `ZenohClient`.
- Produces: when `gis.liveliness_key` is set (e.g. `gis/live/robot_3`), the sink declares `session.liveliness().declare_token(KeyExpr(key))` held in a member `std::optional<zenoh::LivelinessToken> live_token_`.

- [ ] **Step 1: Write the integration test as a runnable check** (no gtest — liveliness is runtime). Add a test script `zenoh_bridge/zenoh_sink/test/liveliness_smoke.sh` documenting the expected behavior (sink declares token; a `z_sub` on `@/**` or the gis_query node sees membership). This is verified in Step 4.
- [ ] **Step 2: Implement** — in `zenoh_sink_node.cpp` `ZenohClient::open(...)`, add a `const std::string & liveliness_key` parameter; after `session_` is created:
```cpp
if (!liveliness_key.empty()) {
  live_token_.emplace(session_->liveliness().declare_token(zenoh::KeyExpr(liveliness_key)));
}
```
Add member `std::optional<zenoh::LivelinessToken> live_token_;`; clear it in `close()` before `session_.reset()`. Thread `zenoh_parameters_.liveliness_key` from `start_client()`. Declare the param in the constructor and read it in `configure_zenoh_parameters`.
- [ ] **Step 3: Build** — `docker build ... -t rkd:zenoh-gis .` Expected: `zenoh_sink` + `zenoh_gis_query` compile.
- [ ] **Step 4: Integration test (membership end-to-end)** — run sink (with `gis.liveliness_key:=gis/live/robot_1`, publishing `/demo/fix`) + gis_query in one container; confirm the query node logs robot_1 as live, then kill the sink and confirm it's dropped. Command pattern mirrors the round-trip test from the zenoh_source work. Expected: live set `{robot_1}` then `{}` after kill.
- [ ] **Step 5: Commit** — `git commit -am "feat(zenoh_sink): declare gis liveliness token for brokerless membership"`

---

### Task 8: Geofence queryable (pull) + QoD reply

**Files:**
- Modify: `src/zenoh_gis_query_node.cpp` (declare queryable on `gis/query/geofence`)

**Interfaces:**
- Consumes: `plots_`, `latest_`, `live_robots_`, `point_in_polygon`, `compute_qod`, `parse_selector_params`.
- Produces: a Zenoh queryable that, on a `get` with selector `?plot=<id>`, replies JSON `{"plot":..,"inside":[robots],"qod":{completeness,fidelity}}` plus the QoD as a reply attachment.

- [ ] **Step 1: Implement the queryable** in `on_activate`:
```cpp
auto on_geofence = [this](const zenoh::Query & q) {
    auto params = parse_selector_params(std::string{q.get_keyexpr().as_string_view()} +
      (q.get_parameters().empty() ? "" : "?" + std::string{q.get_parameters()}));
    const std::string plot_id = params.count("plot") ? params["plot"] : "";
    nlohmann::json inside = nlohmann::json::array();
    uint64_t contributed = 0, expected = 0; int64_t max_stale = 0;
    const int64_t now = this->get_clock()->now().nanoseconds();
    std::lock_guard<std::mutex> lk(state_mutex_);
    const Plot * plot = nullptr;
    for (const auto & p : plots_) {if (p.id == plot_id) {plot = &p;}}
    expected = live_robots_.size();
    for (const auto & robot : live_robots_) {
      auto it = latest_.find(robot);
      if (it == latest_.end()) {continue;}
      contributed++;
      max_stale = std::max(max_stale, now - it->second.stamp_ns);
      if (plot && point_in_polygon({it->second.lat, it->second.lon}, plot->ring)) {
        inside.push_back(robot);
      }
    }
    auto qod = compute_qod(contributed, expected, max_stale,
      static_cast<int64_t>(fidelity_horizon_ms_) * 1'000'000LL);
    nlohmann::json reply = {{"plot", plot_id}, {"inside", inside},
      {"qod", {{"completeness", qod.completeness}, {"fidelity", qod.fidelity}}}};
    zenoh::Query::ReplyOptions ro;
    ro.attachment = zenoh::Bytes(reply["qod"].dump());
    q.reply(q.get_keyexpr(), zenoh::Bytes(reply.dump()), std::move(ro));
  };
rt_->geofence_q.emplace(rt_->session->declare_queryable(
    zenoh::KeyExpr("gis/query/geofence"), std::move(on_geofence), zenoh::closures::none));
```
(Add `std::optional<zenoh::Queryable<void>> geofence_q;` to `ZenohRuntime`.)
- [ ] **Step 2: Build** — `docker build ... -t rkd:zenoh-gis .`
- [ ] **Step 3: Integration test (geofence end-to-end)** — in one container: gis_query + a sink publishing a NavSatFix at a coordinate inside `P12` with `gis.liveliness_key:=gis/live/robot_1`; then `ros2 run`/a small `z_get` client (or `zenohd` + `z_get` example if available; otherwise a tiny Python `zenoh` get script written to /tmp) querying `gis/query/geofence?plot=P12`. Expected reply JSON contains `"inside":["robot_1"]` and `qod.completeness == 1.0`.
```bash
# python get client written inside the container:
python3 - <<'PY'
import zenoh, json, time
s = zenoh.open(zenoh.Config())
for r in s.get('gis/query/geofence?plot=P12', zenoh.Queue(), timeout=5):
    print("REPLY", r.ok.payload.decode())
PY
```
Expected: `REPLY {"plot":"P12","inside":["robot_1"],"qod":{"completeness":1.0,...}}`.
- [ ] **Step 4: Commit** — `git commit -am "feat(zenoh_gis_query): geofence pull queryable with QoD"`

---

### Task 9: Geofence continuous mode → gis/alert

**Files:**
- Modify: `src/zenoh_gis_query_node.cpp` (add a publisher + per-sample crossing check)

**Interfaces:**
- Produces: on each `ros2/**` sample, after updating `latest_`, if the robot's inside/outside state for any plot **changed**, publish a crossing event to `gis/alert/geofence/<robot>` (JSON `{robot,plot,event:"enter"|"exit",stamp_ns}`). Maintains `std::unordered_map<std::string,std::set<std::string>> inside_state_` (robot → plots currently inside).

- [ ] **Step 1: Implement** the crossing detection inside `on_position_sample` (compare new inside-set vs `inside_state_[robot]`, emit enter/exit deltas via a declared `zenoh::Publisher` on `gis/alert/geofence/*`). This is the latency-benchmark path (publish timestamp − sample stamp).
- [ ] **Step 2: Build** — `docker build ... -t rkd:zenoh-gis .`
- [ ] **Step 3: Integration test** — sink replays a short trajectory crossing P12's boundary; a `z_sub` on `gis/alert/geofence/**` captures an `enter` then `exit` event. Expected: two alerts with correct `event` values.
- [ ] **Step 4: Commit** — `git commit -am "feat(zenoh_gis_query): continuous geofence crossing alerts"`

---

### Task 10: Collision queryable

**Files:**
- Modify: `src/zenoh_gis_query_node.cpp` (queryable on `gis/query/collision`)

**Interfaces:**
- Produces: on `get gis/query/collision?radius=<m>`, reply JSON `{pairs:[[a,b,dist],...], qod}` using `haversine_m` over all live robots' `latest_` (pairwise; no partitioning — the architectural win). Default radius 2.0 m.

- [ ] **Step 1: Implement** the collision queryable (double loop over live robots with a position, `haversine_m < radius` → pair). Reuse the QoD pattern from Task 8.
- [ ] **Step 2: Build.**
- [ ] **Step 3: Integration test** — two sinks (`robot_1`, `robot_2`) publishing fixes ~1 m apart; `get gis/query/collision?radius=2.0` returns a pair `[robot_1,robot_2,~1.0]`; with `radius=0.5` returns no pairs.
- [ ] **Step 4: Commit** — `git commit -am "feat(zenoh_gis_query): collision queryable (pairwise, no partitioning)"`

---

### Task 11: Proximity queryable

**Files:**
- Modify: `src/zenoh_gis_query_node.cpp` (queryable on `gis/query/proximity`)

**Interfaces:**
- Produces: on `get gis/query/proximity?sensor=<id>&range=<m>`, reply JSON `{sensor, near:[[robot,dist],...], qod}` using `haversine_m` between each live robot and the named `Sensor` from `sensors_`.

- [ ] **Step 1: Implement** the proximity queryable (look up sensor by id; distance each live robot → sensor; include if `< range`, default 10 m).
- [ ] **Step 2: Build.**
- [ ] **Step 3: Integration test** — one sink near sensor `S3`; `get gis/query/proximity?sensor=S3&range=10` returns the robot; `range=1` returns none.
- [ ] **Step 4: Commit** — `git commit -am "feat(zenoh_gis_query): sensor proximity queryable"`

---

### Task 12: RocksDB storage config + history-aware get

**Files:**
- Create: `config/zenoh_storage_rocksdb.json5`
- Modify: `README.md` (document running the router with storage), `src/zenoh_gis_query_node.cpp` (in pull queryables, if a live robot has no in-memory `latest_`, fall back to `session.get("ros2/<robot>/gps/fix")` to pull its last stored value from the storage)

**Interfaces:**
- Produces: a storage-manager router config persisting `ros2/**` to RocksDB; the queryables transparently use stored last-values when a robot is between live samples or recently disconnected (bounded by liveliness).

- [ ] **Step 1: Create** `config/zenoh_storage_rocksdb.json5`:
```json5
{
  plugins: {
    storage_manager: {
      volumes: { rocksdb: {} },
      storages: {
        ros2_history: {
          key_expr: "ros2/**",
          volume: { id: "rocksdb", dir: "/ws/zenoh_storage", create_db: true }
        }
      }
    }
  }
}
```
- [ ] **Step 2: Implement** the storage fallback in the pull queryables: for a live robot missing from `latest_`, issue a bounded `session.get(("ros2/" + robot + "/gps/fix"))`, decode the first reply, and treat staleness from its timestamp (feeds QoD fidelity). Guard with a short timeout.
- [ ] **Step 3: Build.**
- [ ] **Step 4: Integration test (history)** — launch `zenohd --config config/zenoh_storage_rocksdb.json5` (or `ros2 run rmw_zenoh_cpp rmw_zenohd` if that ships the plugin; else the standalone `zenohd` from the vendor) + sink publishing a few fixes; stop the sink (liveliness still briefly present) and confirm a `get gis/query/geofence?plot=P12` still returns the last position from storage with reduced `qod.fidelity`. Document the exact `zenohd` invocation available in the image; if `zenohd` is not packaged, add it to the runtime image in this task (apt `ros-humble-zenoh-...` or the zenoh router binary) and note it in README.
- [ ] **Step 5: Commit** — `git commit -am "feat(zenoh_gis_query): RocksDB storage + history-aware queryable fallback"`

---

### Task 13: Baseline benchmark hook

**Files:**
- Create: `tools/zenoh_gis/run_baseline.sh`, `tools/zenoh_gis/README.md`
- Modify: reuse `tools/benchmark/scaling/` rosbag replay where possible

**Interfaces:**
- Produces: a script that replays the INRAE rosbag at N ∈ {1,5,10,25,50} as N renamed NavSatFix streams through N `zenoh_sink` instances (each with its own `gis.liveliness_key` and key prefix), runs `zenoh_gis_query`, drives the continuous geofence path, and records alert latency (publish stamp − sample stamp), throughput, and drop into CSV — matching the columns of the SIGSPATIAL scaling table.

- [ ] **Step 1: Write** `tools/zenoh_gis/run_baseline.sh` reusing the rosbag at repo root (`rorbots_follower_leader_parcelle_1MONT.bag`) and the scaling harness's replay/rename approach; emit `results/zenoh_baseline_N<n>.csv`.
- [ ] **Step 2: Dry-run** at N=1 in the container; confirm a CSV with non-zero `decoded` and a latency column is produced.
- [ ] **Step 3: Document** the comparison mapping (which CSV column maps to which SIGSPATIAL table cell) in `tools/zenoh_gis/README.md`.
- [ ] **Step 4: Commit** — `git commit -am "feat(tools): zenoh_gis baseline benchmark hook"`

---

## Self-Review

**Spec coverage:** queryable-centric compute (Tasks 6,8–11) ✓; RocksDB storage (Task 12) ✓; liveliness membership (Tasks 6,7) ✓; QoD on replies (Tasks 3,8) ✓; QoS knobs reused (existing zenoh_sink, exercised in Task 13) ✓; three GIS queries (Tasks 8–11) ✓; CDR end-to-end decode (Task 4) ✓; baseline harness vs papers (Task 13) ✓. Deferred to Phase 2 per spec §2/§6: distributed topology, `tc netem` fault injection, InfluxDB+Grafana — intentionally **not** in this plan.

**Placeholder scan:** spatial/parse/QoD/decode tasks contain full code. Node-wiring tasks (6–12) give the load-bearing callback bodies + exact zenoh-cpp calls; remaining boilerplate (param declaration, launch file, README) is "mirror file X" against a named existing file, which the implementer can copy verbatim — acceptable, not a vague placeholder.

**Type consistency:** `LatLon`, `FixSample`, `Plot`, `Sensor`, `QoD`, `parse_selector_params`, `decode_navsatfix`, `point_in_polygon`, `haversine_m`, `compute_qod` names/signatures are consistent across producing and consuming tasks. `live_robots_` / `latest_` / `inside_state_` member names consistent in Tasks 6,8,9.

**Known verification risk (carry into execution):** Task 12 depends on a Zenoh **router with the storage-manager plugin** being available in the image. If `zenohd` isn't packaged by `zenoh_cpp_vendor`, Task 12 Step 4 must add the router binary to the runtime image (apt or the zenoh release) — this is called out inline. Tasks 1–11 and 13 do not depend on the router.
