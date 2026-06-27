// Copyright 2025 Maciej Krupka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "zenoh_gis_query/zenoh_gis_query_node.hpp"

#include <fstream>
#include <iterator>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "ament_index_cpp/get_package_share_directory.hpp"
#include "nlohmann/json.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "zenoh.hxx"
#include "zenoh_gis_query/qod.hpp"
#include "zenoh_gis_query/selector.hpp"

namespace zenoh_gis_query
{
namespace
{
std::string endpoints_to_json5(const std::vector<std::string> & endpoints)
{
  nlohmann::json arr = nlohmann::json::array();
  for (const auto & ep : endpoints) {
    arr.push_back(ep);
  }
  return arr.dump();
}

bool read_file(const std::string & path, std::string * out, std::string * error_message)
{
  std::ifstream f(path);
  if (!f.is_open()) {
    if (error_message) {
      *error_message = "Cannot open file: " + path;
    }
    return false;
  }
  *out = std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
  return true;
}

/// @brief Attempt to pull the latest stored NavSatFix for @p robot from a Zenoh
///        storage (e.g. zenohd + storage-manager + rocksdb backend).
///
/// Issues a bounded Session::get with a 200 ms timeout and decodes the first
/// reply.  Returns an invalid FixSample{} when no reply arrives or the payload
/// cannot be decoded.
///
/// CONCURRENCY: must be called WITHOUT holding state_mutex_. The caller is
/// responsible for snapshotting rt_ into a local shared_ptr while holding the
/// lock, then releasing the lock before passing *session here.
FixSample fetch_stored_fix(
  zenoh::Session & session,
  const std::string & robot,
  const rclcpp::Logger & logger)
{
  try {
    zenoh::Session::GetOptions opts = zenoh::Session::GetOptions::create_default();
    opts.timeout_ms = 200;
    auto handler = session.get(
      zenoh::KeyExpr("ros2/" + robot + "/gps/fix"), "",
      zenoh::channels::FifoChannel(1), std::move(opts));
    // recv() blocks until a reply arrives or the 200 ms timeout closes the channel.
    auto result = handler.recv();
    if (auto * rp = std::get_if<zenoh::Reply>(&result)) {
      if (rp->is_ok()) {
        return decode_navsatfix(rp->get_ok().get_payload().as_vector());
      }
    }
  } catch (const zenoh::ZException & ex) {
    RCLCPP_DEBUG(logger, "Storage get for '%s': %s", robot.c_str(), ex.what());
  }
  return FixSample{};
}
}  // namespace

// ZenohRuntime members are destroyed in reverse declaration order:
// queryable and subscribers before session.
struct ZenohGisQueryNode::ZenohRuntime
{
  std::unique_ptr<zenoh::Session> session;
  std::optional<zenoh::Subscriber<void>> liveliness_sub;
  std::optional<zenoh::Subscriber<void>> position_sub;
  std::optional<zenoh::Queryable<void>> geofence_q;
  std::optional<zenoh::Queryable<void>> collision_q;
  std::optional<zenoh::Queryable<void>> proximity_q;
};

ZenohGisQueryNode::ZenohGisQueryNode(const rclcpp::NodeOptions & options)
: rclcpp_lifecycle::LifecycleNode("zenoh_gis_query", options)
{
  this->declare_parameter<std::string>("zenoh.mode", zenoh_parameters_.mode);
  this->declare_parameter<std::vector<std::string>>("zenoh.connect", zenoh_parameters_.connect);
  this->declare_parameter<std::string>("zenoh.key_expr", zenoh_parameters_.key_expr);
  this->declare_parameter<std::string>("zenoh.live_key_prefix", zenoh_parameters_.live_key_prefix);
  this->declare_parameter<std::string>("gis.plots_path", plots_path_);
  this->declare_parameter<std::string>("gis.sensors_path", sensors_path_);
  this->declare_parameter<int>("gis.fidelity_horizon_ms", fidelity_horizon_ms_);
}

ZenohGisQueryNode::CallbackReturn ZenohGisQueryNode::on_configure(
  const rclcpp_lifecycle::State &)
{
  zenoh_parameters_.mode = this->get_parameter("zenoh.mode").as_string();
  zenoh_parameters_.connect = this->get_parameter("zenoh.connect").as_string_array();
  zenoh_parameters_.key_expr = this->get_parameter("zenoh.key_expr").as_string();
  zenoh_parameters_.live_key_prefix = this->get_parameter("zenoh.live_key_prefix").as_string();
  plots_path_ = this->get_parameter("gis.plots_path").as_string();
  sensors_path_ = this->get_parameter("gis.sensors_path").as_string();
  fidelity_horizon_ms_ = static_cast<int>(
    this->get_parameter("gis.fidelity_horizon_ms").as_int());

  const std::string share_dir =
    ament_index_cpp::get_package_share_directory("zenoh_gis_query");
  if (plots_path_.empty()) {
    plots_path_ = share_dir + "/config/plots.geojson";
  }
  if (sensors_path_.empty()) {
    sensors_path_ = share_dir + "/config/sensors.json";
  }

  std::string plots_json;
  std::string err;
  if (!read_file(plots_path_, &plots_json, &err)) {
    RCLCPP_ERROR(get_logger(), "Failed to read plots: %s", err.c_str());
    return CallbackReturn::FAILURE;
  }
  plots_ = load_plots_geojson(plots_json);

  std::string sensors_json;
  if (!read_file(sensors_path_, &sensors_json, &err)) {
    RCLCPP_ERROR(get_logger(), "Failed to read sensors: %s", err.c_str());
    return CallbackReturn::FAILURE;
  }
  sensors_ = load_sensors_json(sensors_json);

  RCLCPP_INFO(
    get_logger(),
    "Configured zenoh_gis_query: loaded %zu plots from '%s', %zu sensors from '%s'",
    plots_.size(), plots_path_.c_str(),
    sensors_.size(), sensors_path_.c_str());
  return CallbackReturn::SUCCESS;
}

ZenohGisQueryNode::CallbackReturn ZenohGisQueryNode::on_activate(
  const rclcpp_lifecycle::State &)
{
  std::string error_message;
  if (!start_session(&error_message)) {
    RCLCPP_ERROR(get_logger(), "Failed to open Zenoh session: %s", error_message.c_str());
    return CallbackReturn::FAILURE;
  }
  is_active_.store(true, std::memory_order_release);
  RCLCPP_INFO(
    get_logger(), "Activated zenoh_gis_query on key_expr '%s', liveliness prefix '%s'",
    zenoh_parameters_.key_expr.c_str(), zenoh_parameters_.live_key_prefix.c_str());
  return CallbackReturn::SUCCESS;
}

ZenohGisQueryNode::CallbackReturn ZenohGisQueryNode::on_deactivate(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_session();
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    live_robots_.clear();
    latest_.clear();
    inside_state_.clear();
  }
  RCLCPP_INFO(get_logger(), "Deactivated zenoh_gis_query");
  return CallbackReturn::SUCCESS;
}

ZenohGisQueryNode::CallbackReturn ZenohGisQueryNode::on_cleanup(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_session();
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    live_robots_.clear();
    latest_.clear();
    inside_state_.clear();
  }
  plots_.clear();
  sensors_.clear();
  RCLCPP_INFO(get_logger(), "Cleaned up zenoh_gis_query");
  return CallbackReturn::SUCCESS;
}

ZenohGisQueryNode::CallbackReturn ZenohGisQueryNode::on_shutdown(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_session();
  RCLCPP_INFO(get_logger(), "Shutting down zenoh_gis_query");
  return CallbackReturn::SUCCESS;
}

bool ZenohGisQueryNode::start_session(std::string * error_message)
{
  auto runtime = std::make_shared<ZenohRuntime>();
  try {
    zenoh::Config config = zenoh::Config::create_default();

    if (!zenoh_parameters_.mode.empty()) {
      config.insert_json5("mode", std::string("\"") + zenoh_parameters_.mode + "\"");
    }
    if (!zenoh_parameters_.connect.empty()) {
      config.insert_json5(
        "connect/endpoints", endpoints_to_json5(zenoh_parameters_.connect));
    }

    runtime->session =
      std::make_unique<zenoh::Session>(zenoh::Session::open(std::move(config)));

    auto on_liveliness = [this](const zenoh::Sample & s) {
        if (!is_active_.load(std::memory_order_acquire)) {return;}
        std::string key{s.get_keyexpr().as_string_view()};
        std::string robot = key.substr(key.rfind('/') + 1);
        std::lock_guard<std::mutex> lk(state_mutex_);
        if (!is_active_.load(std::memory_order_acquire)) {return;}
        if (s.get_kind() == Z_SAMPLE_KIND_PUT) {
          live_robots_.insert(robot);
        } else {
          live_robots_.erase(robot);
        }
      };

    runtime->liveliness_sub.emplace(
      runtime->session->liveliness_declare_subscriber(
        zenoh::KeyExpr(zenoh_parameters_.live_key_prefix + "/*"),
        std::move(on_liveliness),
        zenoh::closures::none));

    auto on_pos = [this](const zenoh::Sample & s) {
        if (!is_active_.load(std::memory_order_acquire)) {return;}
        if (s.get_kind() != Z_SAMPLE_KIND_PUT) {return;}
        std::string key{s.get_keyexpr().as_string_view()};
        std::vector<uint8_t> payload = s.get_payload().as_vector();
        on_position_sample(key, payload);
      };

    runtime->position_sub.emplace(
      runtime->session->declare_subscriber(
        zenoh::KeyExpr(zenoh_parameters_.key_expr),
        std::move(on_pos),
        zenoh::closures::none));

    // --- geofence queryable ---------------------------------------------------
    // For each live robot: if its position is in latest_ use it directly; if
    // not, attempt a bounded session.get to pull the last stored value from
    // a Zenoh storage (e.g. zenohd + storage-manager + rocksdb backend).
    // CONCURRENCY: rt_ is snapshotted inside state_mutex_ and the get is
    // issued AFTER the lock is released (never hold state_mutex_ across get).
    auto on_geofence = [this](const zenoh::Query & q) {
        if (!is_active_.load(std::memory_order_acquire)) {return;}
        // Parse selector params: reconstruct "key?params" for parse_selector_params.
        const auto params_sv = q.get_parameters();
        const std::string full_selector =
          std::string{q.get_keyexpr().as_string_view()} +
        (params_sv.empty() ? "" : "?" + std::string{params_sv});
        const auto params = parse_selector_params(full_selector);
        const std::string plot_id = params.count("plot") ? params.at("plot") : "";

        const int64_t now = this->get_clock()->now().nanoseconds();

        // plots_ is immutable after on_configure: safe to read without the lock.
        const Plot * plot = nullptr;
        for (const auto & p : plots_) {
          if (p.id == plot_id) {plot = &p; break;}
        }

        // Snapshot mutable state under lock; also capture rt_ for the get below.
        std::set<std::string> live_snap;
        std::unordered_map<std::string, FixSample> latest_snap;
        std::shared_ptr<ZenohRuntime> rt_snap;
        {
          std::lock_guard<std::mutex> lk(state_mutex_);
          live_snap = live_robots_;
          latest_snap = latest_;
          rt_snap = rt_;
        }

        // Attempt storage get for robots missing from the in-memory snapshot.
        // Lock is NOT held here; uses rt_snap (local copy of rt_).
        if (rt_snap && rt_snap->session) {
          for (const auto & robot : live_snap) {
            if (latest_snap.find(robot) == latest_snap.end()) {
              auto fix = fetch_stored_fix(*rt_snap->session, robot, get_logger());
              if (fix.valid) {
                latest_snap[robot] = fix;
              }
            }
          }
        }

        // Compute from snapshots (no lock needed).
        nlohmann::json inside = nlohmann::json::array();
        uint64_t contributed = 0;
        const uint64_t expected = live_snap.size();
        int64_t max_stale = 0;
        for (const auto & robot : live_snap) {
          auto it = latest_snap.find(robot);
          if (it == latest_snap.end()) {continue;}
          contributed++;
          max_stale = std::max(max_stale, now - it->second.stamp_ns);
          if (plot && point_in_polygon({it->second.lat, it->second.lon}, plot->ring)) {
            inside.push_back(robot);
          }
        }

        // Build reply JSON (no lock held).
        // fidelity_horizon_ms_ set in on_configure; immutable after activation.
        const auto qod = compute_qod(
          contributed, expected, max_stale,
          static_cast<int64_t>(fidelity_horizon_ms_) * 1'000'000LL);
        const nlohmann::json reply_json = {
          {"plot", plot_id},
          {"inside", inside},
          {"qod", {{"completeness", qod.completeness}, {"fidelity", qod.fidelity}}}
        };
        zenoh::Query::ReplyOptions ro;
        ro.attachment = zenoh::Bytes(reply_json["qod"].dump());
        q.reply(q.get_keyexpr(), zenoh::Bytes(reply_json.dump()), std::move(ro));
      };

    runtime->geofence_q.emplace(
      runtime->session->declare_queryable(
        zenoh::KeyExpr("gis/query/geofence"),
        std::move(on_geofence),
        zenoh::closures::none));

    // --- collision queryable --------------------------------------------------
    auto on_collision = [this](const zenoh::Query & q) {
        if (!is_active_.load(std::memory_order_acquire)) {return;}
        // Parse selector params: read radius (default 2.0 m).
        const auto params_sv = q.get_parameters();
        const std::string full_selector =
          std::string{q.get_keyexpr().as_string_view()} +
        (params_sv.empty() ? "" : "?" + std::string{params_sv});
        const auto params = parse_selector_params(full_selector);
        double radius = 2.0;
        if (params.count("radius")) {
          try {
            radius = std::stod(params.at("radius"));
          } catch (...) {
            radius = 2.0;
          }
        }

        const int64_t now = this->get_clock()->now().nanoseconds();

        // Snapshot mutable state under lock; capture rt_ for storage fallback.
        std::set<std::string> live_snap;
        std::unordered_map<std::string, FixSample> latest_snap;
        std::shared_ptr<ZenohRuntime> rt_snap;
        {
          std::lock_guard<std::mutex> lk(state_mutex_);
          live_snap = live_robots_;
          latest_snap = latest_;
          rt_snap = rt_;
        }

        // Attempt storage get for robots missing from the in-memory snapshot.
        // Lock is NOT held here; uses rt_snap (local copy of rt_).
        if (rt_snap && rt_snap->session) {
          for (const auto & robot : live_snap) {
            if (latest_snap.find(robot) == latest_snap.end()) {
              auto fix = fetch_stored_fix(*rt_snap->session, robot, get_logger());
              if (fix.valid) {
                latest_snap[robot] = fix;
              }
            }
          }
        }

        // Compute from snapshots (no lock needed).
        nlohmann::json pairs = nlohmann::json::array();
        uint64_t contributed = 0;
        const uint64_t expected = live_snap.size();
        int64_t max_stale = 0;

        // Collect robots that have a known position and track staleness.
        std::vector<std::string> robots_with_pos;
        for (const auto & robot : live_snap) {
          auto it = latest_snap.find(robot);
          if (it == latest_snap.end()) {continue;}
          contributed++;
          max_stale = std::max(max_stale, now - it->second.stamp_ns);
          robots_with_pos.push_back(robot);
        }
        // Iterate over all unordered pairs (a, b) with a < b by index.
        for (size_t i = 0; i < robots_with_pos.size(); ++i) {
          for (size_t j = i + 1; j < robots_with_pos.size(); ++j) {
            const auto & ra = robots_with_pos[i];
            const auto & rb = robots_with_pos[j];
            const auto & fix_a = latest_snap.at(ra);
            const auto & fix_b = latest_snap.at(rb);
            const double dist =
              haversine_m({fix_a.lat, fix_a.lon}, {fix_b.lat, fix_b.lon});
            if (dist < radius) {
              pairs.push_back(nlohmann::json::array({ra, rb, dist}));
            }
          }
        }

        // Build reply JSON (no lock held).
        // fidelity_horizon_ms_ is immutable after activation.
        const auto qod = compute_qod(
          contributed, expected, max_stale,
          static_cast<int64_t>(fidelity_horizon_ms_) * 1'000'000LL);
        const nlohmann::json reply_json = {
          {"radius", radius},
          {"pairs", pairs},
          {"qod", {{"completeness", qod.completeness}, {"fidelity", qod.fidelity}}}
        };
        zenoh::Query::ReplyOptions ro;
        ro.attachment = zenoh::Bytes(reply_json["qod"].dump());
        q.reply(q.get_keyexpr(), zenoh::Bytes(reply_json.dump()), std::move(ro));
      };

    runtime->collision_q.emplace(
      runtime->session->declare_queryable(
        zenoh::KeyExpr("gis/query/collision"),
        std::move(on_collision),
        zenoh::closures::none));

    // --- proximity queryable --------------------------------------------------
    auto on_proximity = [this](const zenoh::Query & q) {
        if (!is_active_.load(std::memory_order_acquire)) {return;}
        // Parse selector params: read sensor id and range (default 10.0 m).
        const auto params_sv = q.get_parameters();
        const std::string full_selector =
          std::string{q.get_keyexpr().as_string_view()} +
        (params_sv.empty() ? "" : "?" + std::string{params_sv});
        const auto params = parse_selector_params(full_selector);
        const std::string sensor_id = params.count("sensor") ? params.at("sensor") : "";
        double range = 10.0;
        if (params.count("range")) {
          try {
            range = std::stod(params.at("range"));
          } catch (...) {
            range = 10.0;
          }
        }

        const int64_t now = this->get_clock()->now().nanoseconds();

        // sensors_ is immutable after on_configure: safe to read without the lock.
        const Sensor * sensor = nullptr;
        for (const auto & s : sensors_) {
          if (s.id == sensor_id) {sensor = &s; break;}
        }

        // Snapshot mutable state under lock; capture rt_ for storage fallback.
        std::set<std::string> live_snap;
        std::unordered_map<std::string, FixSample> latest_snap;
        std::shared_ptr<ZenohRuntime> rt_snap;
        {
          std::lock_guard<std::mutex> lk(state_mutex_);
          live_snap = live_robots_;
          latest_snap = latest_;
          rt_snap = rt_;
        }

        // Attempt storage get for robots missing from the in-memory snapshot.
        // Lock is NOT held here; uses rt_snap (local copy of rt_).
        if (rt_snap && rt_snap->session) {
          for (const auto & robot : live_snap) {
            if (latest_snap.find(robot) == latest_snap.end()) {
              auto fix = fetch_stored_fix(*rt_snap->session, robot, get_logger());
              if (fix.valid) {
                latest_snap[robot] = fix;
              }
            }
          }
        }

        // Compute from snapshots (no lock needed).
        nlohmann::json near = nlohmann::json::array();
        uint64_t contributed = 0;
        const uint64_t expected = live_snap.size();
        int64_t max_stale = 0;
        for (const auto & robot : live_snap) {
          auto it = latest_snap.find(robot);
          if (it == latest_snap.end()) {continue;}
          contributed++;
          max_stale = std::max(max_stale, now - it->second.stamp_ns);
          if (sensor) {
            const double dist =
              haversine_m({it->second.lat, it->second.lon}, sensor->pos);
            if (dist < range) {
              near.push_back(nlohmann::json::array({robot, dist}));
            }
          }
        }

        // Build reply JSON (no lock held).
        // fidelity_horizon_ms_ is immutable after activation.
        const auto qod = compute_qod(
          contributed, expected, max_stale,
          static_cast<int64_t>(fidelity_horizon_ms_) * 1'000'000LL);
        const nlohmann::json reply_json = {
          {"sensor", sensor_id},
          {"near", near},
          {"qod", {{"completeness", qod.completeness}, {"fidelity", qod.fidelity}}}
        };
        zenoh::Query::ReplyOptions ro;
        ro.attachment = zenoh::Bytes(reply_json["qod"].dump());
        q.reply(q.get_keyexpr(), zenoh::Bytes(reply_json.dump()), std::move(ro));
      };

    runtime->proximity_q.emplace(
      runtime->session->declare_queryable(
        zenoh::KeyExpr("gis/query/proximity"),
        std::move(on_proximity),
        zenoh::closures::none));
  } catch (const zenoh::ZException & ex) {
    if (error_message) {
      *error_message = ex.what();
    }
    return false;
  }

  rt_ = std::move(runtime);
  return true;
}

void ZenohGisQueryNode::stop_session()
{
  std::shared_ptr<ZenohRuntime> rt;
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    rt = std::move(rt_);  // rt_ becomes null under the lock
  }
  // rt destructs HERE, outside the lock. The subscriber/queryable destructors
  // block until in-flight Zenoh callbacks drain, and those callbacks take
  // state_mutex_ — so destroying under the lock would deadlock. Must be outside.
}

void ZenohGisQueryNode::on_position_sample(
  const std::string & key, const std::vector<uint8_t> & payload)
{
  auto fix = decode_navsatfix(payload);
  if (!fix.valid) {return;}
  const std::string robot = robot_from_key(key);

  // Crossing event: plot id + "enter" or "exit".
  struct CrossingEvent
  {
    std::string plot_id;
    std::string event;
  };
  std::vector<CrossingEvent> events;
  std::shared_ptr<ZenohRuntime> rt_snap;

  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    if (!is_active_.load(std::memory_order_acquire)) {return;}
    latest_[robot] = fix;

    // Compute new inside-set for this robot across all plots.
    std::set<std::string> new_inside;
    for (const auto & plot : plots_) {
      if (point_in_polygon({fix.lat, fix.lon}, plot.ring)) {
        new_inside.insert(plot.id);
      }
    }

    // Diff against previous inside-set to produce enter/exit events.
    auto & prev_inside = inside_state_[robot];
    for (const auto & pid : new_inside) {
      if (prev_inside.find(pid) == prev_inside.end()) {
        events.push_back({pid, "enter"});
      }
    }
    for (const auto & pid : prev_inside) {
      if (new_inside.find(pid) == new_inside.end()) {
        events.push_back({pid, "exit"});
      }
    }
    prev_inside = std::move(new_inside);
    // Snapshot rt_ under the lock so the refcount keeps ZenohRuntime alive
    // across the put calls below — avoids a data race with stop_session()
    // which does std::exchange(rt_, nullptr) on the deactivate thread.
    rt_snap = rt_;
  }

  // Publish crossing events outside the lock (do not hold state_mutex_ across put).
  // Use rt_snap (local copy), never the member rt_, to avoid the data race.
  if (!events.empty() && rt_snap && rt_snap->session) {
    const std::string alert_key = "gis/alert/geofence/" + robot;
    for (const auto & ev : events) {
      const nlohmann::json alert = {
        {"robot", robot},
        {"plot", ev.plot_id},
        {"event", ev.event},
        {"stamp_ns", fix.stamp_ns}
      };
      try {
        rt_snap->session->put(
          zenoh::KeyExpr(alert_key),
          zenoh::Bytes(alert.dump()));
      } catch (const zenoh::ZException & ex) {
        RCLCPP_WARN(get_logger(), "Geofence alert publish failed: %s", ex.what());
      }
    }
  }
}

// static
std::string ZenohGisQueryNode::robot_from_key(
  const std::string & key, const std::string & prefix)
{
  std::string pfx = prefix;
  if (!pfx.empty() && pfx.back() == '/') {pfx.pop_back();}
  pfx += '/';
  if (key.rfind(pfx, 0) != 0) {return key;}
  const std::string rest = key.substr(pfx.size());
  const auto slash = rest.find('/');
  if (slash == std::string::npos) {return rest;}
  return rest.substr(0, slash);
}

}  // namespace zenoh_gis_query

RCLCPP_COMPONENTS_REGISTER_NODE(zenoh_gis_query::ZenohGisQueryNode)
