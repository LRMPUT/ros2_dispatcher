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
#include <string>
#include <utility>
#include <vector>

#include "ament_index_cpp/get_package_share_directory.hpp"
#include "nlohmann/json.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "zenoh.hxx"

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
}  // namespace

// ZenohRuntime members are destroyed in reverse declaration order:
// subscribers (liveliness_sub, position_sub) before session.
struct ZenohGisQueryNode::ZenohRuntime
{
  std::unique_ptr<zenoh::Session> session;
  std::optional<zenoh::Subscriber<void>> liveliness_sub;
  std::optional<zenoh::Subscriber<void>> position_sub;
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
        std::string key{s.get_keyexpr().as_string_view()};
        std::string robot = key.substr(key.rfind('/') + 1);
        std::lock_guard<std::mutex> lk(state_mutex_);
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
  auto rt = std::exchange(rt_, nullptr);
  (void)rt;
}

void ZenohGisQueryNode::on_position_sample(
  const std::string & key, const std::vector<uint8_t> & payload)
{
  auto fix = decode_navsatfix(payload);
  if (!fix.valid) {return;}
  std::lock_guard<std::mutex> lk(state_mutex_);
  latest_[robot_from_key(key)] = fix;
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
