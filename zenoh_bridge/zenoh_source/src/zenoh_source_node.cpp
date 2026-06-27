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

#include "zenoh_source/zenoh_source_node.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <functional>
#include <optional>
#include <utility>

#include "lifecycle_msgs/msg/state.hpp"
#include "nlohmann/json.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "zenoh.hxx"

#include "zenoh_source/ros_type_validation.hpp"

namespace zenoh_source
{
namespace
{
constexpr int64_t kThrottleIntervalNs = 1'000'000'000LL;  // 1 second

std::string endpoints_to_json5(const std::vector<std::string> & endpoints)
{
  nlohmann::json arr = nlohmann::json::array();
  for (const auto & ep : endpoints) {
    arr.push_back(ep);
  }
  return arr.dump();
}
}  // namespace

std::string derive_ros_topic(
  const std::string & key_expr,
  const std::string & key_prefix,
  const std::string & ros_topic_prefix)
{
  std::string suffix = key_expr;

  // Strip the leading "<key_prefix>/" (or an exact match) if present.
  if (!key_prefix.empty()) {
    std::string pfx = key_prefix;
    if (pfx.back() == '/') {
      pfx.pop_back();
    }
    if (suffix == pfx) {
      suffix.clear();
    } else if (suffix.rfind(pfx + "/", 0) == 0) {
      suffix = suffix.substr(pfx.size() + 1U);
    }
  }

  // Drop any leftover leading slashes from the suffix.
  while (!suffix.empty() && suffix.front() == '/') {
    suffix.erase(suffix.begin());
  }

  // Normalise the output prefix to a single leading slash, no trailing slash.
  std::string prefix = ros_topic_prefix;
  if (!prefix.empty() && prefix.back() == '/') {
    prefix.pop_back();
  }
  if (!prefix.empty() && prefix.front() != '/') {
    prefix.insert(prefix.begin(), '/');
  }

  if (prefix.empty()) {
    return "/" + suffix;
  }
  if (suffix.empty()) {
    return prefix;
  }
  return prefix + "/" + suffix;
}

struct ZenohSourceNode::ZenohRuntime
{
  // Declared before `subscriber` so it is destroyed AFTER it (members are torn
  // down in reverse declaration order): the subscriber must be undeclared while
  // the session is still open.
  std::unique_ptr<zenoh::Session> session;
  std::optional<zenoh::Subscriber<void>> subscriber;
};

ZenohSourceNode::ZenohSourceNode(const rclcpp::NodeOptions & options)
: rclcpp_lifecycle::LifecycleNode("zenoh_source", options)
{
  this->declare_parameter<std::string>("zenoh.mode", zenoh_parameters_.mode);
  this->declare_parameter<std::vector<std::string>>("zenoh.connect", zenoh_parameters_.connect);
  this->declare_parameter<std::vector<std::string>>("zenoh.listen", zenoh_parameters_.listen);
  this->declare_parameter<std::string>("zenoh.config_path", zenoh_parameters_.config_path);
  this->declare_parameter<std::string>("zenoh.key_expr", zenoh_parameters_.key_expr);
  this->declare_parameter<std::string>("zenoh.key_prefix", zenoh_parameters_.key_prefix);
  this->declare_parameter<std::vector<std::string>>(
    "zenoh.allowed_types", zenoh_parameters_.allowed_types);
  this->declare_parameter<std::string>("ros_topic_prefix", ros_topic_prefix_);
  this->declare_parameter<int>("qos_depth", qos_depth_);
  this->declare_parameter<bool>("metrics.enabled", metrics_enabled_);
  this->declare_parameter<int>("metrics.interval_ms", metrics_interval_ms_);
  this->declare_parameter<std::string>("metrics.topic", metrics_topic_);

  on_parameters_set_handle_ = this->add_on_set_parameters_callback(
    std::bind(&ZenohSourceNode::on_parameters_set, this, std::placeholders::_1));
}

ZenohSourceNode::CallbackReturn ZenohSourceNode::on_configure(const rclcpp_lifecycle::State &)
{
  std::string error_message;
  if (!configure_from_parameters(&error_message)) {
    RCLCPP_ERROR(get_logger(), "Failed to configure zenoh_source: %s", error_message.c_str());
    return CallbackReturn::FAILURE;
  }
  RCLCPP_INFO(
    get_logger(), "Configured zenoh_source (key_expr='%s', ros_topic_prefix='%s')",
    zenoh_parameters_.key_expr.c_str(), ros_topic_prefix_.c_str());
  return CallbackReturn::SUCCESS;
}

ZenohSourceNode::CallbackReturn ZenohSourceNode::on_activate(const rclcpp_lifecycle::State &)
{
  std::string error_message;
  if (!start_subscriber(&error_message)) {
    RCLCPP_ERROR(get_logger(), "Failed to start zenoh subscriber: %s", error_message.c_str());
    return CallbackReturn::FAILURE;
  }

  is_active_.store(true, std::memory_order_release);

  if (metrics_enabled_) {
    metrics_pub_ = this->create_publisher<std_msgs::msg::String>(metrics_topic_, 10);
    metrics_pub_->on_activate();
    metrics_timer_ = this->create_wall_timer(
      std::chrono::milliseconds(metrics_interval_ms_),
      std::bind(&ZenohSourceNode::publish_metrics, this));
  }

  if (zenoh_parameters_.allowed_types.empty()) {
    RCLCPP_WARN(
      get_logger(),
      "zenoh.allowed_types is empty: every valid ROS type advertised by an incoming "
      "Zenoh sample will be loaded. Set zenoh.allowed_types to restrict which message "
      "types may be deserialized.");
  }

  RCLCPP_INFO(
    get_logger(), "Activated zenoh_source on key_expr '%s'",
    zenoh_parameters_.key_expr.c_str());
  return CallbackReturn::SUCCESS;
}

ZenohSourceNode::CallbackReturn ZenohSourceNode::on_deactivate(const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_subscriber();

  if (metrics_timer_) {
    metrics_timer_.reset();
  }
  if (metrics_pub_) {
    metrics_pub_->on_deactivate();
    metrics_pub_.reset();
  }

  {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    publishers_.clear();
  }

  RCLCPP_INFO(get_logger(), "Deactivated zenoh_source");
  return CallbackReturn::SUCCESS;
}

ZenohSourceNode::CallbackReturn ZenohSourceNode::on_cleanup(const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_subscriber();
  if (metrics_timer_) {
    metrics_timer_.reset();
  }
  if (metrics_pub_) {
    metrics_pub_.reset();
  }
  std::lock_guard<std::mutex> lock(cache_mutex_);
  publishers_.clear();
  metrics_.clear();
  RCLCPP_INFO(get_logger(), "Cleaned up zenoh_source");
  return CallbackReturn::SUCCESS;
}

ZenohSourceNode::CallbackReturn ZenohSourceNode::on_shutdown(const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_subscriber();
  RCLCPP_INFO(get_logger(), "Shutting down zenoh_source");
  return CallbackReturn::SUCCESS;
}

rcl_interfaces::msg::SetParametersResult ZenohSourceNode::on_parameters_set(
  const std::vector<rclcpp::Parameter> & parameters)
{
  rcl_interfaces::msg::SetParametersResult result;
  result.successful = true;
  result.reason = "accepted";

  const auto & current_state = this->get_current_state();
  const bool is_active =
    current_state.id() == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE;

  if (is_active) {
    result.successful = false;
    result.reason = "deactivate first";
    return result;
  }

  ZenohParameters pending = zenoh_parameters_;
  for (const auto & param : parameters) {
    const auto & name = param.get_name();
    if (name == "zenoh.mode") {
      pending.mode = param.as_string();
    } else if (name == "zenoh.connect") {
      pending.connect = param.as_string_array();
    } else if (name == "zenoh.listen") {
      pending.listen = param.as_string_array();
    } else if (name == "zenoh.config_path") {
      pending.config_path = param.as_string();
    } else if (name == "zenoh.key_expr") {
      pending.key_expr = param.as_string();
    } else if (name == "zenoh.key_prefix") {
      pending.key_prefix = param.as_string();
    } else if (name == "zenoh.allowed_types") {
      pending.allowed_types = param.as_string_array();
    } else if (name == "ros_topic_prefix") {
      ros_topic_prefix_ = param.as_string();
    } else if (name == "qos_depth") {
      const auto depth = param.as_int();
      if (depth <= 0) {
        result.successful = false;
        result.reason = "qos_depth must be greater than zero.";
        return result;
      }
      qos_depth_ = static_cast<int>(depth);
    } else if (name == "metrics.enabled") {
      metrics_enabled_ = param.as_bool();
    } else if (name == "metrics.interval_ms") {
      metrics_interval_ms_ = static_cast<int>(param.as_int());
    } else if (name == "metrics.topic") {
      metrics_topic_ = param.as_string();
    }
  }

  std::string error;
  if (!validate_parameters(pending, &error)) {
    result.successful = false;
    result.reason = error;
    return result;
  }
  zenoh_parameters_ = pending;
  return result;
}

bool ZenohSourceNode::configure_from_parameters(std::string * error_message)
{
  ZenohParameters pending;
  pending.mode = this->get_parameter("zenoh.mode").as_string();
  pending.connect = this->get_parameter("zenoh.connect").as_string_array();
  pending.listen = this->get_parameter("zenoh.listen").as_string_array();
  pending.config_path = this->get_parameter("zenoh.config_path").as_string();
  pending.key_expr = this->get_parameter("zenoh.key_expr").as_string();
  pending.key_prefix = this->get_parameter("zenoh.key_prefix").as_string();
  pending.allowed_types = this->get_parameter("zenoh.allowed_types").as_string_array();
  ros_topic_prefix_ = this->get_parameter("ros_topic_prefix").as_string();
  qos_depth_ = static_cast<int>(this->get_parameter("qos_depth").as_int());
  metrics_enabled_ = this->get_parameter("metrics.enabled").as_bool();
  metrics_interval_ms_ = static_cast<int>(this->get_parameter("metrics.interval_ms").as_int());
  metrics_topic_ = this->get_parameter("metrics.topic").as_string();

  if (qos_depth_ <= 0) {
    *error_message = "qos_depth must be greater than zero.";
    return false;
  }
  if (!validate_parameters(pending, error_message)) {
    return false;
  }
  zenoh_parameters_ = pending;
  return true;
}

bool ZenohSourceNode::validate_parameters(
  const ZenohParameters & pending, std::string * error_message) const
{
  if (pending.mode != "peer" && pending.mode != "client" && pending.mode != "router") {
    *error_message = "zenoh.mode must be 'peer', 'client', or 'router'.";
    return false;
  }
  if (pending.mode == "client" && pending.connect.empty() && pending.config_path.empty()) {
    *error_message = "zenoh.connect must list at least one endpoint in client mode.";
    return false;
  }
  if (pending.key_expr.empty()) {
    *error_message = "zenoh.key_expr cannot be empty.";
    return false;
  }
  return true;
}

bool ZenohSourceNode::start_subscriber(std::string * error_message)
{
  auto runtime = std::make_shared<ZenohRuntime>();
  try {
    zenoh::Config config = zenoh_parameters_.config_path.empty() ?
      zenoh::Config::create_default() :
      zenoh::Config::from_file(zenoh_parameters_.config_path);

    if (!zenoh_parameters_.mode.empty()) {
      config.insert_json5("mode", std::string("\"") + zenoh_parameters_.mode + "\"");
    }
    if (!zenoh_parameters_.connect.empty()) {
      config.insert_json5("connect/endpoints", endpoints_to_json5(zenoh_parameters_.connect));
    }
    if (!zenoh_parameters_.listen.empty()) {
      config.insert_json5("listen/endpoints", endpoints_to_json5(zenoh_parameters_.listen));
    }

    runtime->session =
      std::make_unique<zenoh::Session>(zenoh::Session::open(std::move(config)));

    auto on_sample = [this](const zenoh::Sample & sample) {
        if (!is_active_.load(std::memory_order_acquire)) {
          return;
        }
        if (sample.get_kind() != Z_SAMPLE_KIND_PUT) {
          return;
        }
        std::string key_expr{sample.get_keyexpr().as_string_view()};
        std::vector<uint8_t> payload = sample.get_payload().as_vector();
        std::string ros_type;
        auto attachment = sample.get_attachment();
        if (attachment.has_value()) {
          ros_type = attachment->get().as_string();
        }
        this->handle_sample(key_expr, payload, ros_type);
      };

    runtime->subscriber.emplace(
      runtime->session->declare_subscriber(
        zenoh::KeyExpr(zenoh_parameters_.key_expr), std::move(on_sample), zenoh::closures::none));
  } catch (const zenoh::ZException & ex) {
    if (error_message) {
      *error_message = ex.what();
    }
    return false;
  }

  zenoh_runtime_ = std::move(runtime);
  return true;
}

void ZenohSourceNode::stop_subscriber()
{
  // Dropping the runtime undeclares the subscriber (stopping further callbacks)
  // and then closes the session.
  auto runtime = std::exchange(zenoh_runtime_, nullptr);
  (void)runtime;
}

void ZenohSourceNode::handle_sample(
  const std::string & key_expr,
  const std::vector<uint8_t> & payload,
  const std::string & ros_type)
{
  if (payload.empty()) {
    return;
  }

  auto metrics = get_metrics(key_expr, ros_type);

  if (ros_type.empty()) {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled(next_error_log_time_ns_)) {
      RCLCPP_WARN(
        get_logger(), "Sample on '%s' has no ros_type attachment; skipping.", key_expr.c_str());
    }
    return;
  }

  // Security gate: the type name comes from the untrusted Zenoh network. Validate
  // its format and check the allowlist BEFORE create_generic_publisher dlopen()s
  // any type-support library.
  if (!is_allowed_ros_type_name(ros_type, zenoh_parameters_.allowed_types)) {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled(next_error_log_time_ns_)) {
      if (is_valid_ros_type_name(ros_type)) {
        RCLCPP_WARN(
          get_logger(), "ROS type '%s' not allowed by zenoh.allowed_types; skipping.",
          ros_type.c_str());
      } else {
        RCLCPP_WARN(get_logger(), "Invalid ROS type name '%s'; skipping.", ros_type.c_str());
      }
    }
    return;
  }

  const std::string ros_topic =
    derive_ros_topic(key_expr, zenoh_parameters_.key_prefix, ros_topic_prefix_);

  auto publisher = get_or_create_publisher(ros_topic, ros_type);
  if (!publisher) {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled(next_error_log_time_ns_)) {
      RCLCPP_ERROR(
        get_logger(), "Failed to create publisher for '%s' (%s).",
        ros_topic.c_str(), ros_type.c_str());
    }
    return;
  }

  // The Zenoh payload is already a ROS 2 CDR stream — wrap and republish as-is.
  rclcpp::SerializedMessage serialized(payload.size());
  auto & rmw_serialized = serialized.get_rcl_serialized_message();
  std::memcpy(rmw_serialized.buffer, payload.data(), payload.size());
  rmw_serialized.buffer_length = payload.size();

  try {
    publisher->publish(serialized);
  } catch (const std::exception & ex) {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled(next_error_log_time_ns_)) {
      RCLCPP_WARN(get_logger(), "Publish failed for '%s': %s", ros_topic.c_str(), ex.what());
    }
    return;
  }

  metrics->decoded.fetch_add(1, std::memory_order_relaxed);
  metrics->bytes.fetch_add(static_cast<uint64_t>(payload.size()), std::memory_order_relaxed);
}

rclcpp::GenericPublisher::SharedPtr ZenohSourceNode::get_or_create_publisher(
  const std::string & ros_topic, const std::string & ros_type)
{
  std::lock_guard<std::mutex> lock(cache_mutex_);
  const std::string key = ros_topic + "|" + ros_type;
  auto it = publishers_.find(key);
  if (it != publishers_.end()) {
    return it->second;
  }
  try {
    rclcpp::QoS qos{rclcpp::KeepLast(static_cast<size_t>(qos_depth_))};
    auto publisher = this->create_generic_publisher(ros_topic, ros_type, qos);
    publishers_[key] = publisher;
    return publisher;
  } catch (const std::exception &) {
    return nullptr;
  }
}

std::shared_ptr<ZenohSourceNode::TopicMetrics> ZenohSourceNode::get_metrics(
  const std::string & key, const std::string & ros_type)
{
  std::lock_guard<std::mutex> lock(cache_mutex_);
  auto it = metrics_.find(key);
  if (it != metrics_.end()) {
    return it->second;
  }
  auto m = std::make_shared<TopicMetrics>();
  m->ros_type = ros_type;
  metrics_[key] = m;
  return m;
}

void ZenohSourceNode::publish_metrics()
{
  if (!metrics_enabled_ || !metrics_pub_) {
    return;
  }

  const double interval_sec = static_cast<double>(metrics_interval_ms_) / 1000.0;
  nlohmann::json root = nlohmann::json::array();

  std::lock_guard<std::mutex> lock(cache_mutex_);
  for (auto & kv : metrics_) {
    auto & m = *kv.second;
    const uint64_t decoded = m.decoded.load(std::memory_order_relaxed);
    const uint64_t failed = m.failed.load(std::memory_order_relaxed);
    const uint64_t bytes = m.bytes.load(std::memory_order_relaxed);

    const uint64_t d_decoded = decoded - m.prev_decoded;
    const uint64_t d_failed = failed - m.prev_failed;
    const uint64_t d_bytes = bytes - m.prev_bytes;
    m.prev_decoded = decoded;
    m.prev_failed = failed;
    m.prev_bytes = bytes;

    const double rate = interval_sec > 0.0 ? static_cast<double>(d_decoded) / interval_sec : 0.0;

    root.push_back(
      {
        {"key_expr", kv.first},
        {"msg_type", m.ros_type},
        {"interval_ms", metrics_interval_ms_},
        {"delta", {{"decoded", d_decoded}, {"failed", d_failed}, {"bytes", d_bytes}}},
        {"rates", {{"decoded_per_sec", rate}}},
        {"totals", {{"decoded", decoded}, {"failed", failed}, {"bytes", bytes}}}
      });
  }

  std_msgs::msg::String msg;
  msg.data = root.dump();
  metrics_pub_->publish(msg);
}

bool ZenohSourceNode::should_log_throttled(std::atomic<int64_t> & next_log_time_ns)
{
  const int64_t now_ns = this->get_clock()->now().nanoseconds();
  int64_t expected = next_log_time_ns.load(std::memory_order_acquire);
  if (now_ns < expected) {
    return false;
  }
  return next_log_time_ns.compare_exchange_strong(
    expected, now_ns + kThrottleIntervalNs, std::memory_order_acq_rel);
}

}  // namespace zenoh_source

RCLCPP_COMPONENTS_REGISTER_NODE(zenoh_source::ZenohSourceNode)
