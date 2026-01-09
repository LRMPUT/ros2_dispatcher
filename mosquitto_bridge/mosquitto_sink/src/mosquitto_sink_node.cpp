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

#include "mosquitto_sink/mosquitto_sink_node.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <codecvt>
#include <cstring>
#include <functional>
#include <locale>
#include <stdexcept>
#include <utility>

#include "lifecycle_msgs/msg/state.hpp"
#include "mqtt/async_client.h"
#include "mqtt/ssl_options.h"
#include "nlohmann/json.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "rosbag2_cpp/typesupport_helpers.hpp"
#include "rmw/rmw.h"
#include "rmw/serialized_message.h"
#include "rosidl_runtime_c/message_initialization.h"
#include "rosidl_typesupport_cpp/identifier.hpp"
#include "rosidl_typesupport_introspection_cpp/field_types.hpp"
#include "rosidl_typesupport_introspection_cpp/identifier.hpp"
#include "rosidl_typesupport_introspection_cpp/message_introspection.hpp"
#include "yaml-cpp/yaml.h"

namespace mosquitto_sink
{
namespace
{
constexpr int64_t kThrottleIntervalNs = 1'000'000'000LL;  // 1 second

struct MqttHealth
{
  bool connected{false};
  uint64_t reconnect_count{0};
  std::string last_error;
  std::chrono::steady_clock::time_point connected_since;
};

class MqttClient : public mqtt::callback
{
public:
  MqttClient(const std::string & server_uri, const std::string & client_id)
  : client_(server_uri, client_id)
  {
    client_.set_callback(*this);
  }

  bool connect(const mqtt::connect_options & options, std::string * error_message)
  {
    std::lock_guard<std::mutex> lock(client_mutex_);
    try {
      client_.connect(options)->wait();
      connected_.store(true, std::memory_order_release);
      connected_since_ = std::chrono::steady_clock::now();
      return true;
    } catch (const mqtt::exception & ex) {
      last_error_ = ex.what();
      connected_.store(false, std::memory_order_release);
      if (error_message) {
        *error_message = last_error_;
      }
      return false;
    }
  }

  void disconnect()
  {
    std::lock_guard<std::mutex> lock(client_mutex_);
    try {
      if (client_.is_connected()) {
        client_.disconnect()->wait();
      }
    } catch (const mqtt::exception & ex) {
      last_error_ = ex.what();
    }
    connected_.store(false, std::memory_order_release);
  }

  bool publish(
    const std::string & topic,
    const std::vector<uint8_t> & payload,
    int qos,
    bool retain,
    std::string * error_message)
  {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (!client_.is_connected()) {
      last_error_ = "client not connected";
      if (error_message) {
        *error_message = last_error_;
      }
      return false;
    }
    try {
      client_.publish(topic, payload.data(), payload.size(), qos, retain);
      return true;
    } catch (const mqtt::exception & ex) {
      last_error_ = ex.what();
      if (error_message) {
        *error_message = last_error_;
      }
      return false;
    }
  }

  MqttHealth health() const
  {
    MqttHealth health;
    health.connected = connected_.load(std::memory_order_acquire);
    health.reconnect_count = reconnect_count_.load(std::memory_order_acquire);
    health.last_error = last_error_;
    health.connected_since = connected_since_;
    return health;
  }

  void connected(const std::string & cause) override
  {
    (void)cause;
    connected_.store(true, std::memory_order_release);
    connected_since_ = std::chrono::steady_clock::now();
  }

  void connection_lost(const std::string & cause) override
  {
    connected_.store(false, std::memory_order_release);
    reconnect_count_.fetch_add(1, std::memory_order_relaxed);
    last_error_ = cause;
  }

private:
  mqtt::async_client client_;
  mutable std::mutex client_mutex_;
  std::atomic<bool> connected_{false};
  std::atomic<uint64_t> reconnect_count_{0};
  std::string last_error_;
  std::chrono::steady_clock::time_point connected_since_{};
};

// Calculate percentile from sorted samples
uint64_t calculate_percentile(std::vector<uint64_t> samples, double percentile)
{
  if (samples.empty()) {
    return 0;
  }
  std::sort(samples.begin(), samples.end());
  const size_t index = static_cast<size_t>(percentile * static_cast<double>(samples.size() - 1));
  return samples[index];
}

size_t member_element_size(const rosidl_typesupport_introspection_cpp::MessageMember & member)
{
  switch (member.type_id_) {
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_BOOLEAN:
      return sizeof(bool);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_CHAR:
      return sizeof(char);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_OCTET:
      return sizeof(uint8_t);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT8:
      return sizeof(uint8_t);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT8:
      return sizeof(int8_t);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT16:
      return sizeof(uint16_t);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT16:
      return sizeof(int16_t);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT32:
      return sizeof(uint32_t);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT32:
      return sizeof(int32_t);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT64:
      return sizeof(uint64_t);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT64:
      return sizeof(int64_t);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT:
      return sizeof(float);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_DOUBLE:
      return sizeof(double);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_STRING:
      return sizeof(std::string);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_WSTRING:
      return sizeof(std::u16string);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_MESSAGE: {
      const auto * members =
        static_cast<const rosidl_typesupport_introspection_cpp::MessageMembers *>(
        member.members_->data);
      return members ? members->size_of_ : 0U;
    }
    default:
      return 0U;
  }
}

nlohmann::json build_json_value(
  const rosidl_typesupport_introspection_cpp::MessageMember & member,
  const void * value_ptr);

nlohmann::json build_json_message(
  const rosidl_typesupport_introspection_cpp::MessageMembers & members,
  const void * message_ptr)
{
  nlohmann::json output = nlohmann::json::object();
  for (size_t index = 0; index < members.member_count_; ++index) {
    const auto & member = members.members_[index];
    const uint8_t * field_ptr = static_cast<const uint8_t *>(message_ptr) + member.offset_;
    output[member.name_] = build_json_value(member, field_ptr);
  }
  return output;
}

nlohmann::json build_json_scalar(
  const rosidl_typesupport_introspection_cpp::MessageMember & member,
  const void * value_ptr)
{
  if (!value_ptr) {
    return nullptr;
  }

  switch (member.type_id_) {
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_BOOLEAN:
      return *static_cast<const bool *>(value_ptr);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_CHAR:
      return static_cast<int32_t>(*static_cast<const char *>(value_ptr));
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_OCTET:
      return static_cast<uint32_t>(*static_cast<const uint8_t *>(value_ptr));
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT8:
      return static_cast<uint32_t>(*static_cast<const uint8_t *>(value_ptr));
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT8:
      return static_cast<int32_t>(*static_cast<const int8_t *>(value_ptr));
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT16:
      return static_cast<uint32_t>(*static_cast<const uint16_t *>(value_ptr));
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT16:
      return static_cast<int32_t>(*static_cast<const int16_t *>(value_ptr));
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT32:
      return *static_cast<const uint32_t *>(value_ptr);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT32:
      return *static_cast<const int32_t *>(value_ptr);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT64:
      return *static_cast<const uint64_t *>(value_ptr);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT64:
      return *static_cast<const int64_t *>(value_ptr);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT: {
      const float value = *static_cast<const float *>(value_ptr);
      return std::isfinite(value) ? nlohmann::json(value) : nlohmann::json(nullptr);
    }
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_DOUBLE: {
      const double value = *static_cast<const double *>(value_ptr);
      return std::isfinite(value) ? nlohmann::json(value) : nlohmann::json(nullptr);
    }
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_STRING:
      return *static_cast<const std::string *>(value_ptr);
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_WSTRING: {
      const auto & value = *static_cast<const std::u16string *>(value_ptr);
      std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;
      return converter.to_bytes(value);
    }
    case rosidl_typesupport_introspection_cpp::ROS_TYPE_MESSAGE: {
      if (!value_ptr || !member.members_) {
        return nullptr;
      }
      const auto * members =
        static_cast<const rosidl_typesupport_introspection_cpp::MessageMembers *>(
        member.members_->data);
      return members ? build_json_message(*members, value_ptr) : nlohmann::json(nullptr);
    }
    default:
      return nullptr;
  }
}

nlohmann::json build_json_array(
  const rosidl_typesupport_introspection_cpp::MessageMember & member,
  const void * array_ptr)
{
  nlohmann::json output = nlohmann::json::array();
  if (!array_ptr) {
    return output;
  }

  const size_t size = member.array_size_ ?
    member.array_size_ :
    (member.size_function ? member.size_function(array_ptr) : 0U);

  for (size_t index = 0; index < size; ++index) {
    const void * element_ptr = nullptr;
    // For sequences (std::vector), always use get_const_function
    if (member.get_const_function) {
      element_ptr = member.get_const_function(array_ptr, index);
    } else {
      // For fixed-size arrays, use pointer arithmetic
      const size_t element_size = member_element_size(member);
      if (element_size > 0) {
        element_ptr = static_cast<const uint8_t *>(array_ptr) + (index * element_size);
      }
    }
    if (element_ptr) {
      output.push_back(build_json_scalar(member, element_ptr));
    }
  }
  return output;
}

nlohmann::json build_json_value(
  const rosidl_typesupport_introspection_cpp::MessageMember & member,
  const void * value_ptr)
{
  if (member.is_array_) {
    return build_json_array(member, value_ptr);
  }
  return build_json_scalar(member, value_ptr);
}

bool serialize_message_to_json(
  const rclcpp::SerializedMessage & serialized,
  const rosidl_message_type_support_t * rmw_type_support,
  const rosidl_message_type_support_t * introspection_type_support,
  std::string * output,
  std::string * error_message)
{
  if (!rmw_type_support || !introspection_type_support) {
    if (error_message) {
      *error_message = "Missing type support for JSON serialization.";
    }
    return false;
  }

  const auto * members =
    static_cast<const rosidl_typesupport_introspection_cpp::MessageMembers *>(
    introspection_type_support->data);
  if (!members || members->size_of_ == 0) {
    if (error_message) {
      *error_message = "Introspection metadata unavailable.";
    }
    return false;
  }

  void * message = malloc(members->size_of_);
  if (!message) {
    if (error_message) {
      *error_message = "Failed to allocate message for JSON serialization.";
    }
    return false;
  }

  // Initialize memory to zero - critical for strings/vectors in ROS messages
  memset(message, 0, members->size_of_);

  // Call the message's init function if available
  if (members->init_function) {
    // Initialize with default initialization
    rosidl_runtime_cpp::MessageInitialization init;
    members->init_function(message, init);
  }

  const rmw_serialized_message_t & rmw_serialized =
    serialized.get_rcl_serialized_message();
  if (rmw_deserialize(&rmw_serialized, rmw_type_support, message) != RMW_RET_OK) {
    // Call fini if available to clean up any initialized fields
    if (members->fini_function) {
      members->fini_function(message);
    }
    free(message);
    if (error_message) {
      *error_message = "Failed to deserialize message for JSON serialization.";
    }
    return false;
  }

  nlohmann::json payload = build_json_message(*members, message);
  *output = payload.dump();

  // Clean up
  if (members->fini_function) {
    members->fini_function(message);
  }
  free(message);
  return true;
}

// Load type support dynamically from a message type string (e.g., "geometry_msgs/msg/PoseStamped")
bool load_type_support(
  const std::string & msg_type,
  const rosidl_message_type_support_t ** rmw_type_support,
  const rosidl_message_type_support_t ** introspection_type_support)
{
  try {
    auto ts_lib = rosbag2_cpp::get_typesupport_library(
      msg_type, "rosidl_typesupport_cpp");
    auto introspection_ts_lib = rosbag2_cpp::get_typesupport_library(
      msg_type, "rosidl_typesupport_introspection_cpp");

    if (!ts_lib || !introspection_ts_lib) {
      return false;
    }

    *rmw_type_support = rosbag2_cpp::get_typesupport_handle(
      msg_type, "rosidl_typesupport_cpp", ts_lib);
    *introspection_type_support = rosbag2_cpp::get_typesupport_handle(
      msg_type, "rosidl_typesupport_introspection_cpp", introspection_ts_lib);

    return (*rmw_type_support != nullptr) && (*introspection_type_support != nullptr);
  } catch (...) {
    return false;
  }
}
}  // namespace

struct MosquittoSinkNode::MqttRuntime
{
  explicit MqttRuntime(const std::string & server_uri, const std::string & client_id)
  : client(std::make_shared<MqttClient>(server_uri, client_id))
  {
  }

  std::shared_ptr<MqttClient> client;
};

MosquittoSinkNode::ActiveSubscription::ActiveSubscription(ActiveSubscription && other) noexcept
: subscription(std::move(other.subscription)),
  topic_name(std::move(other.topic_name)),
  msg_type(std::move(other.msg_type)),
  runtime_state(std::move(other.runtime_state))
{}

MosquittoSinkNode::ActiveSubscription & MosquittoSinkNode::ActiveSubscription::operator=(
  ActiveSubscription && other) noexcept
{
  if (this != &other) {
    subscription = std::move(other.subscription);
    topic_name = std::move(other.topic_name);
    msg_type = std::move(other.msg_type);
    runtime_state = std::move(other.runtime_state);
  }
  return *this;
}

std::vector<SubscriptionConfig> parse_subscriptions_yaml(const std::string & yaml_text)
{
  std::vector<SubscriptionConfig> configs;
  if (yaml_text.empty()) {
    return configs;
  }

  YAML::Node root;
  try {
    root = YAML::Load(yaml_text);
  } catch (const YAML::ParserException & ex) {
    throw std::runtime_error(std::string("Failed to parse subscriptions_yaml: ") + ex.what());
  }

  if (!root.IsSequence()) {
    throw std::runtime_error("Parameter 'subscriptions_yaml' must be a YAML sequence.");
  }

  for (std::size_t idx = 0; idx < root.size(); ++idx) {
    const YAML::Node & entry = root[idx];
    if (!entry.IsMap()) {
      throw std::runtime_error("Each subscription entry must be a map.");
    }

    auto topic_node = entry["topic_name"];
    auto msg_type_node = entry["msg_type"];
    if (!topic_node || !msg_type_node) {
      throw std::runtime_error("Each subscription entry requires 'topic_name' and 'msg_type'.");
    }

    auto topic_name = topic_node.as<std::string>();
    auto msg_type = msg_type_node.as<std::string>();
    std::optional<std::string> mqtt_name;
    if (auto mqtt_name_node = entry["mqtt_name"]) {
      mqtt_name = mqtt_name_node.as<std::string>();
    }

    topic_name.erase(
      topic_name.begin(),
      std::find_if(topic_name.begin(), topic_name.end(), [](unsigned char ch) {
        return !std::isspace(static_cast<int>(ch));
      }));
    topic_name.erase(
      std::find_if(topic_name.rbegin(), topic_name.rend(), [](unsigned char ch) {
        return !std::isspace(static_cast<int>(ch));
      }).base(),
      topic_name.end());

    msg_type.erase(
      msg_type.begin(),
      std::find_if(msg_type.begin(), msg_type.end(), [](unsigned char ch) {
        return !std::isspace(static_cast<int>(ch));
      }));
    msg_type.erase(
      std::find_if(msg_type.rbegin(), msg_type.rend(), [](unsigned char ch) {
        return !std::isspace(static_cast<int>(ch));
      }).base(),
      msg_type.end());

    if (topic_name.empty() || msg_type.empty()) {
      throw std::runtime_error("Subscription entries must have non-empty topic_name and msg_type.");
    }

    if (mqtt_name) {
      mqtt_name->erase(
        mqtt_name->begin(),
        std::find_if(mqtt_name->begin(), mqtt_name->end(), [](unsigned char ch) {
          return !std::isspace(static_cast<int>(ch));
        }));
      mqtt_name->erase(
        std::find_if(mqtt_name->rbegin(), mqtt_name->rend(), [](unsigned char ch) {
          return !std::isspace(static_cast<int>(ch));
        }).base(),
        mqtt_name->end());
      if (mqtt_name->empty()) {
        mqtt_name.reset();
      }
    }

    configs.push_back({topic_name, msg_type, mqtt_name});
  }

  return configs;
}

MosquittoSinkNode::MosquittoSinkNode(const rclcpp::NodeOptions & options)
: rclcpp_lifecycle::LifecycleNode("mosquitto_sink", options)
{
  this->declare_parameter<std::string>("subscriptions_yaml", "");
  this->declare_parameter<int>("qos_depth", qos_depth_);
  // Metrics parameters
  this->declare_parameter<bool>("metrics.enabled", metrics_enabled_);
  this->declare_parameter<int>("metrics.interval_ms", metrics_interval_ms_);
  this->declare_parameter<std::string>("metrics.topic", metrics_topic_);
  this->declare_parameter<std::string>("mqtt.broker_host", mqtt_parameters_.broker_host);
  this->declare_parameter<int>("mqtt.broker_port", mqtt_parameters_.broker_port);
  this->declare_parameter<std::string>("mqtt.client_id", mqtt_parameters_.client_id);
  this->declare_parameter<std::string>("mqtt.username", mqtt_parameters_.username);
  this->declare_parameter<std::string>("mqtt.password", mqtt_parameters_.password);
  this->declare_parameter<int>("mqtt.qos", mqtt_parameters_.qos);
  this->declare_parameter<bool>("mqtt.retain", mqtt_parameters_.retain);
  this->declare_parameter<int>("mqtt.keep_alive_seconds", mqtt_parameters_.keep_alive_seconds);
  this->declare_parameter<std::string>("mqtt.topic_prefix", mqtt_parameters_.topic_prefix);
  this->declare_parameter<std::string>("mqtt.topic_mapping_mode", "prefix_ros_topic");
  this->declare_parameter<std::string>("mqtt.fixed_topic", mqtt_parameters_.fixed_topic);
  this->declare_parameter<std::string>("mqtt.payload_format", "cdr");
  this->declare_parameter<bool>("mqtt.use_tls", mqtt_parameters_.use_tls);
  this->declare_parameter<std::string>("mqtt.ca_cert_path", mqtt_parameters_.ca_cert_path);
  this->declare_parameter<std::string>("mqtt.lwt_topic", mqtt_parameters_.lwt_topic);
  this->declare_parameter<std::string>("mqtt.lwt_payload", mqtt_parameters_.lwt_payload);
  this->declare_parameter<int>("mqtt.lwt_qos", mqtt_parameters_.lwt_qos);
  this->declare_parameter<bool>("mqtt.lwt_retain", mqtt_parameters_.lwt_retain);

  on_parameters_set_handle_ = this->add_on_set_parameters_callback(
    std::bind(&MosquittoSinkNode::on_parameters_set, this, std::placeholders::_1));
}

MosquittoSinkNode::CallbackReturn MosquittoSinkNode::on_configure(
  const rclcpp_lifecycle::State &)
{
  std::string error_message;
  if (!configure_from_parameters(&error_message)) {
    RCLCPP_ERROR(get_logger(), "Failed to configure mosquitto_sink: %s", error_message.c_str());
    return CallbackReturn::FAILURE;
  }

  if (configured_subscriptions_.empty()) {
    RCLCPP_WARN(
      get_logger(),
      "Configured mosquitto_sink with no subscriptions. Activate after setting 'subscriptions_yaml'.");
  }

  RCLCPP_INFO(
    get_logger(), "Configured mosquitto_sink with %zu subscription entries",
    configured_subscriptions_.size());
  return CallbackReturn::SUCCESS;
}

MosquittoSinkNode::CallbackReturn MosquittoSinkNode::on_activate(
  const rclcpp_lifecycle::State &)
{
  if (!start_client()) {
    RCLCPP_ERROR(get_logger(), "Failed to start MQTT client");
    return CallbackReturn::FAILURE;
  }

  if (!build_subscriptions()) {
    stop_client();
    return CallbackReturn::FAILURE;
  }

  is_active_.store(true, std::memory_order_release);

  // Setup metrics publisher and timer if enabled
  if (metrics_enabled_) {
    metrics_pub_ = this->create_publisher<std_msgs::msg::String>(metrics_topic_, 10);
    metrics_pub_->on_activate();
    metrics_timer_ = this->create_wall_timer(
      std::chrono::milliseconds(metrics_interval_ms_),
      std::bind(&MosquittoSinkNode::publish_metrics, this));
  }
  RCLCPP_INFO(
    get_logger(), "Activated mosquitto_sink with %zu active subscriptions",
    active_subscriptions_.size());
  return CallbackReturn::SUCCESS;
}

MosquittoSinkNode::CallbackReturn MosquittoSinkNode::on_deactivate(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  clear_subscriptions();
  stop_client();

  // Stop metrics reporting
  if (metrics_timer_) {
    metrics_timer_.reset();
  }
  if (metrics_pub_) {
    metrics_pub_->on_deactivate();
    metrics_pub_.reset();
  }

  RCLCPP_INFO(get_logger(), "Deactivated mosquitto_sink and cleared subscriptions");
  return CallbackReturn::SUCCESS;
}

MosquittoSinkNode::CallbackReturn MosquittoSinkNode::on_cleanup(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_client();
  clear_subscriptions();
  configured_subscriptions_.clear();

  if (metrics_timer_) {
    metrics_timer_.reset();
  }
  if (metrics_pub_) {
    metrics_pub_.reset();
  }

  RCLCPP_INFO(get_logger(), "Cleaned up mosquitto_sink configuration and runtime state");
  return CallbackReturn::SUCCESS;
}

MosquittoSinkNode::CallbackReturn MosquittoSinkNode::on_shutdown(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_client();
  clear_subscriptions();
  configured_subscriptions_.clear();

  RCLCPP_INFO(get_logger(), "Shutting down mosquitto_sink");
  return CallbackReturn::SUCCESS;
}

rcl_interfaces::msg::SetParametersResult MosquittoSinkNode::on_parameters_set(
  const std::vector<rclcpp::Parameter> & parameters)
{
  rcl_interfaces::msg::SetParametersResult result;
  result.successful = true;
  result.reason = "accepted";

  bool update_required = false;
  std::string pending_yaml;
  int pending_depth = qos_depth_;
  MqttParameters pending_mqtt = mqtt_parameters_;
  bool mqtt_update_required = false;
  bool metrics_update_required = false;
  bool pending_metrics_enabled = metrics_enabled_;
  int pending_metrics_interval_ms = metrics_interval_ms_;
  std::string pending_metrics_topic = metrics_topic_;

  for (const auto & param : parameters) {
    const auto & name = param.get_name();
    if (name == "subscriptions_yaml") {
      const auto & current_state = this->get_current_state();
      if (current_state.id() == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE) {
        result.successful = false;
        result.reason = "deactivate first";
        return result;
      }
      pending_yaml = param.as_string();
      update_required = true;
    } else if (name == "qos_depth") {
      pending_depth = param.as_int();
    } else if (name.rfind("mqtt.", 0) == 0) {
      const auto & current_state = this->get_current_state();
      if (current_state.id() == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE) {
        result.successful = false;
        result.reason = "deactivate first";
        return result;
      }
      mqtt_update_required = true;
      if (name == "mqtt.broker_host") {
        pending_mqtt.broker_host = param.as_string();
      } else if (name == "mqtt.broker_port") {
        pending_mqtt.broker_port = param.as_int();
      } else if (name == "mqtt.client_id") {
        pending_mqtt.client_id = param.as_string();
      } else if (name == "mqtt.username") {
        pending_mqtt.username = param.as_string();
      } else if (name == "mqtt.password") {
        pending_mqtt.password = param.as_string();
      } else if (name == "mqtt.qos") {
        pending_mqtt.qos = param.as_int();
      } else if (name == "mqtt.retain") {
        pending_mqtt.retain = param.as_bool();
      } else if (name == "mqtt.keep_alive_seconds") {
        pending_mqtt.keep_alive_seconds = param.as_int();
      } else if (name == "mqtt.topic_prefix") {
        pending_mqtt.topic_prefix = param.as_string();
      } else if (name == "mqtt.topic_mapping_mode") {
        auto mode_value = param.as_string();
        if (mode_value == "prefix_ros_topic") {
          pending_mqtt.topic_mapping_mode = TopicMappingMode::PREFIX_ROS_TOPIC;
        } else if (mode_value == "fixed") {
          pending_mqtt.topic_mapping_mode = TopicMappingMode::FIXED;
        } else {
          result.successful = false;
          result.reason = "invalid mqtt.topic_mapping_mode";
          return result;
        }
      } else if (name == "mqtt.fixed_topic") {
        pending_mqtt.fixed_topic = param.as_string();
      } else if (name == "mqtt.payload_format") {
        auto format_value = param.as_string();
        if (format_value == "cdr") {
          pending_mqtt.payload_format = PayloadFormat::CDR;
        } else if (format_value == "json") {
          pending_mqtt.payload_format = PayloadFormat::JSON;
        } else {
          result.successful = false;
          result.reason = "invalid mqtt.payload_format";
          return result;
        }
      } else if (name == "mqtt.use_tls") {
        pending_mqtt.use_tls = param.as_bool();
      } else if (name == "mqtt.ca_cert_path") {
        pending_mqtt.ca_cert_path = param.as_string();
      } else if (name == "mqtt.lwt_topic") {
        pending_mqtt.lwt_topic = param.as_string();
      } else if (name == "mqtt.lwt_payload") {
        pending_mqtt.lwt_payload = param.as_string();
      } else if (name == "mqtt.lwt_qos") {
        pending_mqtt.lwt_qos = param.as_int();
      } else if (name == "mqtt.lwt_retain") {
        pending_mqtt.lwt_retain = param.as_bool();
      }
    } else if (name.rfind("metrics.", 0) == 0) {
      const auto & current_state = this->get_current_state();
      if (current_state.id() == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE) {
        result.successful = false;
        result.reason = "deactivate first";
        return result;
      }
      metrics_update_required = true;
      if (name == "metrics.enabled") {
        pending_metrics_enabled = param.as_bool();
      } else if (name == "metrics.interval_ms") {
        pending_metrics_interval_ms = param.as_int();
      } else if (name == "metrics.topic") {
        pending_metrics_topic = param.as_string();
      }
    }
  }

  if (update_required) {
    try {
      configured_subscriptions_ = parse_subscriptions_yaml(pending_yaml);
      RCLCPP_INFO(
        get_logger(), "Updated subscriptions via parameter callback: %zu entries",
        configured_subscriptions_.size());
    } catch (const std::exception & ex) {
      result.successful = false;
      result.reason = ex.what();
      return result;
    }
  }

  std::string qos_error;
  if (!validate_qos_depth(pending_depth, &qos_error)) {
    result.successful = false;
    result.reason = qos_error;
    return result;
  }
  qos_depth_ = pending_depth;

  if (mqtt_update_required) {
    std::string mqtt_error;
    if (!validate_mqtt_parameters(pending_mqtt, &mqtt_error)) {
      result.successful = false;
      result.reason = mqtt_error;
      return result;
    }
    mqtt_parameters_ = pending_mqtt;
  }

  if (metrics_update_required) {
    metrics_enabled_ = pending_metrics_enabled;
    metrics_interval_ms_ = pending_metrics_interval_ms;
    metrics_topic_ = pending_metrics_topic;
  }

  return result;
}

bool MosquittoSinkNode::configure_from_parameters(std::string * error_message)
{
  qos_depth_ = this->get_parameter("qos_depth").as_int();
  std::string yaml_config = this->get_parameter("subscriptions_yaml").as_string();

  // Metrics config
  metrics_enabled_ = this->get_parameter("metrics.enabled").as_bool();
  metrics_interval_ms_ = this->get_parameter("metrics.interval_ms").as_int();
  metrics_topic_ = this->get_parameter("metrics.topic").as_string();

  std::string qos_error;
  if (!validate_qos_depth(qos_depth_, &qos_error)) {
    *error_message = qos_error;
    return false;
  }

  try {
    configured_subscriptions_ = parse_subscriptions_yaml(yaml_config);
  } catch (const std::exception & ex) {
    *error_message = ex.what();
    return false;
  }

  return configure_mqtt_parameters(error_message);
}

bool MosquittoSinkNode::validate_qos_depth(int qos_depth, std::string * error_message) const
{
  if (qos_depth <= 0) {
    *error_message = "qos_depth must be greater than zero.";
    return false;
  }
  return true;
}

bool MosquittoSinkNode::validate_mqtt_parameters(
  const MqttParameters & pending, std::string * error_message) const
{
  if (pending.broker_host.empty()) {
    *error_message = "mqtt.broker_host cannot be empty.";
    return false;
  }
  if (pending.broker_port <= 0) {
    *error_message = "mqtt.broker_port must be greater than zero.";
    return false;
  }
  if (pending.client_id.empty()) {
    *error_message = "mqtt.client_id cannot be empty.";
    return false;
  }
  if (pending.qos < 0 || pending.qos > 2) {
    *error_message = "mqtt.qos must be 0, 1, or 2.";
    return false;
  }
  if (pending.keep_alive_seconds <= 0) {
    *error_message = "mqtt.keep_alive_seconds must be greater than zero.";
    return false;
  }
  if (pending.topic_mapping_mode == TopicMappingMode::FIXED && pending.fixed_topic.empty()) {
    *error_message = "mqtt.fixed_topic cannot be empty when mapping mode is 'fixed'.";
    return false;
  }
  if (pending.lwt_qos < 0 || pending.lwt_qos > 2) {
    *error_message = "mqtt.lwt_qos must be 0, 1, or 2.";
    return false;
  }
  return true;
}

bool MosquittoSinkNode::configure_mqtt_parameters(std::string * error_message)
{
  MqttParameters pending = mqtt_parameters_;
  pending.broker_host = this->get_parameter("mqtt.broker_host").as_string();
  pending.broker_port = this->get_parameter("mqtt.broker_port").as_int();
  pending.client_id = this->get_parameter("mqtt.client_id").as_string();
  pending.username = this->get_parameter("mqtt.username").as_string();
  pending.password = this->get_parameter("mqtt.password").as_string();
  pending.qos = this->get_parameter("mqtt.qos").as_int();
  pending.retain = this->get_parameter("mqtt.retain").as_bool();
  pending.keep_alive_seconds = this->get_parameter("mqtt.keep_alive_seconds").as_int();
  pending.topic_prefix = this->get_parameter("mqtt.topic_prefix").as_string();

  auto mapping_mode = this->get_parameter("mqtt.topic_mapping_mode").as_string();
  if (mapping_mode == "prefix_ros_topic") {
    pending.topic_mapping_mode = TopicMappingMode::PREFIX_ROS_TOPIC;
  } else if (mapping_mode == "fixed") {
    pending.topic_mapping_mode = TopicMappingMode::FIXED;
  } else {
    *error_message = "Invalid mqtt.topic_mapping_mode value.";
    return false;
  }

  pending.fixed_topic = this->get_parameter("mqtt.fixed_topic").as_string();
  auto payload_format = this->get_parameter("mqtt.payload_format").as_string();
  if (payload_format == "cdr") {
    pending.payload_format = PayloadFormat::CDR;
  } else if (payload_format == "json") {
    pending.payload_format = PayloadFormat::JSON;
  } else {
    *error_message = "Invalid mqtt.payload_format value.";
    return false;
  }
  pending.use_tls = this->get_parameter("mqtt.use_tls").as_bool();
  pending.ca_cert_path = this->get_parameter("mqtt.ca_cert_path").as_string();
  pending.lwt_topic = this->get_parameter("mqtt.lwt_topic").as_string();
  pending.lwt_payload = this->get_parameter("mqtt.lwt_payload").as_string();
  pending.lwt_qos = this->get_parameter("mqtt.lwt_qos").as_int();
  pending.lwt_retain = this->get_parameter("mqtt.lwt_retain").as_bool();

  if (!validate_mqtt_parameters(pending, error_message)) {
    return false;
  }

  mqtt_parameters_ = pending;
  return true;
}

bool MosquittoSinkNode::start_client()
{
  const std::string scheme = mqtt_parameters_.use_tls ? "ssl" : "tcp";
  const std::string server_uri =
    scheme + "://" + mqtt_parameters_.broker_host + ":" +
    std::to_string(mqtt_parameters_.broker_port);

  mqtt_runtime_ = std::make_shared<MqttRuntime>(server_uri, mqtt_parameters_.client_id);

  mqtt::connect_options connect_opts;
  connect_opts.set_keep_alive_interval(std::chrono::seconds(mqtt_parameters_.keep_alive_seconds));
  connect_opts.set_clean_session(true);
  connect_opts.set_automatic_reconnect(std::chrono::seconds(1), std::chrono::seconds(30));

  if (!mqtt_parameters_.username.empty()) {
    connect_opts.set_user_name(mqtt_parameters_.username);
  }
  if (!mqtt_parameters_.password.empty()) {
    connect_opts.set_password(mqtt_parameters_.password);
  }

  if (!mqtt_parameters_.lwt_topic.empty()) {
    mqtt::will_options will(
      mqtt_parameters_.lwt_topic,
      mqtt_parameters_.lwt_payload.data(),
      mqtt_parameters_.lwt_payload.size(),
      mqtt_parameters_.lwt_qos,
      mqtt_parameters_.lwt_retain);
    connect_opts.set_will(will);
  }

  if (mqtt_parameters_.use_tls) {
    mqtt::ssl_options ssl_opts;
    if (!mqtt_parameters_.ca_cert_path.empty()) {
      ssl_opts.set_trust_store(mqtt_parameters_.ca_cert_path);
    }
    connect_opts.set_ssl(ssl_opts);
  }

  std::string error_message;
  if (!mqtt_runtime_->client->connect(connect_opts, &error_message)) {
    RCLCPP_ERROR(get_logger(), "MQTT connect failed: %s", error_message.c_str());
    mqtt_runtime_.reset();
    return false;
  }

  return true;
}

void MosquittoSinkNode::stop_client()
{
  auto runtime = std::exchange(mqtt_runtime_, nullptr);
  if (runtime) {
    runtime->client->disconnect();
  }
}

std::string MosquittoSinkNode::map_mqtt_topic(const std::string & ros_topic) const
{
  if (mqtt_parameters_.topic_mapping_mode == TopicMappingMode::FIXED) {
    return mqtt_parameters_.fixed_topic;
  }

  std::string normalized = ros_topic;
  if (!normalized.empty() && normalized.front() == '/') {
    normalized.erase(normalized.begin());
  }

  if (mqtt_parameters_.topic_prefix.empty()) {
    return normalized;
  }

  if (mqtt_parameters_.topic_prefix.back() == '/' || normalized.empty()) {
    return mqtt_parameters_.topic_prefix + normalized;
  }
  return mqtt_parameters_.topic_prefix + "/" + normalized;
}

rclcpp::QoS MosquittoSinkNode::build_qos_profile() const
{
  rclcpp::QoS qos{rclcpp::SystemDefaultsQoS()};
  qos.keep_last(static_cast<size_t>(qos_depth_));
  return qos;
}

bool MosquittoSinkNode::build_subscriptions()
{
  clear_subscriptions();

  if (!mqtt_runtime_) {
    RCLCPP_ERROR(get_logger(), "MQTT client is not initialized");
    return false;
  }

  auto qos = build_qos_profile();

  if (configured_subscriptions_.empty()) {
    return true;
  }

  active_subscriptions_.reserve(configured_subscriptions_.size());
  for (const auto & config : configured_subscriptions_) {
    active_subscriptions_.emplace_back();
    auto & runtime = active_subscriptions_.back();
    runtime.topic_name = config.topic_name;
    runtime.msg_type = config.msg_type;
    runtime.runtime_state = std::make_shared<SubscriptionRuntime>();
    runtime.runtime_state->ros_topic = config.topic_name;
    runtime.runtime_state->msg_type = config.msg_type;
    const std::string & mqtt_topic_name =
      config.mqtt_name ? *config.mqtt_name : config.topic_name;
    runtime.runtime_state->mqtt_topic = map_mqtt_topic(mqtt_topic_name);
    runtime.runtime_state->payload_format = mqtt_parameters_.payload_format;
    if (mqtt_parameters_.payload_format == PayloadFormat::JSON) {
      // Attempt to load type support for JSON serialization
      if (!load_type_support(
            config.msg_type,
            &runtime.runtime_state->rmw_type_support,
            &runtime.runtime_state->introspection_type_support))
      {
        RCLCPP_WARN(
          get_logger(),
          "Failed to load type support for JSON serialization of '%s'. "
          "Falling back to CDR format.",
          config.msg_type.c_str());
        runtime.runtime_state->payload_format = PayloadFormat::CDR;
      } else {
        RCLCPP_INFO(
          get_logger(),
          "Successfully loaded type support for JSON serialization of '%s'",
          config.msg_type.c_str());
      }
    }
    runtime.runtime_state->log_label =
      "topic='" + config.topic_name + "' mqtt_topic='" + runtime.runtime_state->mqtt_topic +
      "' type='" + config.msg_type + "'";

    auto runtime_state = runtime.runtime_state;
    auto callback =
      [this, runtime_state](std::shared_ptr<rclcpp::SerializedMessage> msg) {
        if (!is_active_.load(std::memory_order_acquire)) {
          return;
        }
        auto mqtt_runtime = mqtt_runtime_;
        if (!mqtt_runtime) {
          return;
        }
        auto t0 = std::chrono::steady_clock::now();
        const auto now_ns = this->get_clock()->now().nanoseconds();
        auto next_time_ns =
          runtime_state->next_log_time_ns.load(std::memory_order_acquire);

        std::vector<uint8_t> value;
        if (runtime_state->payload_format == PayloadFormat::JSON) {
          std::string json_payload;
          std::string json_error;
          if (!serialize_message_to_json(
              *msg,
              runtime_state->rmw_type_support,
              runtime_state->introspection_type_support,
              &json_payload,
              &json_error))
          {
            runtime_state->errors.fetch_add(1, std::memory_order_relaxed);
            RCLCPP_WARN_THROTTLE(
              this->get_logger(), *this->get_clock(), 1000,
              "Failed to serialize message to JSON for %s: %s",
              runtime_state->log_label.c_str(), json_error.c_str());
            return;
          }
          value.assign(json_payload.begin(), json_payload.end());
        } else {
          value.resize(msg->size());
          std::memcpy(value.data(), msg->get_rcl_serialized_message().buffer, msg->size());
        }

        auto t1 = std::chrono::steady_clock::now();
        auto serialize_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        runtime_state->serialize_time_ns_accum.fetch_add(
          static_cast<uint64_t>(serialize_ns), std::memory_order_relaxed);
        // update max serialize time
        {
          uint64_t prev = runtime_state->serialize_time_ns_max.load(std::memory_order_relaxed);
          while (static_cast<uint64_t>(serialize_ns) > prev &&
            !runtime_state->serialize_time_ns_max.compare_exchange_weak(
              prev, static_cast<uint64_t>(serialize_ns), std::memory_order_relaxed))
          {
          }
        }

        // Track min/max message size
        const uint64_t msg_size = static_cast<uint64_t>(value.size());
        {
          uint64_t prev_min = runtime_state->msg_size_min.load(std::memory_order_relaxed);
          while (msg_size < prev_min &&
            !runtime_state->msg_size_min.compare_exchange_weak(
              prev_min, msg_size, std::memory_order_relaxed))
          {
          }
        }
        {
          uint64_t prev_max = runtime_state->msg_size_max.load(std::memory_order_relaxed);
          while (msg_size > prev_max &&
            !runtime_state->msg_size_max.compare_exchange_weak(
              prev_max, msg_size, std::memory_order_relaxed))
          {
          }
        }

        // Store latency samples for percentile calculation
        {
          std::lock_guard<std::mutex> lock(runtime_state->latency_mutex);
          runtime_state->serialize_samples.push_back(static_cast<uint64_t>(serialize_ns));
          if (runtime_state->serialize_samples.size() > runtime_state->kMaxSamples) {
            runtime_state->serialize_samples.pop_front();
          }
        }

        runtime_state->msgs_total.fetch_add(1, std::memory_order_relaxed);
        runtime_state->bytes_total.fetch_add(
          static_cast<uint64_t>(value.size()), std::memory_order_relaxed);

        std::string publish_error;
        bool publish_ok = mqtt_runtime->client->publish(
          runtime_state->mqtt_topic,
          value,
          mqtt_parameters_.qos,
          mqtt_parameters_.retain,
          &publish_error);

        auto t2 = std::chrono::steady_clock::now();
        auto send_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();
        runtime_state->send_time_ns_accum.fetch_add(
          static_cast<uint64_t>(send_ns), std::memory_order_relaxed);
        {
          uint64_t prev = runtime_state->send_time_ns_max.load(std::memory_order_relaxed);
          while (static_cast<uint64_t>(send_ns) > prev &&
            !runtime_state->send_time_ns_max.compare_exchange_weak(
              prev, static_cast<uint64_t>(send_ns), std::memory_order_relaxed))
          {
          }
        }
        // Store send latency sample
        {
          std::lock_guard<std::mutex> lock(runtime_state->latency_mutex);
          runtime_state->send_samples.push_back(static_cast<uint64_t>(send_ns));
          if (runtime_state->send_samples.size() > runtime_state->kMaxSamples) {
            runtime_state->send_samples.pop_front();
          }
        }
        if (publish_ok) {
          runtime_state->sent_ok.fetch_add(1, std::memory_order_relaxed);
        } else {
          runtime_state->errors.fetch_add(1, std::memory_order_relaxed);
        }

        if (now_ns >= next_time_ns &&
          runtime_state->next_log_time_ns.compare_exchange_strong(
            next_time_ns, now_ns + kThrottleIntervalNs, std::memory_order_acq_rel))
        {
          auto health = mqtt_runtime->client->health();
          const char * last_error = health.last_error.empty() ? "" : health.last_error.c_str();
          if (health.last_error.empty()) {
            RCLCPP_INFO(
              this->get_logger(),
              "[mosquitto_sink] %s size=%zu bytes sent=%lu errors=%lu",
              runtime_state->log_label.c_str(), msg->size(),
              runtime_state->sent_ok.load(std::memory_order_relaxed),
              runtime_state->errors.load(std::memory_order_relaxed));
          } else {
            RCLCPP_INFO(
              this->get_logger(),
              "[mosquitto_sink] %s size=%zu bytes sent=%lu errors=%lu last_error='%s'",
              runtime_state->log_label.c_str(), msg->size(),
              runtime_state->sent_ok.load(std::memory_order_relaxed),
              runtime_state->errors.load(std::memory_order_relaxed),
              last_error);
          }
        }
      };

    runtime.subscription = this->create_generic_subscription(
      runtime.topic_name, runtime.msg_type, qos, callback);

    if (runtime.subscription == nullptr) {
      RCLCPP_ERROR(
        get_logger(), "Failed to create subscription for topic '%s'", runtime.topic_name.c_str());
      return false;
    }

    // If JSON serialization is requested but type support wasn't loaded yet,
    // try to get it from the subscription
    if (runtime.runtime_state->payload_format == PayloadFormat::JSON &&
      !runtime.runtime_state->rmw_type_support)
    {
      // The subscription was created successfully, so the type support should be available
      try {
        auto ts_lib = rosbag2_cpp::get_typesupport_library(
          runtime.msg_type, "rosidl_typesupport_cpp");
        auto introspection_ts_lib = rosbag2_cpp::get_typesupport_library(
          runtime.msg_type, "rosidl_typesupport_introspection_cpp");

        if (ts_lib && introspection_ts_lib) {
          runtime.runtime_state->rmw_type_support =
            rosbag2_cpp::get_typesupport_handle(
              runtime.msg_type, "rosidl_typesupport_cpp", ts_lib);
          runtime.runtime_state->introspection_type_support =
            rosbag2_cpp::get_typesupport_handle(
              runtime.msg_type, "rosidl_typesupport_introspection_cpp", introspection_ts_lib);
        }
      } catch (...) {
        runtime.runtime_state->rmw_type_support = nullptr;
        runtime.runtime_state->introspection_type_support = nullptr;
      }

      if (runtime.runtime_state->rmw_type_support &&
        runtime.runtime_state->introspection_type_support)
      {
        RCLCPP_INFO(
          get_logger(),
          "Successfully loaded type support (post-subscription) for JSON serialization of '%s'",
          runtime.msg_type.c_str());
      } else {
        RCLCPP_WARN(
          get_logger(),
          "Failed to load type support even after subscription creation for '%s'. "
          "Falling back to CDR format.",
          runtime.msg_type.c_str());
        runtime.runtime_state->payload_format = PayloadFormat::CDR;
      }
    }
  }
  return true;
}

void MosquittoSinkNode::clear_subscriptions()
{
  active_subscriptions_.clear();
}

void MosquittoSinkNode::publish_metrics()
{
  if (!metrics_enabled_ || !metrics_pub_) {
    return;
  }

  nlohmann::json root = nlohmann::json::array();
  const double interval_sec = static_cast<double>(metrics_interval_ms_) / 1000.0;

  MqttHealth health;
  if (mqtt_runtime_) {
    health = mqtt_runtime_->client->health();
  }

  const auto now = std::chrono::steady_clock::now();
  const double connection_uptime_sec = health.connected ?
    std::chrono::duration_cast<std::chrono::duration<double>>(now - health.connected_since).count() :
    0.0;

  for (auto & sub : active_subscriptions_) {
    auto & rt = *sub.runtime_state;

    const uint64_t msgs_total = rt.msgs_total.load(std::memory_order_relaxed);
    const uint64_t sent_ok = rt.sent_ok.load(std::memory_order_relaxed);
    const uint64_t dropped = rt.dropped.load(std::memory_order_relaxed);
    const uint64_t errors = rt.errors.load(std::memory_order_relaxed);
    const uint64_t bytes_total = rt.bytes_total.load(std::memory_order_relaxed);
    const uint64_t serialize_accum = rt.serialize_time_ns_accum.load(std::memory_order_relaxed);
    const uint64_t send_accum = rt.send_time_ns_accum.load(std::memory_order_relaxed);
    const uint64_t msg_size_min = rt.msg_size_min.load(std::memory_order_relaxed);
    const uint64_t msg_size_max = rt.msg_size_max.load(std::memory_order_relaxed);

    const uint64_t d_msgs = msgs_total - rt.prev_msgs_total;
    const uint64_t d_sent = sent_ok - rt.prev_sent_ok;
    const uint64_t d_drop = dropped - rt.prev_dropped;
    const uint64_t d_err = errors - rt.prev_errors;
    const uint64_t d_bytes = bytes_total - rt.prev_bytes_total;
    const uint64_t d_serialize = serialize_accum - rt.prev_serialize_time_ns_accum;
    const uint64_t d_send = send_accum - rt.prev_send_time_ns_accum;

    rt.prev_msgs_total = msgs_total;
    rt.prev_sent_ok = sent_ok;
    rt.prev_dropped = dropped;
    rt.prev_errors = errors;
    rt.prev_bytes_total = bytes_total;
    rt.prev_serialize_time_ns_accum = serialize_accum;
    rt.prev_send_time_ns_accum = send_accum;

    const double recv_rate = interval_sec > 0.0 ? static_cast<double>(d_msgs) / interval_sec : 0.0;
    const double sent_rate = interval_sec > 0.0 ? static_cast<double>(d_sent) / interval_sec : 0.0;

    const uint64_t avg_serialize_ns = d_msgs > 0 ? (d_serialize / d_msgs) : 0ULL;
    const uint64_t avg_send_ns = d_msgs > 0 ? (d_send / d_msgs) : 0ULL;

    // Calculate percentiles from samples
    std::vector<uint64_t> serialize_samples_copy;
    std::vector<uint64_t> send_samples_copy;
    {
      std::lock_guard<std::mutex> lock(rt.latency_mutex);
      serialize_samples_copy.assign(rt.serialize_samples.begin(), rt.serialize_samples.end());
      send_samples_copy.assign(rt.send_samples.begin(), rt.send_samples.end());
    }

    const uint64_t serialize_p95 = calculate_percentile(serialize_samples_copy, 0.95);
    const uint64_t serialize_p99 = calculate_percentile(serialize_samples_copy, 0.99);
    const uint64_t send_p95 = calculate_percentile(send_samples_copy, 0.95);
    const uint64_t send_p99 = calculate_percentile(send_samples_copy, 0.99);

    // Calculate derived metrics
    const double avg_msg_size_bytes = msgs_total > 0 ?
      static_cast<double>(bytes_total) / static_cast<double>(msgs_total) : 0.0;

    // Serialization throughput in MB/s
    const double serialize_throughput_mbps = d_serialize > 0 ?
      (static_cast<double>(d_bytes) / (static_cast<double>(d_serialize) / 1e9)) / 1048576.0 : 0.0;

    // Send throughput in MB/s
    const double send_throughput_mbps = d_send > 0 ?
      (static_cast<double>(d_bytes) / (static_cast<double>(d_send) / 1e9)) / 1048576.0 : 0.0;

    // CPU efficiency: nanoseconds per byte
    const double cpu_ns_per_byte = bytes_total > 0 ?
      static_cast<double>(serialize_accum) / static_cast<double>(bytes_total) : 0.0;

    nlohmann::json entry = {
      {"ros_topic", rt.ros_topic},
      {"mqtt_topic", rt.mqtt_topic},
      {"msg_type", rt.msg_type},
      {"payload_format", rt.payload_format == PayloadFormat::JSON ? "json" : "cdr"},
      {"interval_ms", metrics_interval_ms_},
      {"delta", {
        {"received", d_msgs},
        {"sent_ok", d_sent},
        {"dropped", d_drop},
        {"errors", d_err},
        {"bytes", d_bytes}
      }},
      {"rates", {
        {"received_per_sec", recv_rate},
        {"sent_per_sec", sent_rate}
      }},
      {"message_size", {
        {"avg_bytes", avg_msg_size_bytes},
        {"min_bytes", msg_size_min == UINT64_MAX ? 0 : msg_size_min},
        {"max_bytes", msg_size_max}
      }},
      {"latency_ns", {
        {"serialize_avg", avg_serialize_ns},
        {"serialize_p95", serialize_p95},
        {"serialize_p99", serialize_p99},
        {"serialize_max", rt.serialize_time_ns_max.load(std::memory_order_relaxed)},
        {"send_avg", avg_send_ns},
        {"send_p95", send_p95},
        {"send_p99", send_p99},
        {"send_max", rt.send_time_ns_max.load(std::memory_order_relaxed)}
      }},
      {"throughput", {
        {"serialize_mb_per_sec", serialize_throughput_mbps},
        {"send_mb_per_sec", send_throughput_mbps}
      }},
      {"cpu_efficiency", {
        {"ns_per_byte", cpu_ns_per_byte},
        {"bytes_per_cpu_ms", cpu_ns_per_byte > 0 ? 1000000.0 / cpu_ns_per_byte : 0.0}
      }},
      {"totals", {
        {"received", msgs_total},
        {"sent_ok", sent_ok},
        {"dropped", dropped},
        {"errors", errors},
        {"bytes", bytes_total}
      }},
      {"mqtt_connection", {
        {"connected", health.connected},
        {"uptime_sec", connection_uptime_sec},
        {"reconnect_count", health.reconnect_count},
        {"last_error", health.last_error}
      }}
    };

    root.push_back(entry);
  }

  std_msgs::msg::String msg;
  msg.data = root.dump();
  metrics_pub_->publish(msg);
}

}  // namespace mosquitto_sink

RCLCPP_COMPONENTS_REGISTER_NODE(mosquitto_sink::MosquittoSinkNode)
