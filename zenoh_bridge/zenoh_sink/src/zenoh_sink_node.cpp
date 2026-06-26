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

#include "zenoh_sink/zenoh_sink_node.hpp"

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
#include <sstream>
#include <stdexcept>
#include <utility>

#include "lifecycle_msgs/msg/state.hpp"
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
#include "zenoh.hxx"

namespace zenoh_sink
{
namespace
{
constexpr int64_t kThrottleIntervalNs = 1'000'000'000LL;  // 1 second

struct ZenohHealth
{
  bool connected{false};
  std::string last_error;
  std::chrono::steady_clock::time_point connected_since;
};

// Renders a list of endpoint strings as a JSON5 array literal for zenoh config,
// e.g. {"tcp/a:7447", "tcp/b:7447"} -> ["tcp/a:7447","tcp/b:7447"].
std::string endpoints_to_json5(const std::vector<std::string> & endpoints)
{
  nlohmann::json arr = nlohmann::json::array();
  for (const auto & ep : endpoints) {
    arr.push_back(ep);
  }
  return arr.dump();
}

// Thin wrapper over a zenoh-cpp session that mirrors the transport-client shape
// used by the other sinks (open / close / publish / health). Publishers are
// declared once per key expression and cached. All access is serialized by
// mutex_ so the (MultiThreadedExecutor) topic callbacks can publish safely.
class ZenohClient
{
public:
  // Opens the session and records the per-publisher options. Returns true on
  // success; on failure *error_message (if non-null) describes the problem.
  bool open(
    const std::string & mode,
    const std::vector<std::string> & connect,
    const std::vector<std::string> & listen,
    const std::string & config_path,
    zenoh::CongestionControl congestion_control,
    zenoh::Priority priority,
    bool express,
    std::string * error_message)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
      zenoh::Config config = config_path.empty() ?
        zenoh::Config::create_default() :
        zenoh::Config::from_file(config_path);

      if (!mode.empty()) {
        config.insert_json5("mode", std::string("\"") + mode + "\"");
      }
      if (!connect.empty()) {
        config.insert_json5("connect/endpoints", endpoints_to_json5(connect));
      }
      if (!listen.empty()) {
        config.insert_json5("listen/endpoints", endpoints_to_json5(listen));
      }

      session_ = std::make_unique<zenoh::Session>(zenoh::Session::open(std::move(config)));
      congestion_control_ = congestion_control;
      priority_ = priority;
      express_ = express;
      last_error_.clear();
      connected_.store(true, std::memory_order_release);
      connected_since_ = std::chrono::steady_clock::now();
      return true;
    } catch (const zenoh::ZException & ex) {
      last_error_ = ex.what();
      connected_.store(false, std::memory_order_release);
      session_.reset();
      if (error_message) {
        *error_message = last_error_;
      }
      return false;
    }
  }

  void close()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    publishers_.clear();
    session_.reset();
    connected_.store(false, std::memory_order_release);
  }

  // Declares (and caches) a publisher for keyexpr. Returns false and sets
  // *error_message when the key expression is invalid or the session is down.
  bool declare_publisher(const std::string & keyexpr, std::string * error_message)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return ensure_publisher(keyexpr, error_message) != nullptr;
  }

  // ros_type is attached to each sample (as the zenoh attachment) so a consumer
  // (zenoh_source) can recover the ROS message type to deserialize the CDR
  // payload — mirroring the `ros_type` Kafka record header set by kafka_sink.
  bool publish(
    const std::string & keyexpr,
    const std::vector<uint8_t> & payload,
    const std::string & ros_type,
    std::string * error_message)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    zenoh::Publisher * pub = ensure_publisher(keyexpr, error_message);
    if (!pub) {
      return false;
    }
    zenoh::ZResult err = Z_OK;
    zenoh::Publisher::PutOptions put_opts = zenoh::Publisher::PutOptions::create_default();
    if (!ros_type.empty()) {
      put_opts.attachment = zenoh::Bytes(ros_type);
    }
    pub->put(zenoh::Bytes(payload), std::move(put_opts), &err);
    if (err != Z_OK) {
      last_error_ = "zenoh put failed (code " + std::to_string(err) + ")";
      if (error_message) {
        *error_message = last_error_;
      }
      return false;
    }
    return true;
  }

  ZenohHealth health() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ZenohHealth health;
    health.connected = connected_.load(std::memory_order_acquire);
    health.last_error = last_error_;
    health.connected_since = connected_since_;
    return health;
  }

private:
  // mutex_ must be held by the caller.
  zenoh::Publisher * ensure_publisher(const std::string & keyexpr, std::string * error_message)
  {
    if (!session_) {
      last_error_ = "zenoh session not open";
      if (error_message) {
        *error_message = last_error_;
      }
      return nullptr;
    }
    auto it = publishers_.find(keyexpr);
    if (it != publishers_.end()) {
      return &it->second;
    }
    try {
      zenoh::Session::PublisherOptions opts =
        zenoh::Session::PublisherOptions::create_default();
      opts.congestion_control = congestion_control_;
      opts.priority = priority_;
      opts.is_express = express_;
      zenoh::Publisher pub =
        session_->declare_publisher(zenoh::KeyExpr(keyexpr), std::move(opts));
      auto emplaced = publishers_.emplace(keyexpr, std::move(pub));
      return &emplaced.first->second;
    } catch (const zenoh::ZException & ex) {
      last_error_ = ex.what();
      if (error_message) {
        *error_message = last_error_;
      }
      return nullptr;
    }
  }

  mutable std::mutex mutex_;
  std::unique_ptr<zenoh::Session> session_;
  std::unordered_map<std::string, zenoh::Publisher> publishers_;
  zenoh::CongestionControl congestion_control_{Z_CONGESTION_CONTROL_DROP};
  zenoh::Priority priority_{Z_PRIORITY_DATA};
  bool express_{false};
  std::atomic<bool> connected_{false};
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
// The caller MUST keep ts_lib_out and introspection_ts_lib_out alive for as long as the
// returned pointers are used — dropping them triggers dlclose() and dangling pointers.
bool load_type_support(
  const std::string & msg_type,
  const rosidl_message_type_support_t ** rmw_type_support,
  const rosidl_message_type_support_t ** introspection_type_support,
  std::shared_ptr<rcpputils::SharedLibrary> * ts_lib_out = nullptr,
  std::shared_ptr<rcpputils::SharedLibrary> * introspection_ts_lib_out = nullptr)
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

    if (!*rmw_type_support || !*introspection_type_support) {
      return false;
    }

    if (ts_lib_out) {*ts_lib_out = ts_lib;}
    if (introspection_ts_lib_out) {*introspection_ts_lib_out = introspection_ts_lib;}

    return true;
  } catch (...) {
    return false;
  }
}
// Deserializes a CDR message, overwrites header.frame_id with frame_id, and re-serializes
// into output. Returns true if the field was found and output was updated.
// Used for nebula parsing: lets the consumer identify which robot produced the message.
bool inject_frame_id_into_cdr(
  const rclcpp::SerializedMessage & serialized,
  const rosidl_message_type_support_t * rmw_type_support,
  const rosidl_message_type_support_t * introspection_type_support,
  const std::string & frame_id,
  std::vector<uint8_t> & output)
{
  const auto * members =
    static_cast<const rosidl_typesupport_introspection_cpp::MessageMembers *>(
    introspection_type_support->data);
  if (!members || members->size_of_ == 0) {
    return false;
  }

  void * message = malloc(members->size_of_);
  if (!message) {
    return false;
  }
  memset(message, 0, members->size_of_);
  if (members->init_function) {
    rosidl_runtime_cpp::MessageInitialization init;
    members->init_function(message, init);
  }

  const rmw_serialized_message_t & rmw_serialized = serialized.get_rcl_serialized_message();
  if (rmw_deserialize(&rmw_serialized, rmw_type_support, message) != RMW_RET_OK) {
    if (members->fini_function) {members->fini_function(message);}
    free(message);
    return false;
  }

  bool modified = false;
  for (size_t i = 0; i < members->member_count_; ++i) {
    const auto & member = members->members_[i];
    if (std::string(member.name_) == "header" &&
      member.type_id_ == rosidl_typesupport_introspection_cpp::ROS_TYPE_MESSAGE &&
      member.members_)
    {
      const auto * header_members =
        static_cast<const rosidl_typesupport_introspection_cpp::MessageMembers *>(
        member.members_->data);
      uint8_t * header_ptr = static_cast<uint8_t *>(message) + member.offset_;
      for (size_t j = 0; j < header_members->member_count_; ++j) {
        const auto & hm = header_members->members_[j];
        if (std::string(hm.name_) == "frame_id" &&
          hm.type_id_ == rosidl_typesupport_introspection_cpp::ROS_TYPE_STRING)
        {
          *reinterpret_cast<std::string *>(header_ptr + hm.offset_) = frame_id;
          modified = true;
          break;
        }
      }
      break;
    }
  }

  if (modified) {
    rclcpp::SerializedMessage new_serialized_msg;
    if (rmw_serialize(
        message, rmw_type_support,
        &new_serialized_msg.get_rcl_serialized_message()) == RMW_RET_OK)
    {
      const auto & rcl_msg = new_serialized_msg.get_rcl_serialized_message();
      output.assign(rcl_msg.buffer, rcl_msg.buffer + rcl_msg.buffer_length);
    }
  }

  if (members->fini_function) {members->fini_function(message);}
  free(message);
  return modified;
}

}  // namespace

struct ZenohSinkNode::ZenohRuntime
{
  ZenohRuntime()
  : client(std::make_shared<ZenohClient>())
  {
  }

  std::shared_ptr<ZenohClient> client;
};

ZenohSinkNode::ActiveSubscription::ActiveSubscription(ActiveSubscription && other) noexcept
: subscription(std::move(other.subscription)),
  topic_name(std::move(other.topic_name)),
  msg_type(std::move(other.msg_type)),
  runtime_state(std::move(other.runtime_state))
{}

ZenohSinkNode::ActiveSubscription & ZenohSinkNode::ActiveSubscription::operator=(
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

bool apply_message_key_to_json(std::string & json_payload, const std::string & message_key)
{
  if (message_key.empty()) {
    return false;
  }
  auto j = nlohmann::json::parse(json_payload, nullptr, false);
  if (j.is_discarded() || !j.contains("header") || !j["header"].is_object()) {
    return false;
  }
  j["header"]["frame_id"] = message_key;
  json_payload = j.dump();
  return true;
}

namespace
{
std::string trim_copy(std::string value)
{
  value.erase(
    value.begin(),
    std::find_if(
      value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(static_cast<int>(ch));
      }));
  value.erase(
    std::find_if(
      value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(static_cast<int>(ch));
      }).base(),
    value.end());
  return value;
}
}  // namespace

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

    auto topic_name = trim_copy(topic_node.as<std::string>());
    auto msg_type = trim_copy(msg_type_node.as<std::string>());
    std::optional<std::string> zenoh_name;
    if (auto zenoh_name_node = entry["zenoh_name"]) {
      zenoh_name = trim_copy(zenoh_name_node.as<std::string>());
    }

    if (topic_name.empty() || msg_type.empty()) {
      throw std::runtime_error("Subscription entries must have non-empty topic_name and msg_type.");
    }

    if (zenoh_name && zenoh_name->empty()) {
      zenoh_name.reset();
    }

    configs.push_back({topic_name, msg_type, zenoh_name});
  }

  return configs;
}

ZenohSinkNode::ZenohSinkNode(const rclcpp::NodeOptions & options)
: rclcpp_lifecycle::LifecycleNode("zenoh_sink", options)
{
  this->declare_parameter<std::string>("subscriptions_yaml", "");
  this->declare_parameter<int>("qos_depth", qos_depth_);
  // Metrics parameters
  this->declare_parameter<bool>("metrics.enabled", metrics_enabled_);
  this->declare_parameter<int>("metrics.interval_ms", metrics_interval_ms_);
  this->declare_parameter<std::string>("metrics.topic", metrics_topic_);
  this->declare_parameter<std::string>("zenoh.mode", zenoh_parameters_.mode);
  this->declare_parameter<std::vector<std::string>>("zenoh.connect", zenoh_parameters_.connect);
  this->declare_parameter<std::vector<std::string>>("zenoh.listen", zenoh_parameters_.listen);
  this->declare_parameter<std::string>("zenoh.config_path", zenoh_parameters_.config_path);
  this->declare_parameter<std::string>("zenoh.key_prefix", zenoh_parameters_.key_prefix);
  this->declare_parameter<std::string>("zenoh.topic_mapping_mode", "prefix_ros_topic");
  this->declare_parameter<std::string>("zenoh.fixed_keyexpr", zenoh_parameters_.fixed_keyexpr);
  this->declare_parameter<std::string>("zenoh.payload_format", "cdr");
  this->declare_parameter<std::string>(
    "zenoh.congestion_control", zenoh_parameters_.congestion_control);
  this->declare_parameter<int>("zenoh.priority", zenoh_parameters_.priority);
  this->declare_parameter<bool>("zenoh.express", zenoh_parameters_.express);
  // optional: when set, overrides header.frame_id in published messages (for nebula parsing)
  this->declare_parameter<std::string>("zenoh.message_key", "");

  on_parameters_set_handle_ = this->add_on_set_parameters_callback(
    std::bind(&ZenohSinkNode::on_parameters_set, this, std::placeholders::_1));
}

ZenohSinkNode::CallbackReturn ZenohSinkNode::on_configure(
  const rclcpp_lifecycle::State &)
{
  std::string error_message;
  if (!configure_from_parameters(&error_message)) {
    RCLCPP_ERROR(get_logger(), "Failed to configure zenoh_sink: %s", error_message.c_str());
    return CallbackReturn::FAILURE;
  }

  if (configured_subscriptions_.empty()) {
    RCLCPP_WARN(
      get_logger(),
      "Configured zenoh_sink with no subscriptions. Activate after setting 'subscriptions_yaml'.");
  }

  RCLCPP_INFO(
    get_logger(), "Configured zenoh_sink with %zu subscription entries",
    configured_subscriptions_.size());
  return CallbackReturn::SUCCESS;
}

ZenohSinkNode::CallbackReturn ZenohSinkNode::on_activate(
  const rclcpp_lifecycle::State &)
{
  if (!start_client()) {
    RCLCPP_ERROR(get_logger(), "Failed to open Zenoh session");
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
      std::bind(&ZenohSinkNode::publish_metrics, this));
  }
  RCLCPP_INFO(
    get_logger(), "Activated zenoh_sink with %zu active subscriptions",
    active_subscriptions_.size());
  return CallbackReturn::SUCCESS;
}

ZenohSinkNode::CallbackReturn ZenohSinkNode::on_deactivate(
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

  RCLCPP_INFO(get_logger(), "Deactivated zenoh_sink and cleared subscriptions");
  return CallbackReturn::SUCCESS;
}

ZenohSinkNode::CallbackReturn ZenohSinkNode::on_cleanup(
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

  RCLCPP_INFO(get_logger(), "Cleaned up zenoh_sink configuration and runtime state");
  return CallbackReturn::SUCCESS;
}

ZenohSinkNode::CallbackReturn ZenohSinkNode::on_shutdown(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_client();
  clear_subscriptions();
  configured_subscriptions_.clear();

  RCLCPP_INFO(get_logger(), "Shutting down zenoh_sink");
  return CallbackReturn::SUCCESS;
}

rcl_interfaces::msg::SetParametersResult ZenohSinkNode::on_parameters_set(
  const std::vector<rclcpp::Parameter> & parameters)
{
  rcl_interfaces::msg::SetParametersResult result;
  result.successful = true;
  result.reason = "accepted";

  bool update_required = false;
  std::string pending_yaml;
  int pending_depth = qos_depth_;
  ZenohParameters pending_zenoh = zenoh_parameters_;
  bool zenoh_update_required = false;
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
    } else if (name.rfind("zenoh.", 0) == 0) {
      const auto & current_state = this->get_current_state();
      if (current_state.id() == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE) {
        result.successful = false;
        result.reason = "deactivate first";
        return result;
      }
      zenoh_update_required = true;
      if (name == "zenoh.mode") {
        pending_zenoh.mode = param.as_string();
      } else if (name == "zenoh.connect") {
        pending_zenoh.connect = param.as_string_array();
      } else if (name == "zenoh.listen") {
        pending_zenoh.listen = param.as_string_array();
      } else if (name == "zenoh.config_path") {
        pending_zenoh.config_path = param.as_string();
      } else if (name == "zenoh.key_prefix") {
        pending_zenoh.key_prefix = param.as_string();
      } else if (name == "zenoh.topic_mapping_mode") {
        auto mode_value = param.as_string();
        if (mode_value == "prefix_ros_topic") {
          pending_zenoh.topic_mapping_mode = TopicMappingMode::PREFIX_ROS_TOPIC;
        } else if (mode_value == "fixed") {
          pending_zenoh.topic_mapping_mode = TopicMappingMode::FIXED;
        } else {
          result.successful = false;
          result.reason = "invalid zenoh.topic_mapping_mode";
          return result;
        }
      } else if (name == "zenoh.fixed_keyexpr") {
        pending_zenoh.fixed_keyexpr = param.as_string();
      } else if (name == "zenoh.payload_format") {
        auto format_value = param.as_string();
        if (format_value == "cdr") {
          pending_zenoh.payload_format = PayloadFormat::CDR;
        } else if (format_value == "json") {
          pending_zenoh.payload_format = PayloadFormat::JSON;
        } else {
          result.successful = false;
          result.reason = "invalid zenoh.payload_format";
          return result;
        }
      } else if (name == "zenoh.congestion_control") {
        pending_zenoh.congestion_control = param.as_string();
      } else if (name == "zenoh.priority") {
        pending_zenoh.priority = param.as_int();
      } else if (name == "zenoh.express") {
        pending_zenoh.express = param.as_bool();
      } else if (name == "zenoh.message_key") {
        pending_zenoh.message_key = param.as_string();
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

  if (zenoh_update_required) {
    std::string zenoh_error;
    if (!validate_zenoh_parameters(pending_zenoh, &zenoh_error)) {
      result.successful = false;
      result.reason = zenoh_error;
      return result;
    }
    zenoh_parameters_ = pending_zenoh;
  }

  if (metrics_update_required) {
    metrics_enabled_ = pending_metrics_enabled;
    metrics_interval_ms_ = pending_metrics_interval_ms;
    metrics_topic_ = pending_metrics_topic;
  }

  return result;
}

bool ZenohSinkNode::configure_from_parameters(std::string * error_message)
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

  return configure_zenoh_parameters(error_message);
}

bool ZenohSinkNode::validate_qos_depth(int qos_depth, std::string * error_message) const
{
  if (qos_depth <= 0) {
    *error_message = "qos_depth must be greater than zero.";
    return false;
  }
  return true;
}

bool ZenohSinkNode::validate_zenoh_parameters(
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
  if (pending.key_prefix.empty() &&
    pending.topic_mapping_mode == TopicMappingMode::PREFIX_ROS_TOPIC)
  {
    // An empty prefix is allowed (publishes the bare ROS topic), so this is not
    // an error; kept here for parity with the other sinks' validation shape.
  }
  if (pending.topic_mapping_mode == TopicMappingMode::FIXED && pending.fixed_keyexpr.empty()) {
    *error_message = "zenoh.fixed_keyexpr cannot be empty when mapping mode is 'fixed'.";
    return false;
  }
  if (pending.congestion_control != "drop" && pending.congestion_control != "block") {
    *error_message = "zenoh.congestion_control must be 'drop' or 'block'.";
    return false;
  }
  if (pending.priority < 1 || pending.priority > 7) {
    *error_message = "zenoh.priority must be between 1 and 7.";
    return false;
  }
  return true;
}

bool ZenohSinkNode::configure_zenoh_parameters(std::string * error_message)
{
  ZenohParameters pending = zenoh_parameters_;
  pending.mode = this->get_parameter("zenoh.mode").as_string();
  pending.connect = this->get_parameter("zenoh.connect").as_string_array();
  pending.listen = this->get_parameter("zenoh.listen").as_string_array();
  pending.config_path = this->get_parameter("zenoh.config_path").as_string();
  pending.key_prefix = this->get_parameter("zenoh.key_prefix").as_string();

  auto mapping_mode = this->get_parameter("zenoh.topic_mapping_mode").as_string();
  if (mapping_mode == "prefix_ros_topic") {
    pending.topic_mapping_mode = TopicMappingMode::PREFIX_ROS_TOPIC;
  } else if (mapping_mode == "fixed") {
    pending.topic_mapping_mode = TopicMappingMode::FIXED;
  } else {
    *error_message = "Invalid zenoh.topic_mapping_mode value.";
    return false;
  }

  pending.fixed_keyexpr = this->get_parameter("zenoh.fixed_keyexpr").as_string();
  auto payload_format = this->get_parameter("zenoh.payload_format").as_string();
  if (payload_format == "cdr") {
    pending.payload_format = PayloadFormat::CDR;
  } else if (payload_format == "json") {
    pending.payload_format = PayloadFormat::JSON;
  } else {
    *error_message = "Invalid zenoh.payload_format value.";
    return false;
  }
  pending.congestion_control = this->get_parameter("zenoh.congestion_control").as_string();
  pending.priority = this->get_parameter("zenoh.priority").as_int();
  pending.express = this->get_parameter("zenoh.express").as_bool();
  pending.message_key = this->get_parameter("zenoh.message_key").as_string();

  if (!validate_zenoh_parameters(pending, error_message)) {
    return false;
  }

  zenoh_parameters_ = pending;
  return true;
}

bool ZenohSinkNode::start_client()
{
  zenoh_runtime_ = std::make_shared<ZenohRuntime>();

  const zenoh::CongestionControl congestion_control =
    zenoh_parameters_.congestion_control == "block" ?
    Z_CONGESTION_CONTROL_BLOCK : Z_CONGESTION_CONTROL_DROP;
  const auto priority = static_cast<zenoh::Priority>(zenoh_parameters_.priority);

  std::string error_message;
  if (!zenoh_runtime_->client->open(
      zenoh_parameters_.mode,
      zenoh_parameters_.connect,
      zenoh_parameters_.listen,
      zenoh_parameters_.config_path,
      congestion_control,
      priority,
      zenoh_parameters_.express,
      &error_message))
  {
    RCLCPP_ERROR(get_logger(), "Zenoh session open failed: %s", error_message.c_str());
    zenoh_runtime_.reset();
    return false;
  }

  return true;
}

void ZenohSinkNode::stop_client()
{
  auto runtime = std::exchange(zenoh_runtime_, nullptr);
  if (runtime) {
    runtime->client->close();
  }
}

std::string ZenohSinkNode::map_zenoh_keyexpr(const std::string & ros_topic) const
{
  if (zenoh_parameters_.topic_mapping_mode == TopicMappingMode::FIXED) {
    return zenoh_parameters_.fixed_keyexpr;
  }

  std::string normalized = ros_topic;
  if (!normalized.empty() && normalized.front() == '/') {
    normalized.erase(normalized.begin());
  }

  if (zenoh_parameters_.key_prefix.empty()) {
    return normalized;
  }

  if (zenoh_parameters_.key_prefix.back() == '/' || normalized.empty()) {
    return zenoh_parameters_.key_prefix + normalized;
  }
  return zenoh_parameters_.key_prefix + "/" + normalized;
}

rclcpp::QoS ZenohSinkNode::build_qos_profile() const
{
  rclcpp::QoS qos{rclcpp::SystemDefaultsQoS()};
  qos.keep_last(static_cast<size_t>(qos_depth_));
  return qos;
}

bool ZenohSinkNode::build_subscriptions()
{
  clear_subscriptions();

  if (!zenoh_runtime_) {
    RCLCPP_ERROR(get_logger(), "Zenoh session is not initialized");
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
    const std::string & zenoh_name =
      config.zenoh_name ? *config.zenoh_name : config.topic_name;
    runtime.runtime_state->zenoh_keyexpr = map_zenoh_keyexpr(zenoh_name);
    runtime.runtime_state->message_key = zenoh_parameters_.message_key;
    runtime.runtime_state->payload_format = zenoh_parameters_.payload_format;
    if (zenoh_parameters_.payload_format == PayloadFormat::JSON ||
      !zenoh_parameters_.message_key.empty())
    {
      // Load type support for JSON serialization or CDR frame_id injection (nebula parsing)
      if (!load_type_support(
          config.msg_type,
          &runtime.runtime_state->rmw_type_support,
          &runtime.runtime_state->introspection_type_support,
          &runtime.runtime_state->rmw_ts_lib,
          &runtime.runtime_state->introspection_ts_lib))
      {
        if (zenoh_parameters_.payload_format == PayloadFormat::JSON) {
          RCLCPP_WARN(
            get_logger(),
            "Failed to load type support for JSON serialization of '%s'. "
            "Falling back to CDR format.",
            config.msg_type.c_str());
          runtime.runtime_state->payload_format = PayloadFormat::CDR;
        } else {
          RCLCPP_WARN(
            get_logger(),
            "Failed to load type support for '%s'; message_key frame_id injection disabled.",
            config.msg_type.c_str());
        }
      } else {
        RCLCPP_INFO(
          get_logger(),
          "Successfully loaded type support for '%s'",
          config.msg_type.c_str());
      }
    }
    runtime.runtime_state->log_label =
      "topic='" + config.topic_name + "' keyexpr='" + runtime.runtime_state->zenoh_keyexpr +
      "' type='" + config.msg_type + "'";

    // Pre-declare the publisher so an invalid key expression is surfaced now
    // rather than silently on the first message.
    std::string declare_error;
    if (!zenoh_runtime_->client->declare_publisher(
        runtime.runtime_state->zenoh_keyexpr, &declare_error))
    {
      RCLCPP_WARN(
        get_logger(),
        "Failed to declare Zenoh publisher for %s: %s",
        runtime.runtime_state->log_label.c_str(), declare_error.c_str());
    }

    auto runtime_state = runtime.runtime_state;
    auto callback =
      [this, runtime_state](std::shared_ptr<rclcpp::SerializedMessage> msg) {
        if (!is_active_.load(std::memory_order_acquire)) {
          return;
        }
        auto zenoh_runtime = zenoh_runtime_;
        if (!zenoh_runtime) {
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
          apply_message_key_to_json(json_payload, runtime_state->message_key);
          value.assign(json_payload.begin(), json_payload.end());
        } else {
          value.resize(msg->size());
          std::memcpy(value.data(), msg->get_rcl_serialized_message().buffer, msg->size());
          if (!runtime_state->message_key.empty() &&
            runtime_state->rmw_type_support && runtime_state->introspection_type_support)
          {
            inject_frame_id_into_cdr(
              *msg,
              runtime_state->rmw_type_support,
              runtime_state->introspection_type_support,
              runtime_state->message_key,
              value);
          }
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
        bool publish_ok = zenoh_runtime->client->publish(
          runtime_state->zenoh_keyexpr,
          value,
          runtime_state->msg_type,
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
          auto health = zenoh_runtime->client->health();
          if (health.last_error.empty()) {
            RCLCPP_INFO(
              this->get_logger(),
              "[zenoh_sink] %s size=%zu bytes sent=%lu errors=%lu",
              runtime_state->log_label.c_str(), msg->size(),
              runtime_state->sent_ok.load(std::memory_order_relaxed),
              runtime_state->errors.load(std::memory_order_relaxed));
          } else {
            RCLCPP_INFO(
              this->get_logger(),
              "[zenoh_sink] %s size=%zu bytes sent=%lu errors=%lu last_error='%s'",
              runtime_state->log_label.c_str(), msg->size(),
              runtime_state->sent_ok.load(std::memory_order_relaxed),
              runtime_state->errors.load(std::memory_order_relaxed),
              health.last_error.c_str());
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

    // If type support is still needed (JSON or message_key injection) but wasn't loaded yet,
    // try once more now that the subscription exists
    if ((runtime.runtime_state->payload_format == PayloadFormat::JSON ||
      !runtime.runtime_state->message_key.empty()) &&
      !runtime.runtime_state->rmw_type_support)
    {
      // The subscription was created successfully, so the type support should be available
      try {
        load_type_support(
          runtime.msg_type,
          &runtime.runtime_state->rmw_type_support,
          &runtime.runtime_state->introspection_type_support,
          &runtime.runtime_state->rmw_ts_lib,
          &runtime.runtime_state->introspection_ts_lib);
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
        if (runtime.runtime_state->payload_format == PayloadFormat::JSON) {
          RCLCPP_WARN(
            get_logger(),
            "Failed to load type support even after subscription creation for '%s'. "
            "Falling back to CDR format.",
            runtime.msg_type.c_str());
          runtime.runtime_state->payload_format = PayloadFormat::CDR;
        } else {
          RCLCPP_WARN(
            get_logger(),
            "Failed to load type support for '%s'; message_key frame_id injection disabled.",
            runtime.msg_type.c_str());
        }
      }
    }
  }
  return true;
}

void ZenohSinkNode::clear_subscriptions()
{
  active_subscriptions_.clear();
}

void ZenohSinkNode::publish_metrics()
{
  if (!metrics_enabled_ || !metrics_pub_) {
    return;
  }

  nlohmann::json root = nlohmann::json::array();
  const double interval_sec = static_cast<double>(metrics_interval_ms_) / 1000.0;

  ZenohHealth health;
  if (zenoh_runtime_) {
    health = zenoh_runtime_->client->health();
  }

  const auto now = std::chrono::steady_clock::now();
  const double connection_uptime_sec = health.connected ?
    std::chrono::duration_cast<std::chrono::duration<double>>(
    now -
    health.connected_since).count() :
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
      {"zenoh_keyexpr", rt.zenoh_keyexpr},
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
      {"zenoh_session", {
          {"connected", health.connected},
          {"uptime_sec", connection_uptime_sec},
          {"last_error", health.last_error}
        }}
    };

    root.push_back(entry);
  }

  std_msgs::msg::String msg;
  msg.data = root.dump();
  metrics_pub_->publish(msg);
}

}  // namespace zenoh_sink

RCLCPP_COMPONENTS_REGISTER_NODE(zenoh_sink::ZenohSinkNode)
