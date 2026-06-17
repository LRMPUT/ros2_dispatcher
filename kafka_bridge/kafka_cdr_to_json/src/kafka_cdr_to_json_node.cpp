// Copyright 2025 Maciej Krupka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "kafka_cdr_to_json/kafka_cdr_to_json_node.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cmath>
#include <codecvt>
#include <cstring>
#include <locale>
#include <optional>
#include <sstream>
#include <utility>

#include "nlohmann/json.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "rosbag2_cpp/typesupport_helpers.hpp"
#include "rosidl_runtime_cpp/message_initialization.hpp"
#include "rosidl_typesupport_introspection_cpp/field_types.hpp"
#include "rosidl_typesupport_introspection_cpp/message_introspection.hpp"
#include "rmw/rmw.h"

namespace kafka_cdr_to_json
{
namespace
{
constexpr size_t kMaxLatencySamples = 1000;
constexpr int64_t kThrottleIntervalNs = 1'000'000'000LL;

std::unordered_map<std::string, std::string> parse_topic_mappings(
  const std::string & mappings)
{
  std::unordered_map<std::string, std::string> result;
  std::string current;
  std::istringstream stream(mappings);
  while (std::getline(stream, current, ',')) {
    auto equal_pos = current.find('=');
    if (equal_pos == std::string::npos) {
      continue;
    }
    auto key = current.substr(0, equal_pos);
    auto value = current.substr(equal_pos + 1);
    auto trim = [](std::string & entry) {
        entry.erase(
          entry.begin(),
          std::find_if(entry.begin(), entry.end(), [](unsigned char ch) {
            return !std::isspace(static_cast<int>(ch));
          }));
        entry.erase(
          std::find_if(entry.rbegin(), entry.rend(), [](unsigned char ch) {
            return !std::isspace(static_cast<int>(ch));
          }).base(),
          entry.end());
      };
    trim(key);
    trim(value);
    if (!key.empty() && !value.empty()) {
      result[key] = value;
    }
  }
  return result;
}

uint64_t calculate_percentile(std::vector<uint64_t> samples, double percentile)
{
  if (samples.empty()) {
    return 0;
  }
  std::sort(samples.begin(), samples.end());
  double clamped = std::min(1.0, std::max(0.0, percentile));
  const size_t index = static_cast<size_t>(
    std::ceil(clamped * static_cast<double>(samples.size() - 1)));
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
    if (member.get_const_function) {
      element_ptr = member.get_const_function(array_ptr, index);
    } else {
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
  const std::string & ros_type,
  const std::optional<int64_t> & kafka_timestamp_ms,
  bool include_ros_type,
  bool include_timestamp,
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

  void * message = std::malloc(members->size_of_);
  if (!message) {
    if (error_message) {
      *error_message = "Failed to allocate message for JSON serialization.";
    }
    return false;
  }

  std::memset(message, 0, members->size_of_);

  if (members->init_function) {
    rosidl_runtime_cpp::MessageInitialization init;
    members->init_function(message, init);
  }

  const rmw_serialized_message_t & rmw_serialized =
    serialized.get_rcl_serialized_message();
  if (rmw_deserialize(&rmw_serialized, rmw_type_support, message) != RMW_RET_OK) {
    if (members->fini_function) {
      members->fini_function(message);
    }
    std::free(message);
    if (error_message) {
      *error_message = "Failed to deserialize message for JSON serialization.";
    }
    return false;
  }

  nlohmann::json payload = build_json_message(*members, message);
  if (include_ros_type) {
    payload["__ros_type"] = ros_type;
  }
  if (include_timestamp) {
    if (kafka_timestamp_ms.has_value()) {
      payload["__kafka_timestamp_ms"] = kafka_timestamp_ms.value();
    } else {
      payload["__kafka_timestamp_ms"] = nullptr;
    }
  }

  *output = payload.dump();

  if (members->fini_function) {
    members->fini_function(message);
  }
  std::free(message);
  return true;
}
}  // namespace

KafkaCdrToJsonNode::KafkaCdrToJsonNode(const rclcpp::NodeOptions & options)
: rclcpp_lifecycle::LifecycleNode("kafka_cdr_to_json", options)
{
  declare_parameter("kafka.bootstrap_servers", kafka_parameters_.bootstrap_servers);
  declare_parameter("kafka.group_id", kafka_parameters_.group_id);
  declare_parameter("kafka.input_topic_pattern", kafka_parameters_.input_topic_pattern);
  declare_parameter("kafka.output_topic_prefix", kafka_parameters_.output_topic_prefix);
  declare_parameter("kafka.offset_reset", kafka_parameters_.offset_reset);
  declare_parameter("json.include_ros_type", json_parameters_.include_ros_type);
  declare_parameter("json.include_timestamp", json_parameters_.include_timestamp);
  declare_parameter("metrics.enabled", metrics_enabled_);
  declare_parameter("metrics.interval_ms", metrics_interval_ms_);
  declare_parameter("metrics.topic", metrics_topic_);
  declare_parameter("topic_mappings", std::string{});

  on_parameters_set_handle_ = add_on_set_parameters_callback(
    std::bind(&KafkaCdrToJsonNode::on_parameters_set, this, std::placeholders::_1));
}

KafkaCdrToJsonNode::CallbackReturn KafkaCdrToJsonNode::on_configure(
  const rclcpp_lifecycle::State &)
{
  std::string error;
  if (!configure_from_parameters(&error)) {
    RCLCPP_ERROR(get_logger(), "Configuration failed: %s", error.c_str());
    return CallbackReturn::FAILURE;
  }

  if (!validate_parameters(&error)) {
    RCLCPP_ERROR(get_logger(), "Invalid parameters: %s", error.c_str());
    return CallbackReturn::FAILURE;
  }

  metrics_pub_ = create_publisher<std_msgs::msg::String>(metrics_topic_, rclcpp::QoS(10));
  return CallbackReturn::SUCCESS;
}

KafkaCdrToJsonNode::CallbackReturn KafkaCdrToJsonNode::on_activate(
  const rclcpp_lifecycle::State &)
{
  std::string error;
  if (!start_consumer(&error)) {
    RCLCPP_ERROR(get_logger(), "Failed to start Kafka consumer: %s", error.c_str());
    return CallbackReturn::FAILURE;
  }

  if (!start_producer(&error)) {
    RCLCPP_ERROR(get_logger(), "Failed to start Kafka producer: %s", error.c_str());
    stop_consumer();
    return CallbackReturn::FAILURE;
  }

  is_active_.store(true, std::memory_order_release);
  if (metrics_pub_) {
    metrics_pub_->on_activate();
  }
  reset_metrics_timer();

  running_.store(true, std::memory_order_release);
  consumer_thread_ = std::thread([this]() { poll_loop(); });
  return CallbackReturn::SUCCESS;
}

KafkaCdrToJsonNode::CallbackReturn KafkaCdrToJsonNode::on_deactivate(
  const rclcpp_lifecycle::State &)
{
  running_.store(false, std::memory_order_release);
  if (consumer_thread_.joinable()) {
    consumer_thread_.join();
  }
  stop_consumer();
  stop_producer();
  is_active_.store(false, std::memory_order_release);
  reset_metrics_timer();
  if (metrics_pub_) {
    metrics_pub_->on_deactivate();
  }
  return CallbackReturn::SUCCESS;
}

KafkaCdrToJsonNode::CallbackReturn KafkaCdrToJsonNode::on_cleanup(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  running_.store(false, std::memory_order_release);
  if (consumer_thread_.joinable()) {
    consumer_thread_.join();
  }
  stop_consumer();
  stop_producer();
  metrics_timer_.reset();
  metrics_pub_.reset();
  {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    type_support_cache_.clear();
    metrics_.clear();
  }
  return CallbackReturn::SUCCESS;
}

KafkaCdrToJsonNode::CallbackReturn KafkaCdrToJsonNode::on_shutdown(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  running_.store(false, std::memory_order_release);
  if (consumer_thread_.joinable()) {
    consumer_thread_.join();
  }
  stop_consumer();
  stop_producer();
  return CallbackReturn::SUCCESS;
}

rcl_interfaces::msg::SetParametersResult KafkaCdrToJsonNode::on_parameters_set(
  const std::vector<rclcpp::Parameter> & parameters)
{
  rcl_interfaces::msg::SetParametersResult result;
  result.successful = false;
  result.reason = "Cannot change parameters while node is active.";

  if (is_active_.load(std::memory_order_acquire)) {
    return result;
  }

  for (const auto & parameter : parameters) {
    if (parameter.get_name() == "kafka.bootstrap_servers") {
      kafka_parameters_.bootstrap_servers = parameter.as_string();
    } else if (parameter.get_name() == "kafka.group_id") {
      kafka_parameters_.group_id = parameter.as_string();
    } else if (parameter.get_name() == "kafka.input_topic_pattern") {
      kafka_parameters_.input_topic_pattern = parameter.as_string();
    } else if (parameter.get_name() == "kafka.output_topic_prefix") {
      kafka_parameters_.output_topic_prefix = parameter.as_string();
    } else if (parameter.get_name() == "kafka.offset_reset") {
      kafka_parameters_.offset_reset = parameter.as_string();
    } else if (parameter.get_name() == "json.include_ros_type") {
      json_parameters_.include_ros_type = parameter.as_bool();
    } else if (parameter.get_name() == "json.include_timestamp") {
      json_parameters_.include_timestamp = parameter.as_bool();
    } else if (parameter.get_name() == "metrics.enabled") {
      metrics_enabled_ = parameter.as_bool();
    } else if (parameter.get_name() == "metrics.interval_ms") {
      metrics_interval_ms_ = parameter.as_int();
    } else if (parameter.get_name() == "metrics.topic") {
      metrics_topic_ = parameter.as_string();
    } else if (parameter.get_name() == "topic_mappings") {
      topic_mappings_ = parse_topic_mappings(parameter.as_string());
    }
  }

  std::string error;
  if (!validate_parameters(&error)) {
    result.reason = error;
    result.successful = false;
    return result;
  }

  result.successful = true;
  result.reason = "Parameters accepted.";
  return result;
}

bool KafkaCdrToJsonNode::configure_from_parameters(std::string * error_message)
{
  get_parameter("kafka.bootstrap_servers", kafka_parameters_.bootstrap_servers);
  get_parameter("kafka.group_id", kafka_parameters_.group_id);
  get_parameter("kafka.input_topic_pattern", kafka_parameters_.input_topic_pattern);
  get_parameter("kafka.output_topic_prefix", kafka_parameters_.output_topic_prefix);
  get_parameter("kafka.offset_reset", kafka_parameters_.offset_reset);
  get_parameter("json.include_ros_type", json_parameters_.include_ros_type);
  get_parameter("json.include_timestamp", json_parameters_.include_timestamp);
  get_parameter("metrics.enabled", metrics_enabled_);
  get_parameter("metrics.interval_ms", metrics_interval_ms_);
  get_parameter("metrics.topic", metrics_topic_);

  std::string mapping_text;
  get_parameter("topic_mappings", mapping_text);
  topic_mappings_ = parse_topic_mappings(mapping_text);

  return validate_parameters(error_message);
}

bool KafkaCdrToJsonNode::validate_parameters(std::string * error_message) const
{
  if (kafka_parameters_.bootstrap_servers.empty()) {
    if (error_message) {
      *error_message = "kafka.bootstrap_servers must be non-empty.";
    }
    return false;
  }
  if (kafka_parameters_.group_id.empty()) {
    if (error_message) {
      *error_message = "kafka.group_id must be non-empty.";
    }
    return false;
  }
  if (kafka_parameters_.input_topic_pattern.empty()) {
    if (error_message) {
      *error_message = "kafka.input_topic_pattern must be non-empty.";
    }
    return false;
  }
  if (kafka_parameters_.offset_reset != "latest" && kafka_parameters_.offset_reset != "earliest") {
    if (error_message) {
      *error_message = "kafka.offset_reset must be 'latest' or 'earliest'.";
    }
    return false;
  }
  if (metrics_interval_ms_ <= 0) {
    if (error_message) {
      *error_message = "metrics.interval_ms must be > 0.";
    }
    return false;
  }
  if (metrics_enabled_ && metrics_topic_.empty()) {
    if (error_message) {
      *error_message = "metrics.topic must be non-empty when metrics are enabled.";
    }
    return false;
  }
  return true;
}

bool KafkaCdrToJsonNode::start_consumer(std::string * error_message)
{
  std::string errstr;
  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!conf) {
    if (error_message) {
      *error_message = "Unable to create Kafka configuration.";
    }
    return false;
  }

  auto set_conf = [&](const std::string & key, const std::string & value) {
      return conf->set(key, value, errstr) == RdKafka::Conf::CONF_OK;
    };

  if (!set_conf("bootstrap.servers", kafka_parameters_.bootstrap_servers) ||
    !set_conf("group.id", kafka_parameters_.group_id) ||
    !set_conf("enable.auto.commit", "true") ||
    !set_conf("auto.offset.reset", kafka_parameters_.offset_reset) ||
    !set_conf("allow.auto.create.topics", "false"))
  {
    if (error_message) {
      *error_message = errstr;
    }
    return false;
  }

  auto consumer = std::unique_ptr<RdKafka::KafkaConsumer>(
    RdKafka::KafkaConsumer::create(conf.get(), errstr));
  if (!consumer) {
    if (error_message) {
      *error_message = errstr;
    }
    return false;
  }

  std::vector<std::string> topics{ kafka_parameters_.input_topic_pattern };
  RdKafka::ErrorCode err = consumer->subscribe(topics);
  if (err != RdKafka::ERR_NO_ERROR) {
    if (error_message) {
      *error_message = RdKafka::err2str(err);
    }
    return false;
  }

  consumer_ = std::move(consumer);
  return true;
}

void KafkaCdrToJsonNode::stop_consumer()
{
  if (consumer_) {
    consumer_->close();
    consumer_.reset();
  }
}

bool KafkaCdrToJsonNode::start_producer(std::string * error_message)
{
  kafka_client::KafkaProducerConfig config;
  config.bootstrap_servers = kafka_parameters_.bootstrap_servers;
  config.client_id = get_name();
  producer_ = std::make_shared<kafka_client::KafkaProducer>(config);
  if (!producer_->start()) {
    producer_.reset();
    if (error_message) {
      *error_message = "Unable to start Kafka producer.";
    }
    return false;
  }

  const auto health = producer_->health();
  if (health.status == kafka_client::ProducerStatus::DEGRADED) {
    RCLCPP_WARN(
      get_logger(), "Kafka producer started in degraded mode: %s",
      health.last_error.c_str());
  }
  return true;
}

void KafkaCdrToJsonNode::stop_producer()
{
  auto producer = std::exchange(producer_, nullptr);
  if (producer) {
    producer->stop();
  }
}

void KafkaCdrToJsonNode::poll_loop()
{
  while (running_.load(std::memory_order_acquire)) {
    if (!consumer_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }
    std::unique_ptr<RdKafka::Message> message(consumer_->consume(1000));
    if (!message) {
      continue;
    }
    if (message->err() == RdKafka::ERR__TIMED_OUT) {
      continue;
    }
    if (message->err() == RdKafka::ERR__PARTITION_EOF) {
      continue;
    }
    if (message->err() != RdKafka::ERR_NO_ERROR) {
      if (should_log_throttled("consume_error")) {
        RCLCPP_ERROR(get_logger(), "Kafka consume error: %s", message->errstr().c_str());
      }
      continue;
    }
    process_message(message.get());
  }
}

void KafkaCdrToJsonNode::process_message(RdKafka::Message * message)
{
  const auto payload_size = static_cast<size_t>(message->len());
  const uint8_t * payload = static_cast<const uint8_t *>(message->payload());
  if (!payload || payload_size == 0) {
    return;
  }

  std::string ros_type;
  if (auto headers = message->headers()) {
    auto header_list = headers->get("ros_type");
    if (!header_list.empty()) {
      const auto & header = header_list[0];
      ros_type.assign(
        static_cast<const char *>(header.value()),
        header.value_size());
    }
  }

  const std::string input_topic = message->topic_name();
  const std::string output_topic = map_output_topic(input_topic);
  const std::string resolved_ros_type = ros_type.empty() ? "unknown" : ros_type;

  std::shared_ptr<TopicMetrics> metrics;
  {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto it = metrics_.find(input_topic);
    if (it == metrics_.end()) {
      auto entry = std::make_shared<TopicMetrics>();
      entry->input_topic = input_topic;
      entry->output_topic = output_topic;
      entry->ros_type = resolved_ros_type;
      it = metrics_.emplace(input_topic, entry).first;
    } else {
      it->second->output_topic = output_topic;
      it->second->ros_type = resolved_ros_type;
    }
    metrics = it->second;
  }

  metrics->received.fetch_add(1, std::memory_order_relaxed);
  metrics->bytes.fetch_add(payload_size, std::memory_order_relaxed);

  auto timestamp = message->timestamp();
  std::optional<int64_t> kafka_timestamp_ms;
  if (timestamp.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE &&
    timestamp.timestamp >= 0)
  {
    kafka_timestamp_ms = timestamp.timestamp;
    auto now = std::chrono::system_clock::now();
    auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    auto msg_ns = std::chrono::nanoseconds(timestamp.timestamp * 1000000LL);
    if (now_ns > msg_ns) {
      auto latency = static_cast<uint64_t>((now_ns - msg_ns).count());
      metrics->latency_ns_max.store(
        std::max(metrics->latency_ns_max.load(std::memory_order_relaxed), latency),
        std::memory_order_relaxed);
      std::lock_guard<std::mutex> lock(metrics->metrics_mutex);
      metrics->latency_samples.push_back(latency);
      if (metrics->latency_samples.size() > kMaxLatencySamples) {
        metrics->latency_samples.pop_front();
      }
    }
  }

  if (ros_type.empty()) {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled("missing_ros_type")) {
      RCLCPP_WARN(get_logger(), "Missing ros_type header, skipping message on %s.",
        input_topic.c_str());
    }
    return;
  }

  TypeSupportCacheEntry type_support;
  std::string type_error;
  if (!ensure_type_support(ros_type, &type_support, &type_error)) {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled("type_support")) {
      RCLCPP_WARN(get_logger(), "Failed to load type support for '%s': %s",
        ros_type.c_str(), type_error.c_str());
    }
    return;
  }

  rclcpp::SerializedMessage serialized(payload_size);
  auto & rmw_serialized = serialized.get_rcl_serialized_message();
  std::memcpy(rmw_serialized.buffer, payload, payload_size);
  rmw_serialized.buffer_length = payload_size;

  std::string json_payload;
  std::string json_error;
  if (!serialize_message_to_json(
      serialized,
      type_support.rmw_type_support,
      type_support.introspection_type_support,
      ros_type,
      kafka_timestamp_ms,
      json_parameters_.include_ros_type,
      json_parameters_.include_timestamp,
      &json_payload,
      &json_error))
  {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled("json_serialize")) {
      RCLCPP_WARN(get_logger(), "Failed to serialize JSON for '%s': %s",
        ros_type.c_str(), json_error.c_str());
    }
    return;
  }

  if (!producer_) {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled("producer_missing")) {
      RCLCPP_ERROR(get_logger(), "Kafka producer unavailable for %s", input_topic.c_str());
    }
    return;
  }

  const std::vector<uint8_t> json_bytes(json_payload.begin(), json_payload.end());
  kafka_client::KafkaSendResult send_result = producer_->send(
    output_topic,
    {},
    json_bytes,
    kafka_timestamp_ms.value_or(0),
    {});

  if (send_result.status == kafka_client::SendStatus::QUEUE_FULL && send_result.buffered) {
    metrics->converted.fetch_add(1, std::memory_order_relaxed);
    {
      std::lock_guard<std::mutex> lock(metrics->metrics_mutex);
      const uint64_t json_size = json_bytes.size();
      metrics->json_size_total += json_size;
      if (metrics->json_size_min == 0 || json_size < metrics->json_size_min) {
        metrics->json_size_min = json_size;
      }
      metrics->json_size_max = std::max(metrics->json_size_max, json_size);
    }
    return;
  }

  if (send_result.status == kafka_client::SendStatus::SENT) {
    metrics->converted.fetch_add(1, std::memory_order_relaxed);
    {
      std::lock_guard<std::mutex> lock(metrics->metrics_mutex);
      const uint64_t json_size = json_bytes.size();
      metrics->json_size_total += json_size;
      if (metrics->json_size_min == 0 || json_size < metrics->json_size_min) {
        metrics->json_size_min = json_size;
      }
      metrics->json_size_max = std::max(metrics->json_size_max, json_size);
    }
    return;
  }

  metrics->failed.fetch_add(1, std::memory_order_relaxed);
  if (send_result.status == kafka_client::SendStatus::QUEUE_FULL ||
    send_result.status == kafka_client::SendStatus::PRODUCER_UNAVAILABLE)
  {
    if (should_log_throttled("producer_backpressure")) {
      RCLCPP_WARN(get_logger(), "Kafka producer backpressure on %s: %s",
        output_topic.c_str(), send_result.error_message.c_str());
    }
    return;
  }

  if (should_log_throttled("producer_error")) {
    RCLCPP_ERROR(get_logger(), "Failed to publish JSON to Kafka: %s",
      send_result.error_message.c_str());
  }
  return;
}

bool KafkaCdrToJsonNode::ensure_type_support(
  const std::string & ros_type,
  TypeSupportCacheEntry * entry,
  std::string * error_message)
{
  std::lock_guard<std::mutex> lock(cache_mutex_);
  auto it = type_support_cache_.find(ros_type);
  if (it != type_support_cache_.end()) {
    *entry = it->second;
    return true;
  }

  try {
    auto ts_lib = rosbag2_cpp::get_typesupport_library(
      ros_type, "rosidl_typesupport_cpp");
    auto introspection_ts_lib = rosbag2_cpp::get_typesupport_library(
      ros_type, "rosidl_typesupport_introspection_cpp");

    if (!ts_lib || !introspection_ts_lib) {
      if (error_message) {
        *error_message = "Type support libraries not found.";
      }
      return false;
    }

    const auto * rmw_type_support = rosbag2_cpp::get_typesupport_handle(
      ros_type, "rosidl_typesupport_cpp", ts_lib);
    const auto * introspection_type_support = rosbag2_cpp::get_typesupport_handle(
      ros_type, "rosidl_typesupport_introspection_cpp", introspection_ts_lib);

    if (!rmw_type_support || !introspection_type_support) {
      if (error_message) {
        *error_message = "Type support handles not found.";
      }
      return false;
    }

    TypeSupportCacheEntry cached;
    cached.rmw_type_support = rmw_type_support;
    cached.introspection_type_support = introspection_type_support;
    cached.rmw_library = ts_lib;
    cached.introspection_library = introspection_ts_lib;
    type_support_cache_.emplace(ros_type, cached);
    *entry = cached;
    return true;
  } catch (const std::exception & ex) {
    if (error_message) {
      *error_message = ex.what();
    }
    return false;
  } catch (...) {
    if (error_message) {
      *error_message = "Unknown error while loading type support.";
    }
    return false;
  }
}

std::string KafkaCdrToJsonNode::map_output_topic(const std::string & input_topic) const
{
  auto mapping_it = topic_mappings_.find(input_topic);
  if (mapping_it != topic_mappings_.end()) {
    return mapping_it->second;
  }

  if (kafka_parameters_.output_topic_prefix.empty()) {
    return input_topic;
  }

  if (kafka_parameters_.output_topic_prefix.back() == '.' || input_topic.empty()) {
    return kafka_parameters_.output_topic_prefix + input_topic;
  }

  return kafka_parameters_.output_topic_prefix + "." + input_topic;
}

void KafkaCdrToJsonNode::publish_metrics()
{
  if (!metrics_enabled_ || !metrics_pub_ || !metrics_pub_->is_activated()) {
    return;
  }

  const auto now = get_clock()->now();
  double interval_s = (now - last_metrics_time_).seconds();
  if (interval_s <= 0.0) {
    interval_s = static_cast<double>(metrics_interval_ms_) / 1000.0;
  }
  last_metrics_time_ = now;

  std::vector<std::shared_ptr<TopicMetrics>> entries;
  {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    entries.reserve(metrics_.size());
    for (const auto & item : metrics_) {
      entries.push_back(item.second);
    }
  }

  nlohmann::json root = nlohmann::json::object();
  root["timestamp_ns"] = now.nanoseconds();
  root["topics"] = nlohmann::json::array();

  for (const auto & metrics : entries) {
    const uint64_t received = metrics->received.load(std::memory_order_relaxed);
    const uint64_t converted = metrics->converted.load(std::memory_order_relaxed);
    const uint64_t failed = metrics->failed.load(std::memory_order_relaxed);
    const uint64_t bytes = metrics->bytes.load(std::memory_order_relaxed);

    const uint64_t received_delta = received - metrics->prev_received;
    const uint64_t converted_delta = converted - metrics->prev_converted;
    const uint64_t failed_delta = failed - metrics->prev_failed;
    const uint64_t bytes_delta = bytes - metrics->prev_bytes;

    metrics->prev_received = received;
    metrics->prev_converted = converted;
    metrics->prev_failed = failed;
    metrics->prev_bytes = bytes;

    std::vector<uint64_t> samples;
    uint64_t json_min = 0;
    uint64_t json_max = 0;
    uint64_t json_total = 0;
    {
      std::lock_guard<std::mutex> lock(metrics->metrics_mutex);
      samples.assign(metrics->latency_samples.begin(), metrics->latency_samples.end());
      json_min = metrics->json_size_min;
      json_max = metrics->json_size_max;
      json_total = metrics->json_size_total;
    }

    const uint64_t latency_max = metrics->latency_ns_max.load(std::memory_order_relaxed);
    const double rate_msgs = interval_s > 0.0 ?
      static_cast<double>(received_delta) / interval_s :
      0.0;
    const double rate_bytes = interval_s > 0.0 ?
      static_cast<double>(bytes_delta) / interval_s :
      0.0;
    const double avg_json_size = converted > 0 ?
      static_cast<double>(json_total) / static_cast<double>(converted) :
      0.0;

    nlohmann::json entry = nlohmann::json::object();
    entry["input_topic"] = metrics->input_topic;
    entry["output_topic"] = metrics->output_topic;
    entry["ros_type"] = metrics->ros_type;
    entry["received"] = received;
    entry["converted"] = converted;
    entry["failed"] = failed;
    entry["delta_received"] = received_delta;
    entry["delta_converted"] = converted_delta;
    entry["delta_failed"] = failed_delta;
    entry["bytes"] = bytes;
    entry["rate_msgs_per_s"] = rate_msgs;
    entry["rate_bytes_per_s"] = rate_bytes;
    entry["latency_ns_p50"] = calculate_percentile(samples, 0.50);
    entry["latency_ns_p95"] = calculate_percentile(samples, 0.95);
    entry["latency_ns_p99"] = calculate_percentile(samples, 0.99);
    entry["latency_ns_max"] = latency_max;
    entry["json_size_min"] = json_min;
    entry["json_size_max"] = json_max;
    entry["json_size_avg"] = avg_json_size;
    root["topics"].push_back(entry);
  }

  std_msgs::msg::String msg;
  msg.data = root.dump();
  metrics_pub_->publish(msg);
}

void KafkaCdrToJsonNode::reset_metrics_timer()
{
  metrics_timer_.reset();
  if (!metrics_enabled_ || !metrics_pub_ || !is_active_.load(std::memory_order_acquire)) {
    return;
  }

  last_metrics_time_ = get_clock()->now();
  metrics_timer_ = create_wall_timer(
    std::chrono::milliseconds(metrics_interval_ms_),
    [this]() {
      publish_metrics();
    });
}

bool KafkaCdrToJsonNode::should_log_throttled(const std::string & key)
{
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  const int64_t now_ns =
    std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();

  std::lock_guard<std::mutex> lock(log_mutex_);
  int64_t & next = error_log_next_ns_[key];
  if (now_ns < next) {
    return false;
  }
  next = now_ns + kThrottleIntervalNs;
  return true;
}

}  // namespace kafka_cdr_to_json

RCLCPP_COMPONENTS_REGISTER_NODE(kafka_cdr_to_json::KafkaCdrToJsonNode)
