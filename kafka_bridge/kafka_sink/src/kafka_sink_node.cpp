// Copyright 2025 Maciej Krupka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "kafka_sink/kafka_sink_node.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <utility>

#include "lifecycle_msgs/msg/state.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "yaml-cpp/yaml.h"

namespace kafka_sink
{
namespace
{
constexpr int64_t kThrottleIntervalNs = 1'000'000'000LL;  // 1 second
}  // namespace

KafkaSinkNode::ActiveSubscription::ActiveSubscription(ActiveSubscription && other) noexcept
: subscription(std::move(other.subscription)),
  topic_name(std::move(other.topic_name)),
  msg_type(std::move(other.msg_type)),
  runtime_state(std::move(other.runtime_state))
{}

KafkaSinkNode::ActiveSubscription & KafkaSinkNode::ActiveSubscription::operator=(
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

    configs.push_back({topic_name, msg_type});
  }

  return configs;
}

KafkaSinkNode::KafkaSinkNode(const rclcpp::NodeOptions & options)
: rclcpp_lifecycle::LifecycleNode("kafka_sink", options)
{
  this->declare_parameter<std::string>("subscriptions_yaml", "");
  this->declare_parameter<int>("qos_depth", qos_depth_);
  this->declare_parameter<std::string>(
    "kafka.bootstrap_servers", kafka_parameters_.bootstrap_servers);
  this->declare_parameter<std::string>("kafka.client_id", kafka_parameters_.client_id);
  this->declare_parameter<std::string>("kafka.acks", kafka_parameters_.acks);
  this->declare_parameter<std::string>("kafka.topic_prefix", kafka_parameters_.topic_prefix);
  this->declare_parameter<std::string>("kafka.message_key", kafka_parameters_.message_key);
  this->declare_parameter<std::string>("kafka.topic_mapping_mode", "prefix_ros_topic");
  this->declare_parameter<std::string>("kafka.fixed_topic", kafka_parameters_.fixed_topic);
  this->declare_parameter<bool>("kafka.strict_startup", kafka_parameters_.strict_startup);
  this->declare_parameter<int>(
    "kafka.max_queue_messages", static_cast<int>(kafka_parameters_.max_queue_messages));
  this->declare_parameter<bool>("kafka.drop_when_full", kafka_parameters_.drop_when_full);
  this->declare_parameter<int>("kafka.linger_ms", -1);
  this->declare_parameter<int>("kafka.batch_size", -1);

  on_parameters_set_handle_ = this->add_on_set_parameters_callback(
    std::bind(&KafkaSinkNode::on_parameters_set, this, std::placeholders::_1));
}

KafkaSinkNode::CallbackReturn KafkaSinkNode::on_configure(const rclcpp_lifecycle::State &)
{
  std::string error_message;
  if (!configure_from_parameters(&error_message)) {
    RCLCPP_ERROR(get_logger(), "Failed to configure kafka_sink: %s", error_message.c_str());
    return CallbackReturn::FAILURE;
  }

  if (configured_subscriptions_.empty()) {
    RCLCPP_WARN(
      get_logger(),
      "Configured kafka_sink with no subscriptions. Activate after setting 'subscriptions_yaml'.");
  }

  RCLCPP_INFO(
    get_logger(), "Configured kafka_sink with %zu subscription entries",
    configured_subscriptions_.size());
  return CallbackReturn::SUCCESS;
}

KafkaSinkNode::CallbackReturn KafkaSinkNode::on_activate(const rclcpp_lifecycle::State &)
{
  if (!start_producer()) {
    RCLCPP_ERROR(get_logger(), "Failed to start Kafka producer");
    return CallbackReturn::FAILURE;
  }

  if (!build_subscriptions()) {
    stop_producer();
    return CallbackReturn::FAILURE;
  }

  is_active_.store(true, std::memory_order_release);
  RCLCPP_INFO(
    get_logger(), "Activated kafka_sink with %zu active subscriptions",
    active_subscriptions_.size());
  return CallbackReturn::SUCCESS;
}

KafkaSinkNode::CallbackReturn KafkaSinkNode::on_deactivate(const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  clear_subscriptions();
  stop_producer();

  RCLCPP_INFO(get_logger(), "Deactivated kafka_sink and cleared subscriptions");
  return CallbackReturn::SUCCESS;
}

KafkaSinkNode::CallbackReturn KafkaSinkNode::on_cleanup(const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_producer();
  clear_subscriptions();
  configured_subscriptions_.clear();

  RCLCPP_INFO(get_logger(), "Cleaned up kafka_sink configuration and runtime state");
  return CallbackReturn::SUCCESS;
}

KafkaSinkNode::CallbackReturn KafkaSinkNode::on_shutdown(const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  stop_producer();
  clear_subscriptions();
  configured_subscriptions_.clear();

  RCLCPP_INFO(get_logger(), "Shutting down kafka_sink");
  return CallbackReturn::SUCCESS;
}

rcl_interfaces::msg::SetParametersResult KafkaSinkNode::on_parameters_set(
  const std::vector<rclcpp::Parameter> & parameters)
{
  rcl_interfaces::msg::SetParametersResult result;
  result.successful = true;
  result.reason = "accepted";

  bool update_required = false;
  std::string pending_yaml;
  int pending_depth = qos_depth_;
  KafkaParameters pending_kafka = kafka_parameters_;
  bool kafka_update_required = false;

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
    } else if (name.rfind("kafka.", 0) == 0) {
      const auto & current_state = this->get_current_state();
      if (current_state.id() == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE) {
        result.successful = false;
        result.reason = "deactivate first";
        return result;
      }
      kafka_update_required = true;
      if (name == "kafka.bootstrap_servers") {
        pending_kafka.bootstrap_servers = param.as_string();
      } else if (name == "kafka.client_id") {
        pending_kafka.client_id = param.as_string();
      } else if (name == "kafka.acks") {
        pending_kafka.acks = param.as_string();
      } else if (name == "kafka.topic_prefix") {
        pending_kafka.topic_prefix = param.as_string();
      } else if (name == "kafka.message_key") {
        pending_kafka.message_key = param.as_string();
      } else if (name == "kafka.topic_mapping_mode") {
        auto mode_value = param.as_string();
        if (mode_value == "prefix_ros_topic") {
          pending_kafka.topic_mapping_mode = TopicMappingMode::PREFIX_ROS_TOPIC;
        } else if (mode_value == "fixed") {
          pending_kafka.topic_mapping_mode = TopicMappingMode::FIXED;
        } else {
          result.successful = false;
          result.reason = "invalid kafka.topic_mapping_mode";
          return result;
        }
      } else if (name == "kafka.fixed_topic") {
        pending_kafka.fixed_topic = param.as_string();
      } else if (name == "kafka.strict_startup") {
        pending_kafka.strict_startup = param.as_bool();
      } else if (name == "kafka.max_queue_messages") {
        int value = param.as_int();
        pending_kafka.max_queue_messages = value > 0 ? static_cast<std::size_t>(value) : 0U;
      } else if (name == "kafka.drop_when_full") {
        pending_kafka.drop_when_full = param.as_bool();
      } else if (name == "kafka.linger_ms") {
        int value = param.as_int();
        pending_kafka.linger_ms = value >= 0 ? std::optional<int>{value} : std::nullopt;
      } else if (name == "kafka.batch_size") {
        int value = param.as_int();
        pending_kafka.batch_size = value >= 0 ? std::optional<int>{value} : std::nullopt;
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

  if (kafka_update_required) {
    std::string kafka_error;
    if (!validate_kafka_parameters(pending_kafka, &kafka_error)) {
      result.successful = false;
      result.reason = kafka_error;
      return result;
    }
    kafka_parameters_ = pending_kafka;
  }

  return result;
}

bool KafkaSinkNode::configure_from_parameters(std::string * error_message)
{
  qos_depth_ = this->get_parameter("qos_depth").as_int();
  std::string yaml_config = this->get_parameter("subscriptions_yaml").as_string();

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

  return configure_kafka_parameters(error_message);
}

bool KafkaSinkNode::validate_qos_depth(int qos_depth, std::string * error_message) const
{
  if (qos_depth <= 0) {
    *error_message = "qos_depth must be greater than zero.";
    return false;
  }
  return true;
}

bool KafkaSinkNode::validate_kafka_parameters(
  const KafkaParameters & pending, std::string * error_message) const
{
  if (pending.bootstrap_servers.empty()) {
    *error_message = "kafka.bootstrap_servers cannot be empty.";
    return false;
  }
  if (pending.client_id.empty()) {
    *error_message = "kafka.client_id cannot be empty.";
    return false;
  }
  if (pending.acks.empty()) {
    *error_message = "kafka.acks cannot be empty.";
    return false;
  }
  if (pending.message_key.empty()) {
    *error_message = "kafka.message_key cannot be empty.";
    return false;
  }
  if (pending.max_queue_messages == 0U) {
    *error_message = "kafka.max_queue_messages must be greater than zero.";
    return false;
  }
  if (pending.topic_mapping_mode == TopicMappingMode::FIXED && pending.fixed_topic.empty()) {
    *error_message = "kafka.fixed_topic cannot be empty when mapping mode is 'fixed'.";
    return false;
  }
  return true;
}

bool KafkaSinkNode::configure_kafka_parameters(std::string * error_message)
{
  KafkaParameters pending = kafka_parameters_;
  pending.bootstrap_servers = this->get_parameter("kafka.bootstrap_servers").as_string();
  pending.client_id = this->get_parameter("kafka.client_id").as_string();
  pending.acks = this->get_parameter("kafka.acks").as_string();
  pending.topic_prefix = this->get_parameter("kafka.topic_prefix").as_string();
  pending.message_key = this->get_parameter("kafka.message_key").as_string();

  auto mapping_mode = this->get_parameter("kafka.topic_mapping_mode").as_string();
  if (mapping_mode == "prefix_ros_topic") {
    pending.topic_mapping_mode = TopicMappingMode::PREFIX_ROS_TOPIC;
  } else if (mapping_mode == "fixed") {
    pending.topic_mapping_mode = TopicMappingMode::FIXED;
  } else {
    *error_message = "Invalid kafka.topic_mapping_mode value.";
    return false;
  }

  pending.fixed_topic = this->get_parameter("kafka.fixed_topic").as_string();
  pending.strict_startup = this->get_parameter("kafka.strict_startup").as_bool();
  int max_queue_messages_value = this->get_parameter("kafka.max_queue_messages").as_int();
  pending.max_queue_messages = max_queue_messages_value > 0 ?
    static_cast<std::size_t>(max_queue_messages_value) :
    0U;
  pending.drop_when_full = this->get_parameter("kafka.drop_when_full").as_bool();

  int linger_value = this->get_parameter("kafka.linger_ms").as_int();
  pending.linger_ms = linger_value >= 0 ? std::optional<int>{linger_value} : std::nullopt;
  int batch_value = this->get_parameter("kafka.batch_size").as_int();
  pending.batch_size = batch_value >= 0 ? std::optional<int>{batch_value} : std::nullopt;

  if (!validate_kafka_parameters(pending, error_message)) {
    return false;
  }

  kafka_parameters_ = pending;
  return true;
}

kafka_client::KafkaProducerConfig KafkaSinkNode::build_producer_config() const
{
  kafka_client::KafkaProducerConfig config;
  config.bootstrap_servers = kafka_parameters_.bootstrap_servers;
  config.client_id = kafka_parameters_.client_id;
  config.acks = kafka_parameters_.acks;
  config.linger_ms = kafka_parameters_.linger_ms;
  config.batch_size = kafka_parameters_.batch_size;
  config.max_queue_messages = kafka_parameters_.max_queue_messages;
  config.drop_when_full = kafka_parameters_.drop_when_full;
  config.startup_mode = kafka_parameters_.strict_startup ?
    kafka_client::StartupMode::STRICT :
    kafka_client::StartupMode::TOLERANT;
  return config;
}

bool KafkaSinkNode::start_producer()
{
  kafka_producer_ = std::make_shared<kafka_client::KafkaProducer>(build_producer_config());
  if (!kafka_producer_->start()) {
    kafka_producer_.reset();
    return false;
  }

  const auto health = kafka_producer_->health();
  if (health.status == kafka_client::ProducerStatus::DEGRADED) {
    RCLCPP_WARN(get_logger(), "Kafka producer started in degraded mode: %s", health.last_error.c_str());
  }
  return true;
}

void KafkaSinkNode::stop_producer()
{
  auto producer = std::exchange(kafka_producer_, nullptr);
  if (producer) {
    producer->stop();
  }
}

std::string KafkaSinkNode::map_kafka_topic(const std::string & ros_topic) const
{
  if (kafka_parameters_.topic_mapping_mode == TopicMappingMode::FIXED) {
    return kafka_parameters_.fixed_topic;
  }

  std::string normalized = ros_topic;
  if (!normalized.empty() && normalized.front() == '/') {
    normalized.erase(normalized.begin());
  }
  std::replace(normalized.begin(), normalized.end(), '/', '.');

  if (kafka_parameters_.topic_prefix.empty()) {
    return normalized;
  }

  if (kafka_parameters_.topic_prefix.back() == '.' || normalized.empty()) {
    return kafka_parameters_.topic_prefix + normalized;
  }
  return kafka_parameters_.topic_prefix + "." + normalized;
}

rclcpp::QoS KafkaSinkNode::build_qos_profile() const
{
  rclcpp::QoS qos{rclcpp::SystemDefaultsQoS()};
  qos.keep_last(static_cast<size_t>(qos_depth_));
  return qos;
}

bool KafkaSinkNode::build_subscriptions()
{
  clear_subscriptions();

  if (!kafka_producer_) {
    RCLCPP_ERROR(get_logger(), "Kafka producer is not initialized");
    return false;
  }

  auto qos = build_qos_profile();

  if (configured_subscriptions_.empty()) {
    return true;
  }

  const std::vector<uint8_t> message_key_bytes(
    kafka_parameters_.message_key.begin(), kafka_parameters_.message_key.end());

  active_subscriptions_.reserve(configured_subscriptions_.size());
  for (const auto & config : configured_subscriptions_) {
    active_subscriptions_.emplace_back();
    auto & runtime = active_subscriptions_.back();
    runtime.topic_name = config.topic_name;
    runtime.msg_type = config.msg_type;
    runtime.runtime_state = std::make_shared<SubscriptionRuntime>();
    runtime.runtime_state->ros_topic = config.topic_name;
    runtime.runtime_state->msg_type = config.msg_type;
    runtime.runtime_state->kafka_topic = map_kafka_topic(config.topic_name);
    runtime.runtime_state->log_label =
      "topic='" + config.topic_name + "' kafka_topic='" + runtime.runtime_state->kafka_topic +
      "' type='" + config.msg_type + "'";

    auto runtime_state = runtime.runtime_state;
    auto callback =
      [this, runtime_state, message_key_bytes](std::shared_ptr<rclcpp::SerializedMessage> msg) {
        if (!is_active_.load(std::memory_order_acquire)) {
          return;
        }
        auto producer = kafka_producer_;
        if (!producer) {
          return;
        }

        const auto now_ns = this->get_clock()->now().nanoseconds();
        auto next_time_ns =
          runtime_state->next_log_time_ns.load(std::memory_order_acquire);
        const auto stamp_ms = now_ns / 1'000'000;

        std::vector<uint8_t> value(msg->size());
        std::memcpy(value.data(), msg->get_rcl_serialized_message().buffer, msg->size());

        // Echo key and topic name
        std::string key_str(message_key_bytes.begin(), message_key_bytes.end());
        RCLCPP_DEBUG(
          this->get_logger(),
          "[kafka_sink_callback] key='%s', topic='%s'",
          key_str.c_str(), runtime_state->kafka_topic.c_str());

        std::vector<kafka_client::KafkaHeader> headers;
        headers.reserve(3);
        headers.push_back({"ros_topic", runtime_state->ros_topic});
        headers.push_back({"ros_type", runtime_state->msg_type});
        headers.push_back({"stamp_ms", std::to_string(stamp_ms)});

        auto result = producer->send(
          runtime_state->kafka_topic, message_key_bytes, value, stamp_ms, headers);
        if (result.status == kafka_client::SendStatus::SENT) {
          runtime_state->sent_ok.fetch_add(1, std::memory_order_relaxed);
        } else if (result.status == kafka_client::SendStatus::QUEUE_FULL && !result.buffered) {
          runtime_state->dropped.fetch_add(1, std::memory_order_relaxed);
        } else {
          runtime_state->errors.fetch_add(1, std::memory_order_relaxed);
        }

        if (now_ns >= next_time_ns &&
          runtime_state->next_log_time_ns.compare_exchange_strong(
            next_time_ns, now_ns + kThrottleIntervalNs, std::memory_order_acq_rel))
        {
          auto health = producer->health();
          const char * last_error = health.last_error.empty() ? "" : health.last_error.c_str();
          if (health.last_error.empty()) {
            RCLCPP_INFO(
              this->get_logger(),
              "[kafka_sink] %s size=%zu bytes sent=%lu dropped=%lu errors=%lu",
              runtime_state->log_label.c_str(), msg->size(),
              runtime_state->sent_ok.load(std::memory_order_relaxed),
              runtime_state->dropped.load(std::memory_order_relaxed),
              runtime_state->errors.load(std::memory_order_relaxed));
          } else {
            RCLCPP_INFO(
              this->get_logger(),
              "[kafka_sink] %s size=%zu bytes sent=%lu dropped=%lu errors=%lu last_error='%s'",
              runtime_state->log_label.c_str(), msg->size(),
              runtime_state->sent_ok.load(std::memory_order_relaxed),
              runtime_state->dropped.load(std::memory_order_relaxed),
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
      active_subscriptions_.pop_back();
      clear_subscriptions();
      return false;
    }
  }
  return true;
}

void KafkaSinkNode::clear_subscriptions()
{
  active_subscriptions_.clear();
}

}  // namespace kafka_sink

RCLCPP_COMPONENTS_REGISTER_NODE(kafka_sink::KafkaSinkNode)
