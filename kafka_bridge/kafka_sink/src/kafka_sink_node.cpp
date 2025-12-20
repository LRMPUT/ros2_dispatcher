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
#include <cctype>
#include <cstdint>
#include <functional>
#include <optional>
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
constexpr int64_t kErrorThrottleIntervalNs = 5'000'000'000LL;  // 5 seconds
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
  this->declare_parameter<std::string>("kafka.bootstrap_servers", "");
  this->declare_parameter<std::string>("kafka.client_id", this->get_name());
  this->declare_parameter<std::string>("kafka.acks", "all");
  this->declare_parameter<int>("kafka.linger_ms", -1);
  this->declare_parameter<int>("kafka.batch_size", -1);
  this->declare_parameter<int>("kafka.queue_buffering_max_kbytes", -1);
  this->declare_parameter<int>("kafka.max_in_flight", -1);
  this->declare_parameter<int>("kafka.retries", -1);
  this->declare_parameter<int>("kafka.retry_backoff_ms", -1);
  this->declare_parameter<bool>("kafka.enable_idempotence", true);
  this->declare_parameter<int>("kafka.max_pending_messages", 0);
  this->declare_parameter<std::string>("kafka.start_mode", "strict");
  this->declare_parameter<std::string>("kafka.topic_prefix", "");

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

  kafka_producer_ = std::make_unique<kafka_client::KafkaProducer>(kafka_config_);
  std::string producer_error;
  if (!kafka_producer_->start(&producer_error)) {
    RCLCPP_ERROR(get_logger(), "Failed to start Kafka producer: %s", producer_error.c_str());
    kafka_producer_.reset();
    return CallbackReturn::FAILURE;
  }

  const auto health = kafka_producer_->health();
  if (health.state == kafka_client::ProducerState::DEGRADED) {
    RCLCPP_WARN(
      get_logger(), "Kafka producer started in degraded mode: %s", health.last_error.c_str());
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
  is_active_.store(true, std::memory_order_release);
  if (!build_subscriptions()) {
    is_active_.store(false, std::memory_order_release);
    return CallbackReturn::FAILURE;
  }

  RCLCPP_INFO(
    get_logger(), "Activated kafka_sink with %zu active subscriptions",
    active_subscriptions_.size());
  return CallbackReturn::SUCCESS;
}

KafkaSinkNode::CallbackReturn KafkaSinkNode::on_deactivate(const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  clear_subscriptions();

  RCLCPP_INFO(get_logger(), "Deactivated kafka_sink and cleared subscriptions");
  return CallbackReturn::SUCCESS;
}

KafkaSinkNode::CallbackReturn KafkaSinkNode::on_cleanup(const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  clear_subscriptions();
  configured_subscriptions_.clear();
  if (kafka_producer_) {
    kafka_producer_->stop();
    kafka_producer_.reset();
  }

  RCLCPP_INFO(get_logger(), "Cleaned up kafka_sink configuration and runtime state");
  return CallbackReturn::SUCCESS;
}

KafkaSinkNode::CallbackReturn KafkaSinkNode::on_shutdown(const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  clear_subscriptions();
  configured_subscriptions_.clear();
  if (kafka_producer_) {
    kafka_producer_->stop();
    kafka_producer_.reset();
  }

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

  for (const auto & param : parameters) {
    if (param.get_name() == "subscriptions_yaml") {
      const auto & current_state = this->get_current_state();
      if (current_state.id() == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE) {
        result.successful = false;
        result.reason = "deactivate first";
        return result;
      }
      pending_yaml = param.as_string();
      update_required = true;
    } else if (param.get_name() == "qos_depth") {
      pending_depth = param.as_int();
    } else if (param.get_name().rfind("kafka.", 0) == 0) {
      const auto & current_state = this->get_current_state();
      if (current_state.id() != lifecycle_msgs::msg::State::PRIMARY_STATE_UNCONFIGURED) {
        result.successful = false;
        result.reason = "Kafka parameters require the node to be unconfigured";
        return result;
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

  kafka_topic_prefix_ = this->get_parameter("kafka.topic_prefix").as_string();
  kafka_config_.bootstrap_servers = this->get_parameter("kafka.bootstrap_servers").as_string();
  kafka_config_.client_id = this->get_parameter("kafka.client_id").as_string();
  kafka_config_.acks = this->get_parameter("kafka.acks").as_string();
  kafka_config_.enable_idempotence = this->get_parameter("kafka.enable_idempotence").as_bool();
  const auto start_mode = this->get_parameter("kafka.start_mode").as_string();
  if (start_mode == "strict") {
    kafka_config_.start_mode = kafka_client::StartMode::STRICT;
  } else if (start_mode == "tolerant") {
    kafka_config_.start_mode = kafka_client::StartMode::TOLERANT;
  } else {
    *error_message = "kafka.start_mode must be either 'strict' or 'tolerant'";
    return false;
  }

  const auto optional_or_reset = [](int value) -> std::optional<int> {
      return value >= 0 ? std::optional<int>(value) : std::nullopt;
    };

  kafka_config_.linger_ms = optional_or_reset(this->get_parameter("kafka.linger_ms").as_int());
  kafka_config_.batch_size = optional_or_reset(this->get_parameter("kafka.batch_size").as_int());
  kafka_config_.queue_buffering_max_kbytes = optional_or_reset(
    this->get_parameter("kafka.queue_buffering_max_kbytes").as_int());
  kafka_config_.max_in_flight = optional_or_reset(
    this->get_parameter("kafka.max_in_flight").as_int());
  kafka_config_.retries = optional_or_reset(this->get_parameter("kafka.retries").as_int());
  kafka_config_.retry_backoff_ms = optional_or_reset(
    this->get_parameter("kafka.retry_backoff_ms").as_int());
  const auto max_pending_messages =
    this->get_parameter("kafka.max_pending_messages").as_int();
  kafka_config_.max_pending_messages =
    max_pending_messages > 0 ? static_cast<std::size_t>(max_pending_messages) : 0U;

  if (kafka_config_.client_id.empty()) {
    kafka_config_.client_id = this->get_name();
  }

  if (kafka_config_.bootstrap_servers.empty()) {
    *error_message = "kafka.bootstrap_servers cannot be empty";
    return false;
  }

  return true;
}

bool KafkaSinkNode::validate_qos_depth(int qos_depth, std::string * error_message) const
{
  if (qos_depth <= 0) {
    *error_message = "qos_depth must be greater than zero.";
    return false;
  }
  return true;
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

  auto qos = build_qos_profile();

  if (!kafka_producer_) {
    RCLCPP_ERROR(get_logger(), "Kafka producer not initialised");
    return false;
  }

  active_subscriptions_.reserve(configured_subscriptions_.size());
  for (const auto & config : configured_subscriptions_) {
    active_subscriptions_.emplace_back();
    auto & runtime = active_subscriptions_.back();
    runtime.topic_name = config.topic_name;
    runtime.msg_type = config.msg_type;
    runtime.runtime_state = std::make_shared<SubscriptionRuntime>();
    runtime.runtime_state->log_label =
      "topic='" + config.topic_name + "' type='" + config.msg_type + "'";
    runtime.runtime_state->kafka_topic = resolve_kafka_topic(config.topic_name);

    auto runtime_state = runtime.runtime_state;
    const auto kafka_topic = runtime.runtime_state->kafka_topic;
    const auto ros_topic = runtime.topic_name;
    const auto ros_type = runtime.msg_type;
    auto callback =
      [this, runtime_state, kafka_topic, ros_topic, ros_type](
        std::shared_ptr<rclcpp::SerializedMessage> msg) {
        if (!is_active_.load(std::memory_order_acquire)) {
          return;
        }

        const auto now_ns = this->get_clock()->now().nanoseconds();
        auto next_time_ns =
          runtime_state->next_log_time_ns.load(std::memory_order_acquire);
        if (now_ns >= next_time_ns &&
          runtime_state->next_log_time_ns.compare_exchange_strong(
            next_time_ns, now_ns + kThrottleIntervalNs, std::memory_order_acq_rel))
        {
          RCLCPP_INFO(
            this->get_logger(), "[kafka_sink] %s size=%zu bytes",
            runtime_state->log_label.c_str(), msg->size());
        }

        const int64_t timestamp_ms = now_ns / 1'000'000;
        auto payload = make_buffer_view(*msg);
        std::vector<kafka_client::Header> headers;
        headers.reserve(4);
        headers.push_back({"ros_topic", ros_topic});
        headers.push_back({"ros_type", ros_type});
        headers.push_back({"ros_encoding", "cdr"});
        headers.push_back({"ros_timestamp_ns", std::to_string(now_ns)});

        auto result = kafka_producer_->send(
          kafka_topic,
          kafka_client::BufferView{},
          payload,
          timestamp_ms,
          headers);

        if (result.accepted) {
          runtime_state->sent.fetch_add(1, std::memory_order_relaxed);
        } else if (result.dropped_queue_full) {
          runtime_state->dropped.fetch_add(1, std::memory_order_relaxed);
          log_send_result(*runtime_state, result);
        } else {
          runtime_state->failures.fetch_add(1, std::memory_order_relaxed);
          log_send_result(*runtime_state, result);
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

std::string KafkaSinkNode::resolve_kafka_topic(const std::string & ros_topic) const
{
  std::string sanitized = ros_topic;
  while (!sanitized.empty() && sanitized.front() == '/') {
    sanitized.erase(sanitized.begin());
  }

  if (sanitized.empty()) {
    sanitized = ros_topic;
  }

  if (!kafka_topic_prefix_.empty()) {
    sanitized = kafka_topic_prefix_ + sanitized;
  }
  return sanitized;
}

kafka_client::BufferView KafkaSinkNode::make_buffer_view(
  const rclcpp::SerializedMessage & message) const
{
  const auto & rcl_message = message.get_rcl_serialized_message();
  return kafka_client::BufferView{rcl_message.buffer, message.size()};
}

void KafkaSinkNode::log_send_result(
  SubscriptionRuntime & runtime_state, const kafka_client::SendResult & result)
{
  const auto now_ns = this->get_clock()->now().nanoseconds();
  auto next_log_ns = runtime_state.next_error_log_time_ns.load(std::memory_order_acquire);
  if (now_ns < next_log_ns) {
    return;
  }
  if (!runtime_state.next_error_log_time_ns.compare_exchange_strong(
      next_log_ns, now_ns + kErrorThrottleIntervalNs, std::memory_order_acq_rel))
  {
    return;
  }

  if (result.dropped_queue_full) {
    RCLCPP_WARN(
      get_logger(), "[kafka_sink] %s Kafka queue full; dropping payload (%s)",
      runtime_state.log_label.c_str(), result.error_message.c_str());
  } else {
    RCLCPP_ERROR(
      get_logger(), "[kafka_sink] %s failed to enqueue payload: %s",
      runtime_state.log_label.c_str(), result.error_message.c_str());
  }
}

}  // namespace kafka_sink

RCLCPP_COMPONENTS_REGISTER_NODE(kafka_sink::KafkaSinkNode)
