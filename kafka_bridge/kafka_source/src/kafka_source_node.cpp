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

#include "kafka_source/kafka_source_node.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cctype>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <utility>

#include "rclcpp/clock.hpp"
#include "rclcpp/qos.hpp"
#include "rosbag2_cpp/typesupport_helpers.hpp"
#include "rosidl_runtime_cpp/message_initialization.hpp"
#include "rosidl_typesupport_introspection_cpp/message_introspection.hpp"
#include "rmw/rmw.h"

namespace kafka_source
{
namespace
{
constexpr size_t kMaxLatencySamples = 1000;

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

std::string json_escape(const std::string & input)
{
  std::ostringstream escaped;
  for (char ch : input) {
    switch (ch) {
      case '\\':
        escaped << "\\\\";
        break;
      case '"':
        escaped << "\\\"";
        break;
      case '\n':
        escaped << "\\n";
        break;
      case '\r':
        escaped << "\\r";
        break;
      case '\t':
        escaped << "\\t";
        break;
      default:
        escaped << ch;
        break;
    }
  }
  return escaped.str();
}

uint64_t percentile_from_samples(const std::vector<uint64_t> & sorted, double percentile)
{
  if (sorted.empty()) {
    return 0;
  }
  double clamped = std::min(1.0, std::max(0.0, percentile));
  size_t index = static_cast<size_t>(std::ceil(clamped * (sorted.size() - 1)));
  return sorted[index];
}
}  // namespace

KafkaSourceNode::KafkaSourceNode(const rclcpp::NodeOptions & options)
: rclcpp_lifecycle::LifecycleNode("kafka_source", options)
{
  declare_parameter("kafka.bootstrap_servers", kafka_parameters_.bootstrap_servers);
  declare_parameter("kafka.group_id", kafka_parameters_.group_id);
  declare_parameter("kafka.topic_pattern", kafka_parameters_.topic_pattern);
  declare_parameter("kafka.offset_reset", kafka_parameters_.offset_reset);
  declare_parameter("ros_topic_prefix", ros_topic_prefix_);
  declare_parameter("qos_depth", qos_depth_);
  declare_parameter("metrics.enabled", metrics_enabled_);
  declare_parameter("metrics.interval_ms", metrics_interval_ms_);
  declare_parameter("metrics.topic", metrics_topic_);
  declare_parameter("topic_mappings", std::string{});

  on_parameters_set_handle_ = add_on_set_parameters_callback(
    std::bind(&KafkaSourceNode::on_parameters_set, this, std::placeholders::_1));
}

KafkaSourceNode::CallbackReturn KafkaSourceNode::on_configure(
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

KafkaSourceNode::CallbackReturn KafkaSourceNode::on_activate(
  const rclcpp_lifecycle::State &)
{
  std::string error;
  if (!start_consumer(&error)) {
    RCLCPP_ERROR(get_logger(), "Failed to start Kafka consumer: %s", error.c_str());
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

KafkaSourceNode::CallbackReturn KafkaSourceNode::on_deactivate(
  const rclcpp_lifecycle::State &)
{
  running_.store(false, std::memory_order_release);
  if (consumer_thread_.joinable()) {
    consumer_thread_.join();
  }
  stop_consumer();
  is_active_.store(false, std::memory_order_release);
  reset_metrics_timer();
  if (metrics_pub_) {
    metrics_pub_->on_deactivate();
  }
  return CallbackReturn::SUCCESS;
}

KafkaSourceNode::CallbackReturn KafkaSourceNode::on_cleanup(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  running_.store(false, std::memory_order_release);
  if (consumer_thread_.joinable()) {
    consumer_thread_.join();
  }
  stop_consumer();
  metrics_timer_.reset();
  metrics_pub_.reset();
  {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    type_support_cache_.clear();
    publishers_.clear();
    metrics_.clear();
  }
  return CallbackReturn::SUCCESS;
}

KafkaSourceNode::CallbackReturn KafkaSourceNode::on_shutdown(
  const rclcpp_lifecycle::State &)
{
  is_active_.store(false, std::memory_order_release);
  running_.store(false, std::memory_order_release);
  if (consumer_thread_.joinable()) {
    consumer_thread_.join();
  }
  stop_consumer();
  return CallbackReturn::SUCCESS;
}

rcl_interfaces::msg::SetParametersResult KafkaSourceNode::on_parameters_set(
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
    } else if (parameter.get_name() == "kafka.topic_pattern") {
      kafka_parameters_.topic_pattern = parameter.as_string();
    } else if (parameter.get_name() == "kafka.offset_reset") {
      kafka_parameters_.offset_reset = parameter.as_string();
    } else if (parameter.get_name() == "ros_topic_prefix") {
      ros_topic_prefix_ = parameter.as_string();
    } else if (parameter.get_name() == "qos_depth") {
      qos_depth_ = parameter.as_int();
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

bool KafkaSourceNode::configure_from_parameters(std::string * error_message)
{
  get_parameter("kafka.bootstrap_servers", kafka_parameters_.bootstrap_servers);
  get_parameter("kafka.group_id", kafka_parameters_.group_id);
  get_parameter("kafka.topic_pattern", kafka_parameters_.topic_pattern);
  get_parameter("kafka.offset_reset", kafka_parameters_.offset_reset);
  get_parameter("ros_topic_prefix", ros_topic_prefix_);
  get_parameter("qos_depth", qos_depth_);
  get_parameter("metrics.enabled", metrics_enabled_);
  get_parameter("metrics.interval_ms", metrics_interval_ms_);
  get_parameter("metrics.topic", metrics_topic_);

  std::string mapping_text;
  get_parameter("topic_mappings", mapping_text);
  topic_mappings_ = parse_topic_mappings(mapping_text);

  return validate_parameters(error_message);
}

bool KafkaSourceNode::validate_parameters(std::string * error_message) const
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
  if (kafka_parameters_.topic_pattern.empty()) {
    if (error_message) {
      *error_message = "kafka.topic_pattern must be non-empty.";
    }
    return false;
  }
  if (kafka_parameters_.offset_reset != "latest" && kafka_parameters_.offset_reset != "earliest") {
    if (error_message) {
      *error_message = "kafka.offset_reset must be 'latest' or 'earliest'.";
    }
    return false;
  }
  if (qos_depth_ <= 0) {
    if (error_message) {
      *error_message = "qos_depth must be > 0.";
    }
    return false;
  }
  if (metrics_interval_ms_ <= 0) {
    if (error_message) {
      *error_message = "metrics.interval_ms must be > 0.";
    }
    return false;
  }
  return true;
}

bool KafkaSourceNode::start_consumer(std::string * error_message)
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

  std::vector<std::string> topics{ kafka_parameters_.topic_pattern };
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

void KafkaSourceNode::stop_consumer()
{
  if (consumer_) {
    consumer_->close();
    consumer_.reset();
  }
}

void KafkaSourceNode::poll_loop()
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
      if (should_log_throttled(next_error_log_time_ns_)) {
        RCLCPP_ERROR(get_logger(), "Kafka consume error: %s", message->errstr().c_str());
      }
      continue;
    }
    process_message(message.get());
  }
}

void KafkaSourceNode::process_message(RdKafka::Message * message)
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

  if (ros_type.empty()) {
    if (should_log_throttled(next_error_log_time_ns_)) {
      RCLCPP_WARN(get_logger(), "Missing ros_type header, skipping message.");
    }
    return;
  }

  const std::string kafka_topic = message->topic_name();
  std::string ros_topic;
  auto mapping_it = topic_mappings_.find(kafka_topic);
  if (mapping_it != topic_mappings_.end()) {
    ros_topic = mapping_it->second;
  } else {
    ros_topic = derive_ros_topic(kafka_topic);
  }

  std::shared_ptr<TopicMetrics> metrics;
  {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto key = ros_topic + "|" + ros_type;
    auto it = metrics_.find(key);
    if (it == metrics_.end()) {
      auto entry = std::make_shared<TopicMetrics>();
      entry->ros_topic = ros_topic;
      entry->ros_type = ros_type;
      it = metrics_.emplace(key, entry).first;
    }
    metrics = it->second;
  }

  metrics->received.fetch_add(1, std::memory_order_relaxed);
  metrics->bytes.fetch_add(payload_size, std::memory_order_relaxed);

  auto timestamp = message->timestamp();
  if (timestamp.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE &&
    timestamp.timestamp >= 0)
  {
    auto now = std::chrono::system_clock::now();
    auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    auto msg_ns = std::chrono::nanoseconds(timestamp.timestamp * 1000000LL);
    if (now_ns > msg_ns) {
      auto latency = static_cast<uint64_t>((now_ns - msg_ns).count());
      metrics->latency_ns_accum.fetch_add(latency, std::memory_order_relaxed);
      metrics->latency_ns_max.store(
        std::max(metrics->latency_ns_max.load(std::memory_order_relaxed), latency),
        std::memory_order_relaxed);
      std::lock_guard<std::mutex> lock(metrics->latency_mutex);
      metrics->latency_samples.push_back(latency);
      if (metrics->latency_samples.size() > kMaxLatencySamples) {
        metrics->latency_samples.pop_front();
      }
    }
  }

  TypeSupportCacheEntry type_support;
  std::string type_error;
  if (!ensure_type_support(ros_type, &type_support, &type_error)) {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled(next_error_log_time_ns_)) {
      RCLCPP_WARN(get_logger(), "Failed to load type support for '%s': %s",
        ros_type.c_str(), type_error.c_str());
    }
    return;
  }

  rclcpp::SerializedMessage serialized_in(payload_size);
  auto & rmw_serialized_in = serialized_in.get_rcl_serialized_message();
  std::memcpy(rmw_serialized_in.buffer, payload, payload_size);
  rmw_serialized_in.buffer_length = payload_size;

  // The incoming Kafka payload is already a ROS 2 CDR stream, so skip the
  // deserialize/serialize round-trip and publish it directly.
  rclcpp::SerializedMessage serialized_out(payload_size);
  auto & rmw_serialized_out = serialized_out.get_rcl_serialized_message();
  // Copy the CDR payload from the incoming buffer into the ROS 2 serialized message.
  std::memcpy(
    rmw_serialized_out.buffer,
    rmw_serialized_in.buffer,
    rmw_serialized_in.buffer_length);
  rmw_serialized_out.buffer_length = rmw_serialized_in.buffer_length;
  auto publisher = get_or_create_publisher(ros_topic, ros_type);
  if (!publisher) {
    metrics->failed.fetch_add(1, std::memory_order_relaxed);
    if (should_log_throttled(next_error_log_time_ns_)) {
      RCLCPP_ERROR(get_logger(), "Failed to create publisher for '%s'", ros_topic.c_str());
    }
    return;
  }

  publisher->publish(serialized_out);
  metrics->decoded.fetch_add(1, std::memory_order_relaxed);
}

bool KafkaSourceNode::ensure_type_support(
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
        *error_message = "Typesupport library not found.";
      }
      return false;
    }

    const rosidl_message_type_support_t * rmw_type_support =
      rosbag2_cpp::get_typesupport_handle(
      ros_type, "rosidl_typesupport_cpp", ts_lib);
    const rosidl_message_type_support_t * introspection_type_support =
      rosbag2_cpp::get_typesupport_handle(
      ros_type, "rosidl_typesupport_introspection_cpp", introspection_ts_lib);

    if (!rmw_type_support || !introspection_type_support) {
      if (error_message) {
        *error_message = "Typesupport handle not found.";
      }
      return false;
    }

    TypeSupportCacheEntry cache_entry;
    cache_entry.rmw_type_support = rmw_type_support;
    cache_entry.introspection_type_support = introspection_type_support;
    cache_entry.rmw_library = ts_lib;
    cache_entry.introspection_library = introspection_ts_lib;
    type_support_cache_[ros_type] = cache_entry;
    *entry = cache_entry;
    return true;
  } catch (const std::exception & ex) {
    if (error_message) {
      *error_message = ex.what();
    }
    return false;
  } catch (...) {
    if (error_message) {
      *error_message = "Unknown error loading typesupport.";
    }
    return false;
  }
}

rclcpp::GenericPublisher::SharedPtr KafkaSourceNode::get_or_create_publisher(
  const std::string & ros_topic,
  const std::string & ros_type)
{
  std::lock_guard<std::mutex> lock(cache_mutex_);
  auto key = ros_topic + "|" + ros_type;
  auto it = publishers_.find(key);
  if (it != publishers_.end()) {
    return it->second;
  }

  rclcpp::QoS qos{rclcpp::KeepLast(qos_depth_)};
  auto publisher = create_generic_publisher(ros_topic, ros_type, qos);
  publishers_[key] = publisher;
  return publisher;
}

std::string KafkaSourceNode::derive_ros_topic(const std::string & kafka_topic) const
{
  std::string suffix = kafka_topic;
  if (suffix.rfind("ros2.", 0) == 0) {
    suffix = suffix.substr(5);
  }
  std::replace(suffix.begin(), suffix.end(), '.', '/');

  std::string prefix = ros_topic_prefix_;
  if (!prefix.empty() && prefix.back() == '/') {
    prefix.pop_back();
  }
  if (!prefix.empty() && prefix.front() != '/') {
    prefix.insert(prefix.begin(), '/');
  }

  // Handle empty suffix explicitly to avoid trailing slashes such as "prefix/".
  if (suffix.empty()) {
    if (prefix.empty()) {
      // Fall back to the root topic.
      return "/";
    }
    return prefix;
  }

  // Build the raw topic from prefix and suffix.
  std::string topic;
  if (prefix.empty()) {
    topic = "/" + suffix;
  } else {
    topic = prefix + "/" + suffix;
  }

  // Normalize: collapse multiple consecutive slashes into a single slash.
  auto new_end = std::unique(
    topic.begin(), topic.end(),
    [](char a, char b) {
      return a == '/' && b == '/';
    });
  topic.erase(new_end, topic.end());

  // Remove a trailing slash, except when the topic is exactly "/".
  if (topic.size() > 1 && topic.back() == '/') {
    topic.pop_back();
  }

  return topic;
}

void KafkaSourceNode::publish_metrics()
{
  if (!metrics_enabled_ || !metrics_pub_) {
    return;
  }

  const auto now = get_clock()->now();
  const double interval_sec = static_cast<double>(metrics_interval_ms_) / 1000.0;

  std::ostringstream json;
  json << '{';
  json << "\"timestamp_ns\":" << now.nanoseconds() << ',';
  json << "\"topics\":[";

  bool first = true;
  std::lock_guard<std::mutex> lock(cache_mutex_);
  for (auto & entry : metrics_) {
    auto & metrics = entry.second;
    if (!first) {
      json << ',';
    }
    first = false;

    uint64_t received = metrics->received.load(std::memory_order_relaxed);
    uint64_t decoded = metrics->decoded.load(std::memory_order_relaxed);
    uint64_t failed = metrics->failed.load(std::memory_order_relaxed);
    uint64_t bytes = metrics->bytes.load(std::memory_order_relaxed);

    uint64_t received_delta = received - metrics->prev_received;
    uint64_t decoded_delta = decoded - metrics->prev_decoded;
    uint64_t failed_delta = failed - metrics->prev_failed;
    uint64_t bytes_delta = bytes - metrics->prev_bytes;

    metrics->prev_received = received;
    metrics->prev_decoded = decoded;
    metrics->prev_failed = failed;
    metrics->prev_bytes = bytes;

    std::vector<uint64_t> samples;
    {
      std::lock_guard<std::mutex> sample_lock(metrics->latency_mutex);
      samples.assign(metrics->latency_samples.begin(), metrics->latency_samples.end());
    }
    std::sort(samples.begin(), samples.end());

    json << '{';
    json << "\"ros_topic\":\"" << json_escape(metrics->ros_topic) << "\",";
    json << "\"ros_type\":\"" << json_escape(metrics->ros_type) << "\",";
    json << "\"received\":" << received << ',';
    json << "\"decoded\":" << decoded << ',';
    json << "\"failed\":" << failed << ',';
    json << "\"bytes\":" << bytes << ',';
    json << "\"rate_msgs_per_s\":" << std::fixed << std::setprecision(3)
         << (interval_sec > 0.0 ? static_cast<double>(received_delta) / interval_sec : 0.0)
         << ',';
    json << "\"rate_bytes_per_s\":" << std::fixed << std::setprecision(3)
         << (interval_sec > 0.0 ? static_cast<double>(bytes_delta) / interval_sec : 0.0)
         << ',';
    json << "\"delta_decoded\":" << decoded_delta << ',';
    json << "\"delta_failed\":" << failed_delta << ',';
    json << "\"latency_ns_p50\":" << percentile_from_samples(samples, 0.50) << ',';
    json << "\"latency_ns_p95\":" << percentile_from_samples(samples, 0.95) << ',';
    json << "\"latency_ns_p99\":" << percentile_from_samples(samples, 0.99) << ',';
    json << "\"latency_ns_max\":" << metrics->latency_ns_max.load(std::memory_order_relaxed);
    json << '}';
  }
  json << "]}";

  std_msgs::msg::String msg;
  msg.data = json.str();
  metrics_pub_->publish(msg);
}

void KafkaSourceNode::reset_metrics_timer()
{
  if (metrics_timer_) {
    metrics_timer_->cancel();
    metrics_timer_.reset();
  }
  if (metrics_enabled_ && is_active_.load(std::memory_order_acquire)) {
    auto period = std::chrono::milliseconds(metrics_interval_ms_);
    metrics_timer_ = create_wall_timer(
      period, [this]() { publish_metrics(); });
  }
}

bool KafkaSourceNode::should_log_throttled(std::atomic<int64_t> & next_log_time_ns)
{
  int64_t now_ns = get_clock()->now().nanoseconds();
  int64_t expected = next_log_time_ns.load(std::memory_order_relaxed);
  if (now_ns >= expected) {
    next_log_time_ns.store(now_ns + 1000000000LL, std::memory_order_relaxed);
    return true;
  }
  return false;
}

}  // namespace kafka_source

#include "rclcpp_components/register_node_macro.hpp"
RCLCPP_COMPONENTS_REGISTER_NODE(kafka_source::KafkaSourceNode)
