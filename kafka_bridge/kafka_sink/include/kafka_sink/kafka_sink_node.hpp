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

#ifndef KAFKA_SINK__KAFKA_SINK_NODE_HPP_
#define KAFKA_SINK__KAFKA_SINK_NODE_HPP_

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "kafka_client/kafka_producer.hpp"
#include "rcpputils/shared_library.hpp"
#include "rcl_interfaces/msg/set_parameters_result.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "std_msgs/msg/string.hpp"
#include "rosidl_runtime_c/message_type_support_struct.h"
#include "kafka_sink/visibility_control.hpp"

namespace kafka_sink
{

struct SubscriptionConfig
{
  std::string topic_name;
  std::string msg_type;
  std::optional<std::string> kafka_name;
  std::optional<std::string> kafka_key;
};

std::vector<SubscriptionConfig> parse_subscriptions_yaml(const std::string & yaml_text);

class KAFKA_SINK_PUBLIC KafkaSinkNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit KafkaSinkNode(const rclcpp::NodeOptions & options);

  using CallbackReturn =
    rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn;

  CallbackReturn on_configure(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_activate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_deactivate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_cleanup(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_shutdown(const rclcpp_lifecycle::State & state) override;

private:
  enum class TopicMappingMode
  {
    PREFIX_ROS_TOPIC,
    FIXED
  };

  enum class PayloadFormat
  {
    CDR,
    JSON
  };

  struct KafkaParameters
  {
    std::string bootstrap_servers{"localhost:9092"};
    std::string client_id{"kafka_sink"};
    std::string acks{"all"};
    std::string topic_prefix{"ros2"};
    std::string message_key{"robot"};
    TopicMappingMode topic_mapping_mode{TopicMappingMode::PREFIX_ROS_TOPIC};
    std::string fixed_topic{"ros2.raw"};
    bool strict_startup{false};
    std::size_t max_queue_messages{1024};
    bool drop_when_full{true};
    std::optional<int> linger_ms;
    std::optional<int> batch_size;
    PayloadFormat payload_format{PayloadFormat::CDR};
  };

  struct SubscriptionRuntime
  {
    std::string log_label;
    std::string ros_topic;
    std::string msg_type;
    std::string kafka_topic;
    std::vector<uint8_t> message_key_bytes;
    PayloadFormat payload_format{PayloadFormat::CDR};
    const rosidl_message_type_support_t * rmw_type_support{nullptr};
    const rosidl_message_type_support_t * introspection_type_support{nullptr};
    // Keep shared library handles alive so type support pointers remain valid
    std::shared_ptr<rcpputils::SharedLibrary> rmw_ts_lib;
    std::shared_ptr<rcpputils::SharedLibrary> introspection_ts_lib;
    std::atomic<int64_t> next_log_time_ns{0};
    std::atomic<uint64_t> sent_ok{0};
    std::atomic<uint64_t> dropped{0};
    std::atomic<uint64_t> errors{0};

    // Metrics
    std::atomic<uint64_t> msgs_total{0};
    std::atomic<uint64_t> bytes_total{0};
    std::atomic<uint64_t> serialize_time_ns_accum{0};
    std::atomic<uint64_t> send_time_ns_accum{0};
    std::atomic<uint64_t> serialize_time_ns_max{0};
    std::atomic<uint64_t> send_time_ns_max{0};
    std::atomic<uint64_t> msg_size_min{UINT64_MAX};
    std::atomic<uint64_t> msg_size_max{0};
    std::atomic<uint64_t> msg_seq{0};

    // Latency sample buffers for percentile calculation (protected by mutex)
    std::mutex latency_mutex;
    std::deque<uint64_t> serialize_samples;
    std::deque<uint64_t> send_samples;
    static constexpr size_t kMaxSamples = 1000;

    // Snapshot values used by periodic metrics reporting
    uint64_t prev_msgs_total{0};
    uint64_t prev_sent_ok{0};
    uint64_t prev_dropped{0};
    uint64_t prev_errors{0};
    uint64_t prev_bytes_total{0};
    uint64_t prev_serialize_time_ns_accum{0};
    uint64_t prev_send_time_ns_accum{0};
  };

  struct ActiveSubscription
  {
    ActiveSubscription() = default;
    ActiveSubscription(const ActiveSubscription &) = delete;
    ActiveSubscription & operator=(const ActiveSubscription &) = delete;
    ActiveSubscription(ActiveSubscription && other) noexcept;
    ActiveSubscription & operator=(ActiveSubscription && other) noexcept;

    rclcpp::GenericSubscription::SharedPtr subscription;
    std::string topic_name;
    std::string msg_type;
    std::shared_ptr<SubscriptionRuntime> runtime_state;
  };

  rcl_interfaces::msg::SetParametersResult on_parameters_set(
    const std::vector<rclcpp::Parameter> & parameters);

  bool build_subscriptions();
  void clear_subscriptions();
  bool configure_from_parameters(std::string * error_message);
  bool validate_qos_depth(int qos_depth, std::string * error_message) const;
  bool validate_kafka_parameters(const KafkaParameters & pending, std::string * error_message) const;
  bool configure_kafka_parameters(std::string * error_message);
  bool start_producer();
  void stop_producer();
  std::string map_kafka_topic(const std::string & ros_topic) const;
  kafka_client::KafkaProducerConfig build_producer_config() const;
  rclcpp::QoS build_qos_profile() const;

  // Metrics handling
  void publish_metrics();

  std::vector<SubscriptionConfig> configured_subscriptions_;
  std::vector<ActiveSubscription> active_subscriptions_;
  KafkaParameters kafka_parameters_;
  std::shared_ptr<kafka_client::KafkaProducer> kafka_producer_;

  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr
    on_parameters_set_handle_;

  std::atomic_bool is_active_{false};
  int qos_depth_{10};

  // Metrics configuration/state
  bool metrics_enabled_{false};
  int metrics_interval_ms_{1000};
  std::string metrics_topic_{"kafka_sink/metrics"};
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::String>::SharedPtr metrics_pub_;
  rclcpp::TimerBase::SharedPtr metrics_timer_;

  bool telemetry_enabled_{false};
  int telemetry_log_every_n_{1};
};
}  // namespace kafka_sink

#endif  // KAFKA_SINK__KAFKA_SINK_NODE_HPP_
