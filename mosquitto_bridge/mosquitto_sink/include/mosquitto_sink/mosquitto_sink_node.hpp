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

#ifndef MOSQUITTO_SINK__MOSQUITTO_SINK_NODE_HPP_
#define MOSQUITTO_SINK__MOSQUITTO_SINK_NODE_HPP_

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "rcl_interfaces/msg/set_parameters_result.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "std_msgs/msg/string.hpp"
#include "rosidl_runtime_c/message_type_support_struct.h"
#include "mosquitto_sink/visibility_control.hpp"

namespace mosquitto_sink
{

struct SubscriptionConfig
{
  std::string topic_name;
  std::string msg_type;
  std::optional<std::string> mqtt_name;
};

std::vector<SubscriptionConfig> parse_subscriptions_yaml(const std::string & yaml_text);

class MOSQUITTO_SINK_PUBLIC MosquittoSinkNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit MosquittoSinkNode(const rclcpp::NodeOptions & options);

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

  struct MqttParameters
  {
    std::string broker_host{"localhost"};
    int broker_port{1883};
    std::string client_id{"mosquitto_sink"};
    std::string username;
    std::string password;
    int qos{1};
    bool retain{false};
    int keep_alive_seconds{60};
    std::string topic_prefix{"ros2"};
    TopicMappingMode topic_mapping_mode{TopicMappingMode::PREFIX_ROS_TOPIC};
    std::string fixed_topic{"ros2/raw"};
    PayloadFormat payload_format{PayloadFormat::CDR};
    bool use_tls{false};
    std::string ca_cert_path;
    std::string lwt_topic;
    std::string lwt_payload{"mosquitto_sink disconnected"};
    int lwt_qos{1};
    bool lwt_retain{false};
  };

  struct SubscriptionRuntime
  {
    std::string log_label;
    std::string ros_topic;
    std::string msg_type;
    std::string mqtt_topic;
    PayloadFormat payload_format{PayloadFormat::CDR};
    const rosidl_message_type_support_t * rmw_type_support{nullptr};
    const rosidl_message_type_support_t * introspection_type_support{nullptr};
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
  bool validate_mqtt_parameters(const MqttParameters & pending, std::string * error_message) const;
  bool configure_mqtt_parameters(std::string * error_message);
  bool start_client();
  void stop_client();
  std::string map_mqtt_topic(const std::string & ros_topic) const;
  rclcpp::QoS build_qos_profile() const;

  // Metrics handling
  void publish_metrics();

  std::vector<SubscriptionConfig> configured_subscriptions_;
  std::vector<ActiveSubscription> active_subscriptions_;
  MqttParameters mqtt_parameters_;

  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr
    on_parameters_set_handle_;

  std::atomic_bool is_active_{false};
  int qos_depth_{10};

  // Metrics configuration/state
  bool metrics_enabled_{false};
  int metrics_interval_ms_{1000};
  std::string metrics_topic_{"mosquitto_sink/metrics"};
  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::String>::SharedPtr metrics_pub_;
  rclcpp::TimerBase::SharedPtr metrics_timer_;

  struct MqttRuntime;
  std::shared_ptr<MqttRuntime> mqtt_runtime_;
};
}  // namespace mosquitto_sink

#endif  // MOSQUITTO_SINK__MOSQUITTO_SINK_NODE_HPP_
