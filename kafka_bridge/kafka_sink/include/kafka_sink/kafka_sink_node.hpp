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
#include <memory>
#include <string>
#include <vector>

#include "rcl_interfaces/msg/set_parameters_result.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "kafka_sink/visibility_control.hpp"

namespace kafka_sink
{

struct SubscriptionConfig
{
  std::string topic_name;
  std::string msg_type;
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
  struct SubscriptionRuntime
  {
    std::string log_label;
    std::atomic<int64_t> next_log_time_ns{0};
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
  rclcpp::QoS build_qos_profile() const;

  std::vector<SubscriptionConfig> configured_subscriptions_;
  std::vector<ActiveSubscription> active_subscriptions_;

  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr
    on_parameters_set_handle_;

  std::atomic_bool is_active_{false};
  int qos_depth_{10};
};
}  // namespace kafka_sink

#endif  // KAFKA_SINK__KAFKA_SINK_NODE_HPP_
