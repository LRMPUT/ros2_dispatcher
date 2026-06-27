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

#ifndef ZENOH_SOURCE__ZENOH_SOURCE_NODE_HPP_
#define ZENOH_SOURCE__ZENOH_SOURCE_NODE_HPP_

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "rcl_interfaces/msg/set_parameters_result.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp/generic_publisher.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "std_msgs/msg/string.hpp"
#include "zenoh_source/visibility_control.hpp"

namespace zenoh_source
{

// Maps a Zenoh key expression back to a ROS 2 topic name (the reverse of
// zenoh_sink's mapping). `key_prefix` (e.g. "ros2") is stripped if present and
// `ros_topic_prefix` (e.g. "/zenoh_decoded") is prepended:
//   "ros2/a/b/c" -> "/zenoh_decoded/a/b/c".
std::string derive_ros_topic(
  const std::string & key_expr,
  const std::string & key_prefix,
  const std::string & ros_topic_prefix);

class ZENOH_SOURCE_PUBLIC ZenohSourceNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit ZenohSourceNode(const rclcpp::NodeOptions & options);

  using CallbackReturn =
    rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn;

  CallbackReturn on_configure(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_activate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_deactivate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_cleanup(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_shutdown(const rclcpp_lifecycle::State & state) override;

private:
  struct ZenohParameters
  {
    std::string mode{"peer"};
    std::vector<std::string> connect;
    std::vector<std::string> listen;
    std::string config_path;
    // Key expression to subscribe to. "ros2/**" matches everything published by
    // zenoh_sink with the default key_prefix.
    std::string key_expr{"ros2/**"};
    // Prefix stripped from the key expression when deriving the ROS topic name.
    std::string key_prefix{"ros2"};
    // Allowlist of ROS types that may be deserialized. Empty = allow any *valid*
    // type name (logs a warning — the type comes from the untrusted network).
    std::vector<std::string> allowed_types;
  };

  struct TopicMetrics
  {
    std::atomic<uint64_t> decoded{0};
    std::atomic<uint64_t> failed{0};
    std::atomic<uint64_t> bytes{0};
    std::string ros_type;
    uint64_t prev_decoded{0};
    uint64_t prev_failed{0};
    uint64_t prev_bytes{0};
  };

  rcl_interfaces::msg::SetParametersResult on_parameters_set(
    const std::vector<rclcpp::Parameter> & parameters);

  bool configure_from_parameters(std::string * error_message);
  bool validate_parameters(const ZenohParameters & pending, std::string * error_message) const;
  bool start_subscriber(std::string * error_message);
  void stop_subscriber();
  // Receive path, invoked from the zenoh subscriber callback thread. All zenoh
  // types are unwrapped in the .cpp before this is called.
  void handle_sample(
    const std::string & key_expr,
    const std::vector<uint8_t> & payload,
    const std::string & ros_type);
  rclcpp::GenericPublisher::SharedPtr get_or_create_publisher(
    const std::string & ros_topic, const std::string & ros_type);
  std::shared_ptr<TopicMetrics> get_metrics(const std::string & key, const std::string & ros_type);
  void publish_metrics();
  bool should_log_throttled(std::atomic<int64_t> & next_log_time_ns);

  ZenohParameters zenoh_parameters_;
  std::string ros_topic_prefix_{"/zenoh_decoded"};
  int qos_depth_{10};
  bool metrics_enabled_{true};
  int metrics_interval_ms_{1000};
  std::string metrics_topic_{"zenoh_source/metrics"};

  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr on_parameters_set_handle_;

  std::atomic_bool is_active_{false};
  std::atomic<int64_t> next_error_log_time_ns_{0};

  // cache_mutex_ guards the publisher cache and the metrics map — both touched
  // from the zenoh subscriber callback thread.
  std::mutex cache_mutex_;
  std::unordered_map<std::string, rclcpp::GenericPublisher::SharedPtr> publishers_;
  std::unordered_map<std::string, std::shared_ptr<TopicMetrics>> metrics_;

  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::String>::SharedPtr metrics_pub_;
  rclcpp::TimerBase::SharedPtr metrics_timer_;

  struct ZenohRuntime;
  std::shared_ptr<ZenohRuntime> zenoh_runtime_;
};

}  // namespace zenoh_source

#endif  // ZENOH_SOURCE__ZENOH_SOURCE_NODE_HPP_
