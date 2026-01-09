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

#ifndef KAFKA_SOURCE__KAFKA_SOURCE_NODE_HPP_
#define KAFKA_SOURCE__KAFKA_SOURCE_NODE_HPP_

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <librdkafka/rdkafkacpp.h>

#include "kafka_source/visibility_control.hpp"
#include "rcpputils/shared_library.hpp"
#include "rcl_interfaces/msg/set_parameters_result.hpp"
#include "rclcpp/generic_publisher.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp/serialized_message.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "std_msgs/msg/string.hpp"
#include "rosidl_runtime_c/message_type_support_struct.h"

namespace kafka_source
{

class KAFKA_SOURCE_PUBLIC KafkaSourceNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit KafkaSourceNode(const rclcpp::NodeOptions & options);

  using CallbackReturn =
    rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn;

  CallbackReturn on_configure(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_activate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_deactivate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_cleanup(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_shutdown(const rclcpp_lifecycle::State & state) override;

private:
  struct KafkaParameters
  {
    std::string bootstrap_servers{"localhost:9092"};
    std::string group_id{"ros2-kafka-source"};
    std::string topic_pattern{"^ros2\\..*"};
    std::string offset_reset{"latest"};
  };

  struct TopicMetrics
  {
    std::string ros_topic;
    std::string ros_type;
    std::atomic<uint64_t> received{0};
    std::atomic<uint64_t> decoded{0};
    std::atomic<uint64_t> failed{0};
    std::atomic<uint64_t> bytes{0};
    std::atomic<uint64_t> latency_ns_accum{0};
    std::atomic<uint64_t> latency_ns_max{0};
    std::mutex latency_mutex;
    std::deque<uint64_t> latency_samples;
    uint64_t prev_received{0};
    uint64_t prev_decoded{0};
    uint64_t prev_failed{0};
    uint64_t prev_bytes{0};
  };

  struct TypeSupportCacheEntry
  {
    const rosidl_message_type_support_t * rmw_type_support{nullptr};
    const rosidl_message_type_support_t * introspection_type_support{nullptr};
    std::shared_ptr<rcpputils::SharedLibrary> rmw_library;
    std::shared_ptr<rcpputils::SharedLibrary> introspection_library;
  };

  rcl_interfaces::msg::SetParametersResult on_parameters_set(
    const std::vector<rclcpp::Parameter> & parameters);

  bool configure_from_parameters(std::string * error_message);
  bool validate_parameters(std::string * error_message) const;
  bool start_consumer(std::string * error_message);
  void stop_consumer();
  void poll_loop();
  void process_message(RdKafka::Message * message);
  bool ensure_type_support(
    const std::string & ros_type,
    TypeSupportCacheEntry * entry,
    std::string * error_message);
  rclcpp::GenericPublisher::SharedPtr get_or_create_publisher(
    const std::string & ros_topic,
    const std::string & ros_type);
  std::string derive_ros_topic(const std::string & kafka_topic) const;
  void publish_metrics();
  void reset_metrics_timer();
  bool should_log_throttled(std::atomic<int64_t> & next_log_time_ns) const;

  KafkaParameters kafka_parameters_;
  std::string ros_topic_prefix_{"/kafka_decoded"};
  int qos_depth_{10};
  bool metrics_enabled_{true};
  int metrics_interval_ms_{1000};
  std::string metrics_topic_{"kafka_source/metrics"};
  std::unordered_map<std::string, std::string> topic_mappings_;

  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
  std::thread consumer_thread_;
  std::atomic_bool running_{false};
  std::atomic_bool is_active_{false};

  std::mutex cache_mutex_;
  std::unordered_map<std::string, TypeSupportCacheEntry> type_support_cache_;
  std::unordered_map<std::string, rclcpp::GenericPublisher::SharedPtr> publishers_;
  std::unordered_map<std::string, std::shared_ptr<TopicMetrics>> metrics_;

  std::atomic<int64_t> next_error_log_time_ns_{0};

  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr
    on_parameters_set_handle_;

  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::String>::SharedPtr metrics_pub_;
  rclcpp::TimerBase::SharedPtr metrics_timer_;
};

}  // namespace kafka_source

#endif  // KAFKA_SOURCE__KAFKA_SOURCE_NODE_HPP_
