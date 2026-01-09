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

#ifndef KAFKA_CDR_TO_JSON__KAFKA_CDR_TO_JSON_NODE_HPP_
#define KAFKA_CDR_TO_JSON__KAFKA_CDR_TO_JSON_NODE_HPP_

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <librdkafka/rdkafkacpp.h>

#include "kafka_cdr_to_json/visibility_control.hpp"
#include "kafka_client/kafka_producer.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "std_msgs/msg/string.hpp"

namespace rcpputils
{
class SharedLibrary;
}  // namespace rcpputils

namespace kafka_cdr_to_json
{

class KAFKA_CDR_TO_JSON_PUBLIC KafkaCdrToJsonNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit KafkaCdrToJsonNode(const rclcpp::NodeOptions & options);

  rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn on_configure(
    const rclcpp_lifecycle::State & state) override;
  rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn on_activate(
    const rclcpp_lifecycle::State & state) override;
  rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn on_deactivate(
    const rclcpp_lifecycle::State & state) override;
  rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn on_cleanup(
    const rclcpp_lifecycle::State & state) override;
  rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn on_shutdown(
    const rclcpp_lifecycle::State & state) override;

private:
  struct KafkaParameters
  {
    std::string bootstrap_servers{"localhost:9092"};
    std::string group_id{"kafka-cdr-to-json"};
    std::string input_topic_pattern{"^ros2\\..*"};
    std::string offset_reset{"latest"};
  };

  struct TypeSupportEntry
  {
    std::shared_ptr<rcpputils::SharedLibrary> rmw_library;
    std::shared_ptr<rcpputils::SharedLibrary> introspection_library;
    const rosidl_message_type_support_t * rmw_type_support{nullptr};
    const rosidl_message_type_support_t * introspection_type_support{nullptr};
  };

  struct TopicMetrics
  {
    explicit TopicMetrics(std::string input)
    : input_topic(std::move(input)) {}

    std::string input_topic;
    std::string output_topic;
    std::string ros_type;

    std::atomic<uint64_t> received{0};
    std::atomic<uint64_t> converted{0};
    std::atomic<uint64_t> failed{0};
    std::atomic<uint64_t> bytes{0};
    std::atomic<uint64_t> latency_ns_max{0};
    std::atomic<uint64_t> total_json_bytes{0};
    std::atomic<uint64_t> json_count{0};

    uint64_t min_json_bytes{0};
    uint64_t max_json_bytes{0};

    std::mutex size_mutex;
    std::mutex latency_mutex;
    std::mutex metadata_mutex;
    std::deque<uint64_t> latency_samples;

    uint64_t prev_received{0};
    uint64_t prev_converted{0};
    uint64_t prev_failed{0};
    uint64_t prev_bytes{0};
  };

  using CallbackReturn =
    rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn;

  rcl_interfaces::msg::SetParametersResult on_parameters_set(
    const std::vector<rclcpp::Parameter> & parameters);

  bool configure_from_parameters(std::string * error_message);
  bool validate_parameters(std::string * error_message) const;

  bool start_consumer(std::string * error_message);
  void stop_consumer();
  bool start_producer(std::string * error_message);
  void stop_producer();

  void poll_loop();
  void process_message(RdKafka::Message * message);

  std::string resolve_output_topic(const std::string & input_topic) const;
  std::shared_ptr<TopicMetrics> get_or_create_metrics(const std::string & input_topic);
  bool get_type_support(
    const std::string & ros_type,
    TypeSupportEntry * entry,
    std::string * error_message);

  void publish_metrics();
  void reset_metrics_timer();
  bool should_log_throttled(std::atomic<int64_t> & next_log_time_ns);

  KafkaParameters kafka_parameters_;
  std::unordered_map<std::string, std::string> topic_mappings_;
  std::string output_topic_prefix_{"ros2_json"};
  bool include_ros_type_{true};
  bool include_timestamp_{true};
  bool metrics_enabled_{true};
  int metrics_interval_ms_{1000};
  std::string metrics_topic_{"kafka_cdr2json/metrics"};

  std::atomic_bool is_active_{false};
  std::atomic_bool running_{false};
  std::thread consumer_thread_;

  std::mutex cache_mutex_;
  std::unordered_map<std::string, TypeSupportEntry> type_support_cache_;
  std::unordered_map<std::string, std::shared_ptr<TopicMetrics>> metrics_;

  std::mutex producer_mutex_;
  std::shared_ptr<kafka_client::KafkaProducer> producer_;

  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;

  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::String>::SharedPtr metrics_pub_;
  rclcpp::TimerBase::SharedPtr metrics_timer_;
  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr on_parameters_set_handle_;
  rclcpp::Clock::SharedPtr system_clock_;

  std::atomic<int64_t> next_consume_error_log_time_ns_{0};
  std::atomic<int64_t> next_header_error_log_time_ns_{0};
  std::atomic<int64_t> next_type_support_log_time_ns_{0};
  std::atomic<int64_t> next_deserialize_log_time_ns_{0};
  std::atomic<int64_t> next_produce_log_time_ns_{0};
};

}  // namespace kafka_cdr_to_json

#endif  // KAFKA_CDR_TO_JSON__KAFKA_CDR_TO_JSON_NODE_HPP_
