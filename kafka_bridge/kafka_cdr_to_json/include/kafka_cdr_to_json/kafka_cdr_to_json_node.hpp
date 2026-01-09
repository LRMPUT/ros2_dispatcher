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

#ifndef KAFKA_CDR_TO_JSON__KAFKA_CDR_TO_JSON_NODE_HPP_
#define KAFKA_CDR_TO_JSON__KAFKA_CDR_TO_JSON_NODE_HPP_

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

#include "kafka_cdr_to_json/visibility_control.hpp"
#include "kafka_client/kafka_producer.hpp"
#include "rcpputils/shared_library.hpp"
#include "rcl_interfaces/msg/set_parameters_result.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "std_msgs/msg/string.hpp"
#include "rosidl_runtime_c/message_type_support_struct.h"

namespace kafka_cdr_to_json
{

class KAFKA_CDR_TO_JSON_PUBLIC KafkaCdrToJsonNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit KafkaCdrToJsonNode(const rclcpp::NodeOptions & options);

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
    std::string group_id{"ros2-cdr-to-json"};
    std::string input_topic_pattern{"^ros2\\..*"};
    std::string output_topic_prefix{"ros2_json"};
    std::string offset_reset{"latest"};
  };

  struct JsonParameters
  {
    bool include_ros_type{true};
    bool include_timestamp{true};
  };

  struct TopicMetrics
  {
    std::string input_topic;
    std::string output_topic;
    std::string ros_type;
    std::atomic<uint64_t> received{0};
    std::atomic<uint64_t> converted{0};
    std::atomic<uint64_t> failed{0};
    std::atomic<uint64_t> bytes{0};
    std::atomic<uint64_t> latency_ns_max{0};
    std::mutex metrics_mutex;
    std::deque<uint64_t> latency_samples;
    uint64_t json_size_min{0};
    uint64_t json_size_max{0};
    uint64_t json_size_total{0};
    uint64_t prev_received{0};
    uint64_t prev_converted{0};
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
  bool start_producer(std::string * error_message);
  void stop_producer();
  void poll_loop();
  void process_message(RdKafka::Message * message);
  bool ensure_type_support(
    const std::string & ros_type,
    TypeSupportCacheEntry * entry,
    std::string * error_message);
  std::string map_output_topic(const std::string & input_topic) const;
  void publish_metrics();
  void reset_metrics_timer();
  bool should_log_throttled(const std::string & key);

  KafkaParameters kafka_parameters_;
  JsonParameters json_parameters_;
  bool metrics_enabled_{true};
  int metrics_interval_ms_{1000};
  std::string metrics_topic_{"kafka_cdr2json/metrics"};
  std::unordered_map<std::string, std::string> topic_mappings_;

  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
  std::shared_ptr<kafka_client::KafkaProducer> producer_;
  std::thread consumer_thread_;
  std::atomic_bool running_{false};
  std::atomic_bool is_active_{false};

  std::mutex cache_mutex_;
  std::unordered_map<std::string, TypeSupportCacheEntry> type_support_cache_;
  std::unordered_map<std::string, std::shared_ptr<TopicMetrics>> metrics_;

  std::mutex log_mutex_;
  std::unordered_map<std::string, int64_t> error_log_next_ns_;

  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr
    on_parameters_set_handle_;

  rclcpp_lifecycle::LifecyclePublisher<std_msgs::msg::String>::SharedPtr metrics_pub_;
  rclcpp::TimerBase::SharedPtr metrics_timer_;
  rclcpp::Time last_metrics_time_;
};

}  // namespace kafka_cdr_to_json

#endif  // KAFKA_CDR_TO_JSON__KAFKA_CDR_TO_JSON_NODE_HPP_
