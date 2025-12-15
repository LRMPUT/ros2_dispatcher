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

#ifndef INTROSPECTION_MANAGER__INTROSPECTION_MANAGER_NODE_HPP_
#define INTROSPECTION_MANAGER__INTROSPECTION_MANAGER_NODE_HPP_

#include <memory>
#include <rclcpp/rclcpp.hpp>
#include "rcpputils/thread_safety_annotations.hpp"

#include <map>
#include <mutex>
#include <atomic>
#include <thread>
#include <string>
#include <vector>

#include "introspection_manager/introspection_manager.hpp"
#include "introspection_manager_msgs/msg/topic_info.hpp"
#include "introspection_manager_msgs/msg/topics_info.hpp"
#include "introspection_manager_msgs/srv/get_topics.hpp"

namespace introspection_manager
{
using IntrospectionManagerPtr = std::unique_ptr<introspection_manager::IntrospectionManager>;
using TopicMap = std::map<std::string, std::vector<std::string>>;

class INTROSPECTION_MANAGER_PUBLIC IntrospectionManagerNode : public rclcpp::Node
{
public:
  explicit IntrospectionManagerNode(const rclcpp::NodeOptions & options);
  ~IntrospectionManagerNode() override;

private:
  // --- Graph monitoring / updates ---
  void monitor_graph();
  void update_topics();

  // --- Topic filtering ---
  bool is_topic_hidden(const std::string & topic_name) const;
  TopicMap filter_hidden_topics(const TopicMap & topics) const;

  // --- Publishing / services ---
  void publish_topics_info(const TopicMap & topics);
  void handle_get_topics(
    const std::shared_ptr<introspection_manager_msgs::srv::GetTopics::Request> request,
    std::shared_ptr<introspection_manager_msgs::srv::GetTopics::Response> response);

  // --- ROS interfaces ---
  rclcpp::Service<introspection_manager_msgs::srv::GetTopics>::SharedPtr get_topics_service_;
  rclcpp::Publisher<introspection_manager_msgs::msg::TopicsInfo>::SharedPtr topics_info_pub_;
  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr parameter_callback_handle_;

  // --- State (guarded) ---
  std::mutex active_topics_mutex_;
  TopicMap active_topics_
    RCPPUTILS_TSA_GUARDED_BY(active_topics_mutex_);
  TopicMap previous_topics_
    RCPPUTILS_TSA_GUARDED_BY(active_topics_mutex_);

  // --- Threading / events ---
  rclcpp::Event::SharedPtr graph_event_;
  std::thread graph_monitor_thread_;
  std::atomic_bool run_graph_monitor_{false};

  // --- Config flags ---
  bool publish_on_change_;
  bool filter_hidden_;
};
}  // namespace introspection_manager

#endif  // INTROSPECTION_MANAGER__INTROSPECTION_MANAGER_NODE_HPP_
