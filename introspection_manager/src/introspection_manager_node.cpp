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

#include "introspection_manager/introspection_manager_node.hpp"

namespace introspection_manager
{

IntrospectionManagerNode::IntrospectionManagerNode(const rclcpp::NodeOptions & options)
:  Node("introspection_manager", options)
{
  auto node_graph = this->get_node_graph_interface();
  RCLCPP_INFO(this->get_logger(), "Introspection Manager Node has been started.");
  int publisher_queue_depth_param =
    this->declare_parameter<int>("publisher_queue_depth", 10);
  size_t publisher_queue_depth =
    static_cast<size_t>(publisher_queue_depth_param);
  std::string publisher_reliability =
    this->declare_parameter<std::string>("publisher_reliability", "reliable");
  std::string publisher_durability =
    this->declare_parameter<std::string>("publisher_durability", "volatile");
  publish_on_change_ = this->declare_parameter<bool>("publish_on_change", true);
  filter_hidden_ = this->declare_parameter<bool>("filter_hidden", false);
  bool introspection_enabled = this->declare_parameter<bool>("introspection_enabled", true);
  rclcpp::QoS publisher_qos(publisher_queue_depth);
  if (publisher_reliability == "best_effort") {
    publisher_qos.reliability(rclcpp::ReliabilityPolicy::BestEffort);
  } else {
    publisher_qos.reliability(rclcpp::ReliabilityPolicy::Reliable);
  }
  if (publisher_durability == "transient_local") {
    publisher_qos.durability(rclcpp::DurabilityPolicy::TransientLocal);
  } else {
    publisher_qos.durability(rclcpp::DurabilityPolicy::Volatile);
  }
  topics_info_pub_ = this->create_publisher<introspection_manager_msgs::msg::TopicsInfo>(
    "~/topics_info", publisher_qos);
  get_topics_service_ = this->create_service<introspection_manager_msgs::srv::GetTopics>(
    "~/get_topics",
    std::bind(
      &IntrospectionManagerNode::handle_get_topics,
      this,
      std::placeholders::_1,
      std::placeholders::_2));
  {
    auto initial_topics = filter_hidden_topics(this->get_topic_names_and_types());
    std::lock_guard<std::mutex> lock(active_topics_mutex_);
    active_topics_ = initial_topics;
    previous_topics_ = initial_topics;
  }

  parameter_callback_handle_ = this->add_on_set_parameters_callback(
    [this](const std::vector<rclcpp::Parameter> & parameters)
      -> rcl_interfaces::msg::SetParametersResult
    {
      rcl_interfaces::msg::SetParametersResult result;
      result.successful = true;
      result.reason = "";
      for (const auto & param : parameters) {
        if (param.get_name() == "publish_on_change") {
          publish_on_change_ = param.as_bool();
        } else if (param.get_name() == "filter_hidden") {
          filter_hidden_ = param.as_bool();
          // Force an immediate update to apply filtering changes.
          update_topics();
        } else if (param.get_name() == "introspection_enabled") {
          bool enable = param.as_bool();
          if (enable && !run_graph_monitor_.load()) {
            // Start monitoring thread if previously disabled.
            run_graph_monitor_.store(true);
            graph_event_ = this->get_graph_event();
            graph_monitor_thread_ = std::thread(
              &IntrospectionManagerNode::monitor_graph,
              this);
          } else if (!enable && run_graph_monitor_.load()) {
            // Stop monitoring thread if currently enabled.
            run_graph_monitor_.store(false);
            // Wait for the existing thread to exit.
            if (graph_monitor_thread_.joinable()) {
              graph_monitor_thread_.join();
            }
          }
        }
      }
      return result;
    });
  if (introspection_enabled) {
    run_graph_monitor_.store(true);
    graph_event_ = this->get_graph_event();
    graph_monitor_thread_ = std::thread(&IntrospectionManagerNode::monitor_graph, this);
  } else {
    run_graph_monitor_.store(false);
  }
}

void IntrospectionManagerNode::monitor_graph()
{
  while (rclcpp::ok() && run_graph_monitor_.load()) {
    this->wait_for_graph_change(graph_event_, std::chrono::milliseconds(500));
    if (graph_event_->check_and_clear()) {
      update_topics();
      graph_event_ = this->get_graph_event();
    }
  }
}

IntrospectionManagerNode::~IntrospectionManagerNode()
{
  run_graph_monitor_.store(false);
  if (graph_monitor_thread_.joinable()) {
    graph_monitor_thread_.join();
  }
}

bool IntrospectionManagerNode::is_topic_hidden(const std::string & topic_name) const
{
  size_t start = 0;
  while (start < topic_name.size()) {
    size_t end = topic_name.find('/', start);
    std::string token = topic_name.substr(start, end - start);
    if (!token.empty() && token[0] == '_') {
      return true;
    }
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return false;
}

std::map<std::string, std::vector<std::string>>
IntrospectionManagerNode::filter_hidden_topics(
  const std::map<std::string, std::vector<std::string>> & topics) const
{
  if (!filter_hidden_) {
    return topics;
  }

  std::map<std::string, std::vector<std::string>> filtered;
  for (const auto & pair : topics) {
    if (!is_topic_hidden(pair.first)) {
      filtered.insert(pair);
    }
  }
  return filtered;
}

void IntrospectionManagerNode::publish_topics_info(
  const std::map<std::string, std::vector<std::string>> & topics)
{
  introspection_manager_msgs::msg::TopicsInfo msg;
  msg.topics.reserve(topics.size());

  for (const auto & pair : topics) {
    for (const auto & type_name : pair.second) {
      introspection_manager_msgs::msg::TopicInfo info;
      info.name = pair.first;
      info.type = type_name;
      msg.topics.push_back(info);
    }
  }

  topics_info_pub_->publish(msg);
  RCLCPP_DEBUG(
    this->get_logger(),
    "Published updated topics_info with %zu entries",
    msg.topics.size());
}

void IntrospectionManagerNode::update_topics()
{
  auto topics_and_types = filter_hidden_topics(this->get_topic_names_and_types());
  bool changed = (topics_and_types != previous_topics_);

  {
    std::lock_guard<std::mutex> lock(active_topics_mutex_);
    active_topics_ = topics_and_types;
  }

  if (changed && publish_on_change_) {
    publish_topics_info(topics_and_types);
  }

  previous_topics_ = std::move(topics_and_types);

  RCLCPP_DEBUG(
    this->get_logger(),
    "Updated active topic list with %zu entries (%schanged)",
    active_topics_.size(),
    changed ? "" : "not ");
}

void IntrospectionManagerNode::handle_get_topics(
  const std::shared_ptr<introspection_manager_msgs::srv::GetTopics::Request> request,
  std::shared_ptr<introspection_manager_msgs::srv::GetTopics::Response> response)
{
  (void)request;

  std::map<std::string, std::vector<std::string>> topic_map;
  {
    std::lock_guard<std::mutex> lock(active_topics_mutex_);
    topic_map = active_topics_;
  }

  size_t total_entries = 0;
  for (const auto & pair : topic_map) {
    total_entries += pair.second.size();
  }
  response->topics.reserve(total_entries);

  for (const auto & pair : topic_map) {
    const auto & topic_name = pair.first;
    const auto & type_vec = pair.second;
    for (const auto & type_name : type_vec) {
      introspection_manager_msgs::msg::TopicInfo info;
      info.name = topic_name;
      info.type = type_name;
      response->topics.push_back(info);
    }
  }

  RCLCPP_DEBUG(
    this->get_logger(),
    "Served get_topics request with %zu entries", response->topics.size());
}


}  // namespace introspection_manager

#include "rclcpp_components/register_node_macro.hpp"

RCLCPP_COMPONENTS_REGISTER_NODE(introspection_manager::IntrospectionManagerNode)
