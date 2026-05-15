// Copyright 2026 Maciej Krupka
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

#include "gis_health_node/gis_health_node.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "nlohmann/json.hpp"
#include "rclcpp_components/register_node_macro.hpp"

namespace gis_health_node
{

GisHealthNode::GisHealthNode(const rclcpp::NodeOptions & options)
: rclcpp_lifecycle::LifecycleNode("gis_health_node", options)
{
  this->declare_parameter<std::string>("kafka.bootstrap_servers", parameters_.bootstrap_servers);
  this->declare_parameter<std::string>("kafka.client_id", parameters_.client_id);
  this->declare_parameter<std::string>("kafka.acks", parameters_.acks);
  this->declare_parameter<bool>("kafka.strict_startup", parameters_.strict_startup);

  this->declare_parameter<std::vector<std::string>>("gis.robot_list", parameters_.robot_list);
  this->declare_parameter<std::string>("gis.health_topic", parameters_.health_topic);
  this->declare_parameter<std::string>("gis.registration_topic", parameters_.registration_topic);
  this->declare_parameter<double>("gis.health_period_s", parameters_.health_period_s);
  this->declare_parameter<std::string>("gis.odom_kafka_topic", parameters_.odom_kafka_topic);
  this->declare_parameter<std::string>("gis.gps_kafka_topic", parameters_.gps_kafka_topic);
}

GisHealthNode::CallbackReturn GisHealthNode::on_configure(const rclcpp_lifecycle::State & /*state*/)
{
  std::string error_message;
  if (!load_parameters(&error_message)) {
    RCLCPP_ERROR(this->get_logger(), "Parameter validation failed: %s", error_message.c_str());
    return CallbackReturn::FAILURE;
  }

  if (!start_producer()) {
    return CallbackReturn::FAILURE;
  }

  RCLCPP_INFO(
    this->get_logger(),
    "Configured: bootstrap=%s robots=%zu period=%.1fs",
    parameters_.bootstrap_servers.c_str(),
    parameters_.robot_list.size(),
    parameters_.health_period_s);

  return CallbackReturn::SUCCESS;
}

GisHealthNode::CallbackReturn GisHealthNode::on_activate(const rclcpp_lifecycle::State & /*state*/)
{
  for (const auto & robot_id : parameters_.robot_list) {
    send_registration_for(robot_id);
    send_ping_for(robot_id);
  }

  const auto period = std::chrono::milliseconds(
    static_cast<int64_t>(parameters_.health_period_s * 1000.0));
  health_timer_ = this->create_wall_timer(period, [this]() {this->on_health_timer();});

  RCLCPP_INFO(this->get_logger(), "Activated, sent initial REGISTRATION + PING for all robots.");
  return CallbackReturn::SUCCESS;
}

GisHealthNode::CallbackReturn GisHealthNode::on_deactivate(const rclcpp_lifecycle::State & /*state*/)
{
  if (health_timer_) {
    health_timer_->cancel();
    health_timer_.reset();
  }
  RCLCPP_INFO(this->get_logger(), "Deactivated, health timer stopped.");
  return CallbackReturn::SUCCESS;
}

GisHealthNode::CallbackReturn GisHealthNode::on_cleanup(const rclcpp_lifecycle::State & /*state*/)
{
  stop_producer();
  RCLCPP_INFO(this->get_logger(), "Cleaned up.");
  return CallbackReturn::SUCCESS;
}

GisHealthNode::CallbackReturn GisHealthNode::on_shutdown(const rclcpp_lifecycle::State & /*state*/)
{
  if (health_timer_) {
    health_timer_->cancel();
    health_timer_.reset();
  }
  stop_producer();
  return CallbackReturn::SUCCESS;
}

bool GisHealthNode::load_parameters(std::string * error_message)
{
  parameters_.bootstrap_servers = this->get_parameter("kafka.bootstrap_servers").as_string();
  parameters_.client_id = this->get_parameter("kafka.client_id").as_string();
  parameters_.acks = this->get_parameter("kafka.acks").as_string();
  parameters_.strict_startup = this->get_parameter("kafka.strict_startup").as_bool();

  parameters_.robot_list = this->get_parameter("gis.robot_list").as_string_array();
  parameters_.health_topic = this->get_parameter("gis.health_topic").as_string();
  parameters_.registration_topic = this->get_parameter("gis.registration_topic").as_string();
  parameters_.health_period_s = this->get_parameter("gis.health_period_s").as_double();
  parameters_.odom_kafka_topic = this->get_parameter("gis.odom_kafka_topic").as_string();
  parameters_.gps_kafka_topic = this->get_parameter("gis.gps_kafka_topic").as_string();

  if (parameters_.bootstrap_servers.empty()) {
    *error_message = "kafka.bootstrap_servers is required.";
    return false;
  }
  if (parameters_.robot_list.empty()) {
    *error_message = "gis.robot_list must contain at least one robot id.";
    return false;
  }
  if (parameters_.health_period_s <= 0.0) {
    *error_message = "gis.health_period_s must be positive.";
    return false;
  }
  if (parameters_.health_topic.empty() || parameters_.registration_topic.empty()) {
    *error_message = "gis.health_topic and gis.registration_topic must be non-empty.";
    return false;
  }
  return true;
}

bool GisHealthNode::start_producer()
{
  kafka_client::KafkaProducerConfig cfg;
  cfg.bootstrap_servers = parameters_.bootstrap_servers;
  cfg.client_id = parameters_.client_id;
  cfg.acks = parameters_.acks;
  cfg.startup_mode = parameters_.strict_startup
    ? kafka_client::StartupMode::STRICT
    : kafka_client::StartupMode::TOLERANT;

  producer_ = std::make_shared<kafka_client::KafkaProducer>(cfg);
  if (!producer_->start()) {
    RCLCPP_ERROR(
      this->get_logger(),
      "Failed to start Kafka producer (bootstrap=%s)",
      parameters_.bootstrap_servers.c_str());
    producer_.reset();
    return false;
  }
  return true;
}

void GisHealthNode::stop_producer()
{
  if (producer_) {
    producer_->stop();
    producer_.reset();
  }
}

void GisHealthNode::on_health_timer()
{
  for (const auto & robot_id : parameters_.robot_list) {
    send_ping_for(robot_id);
  }
}

void GisHealthNode::send_registration_for(const std::string & robot_id)
{
  if (!producer_) {
    return;
  }
  const std::string payload = build_registration_payload(robot_id);
  const std::vector<uint8_t> key(robot_id.begin(), robot_id.end());
  const std::vector<uint8_t> value(payload.begin(), payload.end());
  const auto result = producer_->send(
    parameters_.registration_topic, key, value, now_ms(), {});
  if (result.status != kafka_client::SendStatus::SENT) {
    RCLCPP_WARN(
      this->get_logger(),
      "REGISTRATION send for '%s' returned status=%d (%s)",
      robot_id.c_str(),
      static_cast<int>(result.status),
      result.error_message.c_str());
  } else {
    RCLCPP_INFO(this->get_logger(), "Sent REGISTRATION for robot '%s'", robot_id.c_str());
  }
}

void GisHealthNode::send_ping_for(const std::string & robot_id)
{
  if (!producer_) {
    return;
  }
  const std::string payload = build_ping_payload(robot_id);
  const std::vector<uint8_t> key(robot_id.begin(), robot_id.end());
  const std::vector<uint8_t> value(payload.begin(), payload.end());
  const auto result = producer_->send(
    parameters_.health_topic, key, value, now_ms(), {});
  if (result.status != kafka_client::SendStatus::SENT) {
    RCLCPP_WARN(
      this->get_logger(),
      "PING send for '%s' returned status=%d (%s)",
      robot_id.c_str(),
      static_cast<int>(result.status),
      result.error_message.c_str());
  }
}

std::string GisHealthNode::build_registration_payload(const std::string & robot_id) const
{
  nlohmann::json topics = nlohmann::json::array();

  topics.push_back(
    {
      {"name", parameters_.odom_kafka_topic},
      {"schema", nlohmann::json::array(
          {
            {{"name", "timestamp"}, {"type", "BIGINT"}},
            {{"name", "position_x"}, {"type", "DOUBLE"}},
            {{"name", "position_y"}, {"type", "DOUBLE"}},
            {{"name", "position_z"}, {"type", "DOUBLE"}},
            {{"name", "source_topic"}, {"type", "VARCHAR"}},
          })}
    });

  topics.push_back(
    {
      {"name", parameters_.gps_kafka_topic},
      {"schema", nlohmann::json::array(
          {
            {{"name", "timestamp"}, {"type", "BIGINT"}},
            {{"name", "latitude"}, {"type", "DOUBLE"}},
            {{"name", "longitude"}, {"type", "DOUBLE"}},
            {{"name", "altitude"}, {"type", "DOUBLE"}},
          })}
    });

  nlohmann::json payload = {
    {"msg_type", "REGISTRATION"},
    {"serial_number", robot_id},
    {"topics", topics},
    {"timestamp", now_ms()}
  };
  return payload.dump();
}

std::string GisHealthNode::build_ping_payload(const std::string & robot_id) const
{
  nlohmann::json payload = {
    {"msg_type", "PING"},
    {"serial_number", robot_id},
    {"robot_name", robot_id},
    {"timestamp", now_ms()}
  };
  return payload.dump();
}

int64_t GisHealthNode::now_ms()
{
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

}  // namespace gis_health_node

RCLCPP_COMPONENTS_REGISTER_NODE(gis_health_node::GisHealthNode)
