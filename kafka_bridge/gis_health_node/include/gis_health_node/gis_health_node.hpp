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

#ifndef GIS_HEALTH_NODE__GIS_HEALTH_NODE_HPP_
#define GIS_HEALTH_NODE__GIS_HEALTH_NODE_HPP_

#include <memory>
#include <string>
#include <vector>

#include "kafka_client/kafka_producer.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"

#include "gis_health_node/visibility_control.hpp"

namespace gis_health_node
{

class GIS_HEALTH_NODE_PUBLIC GisHealthNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit GisHealthNode(const rclcpp::NodeOptions & options);

  using CallbackReturn =
    rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface::CallbackReturn;

  CallbackReturn on_configure(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_activate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_deactivate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_cleanup(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_shutdown(const rclcpp_lifecycle::State & state) override;

private:
  struct Parameters
  {
    std::string bootstrap_servers{"localhost:9092"};
    std::string client_id{"gis_health_node"};
    std::string acks{"1"};
    bool strict_startup{false};

    std::vector<std::string> robot_list{};
    std::string health_topic{"robot_health"};
    std::string registration_topic{"robot_registration"};
    double health_period_s{30.0};
    std::string odom_kafka_topic{"ros_filtered_odom"};
    std::string gps_kafka_topic{"ros_gps_fix"};
  };

  bool load_parameters(std::string * error_message);
  bool start_producer();
  void stop_producer();

  void send_registration_for(const std::string & robot_id);
  void send_ping_for(const std::string & robot_id);
  void on_health_timer();

  std::string build_registration_payload(const std::string & robot_id) const;
  std::string build_ping_payload(const std::string & robot_id) const;

  static int64_t now_ms();

  Parameters parameters_;
  std::shared_ptr<kafka_client::KafkaProducer> producer_;
  rclcpp::TimerBase::SharedPtr health_timer_;
};

}  // namespace gis_health_node

#endif  // GIS_HEALTH_NODE__GIS_HEALTH_NODE_HPP_
