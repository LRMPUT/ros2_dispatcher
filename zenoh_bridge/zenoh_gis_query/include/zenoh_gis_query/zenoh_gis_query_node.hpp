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

#ifndef ZENOH_GIS_QUERY__ZENOH_GIS_QUERY_NODE_HPP_
#define ZENOH_GIS_QUERY__ZENOH_GIS_QUERY_NODE_HPP_

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "rclcpp/node_options.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "zenoh_gis_query/navsatfix_decode.hpp"
#include "zenoh_gis_query/plots.hpp"
#include "zenoh_gis_query/visibility_control.hpp"

namespace zenoh_gis_query
{

class ZENOH_GIS_QUERY_PUBLIC ZenohGisQueryNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit ZenohGisQueryNode(const rclcpp::NodeOptions & options);

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
    std::string key_expr{"ros2/**"};
    std::string live_key_prefix{"gis/live"};
  };

  // PIMPL holding session + subscribers. Members are destroyed in reverse
  // declaration order, so session outlives both subscribers.
  struct ZenohRuntime;

  bool start_session(std::string * error_message);
  void stop_session();

  void on_position_sample(const std::string & key, const std::vector<uint8_t> & payload);

  // Returns the robot-id segment after the key prefix, e.g.
  // robot_from_key("ros2/robot_3/gps/fix") -> "robot_3".
  static std::string robot_from_key(
    const std::string & key, const std::string & prefix = "ros2");

  ZenohParameters zenoh_parameters_;
  std::string plots_path_;
  std::string sensors_path_;
  int fidelity_horizon_ms_{2000};

  std::vector<Plot> plots_;
  std::vector<Sensor> sensors_;

  // Guards live_robots_ and latest_; callbacks run on Zenoh threads.
  std::mutex state_mutex_;
  std::set<std::string> live_robots_;
  std::unordered_map<std::string, FixSample> latest_;

  std::atomic_bool is_active_{false};

  std::shared_ptr<ZenohRuntime> rt_;
};

}  // namespace zenoh_gis_query

#endif  // ZENOH_GIS_QUERY__ZENOH_GIS_QUERY_NODE_HPP_
