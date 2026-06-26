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

#include "rclcpp/node_options.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "zenoh_gis_query/visibility_control.hpp"

namespace zenoh_gis_query
{

/// Stub placeholder — full implementation added in Task 6.
class ZENOH_GIS_QUERY_PUBLIC ZenohGisQueryNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  explicit ZenohGisQueryNode(const rclcpp::NodeOptions & options)
  : rclcpp_lifecycle::LifecycleNode("zenoh_gis_query_node", options) {}
};

}  // namespace zenoh_gis_query

#endif  // ZENOH_GIS_QUERY__ZENOH_GIS_QUERY_NODE_HPP_
