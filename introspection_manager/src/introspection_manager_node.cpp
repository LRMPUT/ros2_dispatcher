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
  introspection_manager_ = std::make_unique<introspection_manager::IntrospectionManager>();
  param_name_ = this->declare_parameter("param_name", 456);
  introspection_manager_->foo(param_name_);
}

}  // namespace introspection_manager

#include "rclcpp_components/register_node_macro.hpp"

RCLCPP_COMPONENTS_REGISTER_NODE(introspection_manager::IntrospectionManagerNode)
