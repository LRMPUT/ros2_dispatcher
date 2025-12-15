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

#include "introspection_manager/introspection_manager.hpp"

namespace introspection_manager
{
using IntrospectionManagerPtr = std::unique_ptr<introspection_manager::IntrospectionManager>;

class INTROSPECTION_MANAGER_PUBLIC IntrospectionManagerNode : public rclcpp::Node
{
public:
  explicit IntrospectionManagerNode(const rclcpp::NodeOptions & options);

private:
  IntrospectionManagerPtr introspection_manager_{nullptr};
  int64_t param_name_{123};
};
}  // namespace introspection_manager

#endif  // INTROSPECTION_MANAGER__INTROSPECTION_MANAGER_NODE_HPP_
