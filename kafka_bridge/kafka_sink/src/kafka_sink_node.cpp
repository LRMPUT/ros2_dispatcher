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

#include "kafka_sink/kafka_sink_node.hpp"

namespace kafka_sink
{

KafkaSinkNode::KafkaSinkNode(const rclcpp::NodeOptions & options)
:  Node("kafka_sink", options)
{
  kafka_sink_ = std::make_unique<kafka_sink::KafkaSink>();
  param_name_ = this->declare_parameter("param_name", 456);
  kafka_sink_->foo(param_name_);
}

}  // namespace kafka_sink

#include "rclcpp_components/register_node_macro.hpp"

RCLCPP_COMPONENTS_REGISTER_NODE(kafka_sink::KafkaSinkNode)
