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

#ifndef KAFKA_CLIENT__ROS_TYPE_VALIDATION_HPP_
#define KAFKA_CLIENT__ROS_TYPE_VALIDATION_HPP_

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

namespace kafka_client
{

inline bool is_valid_ros_type_name(const std::string & ros_type)
{
  constexpr size_t kMaxRosTypeLength = 255;
  if (ros_type.empty() || ros_type.size() > kMaxRosTypeLength) {
    return false;
  }

  const auto first_slash = ros_type.find('/');
  if (first_slash == std::string::npos || first_slash == 0U) {
    return false;
  }
  const auto second_slash = ros_type.find('/', first_slash + 1U);
  if (second_slash == std::string::npos || second_slash == first_slash + 1U) {
    return false;
  }
  if (ros_type.find('/', second_slash + 1U) != std::string::npos) {
    return false;
  }

  const std::string package_name = ros_type.substr(0U, first_slash);
  const std::string interface_kind =
    ros_type.substr(first_slash + 1U, second_slash - first_slash - 1U);
  const std::string type_name = ros_type.substr(second_slash + 1U);

  if (type_name.empty()) {
    return false;
  }
  if (interface_kind != "msg" && interface_kind != "srv" && interface_kind != "action") {
    return false;
  }

  // ROS package and interface names must start with a letter (not a digit or
  // underscore) and otherwise contain only alphanumerics and underscores.
  auto valid_segment = [](const std::string & segment) {
      if (segment.empty() || std::isalpha(static_cast<unsigned char>(segment.front())) == 0) {
        return false;
      }
      return std::all_of(segment.begin(), segment.end(), [](unsigned char ch) {
        return std::isalnum(static_cast<int>(ch)) || ch == '_';
      });
    };

  return valid_segment(package_name) && valid_segment(type_name);
}

inline bool is_allowed_ros_type_name(
  const std::string & ros_type,
  const std::vector<std::string> & allowed_types)
{
  if (!is_valid_ros_type_name(ros_type)) {
    return false;
  }
  if (allowed_types.empty()) {
    return true;
  }
  return std::find(allowed_types.begin(), allowed_types.end(), ros_type) != allowed_types.end();
}

}  // namespace kafka_client

#endif  // KAFKA_CLIENT__ROS_TYPE_VALIDATION_HPP_
