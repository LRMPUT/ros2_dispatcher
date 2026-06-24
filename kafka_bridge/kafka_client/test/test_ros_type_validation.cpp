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

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "kafka_client/ros_type_validation.hpp"

TEST(RosTypeValidationTest, AcceptsValidInterfaceNames)
{
  EXPECT_TRUE(kafka_client::is_valid_ros_type_name("std_msgs/msg/String"));
  EXPECT_TRUE(kafka_client::is_valid_ros_type_name("example_interfaces/srv/AddTwoInts"));
  EXPECT_TRUE(kafka_client::is_valid_ros_type_name("nav2_msgs/action/NavigateToPose"));
}

TEST(RosTypeValidationTest, RejectsMalformedInterfaceNames)
{
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name(""));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("std_msgs/String"));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("std_msgs/msg/"));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("/msg/String"));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("std-msgs/msg/String"));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("std_msgs/unknown/String"));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("std_msgs/msg/String/Extra"));
}

TEST(RosTypeValidationTest, RejectsSegmentsNotStartingWithLetter)
{
  // Package or type segments may not begin with a digit or underscore.
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("0pkg/msg/String"));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("_priv/msg/String"));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("123/msg/Foo"));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("std_msgs/msg/0String"));
  EXPECT_FALSE(kafka_client::is_valid_ros_type_name("std_msgs/msg/_Hidden"));
}

TEST(RosTypeValidationTest, EnforcesAllowlistWhenPresent)
{
  const std::vector<std::string> allowed_types{
    "std_msgs/msg/String",
    "std_msgs/msg/Int32",
  };

  EXPECT_TRUE(kafka_client::is_allowed_ros_type_name("std_msgs/msg/String", allowed_types));
  EXPECT_FALSE(kafka_client::is_allowed_ros_type_name("std_msgs/msg/Bool", allowed_types));
  EXPECT_FALSE(kafka_client::is_allowed_ros_type_name("bad-type", allowed_types));
}
