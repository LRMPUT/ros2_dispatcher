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
#include "zenoh_source/zenoh_source_node.hpp"
#include "zenoh_source/ros_type_validation.hpp"

TEST(DeriveRosTopic, StripsKeyPrefixAndPrependsOutputPrefix) {
  EXPECT_EQ(
    zenoh_source::derive_ros_topic("ros2/robot/odom", "ros2", "/zenoh_decoded"),
    "/zenoh_decoded/robot/odom");
}

TEST(DeriveRosTopic, ExactPrefixMatchYieldsBarePrefix) {
  EXPECT_EQ(zenoh_source::derive_ros_topic("ros2", "ros2", "/zenoh_decoded"), "/zenoh_decoded");
}

TEST(DeriveRosTopic, NonMatchingPrefixIsKept) {
  EXPECT_EQ(
    zenoh_source::derive_ros_topic("other/topic", "ros2", "/zenoh_decoded"),
    "/zenoh_decoded/other/topic");
}

TEST(DeriveRosTopic, EmptyKeyPrefixKeepsWholeKey) {
  EXPECT_EQ(
    zenoh_source::derive_ros_topic("ros2/a/b", "", "/zenoh_decoded"),
    "/zenoh_decoded/ros2/a/b");
}

TEST(DeriveRosTopic, EmptyOutputPrefixYieldsAbsoluteTopic) {
  EXPECT_EQ(zenoh_source::derive_ros_topic("ros2/a/b", "ros2", ""), "/a/b");
}

TEST(DeriveRosTopic, TrailingSlashOnPrefixesNormalised) {
  EXPECT_EQ(
    zenoh_source::derive_ros_topic("ros2/a/b", "ros2/", "/zenoh_decoded/"),
    "/zenoh_decoded/a/b");
}

TEST(RosTypeValidation, AcceptsWellFormedType) {
  EXPECT_TRUE(zenoh_source::is_valid_ros_type_name("std_msgs/msg/String"));
  EXPECT_TRUE(zenoh_source::is_valid_ros_type_name("nav_msgs/msg/Odometry"));
}

TEST(RosTypeValidation, RejectsMalformedType) {
  EXPECT_FALSE(zenoh_source::is_valid_ros_type_name(""));
  EXPECT_FALSE(zenoh_source::is_valid_ros_type_name("std_msgs/String"));
  EXPECT_FALSE(zenoh_source::is_valid_ros_type_name("/msg/String"));
  EXPECT_FALSE(zenoh_source::is_valid_ros_type_name("std_msgs/msg/"));
  EXPECT_FALSE(zenoh_source::is_valid_ros_type_name("std_msgs/bad/String"));
  EXPECT_FALSE(zenoh_source::is_valid_ros_type_name("../../etc/passwd"));
}

TEST(RosTypeValidation, AllowlistEmptyAllowsAnyValid) {
  EXPECT_TRUE(zenoh_source::is_allowed_ros_type_name("std_msgs/msg/String", {}));
  EXPECT_FALSE(zenoh_source::is_allowed_ros_type_name("bad type", {}));
}

TEST(RosTypeValidation, AllowlistRestrictsToListedTypes) {
  std::vector<std::string> allowed{"std_msgs/msg/String"};
  EXPECT_TRUE(zenoh_source::is_allowed_ros_type_name("std_msgs/msg/String", allowed));
  EXPECT_FALSE(zenoh_source::is_allowed_ros_type_name("std_msgs/msg/Int32", allowed));
}
