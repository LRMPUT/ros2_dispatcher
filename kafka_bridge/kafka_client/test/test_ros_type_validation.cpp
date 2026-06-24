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
