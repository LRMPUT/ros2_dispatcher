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

#include <stdexcept>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "kafka_sink/kafka_sink_node.hpp"

TEST(ParseSubscriptions, ValidYamlParses) {
  const std::string yaml_text =
    R"(
  - topic_name: /foo
    msg_type: std_msgs/msg/String
    kafka_name: foo_json
  - topic_name: /bar
    msg_type: std_msgs/msg/Int32
  )";

  auto result = kafka_sink::parse_subscriptions_yaml(yaml_text);
  ASSERT_EQ(result.size(), 2u);
  EXPECT_EQ(result[0].topic_name, "/foo");
  EXPECT_EQ(result[0].msg_type, "std_msgs/msg/String");
  ASSERT_TRUE(result[0].kafka_name.has_value());
  EXPECT_EQ(*result[0].kafka_name, "foo_json");
  EXPECT_EQ(result[1].topic_name, "/bar");
  EXPECT_EQ(result[1].msg_type, "std_msgs/msg/Int32");
  EXPECT_FALSE(result[1].kafka_name.has_value());
}

TEST(ParseSubscriptions, InvalidYamlThrows) {
  const std::string yaml_text =
    R"(
  - topic_name: ""
    msg_type: std_msgs/msg/String
  )";

  EXPECT_THROW(kafka_sink::parse_subscriptions_yaml(yaml_text), std::runtime_error);
}

TEST(ParseSubscriptions, KafkaKeyIsParsed) {
  const std::string yaml_text =
    R"(
  - topic_name: /robot_1/gnss
    msg_type: sensor_msgs/msg/NavSatFix
    kafka_name: fleet.gnss
    kafka_key: robot_1
  )";

  auto result = kafka_sink::parse_subscriptions_yaml(yaml_text);
  ASSERT_EQ(result.size(), 1u);
  ASSERT_TRUE(result[0].kafka_key.has_value());
  EXPECT_EQ(*result[0].kafka_key, "robot_1");
}

TEST(ParseSubscriptions, KafkaKeyAbsentWhenNotSpecified) {
  const std::string yaml_text =
    R"(
  - topic_name: /robot_1/gnss
    msg_type: sensor_msgs/msg/NavSatFix
    kafka_name: fleet.gnss
  )";

  auto result = kafka_sink::parse_subscriptions_yaml(yaml_text);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_FALSE(result[0].kafka_key.has_value());
}

TEST(ParseSubscriptions, MultipleRobotsShareKafkaTopic) {
  const std::string yaml_text =
    R"(
  - topic_name: /robot_1/gnss
    msg_type: sensor_msgs/msg/NavSatFix
    kafka_name: fleet.gnss
    kafka_key: robot_1
  - topic_name: /robot_2/gnss
    msg_type: sensor_msgs/msg/NavSatFix
    kafka_name: fleet.gnss
    kafka_key: robot_2
  )";

  auto result = kafka_sink::parse_subscriptions_yaml(yaml_text);
  ASSERT_EQ(result.size(), 2u);
  ASSERT_TRUE(result[0].kafka_key.has_value());
  EXPECT_EQ(*result[0].kafka_key, "robot_1");
  ASSERT_TRUE(result[1].kafka_key.has_value());
  EXPECT_EQ(*result[1].kafka_key, "robot_2");
  EXPECT_EQ(*result[0].kafka_name, "fleet.gnss");
  EXPECT_EQ(*result[1].kafka_name, "fleet.gnss");
}
