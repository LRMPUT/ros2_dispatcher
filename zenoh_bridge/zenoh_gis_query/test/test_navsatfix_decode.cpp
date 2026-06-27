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

#include <vector>
#include "gtest/gtest.h"
#include "rclcpp/serialization.hpp"
#include "rclcpp/serialized_message.hpp"
#include "sensor_msgs/msg/nav_sat_fix.hpp"
#include "zenoh_gis_query/navsatfix_decode.hpp"

TEST(NavSatFixDecode, RoundTrip) {
  sensor_msgs::msg::NavSatFix msg;
  msg.header.frame_id = "robot_3";
  msg.header.stamp.sec = 5;
  msg.header.stamp.nanosec = 250'000'000;
  msg.latitude = 48.5; msg.longitude = 3.2; msg.altitude = 120.0;
  rclcpp::Serialization<sensor_msgs::msg::NavSatFix> ser;
  rclcpp::SerializedMessage sm;
  ser.serialize_message(&msg, &sm);
  const auto & r = sm.get_rcl_serialized_message();
  std::vector<uint8_t> cdr(r.buffer, r.buffer + r.buffer_length);

  auto s = zenoh_gis_query::decode_navsatfix(cdr);
  ASSERT_TRUE(s.valid);
  EXPECT_DOUBLE_EQ(s.lat, 48.5);
  EXPECT_DOUBLE_EQ(s.lon, 3.2);
  EXPECT_EQ(s.frame_id, "robot_3");
  EXPECT_EQ(s.stamp_ns, 5LL * 1'000'000'000LL + 250'000'000LL);
}
TEST(NavSatFixDecode, EmptyIsInvalid) {
  EXPECT_FALSE(zenoh_gis_query::decode_navsatfix({}).valid);
}
