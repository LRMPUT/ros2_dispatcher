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

#ifndef ZENOH_GIS_QUERY__NAVSATFIX_DECODE_HPP_
#define ZENOH_GIS_QUERY__NAVSATFIX_DECODE_HPP_
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include "rclcpp/serialization.hpp"
#include "rclcpp/serialized_message.hpp"
#include "sensor_msgs/msg/nav_sat_fix.hpp"
namespace zenoh_gis_query
{
struct FixSample
{
  double lat{0}; double lon{0}; double alt{0};
  int64_t stamp_ns{0}; std::string frame_id; bool valid{false};
};
inline FixSample decode_navsatfix(const std::vector<uint8_t> & cdr)
{
  FixSample s;
  if (cdr.empty()) {return s;}
  rclcpp::SerializedMessage sm(cdr.size());
  auto & r = sm.get_rcl_serialized_message();
  std::memcpy(r.buffer, cdr.data(), cdr.size());
  r.buffer_length = cdr.size();
  sensor_msgs::msg::NavSatFix msg;
  try {
    rclcpp::Serialization<sensor_msgs::msg::NavSatFix> ser;
    ser.deserialize_message(&sm, &msg);
  } catch (...) {
    return s;
  }
  s.lat = msg.latitude; s.lon = msg.longitude; s.alt = msg.altitude;
  s.frame_id = msg.header.frame_id;
  s.stamp_ns = static_cast<int64_t>(msg.header.stamp.sec) * 1'000'000'000LL +
    msg.header.stamp.nanosec;
  s.valid = true;
  return s;
}
}  // namespace zenoh_gis_query
#endif  // ZENOH_GIS_QUERY__NAVSATFIX_DECODE_HPP_
