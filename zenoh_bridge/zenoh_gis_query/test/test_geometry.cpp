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
#include "zenoh_gis_query/geometry.hpp"

using zenoh_gis_query::LatLon;
using zenoh_gis_query::point_in_polygon;
using zenoh_gis_query::haversine_m;

TEST(Geometry, PointInsideSquare) {
  std::vector<LatLon> sq{{0, 0}, {0, 1}, {1, 1}, {1, 0}};
  EXPECT_TRUE(point_in_polygon({0.5, 0.5}, sq));
  EXPECT_FALSE(point_in_polygon({1.5, 0.5}, sq));
  EXPECT_FALSE(point_in_polygon({-0.1, 0.5}, sq));
}

TEST(Geometry, HaversineKnownDistance) {
  // ~111.19 km per degree of latitude at the equator
  double d = haversine_m({0.0, 0.0}, {1.0, 0.0});
  EXPECT_NEAR(d, 111195.0, 500.0);
}

TEST(Geometry, HaversineZero) {
  EXPECT_NEAR(haversine_m({48.5, 3.0}, {48.5, 3.0}), 0.0, 1e-6);
}
