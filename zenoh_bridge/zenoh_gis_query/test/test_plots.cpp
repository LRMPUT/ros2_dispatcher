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

#include "gtest/gtest.h"
#include "zenoh_gis_query/plots.hpp"

using zenoh_gis_query::load_plots_geojson;
using zenoh_gis_query::load_sensors_json;

TEST(Plots, ParsesPolygonFeature) {
  const char * gj =
    R"({"type":"FeatureCollection","features":[
    {"type":"Feature","properties":{"id":"P12"},
     "geometry":{"type":"Polygon","coordinates":[[[3.0,48.0],[3.0,48.1],[3.1,48.1],[3.1,48.0],[3.0,48.0]]]}}]})";
  auto plots = load_plots_geojson(gj);
  ASSERT_EQ(plots.size(), 1u);
  EXPECT_EQ(plots[0].id, "P12");
  ASSERT_GE(plots[0].ring.size(), 4u);
  EXPECT_DOUBLE_EQ(plots[0].ring[0].lon, 3.0);   // GeoJSON is [lon,lat]
  EXPECT_DOUBLE_EQ(plots[0].ring[0].lat, 48.0);
}

TEST(Sensors, ParsesArray) {
  const char * js = R"([{"id":"S3","lat":48.05,"lon":3.05}])";
  auto s = load_sensors_json(js);
  ASSERT_EQ(s.size(), 1u);
  EXPECT_EQ(s[0].id, "S3");
  EXPECT_DOUBLE_EQ(s[0].pos.lat, 48.05);
}
