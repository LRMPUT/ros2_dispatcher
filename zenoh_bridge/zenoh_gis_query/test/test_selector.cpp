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
#include "zenoh_gis_query/selector.hpp"
using zenoh_gis_query::parse_selector_params;

TEST(Selector, ParsesParams) {
  auto m = parse_selector_params("gis/query/geofence?plot=P12&window=5s");
  EXPECT_EQ(m["plot"], "P12");
  EXPECT_EQ(m["window"], "5s");
}
TEST(Selector, NoParamsEmpty) {
  EXPECT_TRUE(parse_selector_params("gis/query/collision").empty());
}
TEST(Selector, MissingValueIsEmptyString) {
  auto m = parse_selector_params("k?flag&x=1");
  EXPECT_EQ(m.count("flag"), 1u);
  EXPECT_EQ(m["flag"], "");
  EXPECT_EQ(m["x"], "1");
}
