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
#include "zenoh_gis_query/qod.hpp"
using zenoh_gis_query::compute_qod;

TEST(QoD, CompletenessRatio) {
  auto q = compute_qod(3, 4, 0, 1'000'000'000);
  EXPECT_DOUBLE_EQ(q.completeness, 0.75);
}
TEST(QoD, ExpectedZeroIsZeroCompleteness) {
  EXPECT_DOUBLE_EQ(compute_qod(0, 0, 0, 1).completeness, 0.0);
}
TEST(QoD, FidelityDecaysWithStaleness) {
  auto fresh = compute_qod(1, 1, 0, 1'000'000'000);
  auto stale = compute_qod(1, 1, 500'000'000, 1'000'000'000);
  auto dead = compute_qod(1, 1, 2'000'000'000, 1'000'000'000);
  EXPECT_DOUBLE_EQ(fresh.fidelity, 1.0);
  EXPECT_NEAR(stale.fidelity, 0.5, 1e-9);
  EXPECT_DOUBLE_EQ(dead.fidelity, 0.0);
}
