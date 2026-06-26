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

#ifndef ZENOH_GIS_QUERY__QOD_HPP_
#define ZENOH_GIS_QUERY__QOD_HPP_
#include <algorithm>
#include <cstdint>
namespace zenoh_gis_query
{
struct QoD
{
  double completeness{0.0};
  double fidelity{0.0};
  uint64_t contributed{0};
  uint64_t expected{0};
  int64_t max_staleness_ns{0};
};
inline QoD compute_qod(
  uint64_t contributed, uint64_t expected, int64_t max_staleness_ns,
  int64_t fidelity_horizon_ns)
{
  QoD q;
  q.contributed = contributed;
  q.expected = expected;
  q.max_staleness_ns = max_staleness_ns;
  q.completeness = expected == 0 ? 0.0 :
    static_cast<double>(contributed) / static_cast<double>(expected);
  if (fidelity_horizon_ns <= 0) {
    q.fidelity = 0.0;
  } else {
    const double f = 1.0 - static_cast<double>(max_staleness_ns) /
      static_cast<double>(fidelity_horizon_ns);
    q.fidelity = std::clamp(f, 0.0, 1.0);
  }
  return q;
}
}  // namespace zenoh_gis_query
#endif  // ZENOH_GIS_QUERY__QOD_HPP_
