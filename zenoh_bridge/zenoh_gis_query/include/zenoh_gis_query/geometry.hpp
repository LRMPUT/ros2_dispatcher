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

#ifndef ZENOH_GIS_QUERY__GEOMETRY_HPP_
#define ZENOH_GIS_QUERY__GEOMETRY_HPP_

#include <algorithm>
#include <cmath>
#include <vector>

namespace zenoh_gis_query
{

struct LatLon
{
  double lat;
  double lon;
};

// Ray-casting point-in-polygon. Polygon is an ordered ring (lat=y, lon=x);
// closing edge implied. Good enough for field-plot scale (planar approx).
inline bool point_in_polygon(const LatLon & p, const std::vector<LatLon> & poly)
{
  bool inside = false;
  const size_t n = poly.size();
  for (size_t i = 0, j = n - 1; i < n; j = i++) {
    const double yi = poly[i].lat, xi = poly[i].lon;
    const double yj = poly[j].lat, xj = poly[j].lon;
    const bool intersect = ((yi > p.lat) != (yj > p.lat)) &&
      (p.lon < (xj - xi) * (p.lat - yi) / (yj - yi) + xi);
    if (intersect) {inside = !inside;}
  }
  return inside;
}

inline double haversine_m(const LatLon & a, const LatLon & b)
{
  constexpr double R = 6371000.0;
  constexpr double kDeg2Rad = M_PI / 180.0;
  const double dlat = (b.lat - a.lat) * kDeg2Rad;
  const double dlon = (b.lon - a.lon) * kDeg2Rad;
  const double s = std::sin(dlat / 2) * std::sin(dlat / 2) +
    std::cos(a.lat * kDeg2Rad) * std::cos(b.lat * kDeg2Rad) *
    std::sin(dlon / 2) * std::sin(dlon / 2);
  return 2 * R * std::asin(std::min(1.0, std::sqrt(s)));
}

}  // namespace zenoh_gis_query

#endif  // ZENOH_GIS_QUERY__GEOMETRY_HPP_
