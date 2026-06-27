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

#ifndef ZENOH_GIS_QUERY__PLOTS_HPP_
#define ZENOH_GIS_QUERY__PLOTS_HPP_

#include <string>
#include <utility>
#include <vector>

#include "nlohmann/json.hpp"
#include "zenoh_gis_query/geometry.hpp"

namespace zenoh_gis_query
{

struct Plot
{
  std::string id;
  std::vector<LatLon> ring;
};

struct Sensor
{
  std::string id;
  LatLon pos;
};

inline std::vector<Plot> load_plots_geojson(const std::string & json_text)
{
  std::vector<Plot> plots;
  auto j = nlohmann::json::parse(json_text, nullptr, false);
  if (j.is_discarded() || !j.contains("features")) {return plots;}
  for (const auto & f : j["features"]) {
    Plot p;
    if (f.contains("properties") && f["properties"].contains("id")) {
      p.id = f["properties"]["id"].get<std::string>();
    }
    const auto & geom = f["geometry"];
    if (!geom.contains("coordinates") || geom["coordinates"].empty()) {continue;}
    for (const auto & pt : geom["coordinates"][0]) {     // outer ring; [lon,lat]
      if (pt.size() >= 2) {p.ring.push_back({pt[1].get<double>(), pt[0].get<double>()});}
    }
    plots.push_back(std::move(p));
  }
  return plots;
}

inline std::vector<Sensor> load_sensors_json(const std::string & json_text)
{
  std::vector<Sensor> out;
  auto j = nlohmann::json::parse(json_text, nullptr, false);
  if (j.is_discarded() || !j.is_array()) {return out;}
  for (const auto & s : j) {
    out.push_back({s.value("id", ""), {s.value("lat", 0.0), s.value("lon", 0.0)}});
  }
  return out;
}

}  // namespace zenoh_gis_query

#endif  // ZENOH_GIS_QUERY__PLOTS_HPP_
