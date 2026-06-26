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

#ifndef ZENOH_GIS_QUERY__SELECTOR_HPP_
#define ZENOH_GIS_QUERY__SELECTOR_HPP_
#include <string>
#include <unordered_map>
namespace zenoh_gis_query
{
inline std::unordered_map<std::string, std::string> parse_selector_params(
  const std::string & key_selector)
{
  std::unordered_map<std::string, std::string> out;
  const auto q = key_selector.find('?');
  if (q == std::string::npos) {return out;}
  std::string rest = key_selector.substr(q + 1);
  size_t pos = 0;
  while (pos < rest.size()) {
    size_t amp = rest.find('&', pos);
    std::string pair = rest.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
    size_t eq = pair.find('=');
    if (eq == std::string::npos) {
      out[pair] = "";
    } else {
      out[pair.substr(0, eq)] = pair.substr(eq + 1);
    }
    if (amp == std::string::npos) {break;}
    pos = amp + 1;
  }
  return out;
}
}  // namespace zenoh_gis_query
#endif  // ZENOH_GIS_QUERY__SELECTOR_HPP_
