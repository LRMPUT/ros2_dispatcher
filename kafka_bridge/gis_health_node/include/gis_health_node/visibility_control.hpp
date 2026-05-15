// Copyright 2026 Maciej Krupka
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

#ifndef GIS_HEALTH_NODE__VISIBILITY_CONTROL_HPP_
#define GIS_HEALTH_NODE__VISIBILITY_CONTROL_HPP_

#if defined(__WIN32)
  #if defined(GIS_HEALTH_NODE_BUILDING_DLL) || defined(GIS_HEALTH_NODE_EXPORTS)
    #define GIS_HEALTH_NODE_PUBLIC __declspec(dllexport)
    #define GIS_HEALTH_NODE_LOCAL
  #else
    #define GIS_HEALTH_NODE_PUBLIC __declspec(dllimport)
    #define GIS_HEALTH_NODE_LOCAL
  #endif
#elif defined(__linux__)
  #define GIS_HEALTH_NODE_PUBLIC __attribute__((visibility("default")))
  #define GIS_HEALTH_NODE_LOCAL __attribute__((visibility("hidden")))
#elif defined(__APPLE__)
  #define GIS_HEALTH_NODE_PUBLIC __attribute__((visibility("default")))
  #define GIS_HEALTH_NODE_LOCAL __attribute__((visibility("hidden")))
#else
  #error "Unsupported Build Configuration"
#endif

#endif  // GIS_HEALTH_NODE__VISIBILITY_CONTROL_HPP_
