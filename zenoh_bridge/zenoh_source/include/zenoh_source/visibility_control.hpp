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

#ifndef ZENOH_SOURCE__VISIBILITY_CONTROL_HPP_
#define ZENOH_SOURCE__VISIBILITY_CONTROL_HPP_

////////////////////////////////////////////////////////////////////////////////
#if defined(__WIN32)
  #if defined(ZENOH_SOURCE_BUILDING_DLL) || defined(ZENOH_SOURCE_EXPORTS)
    #define ZENOH_SOURCE_PUBLIC __declspec(dllexport)
    #define ZENOH_SOURCE_LOCAL
  #else  // defined(ZENOH_SOURCE_BUILDING_DLL) || defined(ZENOH_SOURCE_EXPORTS)
    #define ZENOH_SOURCE_PUBLIC __declspec(dllimport)
    #define ZENOH_SOURCE_LOCAL
  #endif  // defined(ZENOH_SOURCE_BUILDING_DLL) || defined(ZENOH_SOURCE_EXPORTS)
#elif defined(__linux__)
  #define ZENOH_SOURCE_PUBLIC __attribute__((visibility("default")))
  #define ZENOH_SOURCE_LOCAL __attribute__((visibility("hidden")))
#elif defined(__APPLE__)
  #define ZENOH_SOURCE_PUBLIC __attribute__((visibility("default")))
  #define ZENOH_SOURCE_LOCAL __attribute__((visibility("hidden")))
#else
  #error "Unsupported Build Configuration"
#endif

#endif  // ZENOH_SOURCE__VISIBILITY_CONTROL_HPP_
