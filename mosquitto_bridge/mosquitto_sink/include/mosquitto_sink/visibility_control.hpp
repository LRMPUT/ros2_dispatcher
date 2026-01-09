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

#ifndef MOSQUITTO_SINK__VISIBILITY_CONTROL_HPP_
#define MOSQUITTO_SINK__VISIBILITY_CONTROL_HPP_

////////////////////////////////////////////////////////////////////////////////
#if defined(__WIN32)
  #if defined(MOSQUITTO_SINK_BUILDING_DLL) || defined(MOSQUITTO_SINK_EXPORTS)
    #define MOSQUITTO_SINK_PUBLIC __declspec(dllexport)
    #define MOSQUITTO_SINK_LOCAL
  #else  // defined(MOSQUITTO_SINK_BUILDING_DLL) || defined(MOSQUITTO_SINK_EXPORTS)
    #define MOSQUITTO_SINK_PUBLIC __declspec(dllimport)
    #define MOSQUITTO_SINK_LOCAL
  #endif  // defined(MOSQUITTO_SINK_BUILDING_DLL) || defined(MOSQUITTO_SINK_EXPORTS)
#elif defined(__linux__)
  #define MOSQUITTO_SINK_PUBLIC __attribute__((visibility("default")))
  #define MOSQUITTO_SINK_LOCAL __attribute__((visibility("hidden")))
#elif defined(__APPLE__)
  #define MOSQUITTO_SINK_PUBLIC __attribute__((visibility("default")))
  #define MOSQUITTO_SINK_LOCAL __attribute__((visibility("hidden")))
#else
  #error "Unsupported Build Configuration"
#endif

#endif  // MOSQUITTO_SINK__VISIBILITY_CONTROL_HPP_
