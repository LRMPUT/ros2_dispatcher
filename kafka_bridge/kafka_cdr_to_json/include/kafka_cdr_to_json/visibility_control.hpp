// Copyright 2025 Maciej Krupka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef KAFKA_CDR_TO_JSON__VISIBILITY_CONTROL_HPP_
#define KAFKA_CDR_TO_JSON__VISIBILITY_CONTROL_HPP_

////////////////////////////////////////////////////////////////////////////////
#if defined(__WIN32)
  #if defined(KAFKA_CDR_TO_JSON_BUILDING_DLL) || defined(KAFKA_CDR_TO_JSON_EXPORTS)
    #define KAFKA_CDR_TO_JSON_PUBLIC __declspec(dllexport)
    #define KAFKA_CDR_TO_JSON_LOCAL
  #else
    #define KAFKA_CDR_TO_JSON_PUBLIC __declspec(dllimport)
    #define KAFKA_CDR_TO_JSON_LOCAL
  #endif
#elif defined(__linux__)
  #define KAFKA_CDR_TO_JSON_PUBLIC __attribute__((visibility("default")))
  #define KAFKA_CDR_TO_JSON_LOCAL __attribute__((visibility("hidden")))
#elif defined(__APPLE__)
  #define KAFKA_CDR_TO_JSON_PUBLIC __attribute__((visibility("default")))
  #define KAFKA_CDR_TO_JSON_LOCAL __attribute__((visibility("hidden")))
#else
  #error "Unsupported Build Configuration"
#endif

#endif  // KAFKA_CDR_TO_JSON__VISIBILITY_CONTROL_HPP_
