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

#ifndef KAFKA_CDR_TO_JSON__VISIBILITY_CONTROL_HPP_
#define KAFKA_CDR_TO_JSON__VISIBILITY_CONTROL_HPP_

#if defined _WIN32 || defined __CYGWIN__
  #ifdef __GNUC__
    #define KAFKA_CDR_TO_JSON_EXPORT __attribute__ ((dllexport))
    #define KAFKA_CDR_TO_JSON_IMPORT __attribute__ ((dllimport))
  #else
    #define KAFKA_CDR_TO_JSON_EXPORT __declspec(dllexport)
    #define KAFKA_CDR_TO_JSON_IMPORT __declspec(dllimport)
  #endif
  #ifdef KAFKA_CDR_TO_JSON_BUILDING_LIBRARY
    #define KAFKA_CDR_TO_JSON_PUBLIC KAFKA_CDR_TO_JSON_EXPORT
  #else
    #define KAFKA_CDR_TO_JSON_PUBLIC KAFKA_CDR_TO_JSON_IMPORT
  #endif
  #define KAFKA_CDR_TO_JSON_PUBLIC_TYPE KAFKA_CDR_TO_JSON_PUBLIC
  #define KAFKA_CDR_TO_JSON_LOCAL
#else
  #define KAFKA_CDR_TO_JSON_EXPORT __attribute__ ((visibility("default")))
  #define KAFKA_CDR_TO_JSON_IMPORT
  #if __GNUC__ >= 4
    #define KAFKA_CDR_TO_JSON_PUBLIC __attribute__ ((visibility("default")))
    #define KAFKA_CDR_TO_JSON_LOCAL  __attribute__ ((visibility("hidden")))
  #else
    #define KAFKA_CDR_TO_JSON_PUBLIC
    #define KAFKA_CDR_TO_JSON_LOCAL
  #endif
  #define KAFKA_CDR_TO_JSON_PUBLIC_TYPE
#endif

#endif  // KAFKA_CDR_TO_JSON__VISIBILITY_CONTROL_HPP_
