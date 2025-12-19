// Copyright 2025
// SPDX-License-Identifier: Apache-2.0

#ifndef KAFKA_CLIENT__VISIBILITY_CONTROL_HPP_
#define KAFKA_CLIENT__VISIBILITY_CONTROL_HPP_

#if defined _WIN32 || defined __CYGWIN__
  #ifdef __GNUC__
    #define KAFKA_CLIENT_EXPORT __attribute__ ((dllexport))
    #define KAFKA_CLIENT_IMPORT __attribute__ ((dllimport))
  #else
    #define KAFKA_CLIENT_EXPORT __declspec(dllexport)
    #define KAFKA_CLIENT_IMPORT __declspec(dllimport)
  #endif
  #ifdef KAFKA_CLIENT_BUILDING_LIBRARY
    #define KAFKA_CLIENT_PUBLIC KAFKA_CLIENT_EXPORT
  #else
    #define KAFKA_CLIENT_PUBLIC KAFKA_CLIENT_IMPORT
  #endif
  #define KAFKA_CLIENT_LOCAL
#else
  #define KAFKA_CLIENT_EXPORT __attribute__ ((visibility("default")))
  #define KAFKA_CLIENT_IMPORT
  #if __GNUC__ >= 4
    #define KAFKA_CLIENT_PUBLIC __attribute__ ((visibility("default")))
    #define KAFKA_CLIENT_LOCAL  __attribute__ ((visibility("hidden")))
  #else
    #define KAFKA_CLIENT_PUBLIC
    #define KAFKA_CLIENT_LOCAL
  #endif
#endif

#endif  // KAFKA_CLIENT__VISIBILITY_CONTROL_HPP_
