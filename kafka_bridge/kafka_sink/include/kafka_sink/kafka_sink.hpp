// Copyright 2025 Maciej Krupka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef KAFKA_SINK__KAFKA_SINK_HPP_
#define KAFKA_SINK__KAFKA_SINK_HPP_

#include <cstdint>

#include "kafka_sink/visibility_control.hpp"


namespace kafka_sink
{

class KAFKA_SINK_PUBLIC KafkaSink
{
public:
  KafkaSink();
  int64_t foo(int64_t bar) const;
};

}  // namespace kafka_sink

#endif  // KAFKA_SINK__KAFKA_SINK_HPP_
