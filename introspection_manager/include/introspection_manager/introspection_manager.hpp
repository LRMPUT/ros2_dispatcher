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

#ifndef INTROSPECTION_MANAGER__INTROSPECTION_MANAGER_HPP_
#define INTROSPECTION_MANAGER__INTROSPECTION_MANAGER_HPP_

#include <cstdint>

#include "introspection_manager/visibility_control.hpp"


namespace introspection_manager
{

class INTROSPECTION_MANAGER_PUBLIC IntrospectionManager
{
public:
  IntrospectionManager();
  int64_t foo(int64_t bar) const;
};

}  // namespace introspection_manager

#endif  // INTROSPECTION_MANAGER__INTROSPECTION_MANAGER_HPP_
