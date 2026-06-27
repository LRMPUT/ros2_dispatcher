cmake_minimum_required(VERSION 3.14)
project(zenoh_sink)

find_package(ament_cmake_auto REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(nlohmann_json REQUIRED)
# zenoh_cpp_vendor provides the header-only zenoh-cpp wrapper over the zenoh-c
# backend. Its config registers the (nested) install prefix that makes the
# `zenohcxx` package findable, so it MUST be found first. The imported target
# `zenohcxx::zenohc` brings the include dirs, links the zenoh-c runtime, and
# defines ZENOHCXX_ZENOHC so <zenoh.hxx> selects the zenoh-c backend.
find_package(zenoh_cpp_vendor REQUIRED)
find_package(zenohcxx REQUIRED)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME} SHARED
  include/zenoh_sink/zenoh_sink_node.hpp
  include/zenoh_sink/visibility_control.hpp
  src/zenoh_sink_node.cpp
)

target_link_libraries(
  ${PROJECT_NAME}
  yaml-cpp
  nlohmann_json::nlohmann_json
  zenohcxx::zenohc
)

rclcpp_components_register_node(${PROJECT_NAME}
  PLUGIN "zenoh_sink::ZenohSinkNode"
  EXECUTABLE ${PROJECT_NAME}_node_exe
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  ament_add_ros_isolated_gtest(test_zenoh_sink
    test/test_zenoh_sink.cpp
  )
  ament_target_dependencies(test_zenoh_sink ${AMENT_DEPENDENCIES})
  target_link_libraries(test_zenoh_sink ${PROJECT_NAME} yaml-cpp)
endif()

ament_auto_package(USE_SCOPED_HEADER_INSTALL_DIR
  INSTALL_TO_SHARE
  launch
  config
)
