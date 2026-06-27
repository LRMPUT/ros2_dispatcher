cmake_minimum_required(VERSION 3.14)
project(zenoh_source)

find_package(ament_cmake_auto REQUIRED)
find_package(nlohmann_json REQUIRED)
# zenoh_cpp_vendor must be found first: its config registers the (nested) install
# prefix that makes the `zenohcxx` package findable. The imported target
# `zenohcxx::zenohc` brings the include dirs, links the zenoh-c runtime, and
# defines ZENOHCXX_ZENOHC so <zenoh.hxx> selects the zenoh-c backend.
find_package(zenoh_cpp_vendor REQUIRED)
find_package(zenohcxx REQUIRED)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME} SHARED
  include/zenoh_source/zenoh_source_node.hpp
  include/zenoh_source/ros_type_validation.hpp
  include/zenoh_source/visibility_control.hpp
  src/zenoh_source_node.cpp
)

target_link_libraries(
  ${PROJECT_NAME}
  nlohmann_json::nlohmann_json
  zenohcxx::zenohc
)

rclcpp_components_register_node(${PROJECT_NAME}
  PLUGIN "zenoh_source::ZenohSourceNode"
  EXECUTABLE ${PROJECT_NAME}_node_exe
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  ament_add_ros_isolated_gtest(test_zenoh_source
    test/test_zenoh_source.cpp
  )
  ament_target_dependencies(test_zenoh_source ${AMENT_DEPENDENCIES})
  target_link_libraries(test_zenoh_source ${PROJECT_NAME})
endif()

ament_auto_package(USE_SCOPED_HEADER_INSTALL_DIR
  INSTALL_TO_SHARE
  launch
  config
)
