cmake_minimum_required(VERSION 3.14)
project(zenoh_gis_query)

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
  include/zenoh_gis_query/zenoh_gis_query_node.hpp
  include/zenoh_gis_query/visibility_control.hpp
  src/zenoh_gis_query_node.cpp)
target_link_libraries(${PROJECT_NAME} nlohmann_json::nlohmann_json zenohcxx::zenohc)
rclcpp_components_register_node(${PROJECT_NAME}
  PLUGIN "zenoh_gis_query::ZenohGisQueryNode" EXECUTABLE ${PROJECT_NAME}_node_exe)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()
  ament_add_ros_isolated_gtest(test_geometry test/test_geometry.cpp)
  ament_target_dependencies(test_geometry ${AMENT_DEPENDENCIES})
  target_link_libraries(test_geometry ${PROJECT_NAME} nlohmann_json::nlohmann_json)
  ament_add_ros_isolated_gtest(test_selector test/test_selector.cpp)
  ament_target_dependencies(test_selector ${AMENT_DEPENDENCIES})
  target_link_libraries(test_selector ${PROJECT_NAME} nlohmann_json::nlohmann_json)
  ament_add_ros_isolated_gtest(test_qod test/test_qod.cpp)
  ament_target_dependencies(test_qod ${AMENT_DEPENDENCIES})
  target_link_libraries(test_qod ${PROJECT_NAME} nlohmann_json::nlohmann_json)
  ament_add_ros_isolated_gtest(test_navsatfix_decode test/test_navsatfix_decode.cpp)
  ament_target_dependencies(test_navsatfix_decode ${AMENT_DEPENDENCIES})
  target_link_libraries(test_navsatfix_decode ${PROJECT_NAME} nlohmann_json::nlohmann_json)
  ament_add_ros_isolated_gtest(test_plots test/test_plots.cpp)
  ament_target_dependencies(test_plots ${AMENT_DEPENDENCIES})
  target_link_libraries(test_plots ${PROJECT_NAME} nlohmann_json::nlohmann_json)
endif()

ament_auto_package(USE_SCOPED_HEADER_INSTALL_DIR INSTALL_TO_SHARE launch config)
