cmake_minimum_required(VERSION 3.14)
project(gis_health_node)

find_package(ament_cmake_auto REQUIRED)
find_package(nlohmann_json REQUIRED)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME} SHARED
  include/gis_health_node/gis_health_node.hpp
  include/gis_health_node/visibility_control.hpp
  src/gis_health_node.cpp
)

target_link_libraries(
  ${PROJECT_NAME}
  kafka_client::kafka_client
  nlohmann_json::nlohmann_json
)

rclcpp_components_register_node(${PROJECT_NAME}
  PLUGIN "gis_health_node::GisHealthNode"
  EXECUTABLE ${PROJECT_NAME}_node_exe
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()
endif()

ament_auto_package(USE_SCOPED_HEADER_INSTALL_DIR
  INSTALL_TO_SHARE
  launch
  config
)
