cmake_minimum_required(VERSION 3.14)
project(introspection_manager)

find_package(ament_cmake_auto REQUIRED)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME} SHARED
  include/introspection_manager/introspection_manager.hpp
  include/introspection_manager/visibility_control.hpp
  src/introspection_manager.cpp
)

ament_auto_add_library(${PROJECT_NAME}_node SHARED
  include/introspection_manager/introspection_manager_node.hpp
  src/introspection_manager_node.cpp
)

rclcpp_components_register_node(${PROJECT_NAME}_node
  PLUGIN "introspection_manager::IntrospectionManagerNode"
  EXECUTABLE ${PROJECT_NAME}_node_exe
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  ament_add_ros_isolated_gtest(test_introspection_manager
    test/test_introspection_manager.cpp
  )
  ament_target_dependencies(test_introspection_manager ${TEST_SOURCES})
  target_link_libraries(test_introspection_manager ${PROJECT_NAME})
endif()

ament_auto_package(USE_SCOPED_HEADER_INSTALL_DIR
  INSTALL_TO_SHARE
  launch
  config
)
