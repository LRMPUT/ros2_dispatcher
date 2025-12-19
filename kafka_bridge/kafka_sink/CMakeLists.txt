cmake_minimum_required(VERSION 3.14)
project(kafka_sink)

find_package(ament_cmake_auto REQUIRED)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME} SHARED
  include/kafka_sink/kafka_sink.hpp
  include/kafka_sink/visibility_control.hpp
  src/kafka_sink.cpp
)

ament_auto_add_library(${PROJECT_NAME}_node SHARED
  include/kafka_sink/kafka_sink_node.hpp
  src/kafka_sink_node.cpp
)

rclcpp_components_register_node(${PROJECT_NAME}_node
  PLUGIN "kafka_sink::KafkaSinkNode"
  EXECUTABLE ${PROJECT_NAME}_node_exe
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  ament_add_ros_isolated_gtest(test_kafka_sink
    test/test_kafka_sink.cpp
  )
  ament_target_dependencies(test_kafka_sink ${TEST_SOURCES})
  target_link_libraries(test_kafka_sink ${PROJECT_NAME})
endif()

ament_auto_package(INSTALL_TO_SHARE
  launch
  config
)
