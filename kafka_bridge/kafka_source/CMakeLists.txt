cmake_minimum_required(VERSION 3.14)
project(kafka_source)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(ament_cmake_auto REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka++)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME} SHARED
  include/kafka_source/kafka_source_node.hpp
  include/kafka_source/visibility_control.hpp
  src/kafka_source_node.cpp
)

target_include_directories(${PROJECT_NAME}
  PUBLIC
    ${RDKAFKA_INCLUDE_DIRS}
)

target_link_libraries(${PROJECT_NAME}
  ${RDKAFKA_LIBRARIES}
  Threads::Threads
)

rclcpp_components_register_node(${PROJECT_NAME}
  PLUGIN "kafka_source::KafkaSourceNode"
  EXECUTABLE ${PROJECT_NAME}_node_exe
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()
endif()

ament_auto_package(INSTALL_TO_SHARE
  config
  launch
)
