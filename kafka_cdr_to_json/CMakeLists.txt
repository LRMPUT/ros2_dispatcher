cmake_minimum_required(VERSION 3.14)
project(kafka_cdr_to_json)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(ament_cmake_auto REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka++)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME} SHARED
  include/kafka_cdr_to_json/kafka_cdr_to_json_node.hpp
  include/kafka_cdr_to_json/visibility_control.hpp
  src/kafka_cdr_to_json_node.cpp
)

target_link_libraries(
  ${PROJECT_NAME}
  ${RDKAFKA_LIBRARIES}
  Threads::Threads
  kafka_client::kafka_client
  nlohmann_json::nlohmann_json
)

target_include_directories(${PROJECT_NAME}
  PUBLIC
    ${RDKAFKA_INCLUDE_DIRS}
)

rclcpp_components_register_node(${PROJECT_NAME}
  PLUGIN "kafka_cdr_to_json::KafkaCdrToJsonNode"
  EXECUTABLE ${PROJECT_NAME}_node_exe
)

ament_auto_package(INSTALL_TO_SHARE
  config
  README.md
)
