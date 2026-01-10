cmake_minimum_required(VERSION 3.14)
project(mosquitto_sink)

find_package(ament_cmake_auto REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(PahoMqttCpp REQUIRED)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME} SHARED
  include/mosquitto_sink/mosquitto_sink_node.hpp
  include/mosquitto_sink/visibility_control.hpp
  src/mosquitto_sink_node.cpp
)

target_link_libraries(
  ${PROJECT_NAME}
  yaml-cpp
  nlohmann_json::nlohmann_json
  PahoMqttCpp::paho-mqttpp3
)

rclcpp_components_register_node(${PROJECT_NAME}
  PLUGIN "mosquitto_sink::MosquittoSinkNode"
  EXECUTABLE ${PROJECT_NAME}_node_exe
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  ament_add_ros_isolated_gtest(test_mosquitto_sink
    test/test_mosquitto_sink.cpp
  )
  ament_target_dependencies(test_mosquitto_sink ${AMENT_DEPENDENCIES})
  target_link_libraries(test_mosquitto_sink ${PROJECT_NAME} yaml-cpp)
endif()

ament_auto_package(INSTALL_TO_SHARE
  launch
  config
)
