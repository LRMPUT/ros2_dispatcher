cmake_minimum_required(VERSION 3.14)
project(dispatcher_controller)

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_lifecycle REQUIRED)
find_package(lifecycle_msgs REQUIRED)
find_package(rcl_interfaces REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(composition_interfaces REQUIRED)
find_package(introspection_manager_msgs REQUIRED)
find_package(rosidl_default_generators REQUIRED)
find_package(yaml-cpp REQUIRED)

set(srv_files
  "srv/ApplySelection.srv"
  "srv/ReloadSelection.srv"
  "srv/StopStreaming.srv"
  "srv/GetStatus.srv"
  "srv/SetSelectionMode.srv"
)

rosidl_generate_interfaces(${PROJECT_NAME}
  ${srv_files}
  DEPENDENCIES builtin_interfaces introspection_manager_msgs
)

ament_export_dependencies(rosidl_default_runtime)

add_library(dispatcher_controller_node SHARED
  src/dispatcher_controller_node.cpp
)

target_include_directories(dispatcher_controller_node
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include/${PROJECT_NAME}>)

ament_target_dependencies(dispatcher_controller_node
  rclcpp
  rclcpp_lifecycle
  lifecycle_msgs
  rcl_interfaces
  rclcpp_components
  composition_interfaces
  introspection_manager_msgs
)

target_link_libraries(dispatcher_controller_node yaml-cpp)

rosidl_get_typesupport_target(cpp_typesupport_target
  ${PROJECT_NAME} rosidl_typesupport_cpp)
target_link_libraries(dispatcher_controller_node "${cpp_typesupport_target}")

rclcpp_components_register_nodes(dispatcher_controller_node "dispatcher_controller::DispatcherControllerNode")

# Create executable
add_executable(dispatcher_controller_node_exe src/main.cpp)
target_link_libraries(dispatcher_controller_node_exe dispatcher_controller_node yaml-cpp)
ament_target_dependencies(dispatcher_controller_node_exe rclcpp rclcpp_components)

install(TARGETS dispatcher_controller_node dispatcher_controller_node_exe
  EXPORT export_dispatcher_controller_node
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION lib/${PROJECT_NAME})

install(DIRECTORY include/
  DESTINATION include/${PROJECT_NAME})

install(DIRECTORY launch
  DESTINATION share/${PROJECT_NAME}/)

ament_export_targets(export_dispatcher_controller_node HAS_LIBRARY_TARGET)

ament_package()
