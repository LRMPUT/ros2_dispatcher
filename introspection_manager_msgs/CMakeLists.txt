cmake_minimum_required(VERSION 3.14)
project(introspection_manager_msgs)

find_package(ament_cmake_auto REQUIRED)
find_package(rosidl_default_generators REQUIRED)

ament_auto_find_build_dependencies()

set(msg_files
  "msg/TopicInfo.msg"
  "msg/TopicsInfo.msg"
  "msg/PluginInstance.msg"
  "srv/GetTopics.srv"
  "srv/ApplyPipeline.srv"
  "srv/StopPipeline.srv"
  "srv/GetPipelineStatus.srv"
)

rosidl_generate_interfaces(${PROJECT_NAME}
  ${msg_files}
  DEPENDENCIES std_msgs
)

ament_export_dependencies(rosidl_default_runtime)
ament_package()
