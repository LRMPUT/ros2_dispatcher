from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    selection_mode = LaunchConfiguration("selection_mode")
    selection_file_path = LaunchConfiguration("selection_file_path")
    kafka_sink_node_name = LaunchConfiguration("kafka_sink_node_name")
    validate_topics = LaunchConfiguration("validate_topics")
    subscriptions_yaml = LaunchConfiguration("subscriptions_yaml")
    qos_depth = LaunchConfiguration("qos_depth")
    controller_log_level = LaunchConfiguration("controller_log_level")
    kafka_sink_log_level = LaunchConfiguration("kafka_sink_log_level")
    introspection_manager_param_file = LaunchConfiguration("introspection_manager_param_file")
    introspection_manager_log_level = LaunchConfiguration("introspection_manager_log_level")

    dispatcher_launch = PathJoinSubstitution(
        [
            FindPackageShare("ros2_kafka_dispatcher_bringup"),
            "launch",
            "dispatcher_controller.launch.py",
        ]
    )
    kafka_sink_launch = PathJoinSubstitution(
        [
            FindPackageShare("ros2_kafka_dispatcher_bringup"),
            "launch",
            "kafka_sink.launch.py",
        ]
    )
    introspection_manager_launch = PathJoinSubstitution(
        [
            FindPackageShare("ros2_kafka_dispatcher_bringup"),
            "launch",
            "introspection_manager.launch.py",
        ]
    )

    return LaunchDescription(
        [
            DeclareLaunchArgument("selection_mode", default_value="file"),
            DeclareLaunchArgument("selection_file_path", default_value=""),
            DeclareLaunchArgument("kafka_sink_node_name", default_value="/kafka_sink"),
            DeclareLaunchArgument("validate_topics", default_value="false"),
            DeclareLaunchArgument("subscriptions_yaml", default_value=""),
            DeclareLaunchArgument("qos_depth", default_value="10"),
            DeclareLaunchArgument("controller_log_level", default_value="debug"),
            DeclareLaunchArgument("kafka_sink_log_level", default_value="info"),
            DeclareLaunchArgument("introspection_manager_param_file", default_value=""),
            DeclareLaunchArgument("introspection_manager_log_level", default_value="DEBUG"),
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(introspection_manager_launch),
                launch_arguments={
                    "introspection_manager_param_file": introspection_manager_param_file,
                    "introspection_manager_log_level": introspection_manager_log_level,
                }.items(),
            ),
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(dispatcher_launch),
                launch_arguments={
                    "selection_mode": selection_mode,
                    "selection_file_path": selection_file_path,
                    "kafka_sink_node_name": kafka_sink_node_name,
                    "validate_topics": validate_topics,
                    "log_level": controller_log_level,
                }.items(),
            ),
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(kafka_sink_launch),
                launch_arguments={
                    "subscriptions_yaml": subscriptions_yaml,
                    "qos_depth": qos_depth,
                    "use_composition": "false",
                    "log_level": kafka_sink_log_level,
                }.items(),
            ),
        ]
    )
