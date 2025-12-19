from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import ComposableNode, ComposableNodeContainer
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    selection_mode = LaunchConfiguration("selection_mode")
    selection_file_path = LaunchConfiguration("selection_file_path")
    kafka_sink_node_name = LaunchConfiguration("kafka_sink_node_name")
    validate_topics = LaunchConfiguration("validate_topics")
    subscriptions_yaml = LaunchConfiguration("subscriptions_yaml")
    qos_depth = LaunchConfiguration("qos_depth")
    container_name = LaunchConfiguration("container_name")
    container_namespace = LaunchConfiguration("container_namespace")
    controller_log_level = LaunchConfiguration("controller_log_level")
    kafka_sink_log_level = LaunchConfiguration("kafka_sink_log_level")

    dispatcher_param_file = PathJoinSubstitution(
        [
            FindPackageShare("ros2_kafka_dispatcher_bringup"),
            "config",
            "dispatcher_controller.yaml",
        ]
    )
    kafka_param_file = PathJoinSubstitution(
        [
            FindPackageShare("ros2_kafka_dispatcher_bringup"),
            "config",
            "kafka_sink.yaml",
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
            DeclareLaunchArgument("container_name", default_value="ros2_kafka_dispatcher_container"),
            DeclareLaunchArgument("container_namespace", default_value=""),
            DeclareLaunchArgument("controller_log_level", default_value="debug"),
            DeclareLaunchArgument("kafka_sink_log_level", default_value="info"),
            ComposableNodeContainer(
                name=container_name,
                namespace=container_namespace,
                package="rclcpp_components",
                executable="component_container_mt",
                output="screen",
                composable_node_descriptions=[
                    ComposableNode(
                        package="kafka_sink",
                        plugin="kafka_sink::KafkaSinkNode",
                        name="kafka_sink",
                        extra_arguments=[{"--ros-args": ["--log-level", kafka_sink_log_level]}],
                        parameters=[
                            kafka_param_file,
                            {
                                "subscriptions_yaml": subscriptions_yaml,
                                "qos_depth": qos_depth,
                            },
                        ],
                    ),
                    ComposableNode(
                        package="dispatcher_controller",
                        plugin="dispatcher_controller::DispatcherControllerNode",
                        name="dispatcher_controller",
                        extra_arguments=[{"--ros-args": ["--log-level", controller_log_level]}],
                        parameters=[
                            dispatcher_param_file,
                            {
                                "selection_mode": selection_mode,
                                "selection_file_path": selection_file_path,
                                "kafka_sink_node_name": kafka_sink_node_name,
                                "validate_topics": validate_topics,
                            },
                        ],
                    ),
                ],
            ),
        ]
    )
