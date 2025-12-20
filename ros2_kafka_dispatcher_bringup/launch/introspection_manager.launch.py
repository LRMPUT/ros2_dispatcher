from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def launch_setup(context, *args, **kwargs):
    param_file = LaunchConfiguration("introspection_manager_param_file").perform(context)
    log_level = LaunchConfiguration("introspection_manager_log_level").perform(context)

    if not param_file:
        param_file = PathJoinSubstitution(
            [
                FindPackageShare("ros2_kafka_dispatcher_bringup"),
                "config",
                "introspection_manager.param.yaml",
            ]
        ).perform(context)

    introspection_node = Node(
        package="introspection_manager",
        executable="introspection_manager_node_exe",
        name="introspection_manager_node",
        parameters=[param_file],
        output="screen",
        arguments=["--ros-args", "--log-level", log_level, "--enable-stdout-logs"],
    )

    return [introspection_node]


def generate_launch_description():
    return LaunchDescription(
        [
            DeclareLaunchArgument("introspection_manager_param_file", default_value=""),
            DeclareLaunchArgument("introspection_manager_log_level", default_value="DEBUG"),
            OpaqueFunction(function=launch_setup),
        ]
    )
