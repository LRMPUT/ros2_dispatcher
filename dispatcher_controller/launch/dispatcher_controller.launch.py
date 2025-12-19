from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription([
        Node(
            package='dispatcher_controller',
            executable='dispatcher_controller_node',
            name='dispatcher_controller',
            output='screen',
            parameters=[{
                'kafka_sink_node_name': '/kafka_sink',
                'validate_topics': False,
                'introspection_service_name': '/introspection_manager/get_topics',
            }],
        )
    ])
