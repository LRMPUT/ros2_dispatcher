"""Launch the mock Kafka server."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description():
    """Generate launch description for mock Kafka server."""
    return LaunchDescription([
        Node(
            package='kafka_mock_server',
            executable='kafka_mock_server_node.py',
            name='kafka_mock_server',
            output='screen',
            parameters=[{
                'host': '0.0.0.0',
                'port': 9092,
            }]
        ),
    ])
