# Copyright 2025 Maciej Krupka
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
