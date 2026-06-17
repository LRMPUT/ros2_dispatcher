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

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    selection_mode = LaunchConfiguration("selection_mode")
    selection_file_path = LaunchConfiguration("selection_file_path")
    kafka_sink_node_name = LaunchConfiguration("kafka_sink_node_name")
    mosquitto_sink_node_name = LaunchConfiguration("mosquitto_sink_node_name")
    validate_topics = LaunchConfiguration("validate_topics")
    param_file = LaunchConfiguration("param_file")
    log_level = LaunchConfiguration("log_level")

    return LaunchDescription(
        [
            DeclareLaunchArgument("selection_mode", default_value="file"),
            DeclareLaunchArgument("selection_file_path", default_value=""),
            DeclareLaunchArgument("kafka_sink_node_name", default_value="/kafka_sink"),
            DeclareLaunchArgument("mosquitto_sink_node_name", default_value="/mosquitto_sink"),
            DeclareLaunchArgument("validate_topics", default_value="false"),
            DeclareLaunchArgument("log_level", default_value="info"),
            DeclareLaunchArgument(
                "param_file",
                default_value=PathJoinSubstitution(
                    [
                        FindPackageShare("ros2_kafka_dispatcher_bringup"),
                        "config",
                        "dispatcher_controller.yaml",
                    ]
                ),
            ),
            Node(
                package="dispatcher_controller",
                executable="dispatcher_controller_node_exe",
                name="dispatcher_controller",
                output="screen",
                arguments=["--ros-args", "--log-level", log_level],
                parameters=[
                    param_file,
                    {
                        "selection_mode": selection_mode,
                        "selection_file_path": selection_file_path,
                        "kafka_sink_node_name": kafka_sink_node_name,
                        "mosquitto_sink_node_name": mosquitto_sink_node_name,
                        "validate_topics": validate_topics,
                    },
                ],
            ),
        ]
    )
