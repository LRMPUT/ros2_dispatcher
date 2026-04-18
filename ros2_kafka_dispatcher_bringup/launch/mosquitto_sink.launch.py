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
from launch.actions import DeclareLaunchArgument, GroupAction
from launch.conditions import IfCondition, UnlessCondition
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import ComposableNodeContainer, LoadComposableNodes, Node
from launch_ros.descriptions import ComposableNode
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    use_composition = LaunchConfiguration("use_composition")
    container_name = LaunchConfiguration("container_name")
    container_namespace = LaunchConfiguration("container_namespace")
    subscriptions_yaml = LaunchConfiguration("subscriptions_yaml")
    qos_depth = LaunchConfiguration("qos_depth")
    param_file = LaunchConfiguration("param_file")
    log_level = LaunchConfiguration("log_level")

    arguments = [
        DeclareLaunchArgument("use_composition", default_value="false"),
        DeclareLaunchArgument("container_name", default_value="mosquitto_sink_container"),
        DeclareLaunchArgument("container_namespace", default_value=""),
        DeclareLaunchArgument("subscriptions_yaml", default_value=""),
        DeclareLaunchArgument("qos_depth", default_value="10"),
        DeclareLaunchArgument("log_level", default_value="info"),
        DeclareLaunchArgument(
            "param_file",
            default_value=PathJoinSubstitution(
                [
                    FindPackageShare("ros2_kafka_dispatcher_bringup"),
                    "config",
                    "mosquitto_sink.yaml",
                ]
            ),
        ),
    ]

    mosquitto_sink_parameters = [
        param_file,
        {"subscriptions_yaml": subscriptions_yaml, "qos_depth": qos_depth},
    ]

    composable_container = GroupAction(
        condition=IfCondition(use_composition),
        actions=[
            ComposableNodeContainer(
                name=container_name,
                namespace=container_namespace,
                package="rclcpp_components",
                executable="component_container_mt",
                output="screen",
            ),
            LoadComposableNodes(
                target_container=container_name,
                composable_node_descriptions=[
                    ComposableNode(
                        package="mosquitto_sink",
                        plugin="mosquitto_sink::MosquittoSinkNode",
                        name="mosquitto_sink",
                        extra_arguments=[{"--ros-args": ["--log-level", log_level]}],
                        parameters=mosquitto_sink_parameters,
                    ),
                ],
            )
        ],
    )

    standalone_node = Node(
        condition=UnlessCondition(use_composition),
        package="mosquitto_sink",
        executable="mosquitto_sink_node_exe",
        name="mosquitto_sink",
        output="screen",
        arguments=["--ros-args", "--log-level", log_level],
        parameters=mosquitto_sink_parameters,
    )

    return LaunchDescription(arguments + [composable_container, standalone_node])
