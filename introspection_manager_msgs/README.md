# introspection_manager_msgs

Message and service definitions shared across the `ros2_kafka_dispatcher` stack. These types are used by the introspection manager, dispatcher controller, and downstream clients to exchange topic discovery data and pipeline configurations. For the system architecture, see the top-level [README](../README.md).

## Build

```bash
rosdep install --from-paths src --ignore-src -y
colcon build --symlink-install --cmake-args -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=On --packages-up-to introspection_manager_msgs
```

## Messages
| Name | Fields | Purpose |
| --- | --- | --- |
| `TopicInfo.msg` | `string name`, `string type` | Topic name/type pair discovered in the ROS 2 graph. |
| `TopicsInfo.msg` | `TopicInfo[] topics` | Snapshot of all topics and their message types. |
| `PluginInstance.msg` | `string plugin_type`, `string[] inputs`, `string output`, `string params_yaml` | Describes a processing plugin instance and its configuration. |

## Services
| Name | Request | Response | Purpose |
| --- | --- | --- | --- |
| `GetTopics.srv` | _(empty)_ | `TopicInfo[] topics` | Returns the current topic/type list from the introspection manager. |
| `ApplyPipeline.srv` | `TopicInfo[] subscriptions`, `PluginInstance[] plugins`, `string kafka_mapping_yaml` | `bool success`, `string message` | Ask the controller to apply a specific subscription/plugin pipeline and Kafka mapping. |
| `GetPipelineStatus.srv` | _(empty)_ | `bool success`, `string message` | Reports controller state and last pipeline status. |
| `StopPipeline.srv` | `bool reset_cached` | `bool success`, `string message` | Stop streaming and optionally clear cached selections. |

## Usage in the stack
- `introspection_manager` publishes `TopicsInfo` and serves `GetTopics` for discovery.
- `dispatcher_controller` consumes `TopicsInfo`, responds to `GetPipelineStatus`, and processes `ApplyPipeline`/`StopPipeline` requests as part of Kafka sink orchestration.
- GUI clients and tooling can depend on this package to stay API-compatible with the rest of the system.
