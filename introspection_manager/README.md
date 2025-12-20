## Overview
`introspection_manager` is a ROS 2 component node that watches the ROS graph, keeps an up-to-date list of topics and their message types, and exposes that data via a service and a latched topic. It is useful for dashboards, monitoring tools, or other nodes that need to discover available data streams at runtime. See the top-level [README](../README.md) for the architecture context.

Key behaviors:
- Tracks the ROS 2 graph and rebuilds a topic → types map whenever the graph changes.
- Optionally filters out hidden topics (name parts starting with `_`).
- Publishes changes on `~/topics_info` and serves the same data on demand via `~/get_topics`.

## Dependencies
Runtime/build: `rclcpp`, `rclcpp_components`, `introspection_manager_msgs`, `rcpputils`, `launch_ros` (see [package.xml](package.xml)). Make sure your workspace has sourced ROS 2 and the custom messages are available.

Install system dependencies and build:
```bash
rosdep install --from-paths src --ignore-src -y
colcon build --symlink-install \
    --cmake-args -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=On \
    --packages-up-to introspection_manager
```

## Running
Launch with the provided parameters file:
```bash
ros2 launch introspection_manager introspection_manager.launch.py \
    introspection_manager_param_file:=/path/to/introspection_manager.param.yaml
```
If `introspection_manager_param_file` is omitted, the default config at `config/introspection_manager.param.yaml` is used.

### Quick checks
- Echo the published topic map:
    ```bash
    ros2 topic echo /introspection_manager/topics_info
    ```
- Query on demand via service:
    ```bash
    ros2 service call /introspection_manager/get_topics \
        introspection_manager_msgs/srv/GetTopics {}
    ```

## Interfaces
### Published topics
| Name | Type | Notes |
| --- | --- | --- |
| `~/topics_info` | `introspection_manager_msgs/msg/TopicsInfo` | Emits the full topic/type list whenever it changes (controlled by `publish_on_change`). |

### Services
| Name | Type | Description |
| --- | --- | --- |
| `~/get_topics` | `introspection_manager_msgs/srv/GetTopics` | Returns the current topic/type list immediately. |

### Parameters (see [config/introspection_manager.param.yaml](config/introspection_manager.param.yaml))
| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `publisher_queue_depth` | int | `1` | Queue depth for `topics_info` publisher. |
| `publisher_reliability` | string | `reliable` | `reliable` or `best_effort`. |
| `publisher_durability` | string | `volatile` | `volatile` or `transient_local`. |
| `publish_on_change` | bool | `true` | Publish `topics_info` whenever the graph changes. |
| `filter_hidden` | bool | `true` | Drop topics containing name segments that start with `_`. |
| `introspection_enabled` | bool | `true` | Enable/disable graph monitoring at runtime. |

## How it works
- `IntrospectionManagerNode` subscribes to graph events and rebuilds an internal `map<string, vector<string>>` of topic → types.
- When enabled, a background thread calls `update_topics()` on graph changes. Changes are published immediately if `publish_on_change` is true.
- The `~/get_topics` service always returns the latest cached snapshot. Filtering is applied before caching when `filter_hidden` is enabled.

## Testing
Run the package tests (includes a small sanity test for the library class):
```bash
colcon test --packages-select introspection_manager
colcon test-result --verbose
```

## Notes
- The node is also built as a component (`introspection_manager_node_exe` entry point) and can be composed into other executables if desired.
