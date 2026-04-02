# Design Spec: ros2_kafka_dispatcher Node Description Document

## Purpose

A standalone reference document for robotics researchers and ROS 2 engineers (autonomous/agricultural robotics). Serves as background for the CEUR 2026 paper "From Architecture to Implementation: Robotic Data Pipelines for Digital Agriculture GIS". Fills the gap in the article: the ROS 2 layer is used but not described.

Technically precise, concise, implementation-oriented. Audience is familiar with ROS 2 topics, QoS, message schemas, and distributed robotic systems.

## File target

`docs/node_description.md`

## Narrative framing

The document should position `ros2_kafka_dispatcher` as the **IoRT data source layer** component of the GIS4IoRT architecture. Field robots run ROS 2 and produce telemetry streams (`nav_msgs/Odometry`, `sensor_msgs/NavSatFix`, etc.). This system bridges those streams into the Kafka/MQTT messaging backbone consumed by the three processing frameworks (ksqlDB, Flink, NebulaStream) described in the article.

## Structure

### 1. Overview
- What the system is: composable ROS 2 pipeline bridging topics to Apache Kafka and MQTT
- Its role in GIS4IoRT: IoRT data source layer, decouples robot-side middleware from the cloud processing tier
- Key design point: generic via `rosidl` type introspection — no hardcoded message types
- Cross-link to architecture.md for topology diagram

### 2. Runtime Configuration
- Three selection modes: `file` (YAML), `gui` (service call), `all` (auto-discovery)
- YAML format with example using agricultural robot topics
- `auto_apply_on_mode_change`, allowlist/denylist, `all_mode_max_topics`
- Cross-link to configuration_reference.md for full parameter list

### 3. Topic Mapping & Multi-Sensor Compatibility
- Generic subscriptions via `rosidl` introspection — any ROS 2 message type
- Explicit examples: `nav_msgs/NavSatFix`, `sensor_msgs/Imu`, `sensor_msgs/PointCloud2`, `nav_msgs/Odometry`
- `topic_tools` plugin integration: ThrottleNode, DropNode, DelayNode, RelayNode, RelayFieldNode
- One-input/one-output contract; composable node loading via `LoadNode`
- Kafka topic naming: `prefix_ros_topic` vs `fixed`; per-topic `kafka_name` override
- Cross-link to architecture.md for supported/unsupported node table

### 4. Schema Transformation
- CDR (default) and JSON serialization via `rosidl` type introspection; no schema registry needed
- `kafka_cdr_to_json` for offline CDR → JSON conversion
- Kafka record headers: `ros_type` (only; other fields are in telemetry/metrics logs, not headers)
- RelayFieldNode for field extraction (payload reduction on bandwidth-constrained links)

### 5. QoS-Aware Forwarding
- ROS 2 subscription: `qos_depth`, `publisher_reliability`, `publisher_durability`
- Kafka: `kafka.acks`, `kafka.max_queue_messages`, `kafka.drop_when_full`, batching params
- MQTT: QoS levels 0/1/2, LWT, TLS/SSL
- Cross-link to configuration_reference.md for full tables

### 6. Dynamic Reconfiguration
- `apply_selection` and `reload_selection` services
- Apply cycle: deactivate → update subscriptions_yaml → reload plugins → configure → activate
- Controller phases: `IDLE`, `BUSY`, `ERROR`
- Connect to GIS4IoRT requirement: dynamic device attachment / intermittent connectivity handling
- Cross-link to api_reference.md for service definitions

### 7. Component Summary Table
- One-line role per node: introspection_manager, dispatcher_controller, kafka_sink, mosquitto_sink, kafka_source, kafka_cdr_to_json, kafka_client

## Constraints
- Do not duplicate full parameter tables — reference configuration_reference.md
- Do not duplicate component topology — reference architecture.md
- Do not reproduce GIS4IoRT architecture description — just position the system within it
- Use YAML examples aligned with agricultural robot topics from the article
