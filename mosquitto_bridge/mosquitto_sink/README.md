# mosquitto_sink

`mosquitto_sink` is a lifecycle-based ROS 2 node that streams configured ROS 2 topics to MQTT via
Mosquitto using the Eclipse Paho C++ client. It mirrors the behavior of `kafka_sink` while
supporting MQTT-specific features like QoS, retain flags, and broker reconnection.

## Build

Install the Paho MQTT C++ client (Ubuntu example):

```bash
sudo apt-get install libpaho-mqttpp3-dev libpaho-mqtt3as1
```

Then build the package:

```bash
colcon build --symlink-install --cmake-args -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=On --packages-up-to mosquitto_sink
```

## Run

Launch the component container (uses `config/mosquitto_sink.param.yaml` by default):

```bash
ros2 launch mosquitto_sink mosquitto_sink_container.launch.py
```

Lifecycle control examples:

```bash
ros2 lifecycle set /mosquitto_sink configure
ros2 lifecycle set /mosquitto_sink activate
ros2 lifecycle set /mosquitto_sink deactivate
```

Set subscriptions dynamically (requires the node to be inactive):

```bash
ros2 param set /mosquitto_sink subscriptions_yaml "[{\"topic_name\": \"/foo\", \"msg_type\": \"std_msgs/msg/String\"}]"
```

## Configuration

The default parameters are in `config/mosquitto_sink.param.yaml`. Key parameters:

- `subscriptions_yaml`: YAML list of ROS 2 topics and message types to stream.
- `qos_depth`: ROS 2 subscription QoS depth.
- `metrics.*`: Enable and configure metrics publishing.
- `mqtt.*`: MQTT connection and publish settings.

Example snippet:

```yaml
subscriptions_yaml: |
  - topic_name: /ros/topic
    msg_type: std_msgs/msg/String
    mqtt_name: custom/mqtt/topic
mqtt.broker_host: "localhost"
mqtt.broker_port: 1883
mqtt.client_id: "mosquitto_sink"
mqtt.qos: 1
mqtt.retain: false
mqtt.topic_prefix: "ros2"
mqtt.topic_mapping_mode: "prefix_ros_topic"
mqtt.payload_format: "cdr"
```

### MQTT topic mapping

- `prefix_ros_topic`: Removes the leading `/` from a ROS topic and prefixes it with
  `mqtt.topic_prefix`. Example: `/demo/chatter` with prefix `ros2` maps to `ros2/demo/chatter`.
- `fixed`: All messages publish to `mqtt.fixed_topic`.

### Payload formats

- `cdr`: raw ROS 2 CDR bytes.
- `json`: JSON serialized using ROS 2 introspection.

## Metrics

When enabled, metrics are published to `metrics.topic` as JSON arrays. Each entry contains:

- Message counts, throughput, and byte totals.
- Serialization and publish latency (avg/p95/p99/max).
- Message size min/max/avg.
- MQTT connection metrics (connected, uptime, reconnect count, last error).

## Comparison with kafka_sink

- MQTT uses QoS levels (0, 1, 2) instead of Kafka acknowledgements.
- MQTT supports a retained flag for state-like topics.
- The client is connection-oriented and reconnects automatically.
- Topic mapping uses `/` separators rather than Kafka-style `.` separators.

## Troubleshooting

- **No MQTT messages**: Check lifecycle state (`ros2 lifecycle get /mosquitto_sink`), ensure
  `subscriptions_yaml` is populated, and validate broker connectivity.
- **Serialization errors**: Verify the message type string in `subscriptions_yaml`.
- **TLS failures**: Confirm `mqtt.use_tls` and `mqtt.ca_cert_path` point to a valid CA file.

## License

Apache License 2.0.
