# Mock Kafka Server

A simple mock Kafka server for testing ROS2 Kafka integration without requiring a full Kafka infrastructure.

## Overview

This package provides a lightweight TCP server that listens on port 9092 (default Kafka port) and accepts connections from Kafka clients. It's designed for testing and development purposes when you don't have access to a real Kafka broker.

## Features

- Listens on configurable host and port (default: 0.0.0.0:9092)
- Accepts multiple client connections
- Logs connection events and received data
- Runs as a ROS2 node for easy integration

## Usage

### Running the Mock Server

```bash
# Source your workspace
source install/setup.bash

# Run the mock server
ros2 run kafka_mock_server kafka_mock_server_node.py
```

Or use the launch file:

```bash
ros2 launch kafka_mock_server kafka_mock_server.launch.py
```

### Configuration

You can configure the server through ROS2 parameters:

```bash
ros2 run kafka_mock_server kafka_mock_server_node.py --ros-args -p host:=127.0.0.1 -p port:=9092
```

## Limitations

This is a **mock server** for testing purposes only. It:

- Does not implement the full Kafka protocol
- Does not store or replay messages
- Does not support topics, partitions, or consumer groups
- Simply accepts connections and logs received data

For production use, deploy a real Kafka broker using Docker or a managed service.

## Building

```bash
colcon build --packages-select kafka_mock_server
```
