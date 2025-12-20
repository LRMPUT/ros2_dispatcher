# kafka_client

Small, reusable Kafka producer library built on top of librdkafka. Designed to be consumed by
ROS 2 nodes that need a non-blocking Kafka sender.

Highlights:
- Minimal configuration surface (bootstrap servers, client id, acks, batching hints).
- Internal polling thread and background reconnect loop (tolerant mode).
- Bounded buffering with drop counters when Kafka back-pressure is hit.
- Thread-safe, non-blocking `send()` API that accepts raw byte payloads and headers.

## Building

```bash
rosdep install --from-paths . --ignore-src -y
colcon build --packages-select kafka_client
```

## Linking from CMake

```cmake
find_package(kafka_client REQUIRED)

add_executable(my_node src/my_node.cpp)
target_link_libraries(my_node PRIVATE kafka_client::kafka_client)
```

## Basic usage

```cpp
#include "kafka_client/kafka_producer.hpp"

kafka_client::KafkaProducerConfig config;
config.bootstrap_servers = "localhost:9092";
config.client_id = "example_node";
config.startup_mode = kafka_client::StartupMode::TOLERANT;

kafka_client::KafkaProducer producer(config);
producer.start();

std::vector<uint8_t> value{/* bytes */};
producer.send("demo.topic", {}, value, 0, {});
```
