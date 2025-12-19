# kafka_client

`kafka_client` is a thin, reusable wrapper around `librdkafka` intended for ROS 2 Kafka integrations.
It exposes a small C++ API for configuring and running a producer without blocking caller threads.

## Building

```bash
colcon build --packages-select kafka_client
```

## Linking in another package

In `package.xml`:

```xml
<depend>kafka_client</depend>
```

In `CMakeLists.txt`:

```cmake
find_package(kafka_client REQUIRED)

add_executable(my_node src/my_node.cpp)
target_link_libraries(my_node PRIVATE kafka_client::kafka_client)
```

Include the header:

```cpp
#include "kafka_client/kafka_producer.hpp"
```

The producer accepts raw byte buffers and metadata headers and performs background polling so
publishers can remain non-blocking.

## Example producer

Build the optional example executable:

```bash
colcon build --packages-select kafka_client --cmake-args -DKAFKA_CLIENT_BUILD_EXAMPLES=ON
```

Run the example (requires a reachable Kafka broker):

```bash
source install/setup.bash
ros2 run kafka_client kafka_client_simple_producer localhost:9092 demo_topic "hello kafka"
```

The example starts the producer in tolerant mode (keeps retrying if the broker is slow to respond)
and sends a single payload with two headers (`example=true`, `sent_at_ms=<timestamp>`). Use
`kafka-console-consumer` or your preferred Kafka client to verify the payload arrives.
