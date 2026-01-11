# Performance metrics and measurement definitions

This document defines the primary and secondary performance metrics for the ROS 2 ↔ Kafka dispatcher, along with precise measurement rules, aggregation, and experimental factors to control.

## Primary metrics

1. **End-to-end latency (publish → Kafka → consumer receipt)**
   - **Definition:** Time from ROS 2 publisher timestamp to consumer receipt timestamp for the same message.
   - **Data points:** For each message, record `t_ros_publish`, `t_kafka_record`, `t_consumer_recv`.
   - **Latency fields:**
     - `latency_end_to_end = t_consumer_recv - t_ros_publish`
     - `latency_kafka_path = t_consumer_recv - t_kafka_record`

2. **Throughput**
   - **Definition:** Message rate and/or payload rate through the system.
   - **Measurement:**
     - `throughput_msgs = total_messages / test_duration_seconds`
     - `throughput_bytes = total_payload_bytes / test_duration_seconds`

3. **Payload size**
   - **Definition:** Size in bytes of the serialized payload per message.
   - **Measurement:**
     - `payload_bytes_raw`: bytes produced by ROS 2 serialization before Kafka send.
     - `payload_bytes_kafka_record`: size of the Kafka record payload as sent (exclude Kafka protocol overhead).

## Secondary metrics

1. **CPU utilization**
   - **Definition:** CPU usage of the ROS 2 publisher, Kafka bridge/sink, and consumer processes.
   - **Measurement:** Average and max CPU across the test window, sampled at a fixed interval (e.g., 1s).

2. **Memory usage (RSS/heap)**
   - **Definition:** RSS and heap usage for the bridge/sink processes.
   - **Measurement:** Average and max memory across the test window, sampled at a fixed interval.

3. **Error rate / drops**
   - **Definition:** Count and ratio of failed operations to total operations.
   - **Measurement:**
     - `serialization_failures`
     - `dropped_messages`
     - `kafka_delivery_errors`
     - `error_rate = (sum of failures) / total_messages`

## Precise measurement definitions

### Timestamps

- **ROS 2 publish time (`t_ros_publish`)**: Capture from the message header timestamp at publish time. If header timestamps are absent, log a monotonic timestamp at the publisher immediately before publish.
- **Kafka record timestamp (`t_kafka_record`)**: Capture Kafka `CreateTime` if producer sets record timestamp; otherwise use producer send time captured just before send.
- **Consumer receipt time (`t_consumer_recv`)**: Record a monotonic timestamp immediately upon message reception by the consumer.

### Size

- **Raw payload bytes (`payload_bytes_raw`)**: Size of the serialized ROS 2 message before Kafka send.
- **Kafka record payload bytes (`payload_bytes_kafka_record`)**: Size of the record value produced to Kafka. Note whether compression is enabled; if enabled, record compressed bytes and compression type.

## Units and aggregation

- **Latency**: milliseconds (ms). Report mean, p50, p95, p99 per test run.
- **Throughput**: messages/sec and bytes/sec. Report sustained average and peak over the test window.
- **Resource usage**: CPU in percent per process; memory in MB. Report average and max over the test window.
- **Error rate**: fraction or percent of total messages.

## Experimental factors to record (control variables)

- **Message rate**: target publish rate (msgs/sec) and actual achieved rate.
- **Payload size**: serialized message size (bytes), including distribution if variable.
- **Topics and partitions**: topic count, partition count, replication factor.
- **ROS 2 QoS**: reliability, durability, history depth, deadline/liveliness settings.
- **Kafka producer config**: acks, linger.ms, batch.size, compression.type, retries, buffer.memory.
- **Kafka consumer config**: fetch.min.bytes, fetch.max.bytes, max.poll.records, auto.offset.reset.
- **System setup**: CPU model, core count, memory, OS version, network topology, containerization settings.
- **Test duration and warm-up**: total runtime, warm-up period, and steady-state measurement window.
