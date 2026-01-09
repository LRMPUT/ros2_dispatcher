# Kafka Broker Docker Setup

This directory contains a Docker Compose configuration for running a local Kafka broker for development and testing purposes.

## Architecture

This setup uses **KRaft mode** (Kafka Raft), the modern Kafka architecture that eliminates the need for Zookeeper. KRaft is the recommended deployment method for Kafka 3.0+ and provides better performance and simplified operations.

## Components

The setup includes three services:

1. **Broker** - Kafka broker with built-in KRaft controller
   - Ports: 9092 (external), 9093 (controller), 9101 (JMX)
   - Image: confluentinc/cp-kafka:latest
   - Combined broker and controller role
   - Auto-creates topics on first use
   - Healthcheck enabled for reliable startup

2. **Schema Registry** - Manages Avro, JSON, and Protobuf schemas
   - Port: 8081
   - Image: confluentinc/cp-schema-registry:latest
   - Useful for structured message formats
   - Provides schema evolution and compatibility checking

3. **Kafka UI** - Modern web interface for Kafka management
   - Port: 8090
   - Image: provectuslabs/kafka-ui:latest
   - Access at: http://localhost:8090

## Usage

### Start the Kafka broker

```bash
docker-compose up -d
```

### Stop the Kafka broker

```bash
docker-compose down
```

### View logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka
```

### Remove all data (clean restart)

```bash
docker-compose down -v
```

## Testing with ROS 2 Kafka Bridge

Once the broker is running, you can use it with the kafka_sink and kafka_source nodes:

```bash
# Start kafka_sink to publish ROS messages to Kafka
ros2 launch kafka_sink kafka_sink_container.launch.py

# Start kafka_source to consume Kafka messages and publish to ROS
ros2 launch kafka_source kafka_source_container.launch.py
```

## Kafka UI Web Interface

Access the Kafka UI web interface at http://localhost:8090 to:
- View topics, partitions, and messages
- Monitor consumer groups and lag
- Inspect broker and schema registry configuration
- Browse message content with schema support
- Create and delete topics
- Monitor cluster health and metrics

## Configuration

The broker is configured with:
- **KRaft mode** - No Zookeeper dependency
- **Single broker** (ID: 1) with combined broker/controller role
- **PLAINTEXT security** (no authentication) - suitable for development
- **Auto-create topics** - Topics are created automatically on first use
- **Internal/External listeners** - Proper separation for container and host access
- **Replication factor: 1** - Suitable for single-node development
- **JMX monitoring** enabled on port 9101

### Listener Configuration

- `EXTERNAL://localhost:9092` - Connect from host machine
- `INTERNAL://broker:29092` - Connect from other Docker containers
- `CONTROLLER://broker:29093` - KRaft controller communication

### Advantages of KRaft Mode

1. **Simplified architecture** - No Zookeeper to manage
2. **Faster startup** - Direct broker-to-broker communication
3. **Better scalability** - Metadata stored in Kafka itself
4. **Future-proof** - Zookeeper support is deprecated in Kafka 4.0+
5. **Improved reliability** - Built-in consensus with Raft protocol

## Using Schema Registry

The Schema Registry is useful when you need:
- Structured message formats (Avro, JSON Schema, Protobuf)
- Schema evolution with backward/forward compatibility
- Reduced message size (with Avro binary format)
- Strong data contracts between producers and consumers

For simple use cases with JSON or raw byte messages, you can disable it by removing the `schema-registry` service dependency in `kafka-ui`.

## Production Considerations

For production deployments, consider:
- Adding authentication (SASL/SSL)
- Configuring multiple brokers for high availability
- Adjusting replication factors (typically 3)
- Setting up proper network configuration and DNS
- Enabling SSL/TLS for encrypted communication
- Configuring retention policies based on storage capacity
- Monitoring with JMX metrics and external tools
