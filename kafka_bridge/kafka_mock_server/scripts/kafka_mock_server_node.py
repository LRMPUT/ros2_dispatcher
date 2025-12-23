#!/usr/bin/env python3

"""
Mock Kafka Server Node.

A simple TCP server that simulates a Kafka broker for testing purposes.
Listens on port 9092 and accepts connections, logging received data.
Implements basic Kafka protocol responses to avoid client timeouts.
"""

import socket
import struct
import threading
import rclpy
from rclpy.node import Node


class KafkaMockServer(Node):
    """Mock Kafka server node that accepts TCP connections."""

    def __init__(self):
        """Initialize the mock Kafka server."""
        super().__init__('kafka_mock_server')
        
        # Declare parameters
        self.declare_parameter('host', '0.0.0.0')
        self.declare_parameter('port', 9092)
        
        self.host = self.get_parameter('host').value
        self.port = self.get_parameter('port').value
        
        self.server_socket = None
        self.running = False
        self.connections = []
        
        # Message storage: {topic: {partition: [Message]}}
        self.messages = {}
        # Offsets tracking: {topic: {partition: next_offset}}
        self.offsets = {}
        
        self.start_server()
        
    def start_server(self):
        """Start the mock Kafka server."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            
            self.get_logger().info(
                f'Mock Kafka server started on {self.host}:{self.port}'
            )
            
            # Start accepting connections in a separate thread
            self.accept_thread = threading.Thread(target=self.accept_connections)
            self.accept_thread.daemon = True
            self.accept_thread.start()
            
        except Exception as e:
            self.get_logger().error(f'Failed to start server: {e}')
            self.running = False
            
    def accept_connections(self):
        """Accept incoming connections."""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                client_socket, address = self.server_socket.accept()
                self.get_logger().info(f'Client connected from {address}')
                
                # Handle each connection in a separate thread
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                self.connections.append((client_socket, client_thread))
                
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.get_logger().error(f'Error accepting connection: {e}')
                    
    def handle_client(self, client_socket, address):
        """Handle client connection."""
        try:
            buffer = b''
            while self.running:
                # Set timeout for receiving data
                client_socket.settimeout(1.0)
                try:
                    data = client_socket.recv(4096)
                    if not data:
                        break
                    
                    buffer += data
                    self.get_logger().info(
                        f'Received {len(data)} bytes from {address}'
                    )
                    
                    # Process complete Kafka messages
                    while len(buffer) >= 4:
                        # First 4 bytes is message size
                        msg_size = struct.unpack('>I', buffer[:4])[0]
                        
                        if len(buffer) < 4 + msg_size:
                            # Wait for more data
                            break
                        
                        # Extract the message
                        message = buffer[4:4+msg_size]
                        buffer = buffer[4+msg_size:]
                        
                        # Try to parse and respond
                        response = self.handle_kafka_message(message)
                        if response:
                            # Send response with size prefix
                            response_with_size = struct.pack('>I', len(response)) + response
                            client_socket.sendall(response_with_size)
                            self.get_logger().info(
                                f'Sent {len(response)} byte response to {address} '
                                f'(total with header: {len(response_with_size)})'
                            )
                    
                except socket.timeout:
                    continue
                    
        except Exception as e:
            self.get_logger().warn(f'Client {address} error: {e}')
        finally:
            self.get_logger().info(f'Client {address} disconnected')
            client_socket.close()
    
    def handle_kafka_message(self, message):
        """Handle Kafka protocol message and return response."""
        try:
            if len(message) < 8:
                return None
            
            # Parse Kafka request header
            api_key, api_version, correlation_id = struct.unpack('>HHI', message[:8])
            
            self.get_logger().info(
                f'Kafka request: API Key={api_key}, Version={api_version}, '
                f'Correlation ID={correlation_id}'
            )
            
            # Handle ApiVersionRequest (API Key 18)
            if api_key == 18:
                return self.create_api_version_response(correlation_id, api_version)
            
            # Handle MetadataRequest (API Key 3)
            elif api_key == 3:
                return self.create_metadata_response(correlation_id, api_version)
            
            # Handle ProduceRequest (API Key 0)
            elif api_key == 0:
                return self.create_produce_response(correlation_id, api_version, message)
            
            # Handle FetchRequest (API Key 1)
            elif api_key == 1:
                return self.create_fetch_response(correlation_id, api_version, message)
            
            # For other requests, return empty response
            return self.create_empty_response(correlation_id)
            
        except Exception as e:
            self.get_logger().warn(f'Error handling Kafka message: {e}')
            return None
    
    def create_api_version_response(self, correlation_id, request_version):
        """Create ApiVersion response."""
        # ApiVersion response format depends on version
        # For v0-v2: correlation_id + error_code + array of api_versions
        # For v3+: includes throttle_time_ms and tagged fields
        
        error_code = 0  # No error
        
        # Define supported APIs (simplified but realistic)
        api_versions = [
            (0, 0, 9),    # Produce
            (1, 0, 13),   # Fetch
            (2, 0, 7),    # ListOffsets
            (3, 0, 12),   # Metadata
            (8, 0, 8),    # OffsetCommit
            (9, 0, 8),    # OffsetFetch
            (18, 0, 3),   # ApiVersion
            (19, 0, 7),   # CreateTopics
        ]
        
        # Response header: correlation_id (4 bytes)
        response = struct.pack('>I', correlation_id)
        
        # Error code (2 bytes)
        response += struct.pack('>h', error_code)
        
        # Array length (4 bytes) - number of API versions
        response += struct.pack('>i', len(api_versions))
        
        # Add each API version entry
        for api_key, min_version, max_version in api_versions:
            response += struct.pack('>h', api_key)        # API key (2 bytes)
            response += struct.pack('>h', min_version)    # Min version (2 bytes)
            response += struct.pack('>h', max_version)    # Max version (2 bytes)
        
        # Throttle time in ms (4 bytes) - for version 1+
        if request_version >= 1:
            response += struct.pack('>i', 0)
        
        self.get_logger().debug(
            f'Created ApiVersion response (v{request_version}, '
            f'{len(api_versions)} APIs, {len(response)} bytes)'
        )
        return response
    
    def create_metadata_response(self, correlation_id, api_version):
        """Create Metadata response."""
        # Metadata response format varies by version
        # Basic structure: correlation_id + throttle_time + brokers + cluster_id + 
        #                  controller_id + topics
        
        response = struct.pack('>I', correlation_id)
        
        # Throttle time (4 bytes) - for v1+
        if api_version >= 1:
            response += struct.pack('>i', 0)
        
        # Brokers array
        response += struct.pack('>i', 1)  # Array length: 1 broker
        response += struct.pack('>i', 1)  # Node ID
        
        # Host string (2 bytes length + string)
        host = b'localhost'
        response += struct.pack('>h', len(host)) + host
        
        # Port (4 bytes)
        response += struct.pack('>i', 9092)
        
        # Rack (nullable string) - for v1+
        if api_version >= 1:
            response += struct.pack('>h', -1)  # null
        
        # Cluster ID (nullable string) - for v2+
        if api_version >= 2:
            response += struct.pack('>h', -1)  # null
        
        # Controller ID (4 bytes) - for v1+
        if api_version >= 1:
            response += struct.pack('>i', 1)
        
        # Topic metadata array
        response += struct.pack('>i', 0)  # Empty array
        
        self.get_logger().debug(f'Created Metadata response (v{api_version})')
        return response
    
    def create_produce_response(self, correlation_id, api_version, message):
        """Create Produce response and store messages."""
        try:
            offset = 8  # Skip header
            
            # For v1+: TransactionalId (nullable string)
            if api_version >= 1:
                transactional_id_len = struct.unpack('>h', message[offset:offset+2])[0]
                offset += 2
                if transactional_id_len > 0:
                    offset += transactional_id_len
            
            # Required Acks (2 bytes)
            offset += 2
            # Timeout (4 bytes)
            offset += 4
            
            # Topics array
            if offset + 4 > len(message):
                return None
                
            topic_count = struct.unpack('>i', message[offset:offset+4])[0]
            offset += 4
            
            # Store topics and build response
            response = struct.pack('>I', correlation_id)
            response += struct.pack('>i', topic_count)
            
            # Process each topic
            for _ in range(min(topic_count, 100)):
                # Topic name (string)
                if offset + 2 > len(message):
                    break
                    
                topic_name_len = struct.unpack('>h', message[offset:offset+2])[0]
                offset += 2
                
                if offset + topic_name_len > len(message):
                    break
                    
                topic_name = message[offset:offset+topic_name_len].decode('utf-8', errors='replace')
                offset += topic_name_len
                
                # Initialize topic storage if needed
                if topic_name not in self.messages:
                    self.messages[topic_name] = {}
                    self.offsets[topic_name] = {}
                
                # Partition count
                if offset + 4 > len(message):
                    break
                    
                partition_count = struct.unpack('>i', message[offset:offset+4])[0]
                offset += 4
                
                # Response: topic name
                response += struct.pack('>h', len(topic_name)) + topic_name.encode('utf-8')
                response += struct.pack('>i', partition_count)
                
                # Process each partition
                for _ in range(min(partition_count, 100)):
                    if offset + 4 > len(message):
                        break
                    
                    partition_id = struct.unpack('>i', message[offset:offset+4])[0]
                    offset += 4
                    
                    # Initialize partition storage
                    if partition_id not in self.messages[topic_name]:
                        self.messages[topic_name][partition_id] = []
                        self.offsets[topic_name][partition_id] = 0
                    
                    # Record batches
                    if offset + 4 > len(message):
                        break
                    
                    record_count = struct.unpack('>i', message[offset:offset+4])[0]
                    offset += 4
                    
                    batch_offsets = []
                    
                    # Process each record batch
                    for _ in range(min(record_count, 1000)):
                        if offset + 8 > len(message):
                            break
                        
                        # Base offset and batch size
                        batch_offset = struct.unpack('>q', message[offset:offset+8])[0]
                        offset += 8
                        
                        if offset + 4 > len(message):
                            break
                        
                        batch_size = struct.unpack('>i', message[offset:offset+4])[0]
                        offset += 4
                        
                        if offset + batch_size > len(message):
                            break
                        
                        batch_data = message[offset:offset+batch_size]
                        offset += batch_size
                        
                        # Store message
                        current_offset = self.offsets[topic_name][partition_id]
                        self.messages[topic_name][partition_id].append({
                            'offset': current_offset,
                            'batch_offset': batch_offset,
                            'payload': batch_data,
                            'timestamp': -1  # System time
                        })
                        batch_offsets.append(current_offset)
                        self.offsets[topic_name][partition_id] += 1
                        
                        self.get_logger().info(
                            f'Stored message: topic={topic_name}, partition={partition_id}, '
                            f'offset={current_offset}, size={batch_size} bytes'
                        )
                    
                    # Response per partition
                    response += struct.pack('>i', partition_id)  # Partition
                    response += struct.pack('>h', 0)             # Error code (NO_ERROR)
                    
                    if batch_offsets:
                        response += struct.pack('>q', batch_offsets[0])  # Base offset
                    else:
                        response += struct.pack('>q', 0)
                    
                    # For v2+: Log append time
                    if api_version >= 2:
                        response += struct.pack('>q', -1)
                    
                    # For v5+: Log start offset
                    if api_version >= 5:
                        response += struct.pack('>q', 0)
            
            # Throttle time (4 bytes) - for v1+
            if api_version >= 1:
                response += struct.pack('>i', 0)
            
            self.get_logger().debug(
                f'Created Produce response (v{api_version}, {topic_count} topics)'
            )
            return response
            
        except Exception as e:
            self.get_logger().warn(f'Error parsing produce request: {e}')
        
        # Fallback: simple success response
        response = struct.pack('>I', correlation_id)
        response += struct.pack('>i', 0)  # Empty topics array
        if api_version >= 1:
            response += struct.pack('>i', 0)  # Throttle time
        return response
    
    def create_empty_response(self, correlation_id):
        """Create minimal response for unsupported requests."""
        # Just correlation_id and error code
        error_code = 0
        response = struct.pack('>IH', correlation_id, error_code)
        return response
    
    def create_fetch_response(self, correlation_id, api_version, message):
        """Create Fetch response by returning stored messages."""
        try:
            offset = 8  # Skip header
            
            # Replica ID (4 bytes)
            if offset + 4 > len(message):
                return None
            offset += 4
            
            # Max wait time (4 bytes)
            if offset + 4 > len(message):
                return None
            offset += 4
            
            # Min bytes (4 bytes)
            if offset + 4 > len(message):
                return None
            offset += 4
            
            # Topic count
            if offset + 4 > len(message):
                return None
            topic_count = struct.unpack('>i', message[offset:offset+4])[0]
            offset += 4
            
            # Build response
            response = struct.pack('>I', correlation_id)
            
            # Throttle time (for v1+)
            if api_version >= 1:
                response += struct.pack('>i', 0)
            
            # Topics array in response
            response += struct.pack('>i', topic_count)
            
            # Process each topic request
            for _ in range(min(topic_count, 100)):
                if offset + 2 > len(message):
                    break
                
                # Topic name
                topic_name_len = struct.unpack('>h', message[offset:offset+2])[0]
                offset += 2
                
                if offset + topic_name_len > len(message):
                    break
                
                topic_name = message[offset:offset+topic_name_len].decode('utf-8', errors='replace')
                offset += topic_name_len
                
                # Partition count
                if offset + 4 > len(message):
                    break
                
                partition_count = struct.unpack('>i', message[offset:offset+4])[0]
                offset += 4
                
                # Response: topic name
                response += struct.pack('>h', len(topic_name)) + topic_name.encode('utf-8')
                response += struct.pack('>i', partition_count)
                
                # Process each partition
                for _ in range(min(partition_count, 100)):
                    if offset + 8 > len(message):
                        break
                    
                    partition_id = struct.unpack('>i', message[offset:offset+4])[0]
                    offset += 4
                    
                    # Fetch offset
                    fetch_offset = struct.unpack('>q', message[offset:offset+8])[0]
                    offset += 8
                    
                    # Max bytes
                    if offset + 4 > len(message):
                        break
                    max_bytes = struct.unpack('>i', message[offset:offset+4])[0]
                    offset += 4
                    
                    # Response: partition id
                    response += struct.pack('>i', partition_id)
                    
                    # Error code (NO_ERROR)
                    response += struct.pack('>h', 0)
                    
                    # High water mark
                    if topic_name in self.messages and partition_id in self.messages[topic_name]:
                        high_water_mark = self.offsets[topic_name][partition_id]
                    else:
                        high_water_mark = 0
                    response += struct.pack('>q', high_water_mark)
                    
                    # For v4+: Last stable offset and log start offset
                    if api_version >= 4:
                        response += struct.pack('>q', high_water_mark)  # Last stable offset
                    if api_version >= 5:
                        response += struct.pack('>q', 0)  # Log start offset
                    
                    # Aborted transactions (v4+) - empty array for now
                    if api_version >= 4:
                        response += struct.pack('>i', 0)  # Empty array
                    
                    # Record batches - return empty for safety to avoid malformed data
                    # This prevents the confluent_kafka library from crashing
                    response += struct.pack('>i', 0)  # Empty record batch array
                    
                    self.get_logger().debug(
                        f'Fetch: topic={topic_name}, partition={partition_id}, '
                        f'fetch_offset={fetch_offset}, high_water_mark={high_water_mark}'
                    )
            
            self.get_logger().debug(f'Created Fetch response (v{api_version})')
            return response
            
        except Exception as e:
            self.get_logger().warn(f'Error handling fetch request: {e}')
            return None
            
    def shutdown(self):
        """Shutdown the server."""
        self.get_logger().info('Shutting down mock Kafka server...')
        self.running = False
        
        # Close all client connections
        for client_socket, _ in self.connections:
            try:
                client_socket.close()
            except:
                pass
                
        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
                
        self.get_logger().info('Mock Kafka server shutdown complete')


def main(args=None):
    """Run the mock Kafka server node."""
    rclpy.init(args=args)
    
    node = KafkaMockServer()
    
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.shutdown()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
