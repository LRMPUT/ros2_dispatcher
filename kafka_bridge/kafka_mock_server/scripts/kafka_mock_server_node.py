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
        """Create Produce response by parsing the request."""
        # ProduceRequest has topics with partitions - we need to respond for each
        try:
            offset = 8  # Skip header
            
            # Parse request (simplified - just extract topic count)
            # In v3+: TransactionalId (nullable string)
            if api_version >= 3:
                transactional_id_len = struct.unpack('>h', message[offset:offset+2])[0]
                offset += 2
                if transactional_id_len > 0:
                    offset += transactional_id_len
            
            # Required Acks (2 bytes)
            offset += 2
            # Timeout (4 bytes)
            offset += 4
            
            # Topics array
            if offset + 4 <= len(message):
                topic_count = struct.unpack('>i', message[offset:offset+4])[0]
                offset += 4
                
                # Build response
                response = struct.pack('>I', correlation_id)
                
                # Respond with same number of topics
                response += struct.pack('>i', topic_count)
                
                # For each topic, add success response
                for _ in range(min(topic_count, 100)):  # Limit to prevent issues
                    # Skip topic name in request (we'll just respond generically)
                    if offset + 2 <= len(message):
                        topic_name_len = struct.unpack('>h', message[offset:offset+2])[0]
                        offset += 2 + topic_name_len
                        
                        # Get partition count
                        if offset + 4 <= len(message):
                            partition_count = struct.unpack('>i', message[offset:offset+4])[0]
                            offset += 4
                            
                            # Topic name in response (empty string)
                            response += struct.pack('>h', 0)
                            
                            # Partitions array
                            response += struct.pack('>i', partition_count)
                            
                            for _ in range(min(partition_count, 100)):
                                # Partition (4 bytes)
                                response += struct.pack('>i', 0)
                                # Error code (2 bytes) - NO_ERROR
                                response += struct.pack('>h', 0)
                                # Base offset (8 bytes)
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
