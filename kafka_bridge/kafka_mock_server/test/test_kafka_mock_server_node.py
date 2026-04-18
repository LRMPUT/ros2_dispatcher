#!/usr/bin/env python3

import importlib.util
import struct
import sys
import types
from pathlib import Path


def _load_module():
    if 'rclpy' not in sys.modules:
        rclpy = types.ModuleType('rclpy')
        rclpy.init = lambda *args, **kwargs: None
        rclpy.spin = lambda *args, **kwargs: None
        rclpy.shutdown = lambda *args, **kwargs: None

        rclpy_node = types.ModuleType('rclpy.node')

        class Node:
            pass

        rclpy_node.Node = Node
        rclpy.node = rclpy_node

        sys.modules['rclpy'] = rclpy
        sys.modules['rclpy.node'] = rclpy_node

    script_path = (
        Path(__file__).resolve().parents[1]
        / 'scripts'
        / 'kafka_mock_server_node.py'
    )
    spec = importlib.util.spec_from_file_location('kafka_mock_server_node', script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _DummyLogger:
    def info(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass

    def warn(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def _make_server(module):
    server = module.KafkaMockServer.__new__(module.KafkaMockServer)
    server.messages = {}
    server.offsets = {}
    server.get_logger = lambda: _DummyLogger()
    return server


def _request_header(api_key, api_version, correlation_id):
    return struct.pack('>HHI', api_key, api_version, correlation_id)


def test_create_api_version_response_includes_throttle_for_v1_plus():
    module = _load_module()
    server = _make_server(module)

    response = server.create_api_version_response(99, 1)

    assert struct.unpack('>I', response[:4])[0] == 99
    assert len(response) > 4
    assert struct.unpack('>i', response[-4:])[0] == 0


def test_create_metadata_response_version_fields():
    module = _load_module()
    server = _make_server(module)

    v0 = server.create_metadata_response(11, 0)
    v2 = server.create_metadata_response(11, 2)

    assert struct.unpack('>I', v0[:4])[0] == 11
    assert struct.unpack('>I', v2[:4])[0] == 11
    assert len(v2) > len(v0)


def test_create_produce_response_stores_message_and_advances_offset():
    module = _load_module()
    server = _make_server(module)

    correlation_id = 33
    api_version = 5
    topic = b'ros2.topic'
    payload = b'abc'

    message = _request_header(0, api_version, correlation_id)
    message += struct.pack('>h', -1)  # nullable transactional_id for v1+
    message += struct.pack('>h', 1)   # required_acks
    message += struct.pack('>i', 1000)  # timeout
    message += struct.pack('>i', 1)  # topic_count
    message += struct.pack('>h', len(topic)) + topic
    message += struct.pack('>i', 1)  # partition_count
    message += struct.pack('>i', 0)  # partition_id
    message += struct.pack('>i', 1)  # record_count
    message += struct.pack('>q', 0)  # batch_offset
    message += struct.pack('>i', len(payload)) + payload

    response = server.create_produce_response(correlation_id, api_version, message)

    assert struct.unpack('>I', response[:4])[0] == correlation_id
    assert 'ros2.topic' in server.messages
    assert 0 in server.messages['ros2.topic']
    stored = server.messages['ros2.topic'][0]
    assert len(stored) == 1
    assert stored[0]['payload'] == payload
    assert server.offsets['ros2.topic'][0] == 1


def test_create_fetch_response_reports_high_water_mark_from_stored_messages():
    module = _load_module()
    server = _make_server(module)

    server.messages = {'ros2.topic': {0: [{'offset': 0, 'payload': b'data'}]}}
    server.offsets = {'ros2.topic': {0: 1}}

    correlation_id = 77
    api_version = 5
    topic = b'ros2.topic'

    message = _request_header(1, api_version, correlation_id)
    message += struct.pack('>i', -1)  # replica_id
    message += struct.pack('>i', 500)  # max_wait_ms
    message += struct.pack('>i', 1)  # min_bytes
    message += struct.pack('>i', 1)  # topic_count
    message += struct.pack('>h', len(topic)) + topic
    message += struct.pack('>i', 1)  # partition_count
    message += struct.pack('>i', 0)  # partition_id
    message += struct.pack('>q', 0)  # fetch_offset
    message += struct.pack('>i', 1024)  # max_bytes

    response = server.create_fetch_response(correlation_id, api_version, message)

    assert struct.unpack('>I', response[:4])[0] == correlation_id
    assert struct.pack('>q', 1) in response


def test_handle_kafka_message_routes_supported_api_keys_and_fallback():
    module = _load_module()
    server = _make_server(module)

    server.create_api_version_response = lambda cid, ver: b'api'
    server.create_metadata_response = lambda cid, ver: b'meta'
    server.create_produce_response = lambda cid, ver, msg: b'prod'
    server.create_fetch_response = lambda cid, ver, msg: b'fetch'
    server.create_empty_response = lambda cid: b'empty'

    assert server.handle_kafka_message(_request_header(18, 1, 10)) == b'api'
    assert server.handle_kafka_message(_request_header(3, 1, 10)) == b'meta'
    assert server.handle_kafka_message(_request_header(0, 1, 10)) == b'prod'
    assert server.handle_kafka_message(_request_header(1, 1, 10)) == b'fetch'
    assert server.handle_kafka_message(_request_header(999, 1, 10)) == b'empty'


def test_handle_kafka_message_returns_none_for_short_payloads():
    module = _load_module()
    server = _make_server(module)

    assert server.handle_kafka_message(b'') is None
    assert server.handle_kafka_message(b'1234567') is None
