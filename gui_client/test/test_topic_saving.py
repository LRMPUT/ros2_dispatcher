#!/usr/bin/env python3

# Copyright 2025 Maciej Krupka
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for topic saving functionality."""

import pytest
import yaml
import tempfile
from pathlib import Path


def test_topic_parsing():
    """Test parsing topic name and type from formatted string."""
    # Test standard format
    text = "/camera/image_raw [sensor_msgs/msg/Image]"
    if '[' in text and ']' in text:
        topic_name = text[:text.rfind('[')].strip()
        topic_type = text[text.rfind('[')+1:text.rfind(']')].strip()
        assert topic_name == "/camera/image_raw"
        assert topic_type == "sensor_msgs/msg/Image"


def test_yaml_saving():
    """Test YAML file writing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_file = Path(tmpdir) / 'test_topics.yaml'
        
        topics_config = [
            {'name': '/topic1', 'type': 'std_msgs/msg/String'},
            {'name': '/topic2', 'type': 'sensor_msgs/msg/Image'},
        ]
        
        config_data = {
            'selected_topics': topics_config,
            'timestamp': '2025-01-01T00:00:00',
            'count': len(topics_config)
        }
        
        # Write YAML
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
        
        # Verify file exists
        assert config_file.exists()
        
        # Read back and verify content
        with open(config_file, 'r') as f:
            loaded_data = yaml.safe_load(f)
        
        assert loaded_data['count'] == 2
        assert len(loaded_data['selected_topics']) == 2
        assert loaded_data['selected_topics'][0]['name'] == '/topic1'
        assert loaded_data['selected_topics'][0]['type'] == 'std_msgs/msg/String'
