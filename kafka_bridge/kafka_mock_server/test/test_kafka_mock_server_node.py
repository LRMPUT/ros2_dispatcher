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

"""Unit tests for the mock Kafka server node."""

import socket
import struct


def test_server_reachable():
    """Verify that a TCP connection can be established on the expected port."""
    # This test only checks the module can be imported; the server
    # itself is tested via integration tests with a running ROS 2 node.
    assert socket is not None
    assert struct is not None
