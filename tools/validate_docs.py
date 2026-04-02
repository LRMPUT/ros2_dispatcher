#!/usr/bin/env python3
"""Validate documentation parameter claims against C++ source declare_parameter calls."""

import re
import sys
from pathlib import Path

REPO = Path(__file__).parent.parent

NODE_SOURCES = {
    "kafka_sink": [
        REPO / "kafka_bridge/kafka_sink/src/kafka_sink_node.cpp",
    ],
    "kafka_source": [
        REPO / "kafka_bridge/kafka_source/src/kafka_source_node.cpp",
    ],
    "kafka_cdr_to_json": [
        REPO / "kafka_bridge/kafka_cdr_to_json/src/kafka_cdr_to_json_node.cpp",
    ],
    "dispatcher_controller": [
        REPO / "dispatcher_controller/src/dispatcher_controller_node.cpp",
    ],
    "introspection_manager": [
        REPO / "introspection_manager/src/introspection_manager_node.cpp",
    ],
    "mosquitto_sink": [
        REPO / "mosquitto_bridge/mosquitto_sink/src/mosquitto_sink_node.cpp",
    ],
}

DECLARE_RE = re.compile(r'declare_parameter(?:<[^(]+>)?\(\s*"([^"]+)"')


def extract_declared_params(cpp_files):
    """Return set of parameter names from declare_parameter calls."""
    params = set()
    for path in cpp_files:
        if not path.exists():
            print(f"  MISSING source: {path}")
            continue
        text = path.read_text()
        params.update(DECLARE_RE.findall(text))
    return params


# Parameters documented in configuration_reference.md per node.
# This is the claim set we validate against source code.
DOC_CLAIMS = {
    "kafka_sink": {
        "subscriptions_yaml", "qos_depth",
        "kafka.bootstrap_servers", "kafka.client_id", "kafka.acks",
        "kafka.topic_prefix", "kafka.topic_mapping_mode", "kafka.fixed_topic",
        "kafka.strict_startup", "kafka.max_queue_messages", "kafka.drop_when_full",
        "kafka.linger_ms", "kafka.batch_size", "kafka.payload_format",
        "metrics.enabled", "metrics.interval_ms", "metrics.topic",
        "telemetry.enabled", "telemetry.log_every_n",
    },
    "kafka_source": {
        "kafka.bootstrap_servers", "kafka.group_id",
        "kafka.topic_pattern", "kafka.offset_reset",
    },
    "kafka_cdr_to_json": {
        "kafka.bootstrap_servers", "kafka.group_id",
        "kafka.input_topic_pattern", "kafka.output_topic_prefix", "kafka.offset_reset",
        "json.include_ros_type", "json.include_timestamp",
        "metrics.enabled", "metrics.interval_ms", "metrics.topic",
        "topic_mappings",
    },
    "dispatcher_controller": {
        "selection_mode", "selection_file_path", "auto_apply_on_mode_change",
        "validate_topics", "kafka_sink_node_name", "mosquitto_sink_node_name",
        "allow_missing_sinks", "component_container_name",
        "introspection_service_name", "introspection_node_name",
        "disable_introspection_after_apply", "all_mode_max_topics",
        "all_mode_allowlist", "all_mode_denylist", "all_mode_hide_rosout",
    },
    "introspection_manager": {
        "publisher_queue_depth", "publisher_reliability", "publisher_durability",
        "publish_on_change", "filter_hidden", "introspection_enabled",
    },
    "mosquitto_sink": {
        "subscriptions_yaml", "qos_depth",
        "mqtt.broker_host", "mqtt.broker_port", "mqtt.client_id",
        "mqtt.username", "mqtt.password", "mqtt.qos", "mqtt.retain",
        "mqtt.keep_alive_seconds", "mqtt.topic_prefix", "mqtt.topic_mapping_mode",
        "mqtt.fixed_topic", "mqtt.payload_format", "mqtt.use_tls",
        "mqtt.ca_cert_path", "mqtt.lwt_topic", "mqtt.lwt_payload",
        "mqtt.lwt_qos", "mqtt.lwt_retain",
        "metrics.enabled", "metrics.interval_ms", "metrics.topic",
    },
}


def run():
    failures = []
    for node, claimed_params in DOC_CLAIMS.items():
        sources = NODE_SOURCES.get(node, [])
        actual_params = extract_declared_params(sources)
        in_docs_not_code = claimed_params - actual_params
        in_code_not_docs = actual_params - claimed_params
        if in_docs_not_code:
            for p in sorted(in_docs_not_code):
                failures.append(f"[{node}] docs claim '{p}' but NOT in declare_parameter")
        if in_code_not_docs:
            for p in sorted(in_code_not_docs):
                print(f"  INFO [{node}] '{p}' declared in code but not in docs (may be intentional)")

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  FAIL: {f}")
        sys.exit(1)
    else:
        print("All documented parameters verified in source code.")


if __name__ == "__main__":
    run()
