-- Minimal pass-through query for paradigm-latency smoke test.
-- Measures the overhead of routing through ksqlDB without UDF compute.
-- A real geofence query (with the INGEOFENCE UDF) is a separate change.

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM ros_gps_fix_stream (
  robot_id VARCHAR KEY,
  latitude DOUBLE,
  longitude DOUBLE,
  altitude DOUBLE,
  timestamp BIGINT,
  t0_ns BIGINT
) WITH (
  KAFKA_TOPIC='ros_gps_fix',
  KEY_FORMAT='KAFKA',
  VALUE_FORMAT='JSON',
  PARTITIONS=1
);

CREATE STREAM robot_geofence_alerts WITH (
  KAFKA_TOPIC='robot_geofence_alerts',
  VALUE_FORMAT='JSON',
  PARTITIONS=1
) AS
SELECT
  robot_id,
  latitude,
  longitude,
  altitude,
  timestamp,
  t0_ns,
  'OUTSIDE' AS msg
FROM ros_gps_fix_stream
EMIT CHANGES;
