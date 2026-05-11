-- ksqlDB pipeline for the Phase 2 scalability matrix.
--
-- Query: bounding-box geofence.  The bag's real coordinates are around
-- lat≈46.34, lon≈3.43 (Massif Central, France).  The per-robot lat/lon
-- shift in robot_replay.py is 1e-4 deg × robot_id, so at robot_id=50 the
-- shifted point is at most (46.345, 3.435) — still well within the box
-- [46.30..46.40, 3.40..3.50].  The WHERE matches every input row, so the
-- output stream's rate equals the input rate and we isolate the paradigm
-- engine's processing cost from any filtering effect.

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM ros_gps_fix_stream (
  robot_id VARCHAR,
  latitude DOUBLE,
  longitude DOUBLE,
  altitude DOUBLE,
  timestamp BIGINT,
  t0_ns BIGINT
) WITH (
  KAFKA_TOPIC='ros_gps_fix',
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
WHERE latitude  BETWEEN 46.0 AND 47.0
  AND longitude BETWEEN 3.0  AND 4.0
EMIT CHANGES;
