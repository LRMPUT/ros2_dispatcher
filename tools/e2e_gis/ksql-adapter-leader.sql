-- Adapter for ros2_kafka_dispatcher (kafka_sink, JSON, nested) -> GIS flat schema
-- Loaded once after ksqldb-server is up.

SET 'auto.offset.reset' = 'earliest';

-- ============== GPS ==============
CREATE STREAM IF NOT EXISTS ros2_leader_gps_raw (
  latitude DOUBLE,
  longitude DOUBLE,
  altitude DOUBLE,
  header STRUCT<stamp STRUCT<sec INT, nanosec INT>, frame_id VARCHAR>
) WITH (
  KAFKA_TOPIC='ros2.leader.gps.fix',
  VALUE_FORMAT='JSON',
  PARTITIONS=1
);

CREATE STREAM IF NOT EXISTS ros_gps_fix_leader_adapter
  WITH (KAFKA_TOPIC='ros_gps_fix', VALUE_FORMAT='JSON', PARTITIONS=2) AS
SELECT
  'leader' AS robot_id,
  (CAST(header->stamp->sec AS BIGINT) * 1000
    + CAST(header->stamp->nanosec / 1000000 AS BIGINT)) AS `timestamp`,
  latitude,
  longitude,
  altitude
FROM ros2_leader_gps_raw
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
PARTITION BY 'leader'
EMIT CHANGES;

-- ============== Odometry ==============
CREATE STREAM IF NOT EXISTS ros2_leader_odom_raw (
  pose STRUCT<pose STRUCT<position STRUCT<x DOUBLE, y DOUBLE, z DOUBLE>>>,
  header STRUCT<stamp STRUCT<sec INT, nanosec INT>, frame_id VARCHAR>
) WITH (
  KAFKA_TOPIC='ros2.leader.localisation.filtered_odom',
  VALUE_FORMAT='JSON',
  PARTITIONS=1
);

CREATE STREAM IF NOT EXISTS ros_filtered_odom_leader_adapter
  WITH (KAFKA_TOPIC='ros_filtered_odom', VALUE_FORMAT='JSON', PARTITIONS=2) AS
SELECT
  'leader' AS robot_id,
  (CAST(header->stamp->sec AS BIGINT) * 1000
    + CAST(header->stamp->nanosec / 1000000 AS BIGINT)) AS `timestamp`,
  pose->pose->position->x AS position_x,
  pose->pose->position->y AS position_y,
  pose->pose->position->z AS position_z,
  '/leader/localisation/filtered_odom' AS source_topic
FROM ros2_leader_odom_raw
PARTITION BY 'leader'
EMIT CHANGES;
