# Zenoh Phase 2b — native storage + InfluxDB + Grafana

Stands up the **brokerless store-and-visualize** stack that pairs with Zenoh
(the InfluxDB pairing from the design discussion): a Zenoh **router** with the
**storage-manager** plugin + **InfluxDB v1 backend** persists `ros2/**` to
InfluxDB, and **Grafana** dashboards it — no Kafka, no ksqlDB. This is the stack
that was blocked in Phase 1 / Task 12 (the plugins aren't in the ROS vendor
packages); here it runs from the official Eclipse Zenoh release artifacts.

## Stack

```
edge zenoh_sink (JSON)  --client-->  zenoh router (zenohd 1.9.0)
                                       storage_manager plugin (bundled)
                                       + libzenoh_backend_influxdb.so (musl, v1.9.0)
                                       storage ros2/** strip_prefix ros2
                                          │
                                          ▼
                                       InfluxDB 1.8  ◄── Grafana (provisioned DS + dashboard)
```

## Run

```bash
docker compose -f tools/zenoh_gis/phase2b/docker-compose.phase2b.yml up -d
# Grafana: http://localhost:3000  (anonymous Admin; dashboard "Zenoh-native storage")
# verify storage:
docker compose -f tools/zenoh_gis/phase2b/docker-compose.phase2b.yml \
  exec -T influxdb influx -database zenoh_ros2 -execute 'SHOW MEASUREMENTS'
docker compose -f tools/zenoh_gis/phase2b/docker-compose.phase2b.yml down
```

The router image (`rkd:zenohd-influx`, built from `Dockerfile.router`) = the
official `eclipse/zenoh:1.9.0` (zenohd + bundled storage-manager) + the
version-matched InfluxDB backend `.so` downloaded at build time.

## Verified result

`ros2/robot_1/gps/fix` (JSON NavSatFix from the edge sink) is persisted as the
InfluxDB measurement `robot_1/gps/fix` and read back through Grafana (e.g. 354
points after a short run). The dashboard shows ingestion rate (points/10s per
key) and the total stored-point count.

## Three hurdles solved (so the next person doesn't re-hit them)

1. **musl, not glibc.** `eclipse/zenoh` is Alpine/musl. The `-gnu` backend `.so`
   fails with `ld-linux-x86-64.so.2: No such file`. Use the
   `x86_64-unknown-linux-musl-standalone` backend variant.
2. **Config schema (zenohd 1.9).** Plugin search is `plugins_loading.search_dirs`
   (not `plugins_search_dirs`); it covers both the storage-manager plugin and the
   backend `.so` (both at `/` in the image).
3. **Startup race.** The storage volume connects to InfluxDB at plugin load and
   fails (`Connection refused`) if InfluxDB isn't ready. The compose gates the
   router on an InfluxDB `healthcheck` (`condition: service_healthy`).

## Storage model + the rich-field sidecar

The InfluxDB backend stores each sample's payload as a single `value` field (the
JSON string) plus Zenoh metadata (encoding, timestamp) — it does **not** parse
JSON into numeric InfluxDB fields. So out of the box, Grafana panels over
**ingestion/counts and raw values** work directly, but charting
`latitude`/`longitude` as numeric series does not (InfluxQL can't parse a JSON
string field). This is the same key→value time-series storage model discussed for
"Zenoh + InfluxDB" — replay/history, not field-level analytics.

To get **numeric trajectory charts**, the stack includes `influx_field_parser.py`
(the `field_parser` service, pure-stdlib Python, no deps): it reads recent
`*/gps/fix` points, extracts `latitude`/`longitude`/`altitude` into a numeric
`gps_fields` measurement tagged by `robot` (idempotent by timestamp), and the
dashboard's "Latitude (parsed) per robot" panel charts it. This is the
field-extraction step the backend omits; for heavier analytics use the Phase-1
queryables or Kafka+ksqlDB.
