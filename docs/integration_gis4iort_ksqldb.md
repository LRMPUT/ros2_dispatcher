# Integracja: ros2_kafka_dispatcher ↔ GIS4IoRT-ksqlDB

Plan integracji oparty na strategii **ksqlDB jako adapter**: nasz `kafka_sink` zostaje
bez zmian w C++; warstwa transformacji JSON na format oczekiwany przez GIS dzieje się
w deklaratywnych zapytaniach ksqlDB.

## Co GIS4IoRT-ksqlDB oczekuje

Repo: https://github.com/AntoniSopata/GIS4IoRT-ksqlDB

| Topik Kafki | Klucz | Wartość (JSON, płaska) |
|---|---|---|
| `ros_gps_fix` | `robot_id` (string) | `{timestamp:BIGINT_ms, latitude, longitude, altitude}` |
| `ros_filtered_odom` | `robot_id` | `{timestamp:BIGINT_ms, position_x, position_y, position_z, source_topic}` |
| `robot_registration` | `robot_id` | `{msg_type:"REGISTRATION", serial_number, topics:[{name, schema}], timestamp}` (raz na start) |
| `robot_health` | `robot_id` | `{msg_type:"PING", serial_number, robot_name, timestamp}` (co 30 s) |

ksqlDB streams `ROS_GPS_FIX_STREAM` / `ROS_FILTERED_ODOM_STREAM` są tworzone **dynamicznie**
przez `robot_manager.py` po odebraniu wiadomości `robot_registration` (zob.
`GIS4IoRT-ksqlDB/deployments/ksqldb/scripts/robot_manager.py:75-98`). Streams mają
`robot_id VARCHAR KEY` i `TIMESTAMP='timestamp'`.

## Co produkuje nasz kafka_sink

- Topiki: `ros2.<normalized_ros_topic>` (np. `/leader/gps/fix` → `ros2.leader.gps.fix`),
  źródło: `kafka_sink_node.cpp:887-907`.
- Format: JSON (po ustawieniu `kafka.payload_format=json`), zagnieżdżony, dokładnie odwzorowuje
  strukturę ROS msg (`kafka_sink_node.cpp:223-288`).
- Klucz wiadomości: statyczny string z parametru `kafka.message_key` (default `"robot"`),
  jeden na sink — `kafka_sink_node.cpp:452, 1072`.
- Headers Kafki: `ros_type=<typ ROS>`.
- Timestamp: ms (zgodny z GIS), `kafka_sink_node.cpp:984-987`.

## Niezgodności i jak je rozwiązujemy

| Problem | Rozwiązanie |
|---|---|
| Inna nazwa topiku (`ros2.leader.gps.fix` vs `ros_gps_fix`) | ksqlDB `CREATE STREAM AS SELECT` z innym `KAFKA_TOPIC` |
| Zagnieżdżony JSON vs płaski (np. `pose.pose.position.x` vs `position_x`) | `STRUCT<>` w schema raw + projekcja `pose->pose->position->x AS position_x` |
| Statyczny klucz `"robot"` vs `robot_id` | `PARTITION BY '<robot_id>'` w ksqlDB (1 sink = 1 robot, twardy literał) |
| Brak `robot_registration` / `robot_health` w kafka_sink | Pozostawiamy lekki Python bridge GIS w trybie tylko-rejestracji (bez subskrypcji ROS) |

## Plan kroków

### 1. Konfiguracja kafka_sink (po stronie ROS)

`ros2_kafka_dispatcher_bringup/config/kafka_sink_for_gis.param.yaml`:

```yaml
/**:
  ros__parameters:
    qos_depth: 10
    subscriptions_yaml: |
      - topic_name: /leader/gps/fix
        msg_type: sensor_msgs/msg/NavSatFix
      - topic_name: /leader/localisation/filtered_odom
        msg_type: nav_msgs/msg/Odometry
    kafka.bootstrap_servers: "localhost:9092"
    kafka.client_id: "kafka_sink_leader"
    kafka.topic_prefix: "ros2"
    kafka.topic_mapping_mode: "prefix_ros_topic"
    kafka.payload_format: "json"
    kafka.message_key: "leader"      # = robot_id w GIS
    kafka.acks: "all"
    kafka.strict_startup: false
```

Dla drugiego robota: skopiuj plik, zmień namespace topiców i `message_key`.

### 2. Adapter ksqlDB (po stronie GIS)

Plik trzymamy w naszym repo: `tools/e2e_gis/ksql-adapter-leader.sql`. Wczytuje się go do
ksqlDB przez REST API (krok 2 w sekcji "Uruchomienie end-to-end"). GIS4IoRT-ksqlDB jest
dołączone jako submodule (`git submodule update --init`) — pozostaje nietknięte.

```sql
SET 'auto.offset.reset' = 'earliest';

-- ============== GPS ==============
CREATE STREAM ros2_leader_gps_raw (
  latitude DOUBLE,
  longitude DOUBLE,
  altitude DOUBLE,
  header STRUCT<stamp STRUCT<sec INT, nanosec INT>, frame_id VARCHAR>
) WITH (
  KAFKA_TOPIC='ros2.leader.gps.fix',
  VALUE_FORMAT='JSON'
);

CREATE STREAM ros_gps_fix_leader_adapter
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
CREATE STREAM ros2_leader_odom_raw (
  pose STRUCT<pose STRUCT<position STRUCT<x DOUBLE, y DOUBLE, z DOUBLE>>>,
  header STRUCT<stamp STRUCT<sec INT, nanosec INT>, frame_id VARCHAR>
) WITH (
  KAFKA_TOPIC='ros2.leader.localisation.filtered_odom',
  VALUE_FORMAT='JSON'
);

CREATE STREAM ros_filtered_odom_leader_adapter
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
```

Dla każdego nowego robota powtórz blok zmieniając `leader` → `<robot_id>` (4 podstawienia
per blok: nazwy streamów, `KAFKA_TOPIC` raw, literał w `'<id>' AS robot_id` i `PARTITION BY`).

### 3. REGISTRATION + healthcheck po stronie dispatchera

Pythonowy `ros2_kafka_healthcheck_bridge.py` z GIS robi trzy rzeczy: subskrybuje ROS,
mostkuje dane do Kafki, oraz wysyła REGISTRATION + PING. Dwie pierwsze przejmuje nasz
`kafka_sink`. Trzecią (REGISTRATION + PING, bez ROS-a) pełni dedykowany lifecycle node
**`gis_health_node`** w tym repo (`kafka_bridge/gis_health_node/`).

Konfiguracja: `kafka_bridge/gis_health_node/config/gis_health_node.param.yaml`

```yaml
/**:
  ros__parameters:
    kafka.bootstrap_servers: "localhost:9092"
    kafka.client_id: "gis_health_node"
    kafka.acks: "1"
    kafka.strict_startup: false

    gis.robot_list: ["leader"]              # robot_id (klucz Kafki)
    gis.health_topic: "robot_health"
    gis.registration_topic: "robot_registration"
    gis.health_period_s: 30.0
    gis.odom_kafka_topic: "ros_filtered_odom"
    gis.gps_kafka_topic: "ros_gps_fix"
```

Co robi:
- `on_activate` → wysyła `REGISTRATION` na `robot_registration` dla każdego robota
  z `robot_list` (z kompletną definicją schematu `ros_filtered_odom` i `ros_gps_fix`),
  zaraz potem pierwszy `PING`
- timer 30 s → `PING` na `robot_health`
- `on_deactivate` → zatrzymuje timer
- `on_cleanup` → niszczy KafkaProducera

Płaski schemat w REGISTRATION pasuje do tego co produkuje **adapter ksqlDB** z kroku 2,
a `robot_manager.py` w GIS automatycznie utworzy `ROS_GPS_FIX_STREAM` /
`ROS_FILTERED_ODOM_STREAM` po odebraniu pierwszej rejestracji (`robot_manager.py:75-98`).
Kolejność uruchamiania: GIS broker → adapter SQL → kafka_sink → gis_health_node.

### 4. Mapowanie nazw

Healthcheck bridge mapuje numeryczne id `101` → namespace `leader`
(`ros2_kafka_healthcheck_bridge.py:33`). Dla spójności trzymaj się jednej konwencji:

- Jeżeli ROS publikuje pod `/leader/...`, użyj `robot_id="leader"` wszędzie
  (kafka_sink `message_key`, ksqlDB `PARTITION BY`, bridge `robot_list`).
- Jeśli wolisz id numeryczne `101`: subskrybuj w sinku `/leader/...`, ale `message_key="101"`,
  i w SQL `'101' AS robot_id`. Wtedy `source_topic` w odom payload też podaj jako
  `/101/localisation/filtered_odom` (tak robi GIS bridge).

### 5. Uruchomienie end-to-end

```bash
# 0. Submodule GIS (raz po klonowaniu repo)
git submodule update --init --recursive

# 1. Broker GIS (Kafka + Schema Registry + ksqlDB + Kafka UI; pomijamy api/ros2_bridge)
cd GIS4IoRT-ksqlDB/deployments/ksqldb
docker compose up -d --build broker schema-registry kafka-setup ksqldb-server \
  ksqldb-init robot-manager kafka-ui
# Sprawdź: localhost:8090 (Kafka UI), localhost:8088/info (ksqlDB)

# 2. Załaduj adapter SQL przez ksqldb-cli (sieć ksqldb_default)
docker run --rm --network ksqldb_default \
  -v "$(pwd)/../../../tools/e2e_gis/ksql-adapter-leader.sql:/tmp/adapter.sql:ro" \
  confluentinc/ksqldb-cli:0.29.0 ksql http://ksqldb-server:8088 -f /tmp/adapter.sql

# 3. kafka_sink (mostkowanie danych ROS → Kafka)
ros2 launch ros2_kafka_dispatcher_bringup kafka_sink.launch.py \
  config_file:=$(pwd)/ros2_kafka_dispatcher_bringup/config/kafka_sink_for_gis.param.yaml
# Następnie przejdź lifecycle: configure → activate
ros2 lifecycle set /kafka_sink configure
ros2 lifecycle set /kafka_sink activate

# 4. gis_health_node (REGISTRATION + PING co 30 s)
ros2 launch gis_health_node gis_health_node.launch.py
ros2 lifecycle set /gis_health_node configure
ros2 lifecycle set /gis_health_node activate

# 5. Ruch ROS — bag, symulator albo realny robot publikujący /leader/gps/fix
#    i /leader/localisation/filtered_odom
ros2 bag play <leader_bag>
```

## Weryfikacja

```bash
# A. Czy raw topic ma wiadomości
docker exec -it broker kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic ros2.leader.gps.fix --max-messages 3

# B. Czy adapter produkuje docelowy topic
docker exec -it broker kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic ros_gps_fix --property print.key=true --max-messages 3

# C. ksqlDB stream
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
> SELECT * FROM ROS_GPS_FIX_STREAM EMIT CHANGES LIMIT 5;
```

Spodziewane:
- B drukuje klucz `leader` (lub `101`) i wartość `{"timestamp":..., "latitude":..., ...}`.
- C drukuje wiersze z `ROBOT_ID`, `TIMESTAMP`, `LATITUDE`, `LONGITUDE`, `ALTITUDE`.

## Ograniczenia i kierunki rozwoju

1. **1 robot = 1 instancja kafka_sink** — bo `kafka.message_key` jest globalnie statyczny.
   Naturalne rozszerzenie: dodać do `kafka_sink` parametr `kafka.message_key_source`
   z opcją `from_topic_segment[N]`, który wyciąga klucz z normalizowanej nazwy topika.
   Wtedy jeden sink mógłby obsłużyć wiele robotów.
2. **Ręczne pisanie SQL per robot** — można wygenerować templatem (np. Jinja2) z
   listy robotów w bringupie.
3. **Nie używamy Schema Registry** — GIS i nasz sink jadą gołym JSON-em. Avro byłby
   krokiem dalej (kafka_sink ma już `payload_format` jako wybór, można dodać `avro`).
4. **Pętle współzależne**: `robot_manager.py` musi być uruchomiony *przed* `gis_health_node`,
   żeby złapać REGISTRATION. Inaczej streams GIS się nie utworzą i adapter nie ma
   kogo zasilić. Bezpieczne: dodaj 2-3 sekundowe opóźnienie między uruchomieniem
   kontenera GIS a `gis_health_node`, albo wpinaj `gis_health_node` w lifecycle
   po uruchomieniu kafka_sink.
5. **Naturalna integracja w bringup**: `dispatcher_controller` może zarządzać
   `gis_health_node` tym samym mechanizmem co `kafka_sink`/`mosquitto_sink` (przez
   `lifecycle_msgs/srv/ChangeState`). Aktualnie node startuje samodzielnie z launch
   file — to jest TODO dla composed bringup.

## Mapa plików i linii

| Co | Plik | Linia |
|---|---|---|
| kafka_sink — JSON build | `kafka_bridge/kafka_sink/src/kafka_sink_node.cpp` | 104-221, 223-288 |
| kafka_sink — normalizacja topiku | `kafka_bridge/kafka_sink/src/kafka_sink_node.cpp` | 887-907 |
| kafka_sink — `message_key` | `kafka_bridge/kafka_sink/src/kafka_sink_node.cpp` | 452, 1072-1086 |
| kafka_sink — `payload_format` switch | `kafka_bridge/kafka_sink/src/kafka_sink_node.cpp` | 461, 830-838, 991-1009 |
| GIS — ksqlDB init | `GIS4IoRT-ksqlDB/deployments/ksqldb/ksql-setup.sql` | 1-19 |
| Dispatcher — gis_health_node lifecycle | `kafka_bridge/gis_health_node/src/gis_health_node.cpp` | 47-104 |
| Dispatcher — REGISTRATION/PING JSON | `kafka_bridge/gis_health_node/src/gis_health_node.cpp` | 213-270 |
| GIS — referencyjny Python bridge (już niewykorzystywany) | `GIS4IoRT-ksqlDB/deployments/ksqldb/scripts/ros2_kafka_healthcheck_bridge.py` | 83-157 |
| GIS — robot_manager (tworzenie streams) | `GIS4IoRT-ksqlDB/deployments/ksqldb/scripts/robot_manager.py` | 75-122 |
| GIS — geofence query | `GIS4IoRT-ksqlDB/app/adapters/ksqldb/routers/geofence.py` | 31-49 |
| GIS — speed query | `GIS4IoRT-ksqlDB/app/adapters/ksqldb/routers/speed.py` | 45-74 |
| GIS — humidity query | `GIS4IoRT-ksqlDB/app/adapters/ksqldb/routers/humidity.py` | 38-74 |
