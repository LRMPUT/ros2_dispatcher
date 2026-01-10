# Mosquitto Broker (mosquitto_brocker)

This folder contains a lightweight Mosquitto MQTT broker setup intended to be run with `docker-compose`.

Contents
- `docker-compose.yml` — compose file that starts the `mosquitto` service and an `mqtt-explorer` helper container.
- `mosquitto.conf` — mosquitto configuration mounted into the container.
- `Dockerfile` — minimal Dockerfile (based on `eclipse-mosquitto:2`).

Usage

Start services with:

```bash
cd src/external/ros2_kafka_dispatcher/mosquitto_bridge/mosquitto_brocker
docker-compose up -d
```

The broker will be reachable on the host port configured by the `MOSQUITTO_PORT` environment variable (defaults to `1884`), mapped to container `1883`.

Configuration
- `mosquitto.conf` is mounted into the container at `/mosquitto/config/mosquitto.conf`.
- Broker data and logs are persisted in compose-managed volumes `mosquitto_data` and `mosquitto_log`.

MQTT Explorer

The compose file previously included an `mqtt-explorer` service which used an image that may not exist on Docker Hub. That service has been removed to avoid compose failures when pulling the image.

If you want a containerized MQTT Explorer, consider one of these options:

- `jlesage/mqtt-explorer` — a maintained web/GUI wrapper image (adjust ports as needed).
- Run the official MQTT Explorer desktop app on your host and connect to the broker at `localhost:${MOSQUITTO_PORT:-1884}`.

If you prefer a containerized MQTT Explorer GUI, add a maintained image to `docker-compose.yml`.

Note: a previously-added `mqtt-explorer` service caused docker pull failures on some systems and has been removed to allow `docker-compose up` to run reliably.

Accessing a GUI

- Run the MQTT Explorer desktop app and connect to `localhost:${MOSQUITTO_PORT:-1884}`.
- Or add a containerized GUI image (for example a maintained image that provides a web UI) and expose its port in `docker-compose.yml`.

Listing / watching topics from the host

You can subscribe to all topics (shows retained messages immediately and live messages as they arrive) with:

```bash
mosquitto_sub -h localhost -p 1883 -t '#' -v 
```

If you want, tell me a specific GUI image (and ports) and I will add it and verify the compose file.

Notes
- The legacy `run.sh` helper script was removed because `docker-compose` covers running/building the broker and services. If you need a local image build workflow, the `Dockerfile` can be used to build a custom image.

If you'd like, I can also:
- Restore `run.sh` instead of removing it
- Remove the `Dockerfile` if you prefer to always use the upstream image
- Change the MQTT Explorer image to a different implementation
