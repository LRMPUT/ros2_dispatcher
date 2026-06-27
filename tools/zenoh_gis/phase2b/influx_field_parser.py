#!/usr/bin/env python3
# Rich-field sidecar: the Zenoh InfluxDB backend stores each NavSatFix as one
# JSON `value` string field (InfluxQL can't parse it). This loop reads recent
# `robot_*/gps/fix` points, extracts latitude/longitude/altitude as NUMERIC
# fields into a `gps_fields` measurement (tagged by robot), so Grafana can chart
# trajectories. Idempotent: re-writing the same (timestamp, robot) overwrites,
# so re-scanning a window never duplicates. Pure stdlib (no pip).
import json
import time
import urllib.parse
import urllib.request

INFLUX = "http://influxdb:8086"
DB = "zenoh_ros2"


def query(q):
    url = f"{INFLUX}/query?" + urllib.parse.urlencode({"db": DB, "q": q, "epoch": "ns"})
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.load(r)


def write(lines):
    if not lines:
        return
    body = "\n".join(lines).encode()
    req = urllib.request.Request(f"{INFLUX}/write?db={DB}&precision=ns", data=body, method="POST")
    urllib.request.urlopen(req, timeout=5).read()


def measurements():
    res = query("SHOW MEASUREMENTS")
    series = res["results"][0].get("series", [])
    if not series:
        return []
    return [v[0] for v in series[0].get("values", []) if v[0].endswith("/gps/fix")]


def main():
    while True:
        try:
            lines = []
            for m in measurements():
                robot = m.split("/")[0]
                res = query(f'SELECT "value" FROM "{m}" WHERE time > now() - 2m')
                series = res["results"][0].get("series", [])
                if not series:
                    continue
                cols = series[0]["columns"]
                ti, vi = cols.index("time"), cols.index("value")
                for row in series[0]["values"]:
                    try:
                        p = json.loads(row[vi])
                        lat, lon = p.get("latitude"), p.get("longitude")
                        alt = p.get("altitude", 0.0)
                        if lat is None or lon is None:
                            continue
                        lines.append(
                            f"gps_fields,robot={robot} "
                            f"latitude={float(lat)},longitude={float(lon)},altitude={float(alt)} {int(row[ti])}")
                    except (ValueError, TypeError):
                        continue
            write(lines)
        except Exception as e:  # noqa: BLE001 - keep the sidecar alive
            print(f"[field_parser] {type(e).__name__}: {e}", flush=True)
        time.sleep(3)


if __name__ == "__main__":
    main()
