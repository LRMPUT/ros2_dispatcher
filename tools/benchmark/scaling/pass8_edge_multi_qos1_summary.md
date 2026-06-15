# Edge multi-topic QoS=1 — aggregated metrics

Each cell: 3 reps × 60 s measurement (10 s warm-up discarded).
Target rate per robot: 92.5 msg/s (NavSatFix 10 Hz + Odometry 20 Hz +
LaserScan 50 Hz + PointCloud2 12.5 Hz). Expected/rep = N × 5550.

| Broker | N | Lines/rep | Expected | Delivery | Avg (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Kafka | 1  | 5,550   | 5,550    | 100.0% | 1.17  | 1.07 | 2.14   | 2.71 |
| Kafka | 5  | 27,654  | 27,750   | 99.7%  | 3.47  | 0.79 | 1.54   | 2.24 |
| Kafka | 10 | 53,368  | 55,500   | 96.2%  | 9.10  | 0.84 | 1.76   | 80.09 |
| Kafka | 25 | 65,528  | 138,750  | 47.2%  | 22.73 | 0.90 | 2.68   | 956.37 |
| Kafka | 50 | 10,635  | 277,500  | 3.8%   | 59.95 | 1.02 | 421.97 | 1348.49 |
| MQTT  | 1  | 5,559   | 5,550    | 100.2% | 0.99  | 0.48 | 4.27   | 14.74 |
| MQTT  | 5  | 5,554   | 27,750   | 20.0%  | 1.12  | 0.59 | 3.65   | 15.03 |
| MQTT  | 10 | 5,531   | 55,500   | 10.0%  | 1.55  | 0.65 | 1.32   | 15.21 |
| MQTT  | 25 | 3,827   | 138,750  | 2.8%   | 2.50  | 0.72 | 1.47   | 13.92 |
| MQTT  | 50 | 0       | 277,500  | 0.0%   | —     | —    | —      | — |

## Headline findings

1. **Mosquitto's single-threaded broker saturates at ~92 msg/s aggregate**,
   regardless of fleet size. Beyond N=1 the absolute delivered rate stays
   flat at ~5500 msg/rep while the input rate grows linearly.

2. **Kafka edge scales to N=10** with ≥96 % delivery and sub-2 ms P95.
   At N=25 the producer queue saturates (47 % delivery, P99 ~1 s tail).
   At N=50 the system is essentially failing (3.8 % delivery, P95 422 ms).

3. The multi-topic mix is dominated by LaserScan @ 50 Hz and PointCloud2
   @ 12.5 Hz × 69 KB — ~11 MB/s per 10 robots, ~50 MB/s at N=50. Both
   single-host brokers hit their limits before the dispatcher does.

4. Latency P50 stays below 1.1 ms for all delivered messages on both
   brokers — the dispatcher itself does not introduce per-message delay.
   The visible degradation is entirely broker-side throughput saturation.
