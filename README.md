# Smart-city streaming platform — Project 7 (Team 442G)

Python publishers → Mosquitto → MQTT→Kafka bridge → Kafka (3 brokers, KRaft) →
stream processor (1-min tumbling windows) → InfluxDB → Grafana, all in Docker Compose.

| Part | Owner | Files |
|---|---|---|
| A — Ingestion & workload | Floreanu Maria-Luiza | `publisher.py`, `mqtt_kafka_bridge.py`, `direct_kafka_producer.py`, `entrypoint.sh` |
| B — Kafka, processing & faults | Gavrilă Diana | `compose.yaml`, `stream_processor.py`, Grafana dashboards |
| C — Measurement, automation & analysis | Pruteanu Ioana-Georgiana | `measurement/`, `analysis/`, `scripts/`, `compose.override.yaml`, this README |

## Requirements
- Docker Desktop (WSL 2 backend), ≥ 8 GB RAM for Docker
- Git (for the repository)
- Python ≥ 3.10 on the host: `python -m pip install -r analysis/requirements-analysis.txt`
- All commands work in PowerShell, the PyCharm terminal or any other shell

## Quick start (live demo)
```powershell
docker compose up -d --build
# Grafana: http://localhost:3000  (admin / see compose.yaml)   InfluxDB: http://localhost:8086
docker compose down
```

## Midterm result (weeks 7–8) — one command
```powershell
python scripts/run_midterm.py            # 100 devices, MQTT path, seeds 1–5 (~18 min), then KPIs, CIs, plots
python scripts/run_midterm.py --seeds 1  # quick: one seed (~3.5 min)
```
Produces the Project 7 midterm plots: `results/plots/segments_p50_p95_mqtt_d100.png`
(per-segment p50/p95) and `results/<run>/lag_time.png` (consumer lag).

## One measured run
```powershell
python scripts/run_one.py mqtt 100 1     # <mqtt|direct> <devices> <seed> [--partitions 6] [--duration 120]
```
Produces `logs/<RUN_ID>/*.csv` (raw per-message logs) and `results/<RUN_ID>/` (KPIs + plots),
and appends one row to `results/summary.csv`.

## Full experiment matrix  *(weeks 9–10 — TODO)*
```powershell
python scripts/run_matrix.py             # all 60 runs, then analysis/ci.py and plots
```

## Measurement method
- **One clock**: every container runs on the same host kernel, `time.time()` everywhere.
- **Per-message log** (`measurement/msglog.py`), one CSV per component in `logs/<RUN_ID>/`:

  | file | columns |
  |---|---|
  | `publisher_*.csv` / `direct_*.csv` | device_id, seq, zone, payload_bytes, ts_device |
  | `bridge_*.csv` | device_id, seq, ts_bridge, mqtt_dup_flag |
  | `processor_*.csv` | device_id, seq, zone, topic, partition, offset, value_bytes, ts_device, ts_bridge, ts_kafka_append, kafka_ts_type, ts_consumed, ts_written |
  | `lag.csv` | ts, topic, partition, end_offset, committed_offset, lag_records |

- `ts_kafka_append` is the broker **LogAppendTime** (`kafka_ts_type = LogAppendTime`), verified with
  `docker compose run -T --rm --no-deps bridge python -m measurement.check_logappendtime`.
- Warm-up: first 20 s of each 120 s run discarded (17 %, inside the required 10–20 %); the last 5 s
  of sending are excluded from loss so in-flight messages are not counted as lost.
- Statistics: mean and 95 % CI (Student t, n−1 degrees of freedom) over 5 seeds per matrix point;
  latency as p50/p95, and p99 only when a run has ≥ 1,000 samples.
- Every result is reported with offered load (msg/s and kbps of application payload), run duration
  and number of samples. Plots come only from `analysis/` scripts over the CSV logs.

### KPI definitions
| KPI | definition |
|---|---|
| segment latency | device→bridge = ts_bridge − ts_device; bridge→Kafka = ts_kafka_append − ts_bridge (direct path: device→Kafka = ts_kafka_append − ts_device); Kafka→consumer = ts_consumed − ts_kafka_append; consumer→InfluxDB = ts_written − ts_consumed |
| end-to-end latency | ts_written − ts_device (device → InfluxDB), first delivery of each message |
| sustained throughput | unique messages written to InfluxDB per second inside the measured window, at a load where consumer lag does not grow |
| consumer lag | records: Σ over partitions (end offset − committed offset), sampled every 1 s; seconds: Kafka→consumer latency (p95) |
| scalability | largest device count with p95 end-to-end latency ≤ 2 s |
| bytes per message | Kafka record value size (JSON vs Avro); on the wire: tshark (part A) |
| lost % | sent (device_id, seq) never consumed ÷ sent |
| duplicated % | extra consumptions of the same (device_id, seq) ÷ unique consumed; MQTT QoS 1 duplicates counted separately at the bridge |
| recovery time *(week 11)* | time from broker restart until lag returns to its pre-fault baseline |
| out-of-order % | messages whose seq is lower than one already consumed from the same device |
| late % | messages whose 1-min window had already closed, i.e. window end ≤ (max ts_device seen so far − watermark delay) |

## Contribution statement
*(to fill in for the report: who did what, with links to commits)*

## Use of AI assistants
*(declaration required by the course rules: which tools, for which parts of the code or text)*

## Repository layout
```
compose.yaml              # stack (B)
compose.override.yaml     # logging volume, RUN_ID, devices/seed, lag sampler (C) — merged automatically
measurement/              # msglog.py, lag_sampler.py, check_logappendtime.py (C)
analysis/                 # kpi.py, ci.py, plots.py (C)
scripts/                  # run_one.py, run_midterm.py, run_matrix.py (C)
logs/, results/           # generated, not committed
```
