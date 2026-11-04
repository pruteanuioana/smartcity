
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from measurement.msglog import MsgLog  # [C-log]

LOG = MsgLog("processor")  # [C-log]

log_row = LOG.write_kafka  # [C-log] log_row(message, data, ts_consumed, ts_written)

# Kafka - cei 3 brokeri
BOOTSTRAP_SERVERS = [
    "kafka:29092",
    "kafka2:29092",
    "kafka3:29092",
]

TOPICS = [
    "telemetry-zone_1",
    "telemetry-zone_2",
    "telemetry-zone_3",
]

# Configurare InfluxDB
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.environ["INFLUX_TOKEN"]
INFLUX_ORG = os.getenv("INFLUX_ORG", "smartcity")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "telemetry")

WINDOW_SIZE = 60

# Conectare la InfluxDB
influx_client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)

write_api = influx_client.write_api(
    write_options=SYNCHRONOUS
)

# Conectare la Kafka
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="smartcity-processor",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda data: json.loads(
        data.decode("utf-8")
    ),
)

# Statistici pe ferestre de 60 de secunde
windows = defaultdict(
    lambda: {"count": 0, "sum": 0.0}
)

print("Stream Processor started!", flush=True)

for message in consumer:
    ts_consumed = time.time()  # [C-log]
    data = message.value

    zone = data.get("zone_id")
    domain = data.get("domain")
    device_id = data.get("device_id")
    value = data.get("value")
    timestamp = data.get("ts_device")

    if not zone or not domain or timestamp is None:
        log_row(message, data, ts_consumed, None)  # [C-log] consumed but not written
        continue

    if not isinstance(value, (int, float)):
        log_row(message, data, ts_consumed, None)  # [C-log] consumed but not written
        continue

    # 1. Salvam mesajul individual in InfluxDB
    raw_point = (
        Point("raw_telemetry")
        .tag("zone", zone)
        .tag("domain", domain)
        .tag("device_id", str(device_id))
        .field("value", float(value))
        .field("seq", int(data.get("seq", 0)))
        .time(
            datetime.fromtimestamp(
                timestamp, tz=timezone.utc
            ),
            WritePrecision.NS,
        )
    )

    write_api.write(
        bucket=INFLUX_BUCKET,
        record=raw_point,
    )
    log_row(message, data, ts_consumed, time.time())  # [C-log] ts_written

    print(
        f"[INFLUX RAW] {zone} | {domain} | {value}",
        flush=True,
    )

    # 2. Agregam mesajele pe ferestre de 60 secunde
    window_start = int(timestamp // WINDOW_SIZE) * WINDOW_SIZE
    key = (zone, domain, window_start)

    windows[key]["count"] += 1
    windows[key]["sum"] += float(value)

    # Inchidem ferestrele dupa timpul curent
    current_time = time.time()

    completed = [
        key for key in windows
        if key[2] + WINDOW_SIZE <= current_time
    ]

    for key in completed:
        stats = windows.pop(key)

        average = stats["sum"] / stats["count"]

        # 3. Salvam statisticile in InfluxDB
        aggregate_point = (
            Point("aggregated_telemetry")
            .tag("zone", key[0])
            .tag("domain", key[1])
            .field("count", stats["count"])
            .field("average", average)
            .time(
                datetime.fromtimestamp(
                    key[2] + WINDOW_SIZE,
                    tz=timezone.utc,
                ),
                WritePrecision.NS,
            )
        )

        write_api.write(
            bucket=INFLUX_BUCKET,
            record=aggregate_point,
        )

        print(
            f"[INFLUX AGG] zone={key[0]} "
            f"domain={key[1]} "
            f"count={stats['count']} "
            f"average={average:.2f}",
            flush=True,
        )
