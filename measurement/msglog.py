"""
Per-message CSV log writer (owner: C - Measurement).

Every component imports this and writes ONE row per message it handles.
Files land in   <LOG_DIR>/<RUN_ID>/<component>_<hostname>.csv
(hostname is added so several publisher containers never overwrite each other).

Environment variables (set by compose.override.yaml / scripts/run_one.py):
    LOG_DIR   default "logs"      (in containers: /app/logs, bind-mounted to ./logs)
    RUN_ID    default "dev"
    MSGLOG    "1" = on (default), "0" = off

All timestamps are time.time() = seconds since epoch (float).
All containers run on the same host kernel, so they share ONE clock.
"""
import atexit
import csv
import os
import signal
import socket
import sys
import time

LOG_DIR = os.getenv("LOG_DIR", "logs")
RUN_ID = os.getenv("RUN_ID", "dev")
ENABLED = os.getenv("MSGLOG", "1") == "1"
FLUSH_EVERY_S = 1.0

# Column schemas, one per component (the "log schema" from the roadmap, section 4).
SCHEMAS = {
    # what was SENT (ground truth for loss): MQTT publisher or direct Kafka producer
    "publisher": ["device_id", "seq", "zone", "payload_bytes", "ts_device"],
    "direct":    ["device_id", "seq", "zone", "payload_bytes", "ts_device"],
    # what the bridge RECEIVED from MQTT (QoS 1 duplicates are counted here)
    "bridge":    ["device_id", "seq", "ts_bridge", "mqtt_dup_flag"],
    # what the stream processor CONSUMED and WROTE: the full per-message row
    "processor": ["device_id", "seq", "zone", "topic", "partition", "offset",
                  "value_bytes", "ts_device", "ts_bridge", "ts_kafka_append",
                  "kafka_ts_type", "ts_consumed", "ts_written"],
}


class MsgLog:
    def __init__(self, component: str):
        self.fields = SCHEMAS[component]
        self._f = None
        if not ENABLED:
            return
        run_dir = os.path.join(LOG_DIR, RUN_ID)
        os.makedirs(run_dir, exist_ok=True)
        path = os.path.join(run_dir, f"{component}_{socket.gethostname()}.csv")
        is_new = not os.path.exists(path)
        self._f = open(path, "a", newline="")
        self._w = csv.writer(self._f, lineterminator="\n")
        if is_new:
            self._w.writerow(self.fields)
        self._last_flush = time.time()
        atexit.register(self.close)
        print(f"[msglog] {component} -> {path}", flush=True)

    def write(self, *values):
        """Write one row; values must follow self.fields order. None -> empty cell."""
        if self._f is None:
            return
        self._w.writerow(values)
        now = time.time()
        if now - self._last_flush >= FLUSH_EVERY_S:   # flush ~1x/s, not every row
            self._f.flush()
            self._last_flush = now

    def write_kafka(self, msg, data, ts_consumed, ts_written):
        """Processor row. Works with kafka-python(-ng) ConsumerRecord AND confluent-kafka Message."""
        topic, partition, offset, nbytes, ts_append, ts_type = kafka_fields(msg)
        self.write(data.get("device_id"), data.get("seq"), data.get("zone", data.get("zone_id")),
                   topic, partition, offset, nbytes,
                   data.get("ts_device"), data.get("ts_bridge"),
                   ts_append, ts_type, ts_consumed, ts_written)

    def close(self):
        if self._f is not None and not self._f.closed:
            self._f.flush()
            self._f.close()


def kafka_fields(msg):
    """(topic, partition, offset, value_bytes, ts_kafka_append [s], ts_type) for either Kafka client.
    ts_type is "LogAppendTime" (set by the broker - what we need) or "CreateTime" (set by the producer)."""
    if callable(getattr(msg, "topic", None)):                 # confluent-kafka Message
        t_type, t_ms = msg.timestamp()                        # 2 = LOG_APPEND_TIME, 1 = CREATE_TIME
        raw = msg.value() or b""
        return (msg.topic(), msg.partition(), msg.offset(), len(raw), t_ms / 1000.0,
                {2: "LogAppendTime", 1: "CreateTime"}.get(t_type, "NotAvailable"))
    # kafka-python / kafka-python-ng ConsumerRecord             # 1 = LogAppendTime, 0 = CreateTime
    return (msg.topic, msg.partition, msg.offset, msg.serialized_value_size, msg.timestamp / 1000.0,
            "LogAppendTime" if msg.timestamp_type == 1 else "CreateTime")


def _on_sigterm(signum, frame):
    # `docker compose stop` sends SIGTERM to PID 1 (our python process).
    # PID 1 ignores SIGTERM unless it installs a handler -> we exit cleanly
    # so atexit runs and the last rows are flushed to disk.
    sys.exit(0)


signal.signal(signal.SIGTERM, _on_sigterm)
