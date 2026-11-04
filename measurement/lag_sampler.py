"""
Consumer-lag sampler (owner: C).

Every --interval seconds it asks Kafka, for the processor's consumer group:
    end offset (last written)  vs  committed offset (last processed & committed)
and writes one row per partition to  <LOG_DIR>/<RUN_ID>/lag.csv

lag_records = end_offset - committed_offset
Note: the processor uses enable_auto_commit=True (commit every 5 s by default),
so committed offsets move in steps; lag in records therefore has ~5 s granularity.
Lag in SECONDS is computed later by analysis/kpi.py from the per-message log
(ts_consumed - ts_kafka_append), which is exact.

Runs as the `lag` service (profile "measure") in compose.override.yaml.
"""
import argparse
import csv
import os
import time

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from measurement.msglog import LOG_DIR, RUN_ID  # also installs the SIGTERM handler

ap = argparse.ArgumentParser()
ap.add_argument("--brokers", default="kafka:29092,kafka2:29092,kafka3:29092")
ap.add_argument("--group", default="smartcity-processor")
ap.add_argument("--exclude-prefix", default="__,c-",
                help="topics to ignore (internal topics, our own test topics)")
ap.add_argument("--interval", type=float, default=1.0)
args = ap.parse_args()
brokers = args.brokers.split(",")

admin = KafkaAdminClient(bootstrap_servers=brokers)
probe = KafkaConsumer(bootstrap_servers=brokers, enable_auto_commit=False)

run_dir = os.path.join(LOG_DIR, RUN_ID)
os.makedirs(run_dir, exist_ok=True)
f = open(os.path.join(run_dir, "lag.csv"), "w", newline="")
w = csv.writer(f, lineterminator="\n")
w.writerow(["ts", "topic", "partition", "end_offset", "committed_offset", "lag_records"])
print(f"[lag] sampling group={args.group} -> {f.name}", flush=True)

try:
    while True:
        t0 = time.time()
        try:
            skip = tuple(p for p in args.exclude_prefix.split(",") if p)
            topics = [t for t in probe.topics() if not t.startswith(skip)]   # works for telemetry-zone_1 or traffic.z1
            tps = [TopicPartition(t, p) for t in topics
                   for p in (probe.partitions_for_topic(t) or [])]
            if tps:
                ends = probe.end_offsets(tps)
                committed = admin.list_consumer_group_offsets(args.group)
                for tp in tps:
                    c = committed.get(tp)
                    c_off = c.offset if c is not None and c.offset >= 0 else ""
                    lag = ends[tp] - c_off if c_off != "" else ""
                    w.writerow([t0, tp.topic, tp.partition, ends[tp], c_off, lag])
                f.flush()
        except Exception as e:                       # a broker may be down in fault runs
            print(f"[lag] sample failed: {e}", flush=True)
        time.sleep(max(0.0, args.interval - (time.time() - t0)))
finally:
    f.close()
