"""
KPI computation for ONE run (owner: C).

Usage:
    python analysis/kpi.py logs/<RUN_ID> [--warmup 20] [--tail 5] [--lateness 5]

Reads   logs/<RUN_ID>/{publisher|direct}_*.csv, bridge_*.csv, processor_*.csv, lag.csv, meta.json
Writes  results/<RUN_ID>/kpi.json
        results/summary.csv          (one row per run; re-running a run replaces its row)

Measurement window (roadmap section 4):
    t0 = first ts_device of the run
    keep messages with  t0 + warmup  <=  ts_device  <=  t_last_sent - tail
    (warm-up: first 20 s discarded; tail: last few seconds excluded so messages still
     "in flight" when the publishers stop are not counted as lost)

Segments (ms):
    mqtt path : device->bridge = ts_bridge - ts_device
                bridge->kafka  = ts_kafka_append - ts_bridge
    direct    : device->kafka  = ts_kafka_append - ts_device
    both      : kafka->consumer  = ts_consumed - ts_kafka_append   (= time lag of the consumer)
                consumer->written = ts_written - ts_consumed        (InfluxDB write)
                end_to_end        = ts_written - ts_device
"""
import argparse
import glob
import json
import os

import numpy as np
import pandas as pd

KEY = ["device_id", "seq"]


def load(run_dir, component):
    files = sorted(glob.glob(os.path.join(run_dir, f"{component}_*.csv")))
    if not files:
        return None
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


P99_MIN_SAMPLES = 1000   # course rule: p99 only when a run yields at least 1,000 samples


def stats_ms(series):
    s = series.dropna().to_numpy() * 1000.0
    if len(s) == 0:
        return {"n": 0}
    out = {"n": int(len(s)), "mean": float(s.mean()),
           "p50": float(np.percentile(s, 50)), "p95": float(np.percentile(s, 95)),
           "p99": float(np.percentile(s, 99)) if len(s) >= P99_MIN_SAMPLES else None,
           "max": float(s.max())}
    return out


def compute(run_dir, warmup=20.0, tail=5.0, lateness=5.0, window=60.0):
    meta_path = os.path.join(run_dir, "meta.json")
    meta = json.load(open(meta_path)) if os.path.exists(meta_path) else {}
    meta.setdefault("run_id", os.path.basename(os.path.normpath(run_dir)))

    sent = load(run_dir, "publisher")
    path = "mqtt"
    if sent is None:
        sent = load(run_dir, "direct")
        path = "direct"
    if sent is None:
        raise SystemExit(f"no publisher_*.csv or direct_*.csv in {run_dir}")
    meta.setdefault("path", path)
    proc = load(run_dir, "processor")
    if proc is None:
        raise SystemExit(f"no processor_*.csv in {run_dir}")
    bridge = load(run_dir, "bridge")

    # ---- measurement window -------------------------------------------------
    t0, t_last = sent.ts_device.min(), sent.ts_device.max()
    w_start, w_end = t0 + warmup, t_last - tail
    w_len = w_end - w_start
    sent_w = sent[(sent.ts_device >= w_start) & (sent.ts_device <= w_end)]
    sent_keys = sent_w[KEY].drop_duplicates()

    proc_w = proc.merge(sent_keys, on=KEY, how="inner")          # only messages sent in window
    proc_w = proc_w.sort_values(["ts_consumed", "partition", "offset"])
    first = proc_w.drop_duplicates(KEY, keep="first")            # first delivery of each message

    # ---- latency per segment -----------------------------------------------
    lat = {}
    if path == "mqtt":
        lat["device_to_bridge"] = stats_ms(first.ts_bridge - first.ts_device)
        lat["bridge_to_kafka"] = stats_ms(first.ts_kafka_append - first.ts_bridge)
    else:
        lat["device_to_kafka"] = stats_ms(first.ts_kafka_append - first.ts_device)
    lat["kafka_to_consumer"] = stats_ms(first.ts_consumed - first.ts_kafka_append)
    lat["consumer_to_written"] = stats_ms(first.ts_written - first.ts_consumed)
    lat["end_to_end"] = stats_ms(first.ts_written - first.ts_device)

    # ---- throughput ---------------------------------------------------------
    n_sent, n_delivered = len(sent_keys), len(first)
    offered = n_sent / w_len
    offered_kbps = sent_w.drop_duplicates(KEY).payload_bytes.sum() * 8 / 1000 / w_len   # application payload
    # sustained throughput = unique messages written per second, counted by ts_written
    written = first.dropna(subset=["ts_written"])
    tw = written[(written.ts_written >= w_start) & (written.ts_written <= w_end)]
    throughput = len(tw) / w_len

    # ---- loss / duplicates --------------------------------------------------
    lost = n_sent - n_delivered
    dup_rows = len(proc_w) - n_delivered
    bridge_dup_pct = None
    if bridge is not None:
        b_w = bridge.merge(sent_keys, on=KEY, how="inner")
        b_unique = len(b_w.drop_duplicates(KEY))
        bridge_dup_pct = 100.0 * (len(b_w) - b_unique) / max(b_unique, 1)
        bridge_missing_pct = 100.0 * (n_sent - b_unique) / max(n_sent, 1)
    not_written = int(first.ts_written.isna().sum())

    # ---- out-of-order (per device, in consumption order) --------------------
    prev_max = first.groupby("device_id").seq.cummax().groupby(first.device_id).shift(1)
    ooo = int((first.seq < prev_max).sum())

    # ---- late vs 1-min tumbling window + watermark --------------------------
    # watermark seen by message i = max(ts_device of messages consumed BEFORE i) - lateness
    wm = first.ts_device.cummax().shift(1) - lateness
    window_end = np.floor(first.ts_device / window) * window + window
    late = int((window_end <= wm).sum())

    # ---- consumer lag (records) from lag sampler ----------------------------
    lag = {}
    lag_path = os.path.join(run_dir, "lag.csv")
    if os.path.exists(lag_path):
        L = pd.read_csv(lag_path).dropna(subset=["lag_records"])
        L = L[(L.ts >= w_start) & (L.ts <= w_end)]
        if len(L):
            total = L.groupby("ts").lag_records.sum()
            slope = float(np.polyfit(total.index - total.index[0], total.values, 1)[0]) if len(total) > 2 else None
            lag = {"mean_records": float(total.mean()), "max_records": float(total.max()),
                   "growth_records_per_s": slope}
    lag["time_lag_p95_s"] = lat["kafka_to_consumer"].get("p95", float("nan")) / 1000.0

    # ---- sanity: LogAppendTime --------------------------------------------
    lat_share = float((proc.kafka_ts_type.astype(str) == "LogAppendTime").mean() * 100.0)

    kpi = {
        **meta,
        "run_duration_s": t_last - t0, "window_s": w_len, "warmup_s": warmup,
        "warmup_pct": 100.0 * warmup / max(t_last - t0, 1e-9), "tail_s": tail,
        "sent": n_sent, "delivered_unique": n_delivered, "samples": n_delivered,
        "offered_msg_s": offered, "offered_kbps": offered_kbps, "throughput_msg_s": throughput,
        "lost_pct": 100.0 * lost / max(n_sent, 1),
        "dup_pct": 100.0 * dup_rows / max(n_delivered, 1),
        "bridge_qos1_dup_pct": bridge_dup_pct,
        "bridge_missing_pct": bridge_missing_pct if bridge is not None else None,
        "not_written_pct": 100.0 * not_written / max(n_delivered, 1),
        "out_of_order_pct": 100.0 * ooo / max(n_delivered, 1),
        "late_pct": 100.0 * late / max(n_delivered, 1), "lateness_s": lateness,
        "kafka_value_bytes_mean": float(proc_w.value_bytes.mean()) if len(proc_w) else None,
        "logappendtime_pct": lat_share,
        "latency_ms": lat, "lag": lag,
    }
    return kpi


def flatten(kpi):
    row = {k: v for k, v in kpi.items() if k not in ("latency_ms", "lag")}
    for seg, st in kpi["latency_ms"].items():
        for k, v in st.items():
            row[f"lat_{seg}_{k}"] = v
    for k, v in kpi["lag"].items():
        row[f"lag_{k}"] = v
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--warmup", type=float, default=20.0)
    ap.add_argument("--tail", type=float, default=5.0)
    ap.add_argument("--lateness", type=float, default=5.0, help="watermark delay (match B's processor)")
    ap.add_argument("--results", default="results")
    a = ap.parse_args()

    kpi = compute(a.run_dir, a.warmup, a.tail, a.lateness)
    out_dir = os.path.join(a.results, kpi["run_id"])
    os.makedirs(out_dir, exist_ok=True)
    json.dump(kpi, open(os.path.join(out_dir, "kpi.json"), "w"), indent=2)

    summary = os.path.join(a.results, "summary.csv")
    row = pd.DataFrame([flatten(kpi)])
    if os.path.exists(summary):
        old = pd.read_csv(summary)
        row = pd.concat([old[old.run_id != kpi["run_id"]], row], ignore_index=True)
    row.to_csv(summary, index=False)

    e2e = kpi["latency_ms"]["end_to_end"]
    print(f"run {kpi['run_id']}  path={kpi['path']}  duration {kpi['run_duration_s']:.0f}s, "
          f"warm-up {kpi['warmup_s']:.0f}s ({kpi['warmup_pct']:.0f}%), measured window {kpi['window_s']:.0f}s")
    print(f"  offered {kpi['offered_msg_s']:.1f} msg/s = {kpi['offered_kbps']:.1f} kbps, "
          f"throughput {kpi['throughput_msg_s']:.1f} msg/s, sent {kpi['sent']}, samples {kpi['samples']}")
    for seg, st in kpi["latency_ms"].items():
        if st.get("n"):
            p99 = f"{st['p99']:8.1f}" if st["p99"] is not None else "     n/a"
            print(f"  {seg:20s} p50 {st['p50']:8.1f}  p95 {st['p95']:8.1f}  p99 {p99} ms  (n={st['n']})")
    bdup = kpi["bridge_qos1_dup_pct"]
    bdup = f"{bdup:.3f}%" if bdup is not None else "n/a"
    print(f"  lost {kpi['lost_pct']:.3f}%  dup {kpi['dup_pct']:.3f}%  bridge QoS1 dup {bdup}  "
          f"out-of-order {kpi['out_of_order_pct']:.3f}%  late {kpi['late_pct']:.3f}%")
    print(f"  lag: {kpi['lag']}")
    if not 9.9 <= kpi["warmup_pct"] <= 20.1:
        print(f"  WARNING: warm-up is {kpi['warmup_pct']:.0f}% of the run; the rule is 10-20%")
    if kpi["logappendtime_pct"] < 100:
        print(f"  WARNING: only {kpi['logappendtime_pct']:.1f}% of records have LogAppendTime!")
    print(f"  -> {out_dir}/kpi.json, {summary}")


if __name__ == "__main__":
    main()
