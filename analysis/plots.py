"""
Plots from the CSV logs only (roadmap: "Plots only from scripts over the CSV logs").
Owner: C.

  python analysis/plots.py run logs/<RUN_ID>      -> results/<RUN_ID>/*.png  (one run)
  python analysis/plots.py segments [--path mqtt --devices 100]
                                                  -> results/plots/segments_p50_p95_<path>_d<N>.png
                                                     (MIDTERM BAR: per-segment p50/p95, mean ± 95 % CI over seeds)
  python analysis/plots.py compare                -> results/plots/*.png      (across runs, needs ci.py first)

Every figure carries the sanity numbers (offered load in msg/s and kbps, duration, samples).
"""
import argparse
import glob
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from kpi import KEY, compute, load  # noqa: E402

# fixed categorical order (never cycled): blue, orange, aqua, yellow, magenta, green, violet, red
C = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
plt.rcParams.update({"figure.dpi": 120, "savefig.dpi": 150, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.grid": True, "grid.alpha": 0.25,
                     "lines.linewidth": 2, "font.size": 10})


def _save(fig, out_dir, name):
    os.makedirs(out_dir, exist_ok=True)
    p = os.path.join(out_dir, name)
    fig.tight_layout()
    fig.savefig(p)
    plt.close(fig)
    print("  ", p)


def plot_run(run_dir, results="results", warmup=20.0):
    run_id = os.path.basename(os.path.normpath(run_dir))
    out = os.path.join(results, run_id)
    sent = load(run_dir, "publisher")
    path = "mqtt"
    if sent is None:
        sent, path = load(run_dir, "direct"), "direct"
    proc = load(run_dir, "processor")
    first = proc.sort_values("ts_consumed").drop_duplicates(KEY, keep="first")
    t0 = sent.ts_device.min()
    k = compute(run_dir, warmup=warmup)
    sub = (f"{run_id}\noffered {k['offered_msg_s']:.0f} msg/s ({k['offered_kbps']:.0f} kbps), "
           f"run {k['run_duration_s']:.0f} s, warm-up {warmup:.0f} s discarded, n = {k['samples']}")

    # 1) CDF of each latency segment
    segs = ([("device→bridge", first.ts_bridge - first.ts_device),
             ("bridge→Kafka", first.ts_kafka_append - first.ts_bridge)] if path == "mqtt" else
            [("device→Kafka", first.ts_kafka_append - first.ts_device)])
    segs += [("Kafka→consumer", first.ts_consumed - first.ts_kafka_append),
             ("consumer→InfluxDB", first.ts_written - first.ts_consumed),
             ("end-to-end", first.ts_written - first.ts_device)]
    m = first.ts_device >= t0 + warmup
    fig, ax = plt.subplots(figsize=(7, 4))
    for i, (name, s) in enumerate(segs):
        v = np.sort((s[m].dropna().to_numpy()) * 1000)
        v = v[v > 0]
        if len(v):
            ax.plot(v, np.arange(1, len(v) + 1) / len(v), color=C[i], label=name)
    for q in (0.5, 0.95, 0.99):
        ax.axhline(q, color="#888", lw=0.8, ls=":")
    ax.set_xscale("log")
    ax.set_xlabel("latency (ms, log scale)")
    ax.set_ylabel("fraction of messages ≤ x")
    ax.set_title(f"Latency CDF per segment — {sub}", fontsize=8, loc="left")
    ax.legend(frameon=False, fontsize=8)
    _save(fig, out, "latency_cdf.png")

    # 2) end-to-end latency over time: per-second p50 / p95
    e = first.assign(t=(first.ts_device - t0).astype(int), e2e=(first.ts_written - first.ts_device) * 1000)
    g = e.groupby("t").e2e
    fig, ax = plt.subplots(figsize=(7, 3.5))
    ax.plot(g.median().index, g.median().values, color=C[0], label="p50")
    ax.plot(g.quantile(0.95).index, g.quantile(0.95).values, color=C[1], label="p95")
    ax.axvspan(0, warmup, color="#999", alpha=0.15, lw=0, label="warm-up (discarded)")
    ax.set_xlabel("time since first message (s)")
    ax.set_ylabel("end-to-end latency (ms)")
    ax.set_title(f"End-to-end latency over time — {sub}", fontsize=8, loc="left")
    ax.legend(frameon=False, fontsize=8)
    _save(fig, out, "e2e_latency_time.png")

    # 3) throughput: offered (sent/s) vs written (unique/s)
    s_rate = ((sent.ts_device - t0).astype(int)).value_counts().sort_index()
    w = first.dropna(subset=["ts_written"])
    w_rate = ((w.ts_written - t0).astype(int)).value_counts().sort_index()
    s_rate, w_rate = s_rate.iloc[:-1], w_rate.iloc[:-1]      # last second is partial
    fig, ax = plt.subplots(figsize=(7, 3.5))
    ax.plot(s_rate.index, s_rate.values, color=C[0], label="offered (sent)")
    ax.plot(w_rate.index, w_rate.values, color=C[1], label="written to InfluxDB")
    ax.axvspan(0, warmup, color="#999", alpha=0.15, lw=0)
    ax.set_xlabel("time since first message (s)")
    ax.set_ylabel("messages / s")
    ax.set_title(f"Throughput — {sub}", fontsize=8, loc="left")
    ax.legend(frameon=False, fontsize=8)
    _save(fig, out, "throughput_time.png")

    # 4) consumer lag (records) from lag sampler
    lp = os.path.join(run_dir, "lag.csv")
    if os.path.exists(lp):
        L = pd.read_csv(lp).dropna(subset=["lag_records"])
        if len(L):
            tot = L.groupby("ts").lag_records.sum()
            fig, ax = plt.subplots(figsize=(7, 3.5))
            ax.step(tot.index - t0, tot.values, where="post", color=C[0])
            ax.axvspan(0, warmup, color="#999", alpha=0.15, lw=0)
            ax.set_xlabel("time since first message (s)")
            ax.set_ylabel("consumer lag (records, all partitions)")
            ax.set_title(f"Consumer lag — {sub}", fontsize=8, loc="left")
            _save(fig, out, "lag_time.png")


SEG_LABELS = {"device_to_bridge": "device→bridge", "bridge_to_kafka": "bridge→Kafka",
              "device_to_kafka": "device→Kafka", "kafka_to_consumer": "Kafka→consumer",
              "consumer_to_written": "consumer→InfluxDB", "end_to_end": "end-to-end"}


def plot_segments(results="results", path="mqtt", devices=100):
    """Midterm bar: per-segment p50/p95 at one scale, mean ± 95 % CI over the seeds that exist."""
    p = os.path.join(results, "summary_ci.csv")
    if not os.path.exists(p):
        raise SystemExit("run  python analysis/ci.py  first")
    df = pd.read_csv(p)
    df = df[(df.path == path) & (df.devices == devices)]
    for col, val in (("partitions", 6), ("serialization", "json"), ("semantics", "at-least-once")):
        if col in df:
            df = df[df[col].astype(str) == str(val)]
    if df.empty:
        raise SystemExit(f"no runs for path={path} devices={devices} in {p}")
    r = df.iloc[0]
    segs = [s for s in SEG_LABELS if f"lat_{s}_p50_mean" in r and not pd.isna(r[f"lat_{s}_p50_mean"])]
    x = np.arange(len(segs))
    wdt = 0.38
    fig, ax = plt.subplots(figsize=(7.5, 4))
    for j, (q, col) in enumerate((("p50", C[0]), ("p95", C[1]))):
        mean = [r[f"lat_{s}_{q}_mean"] for s in segs]
        ci = [r.get(f"lat_{s}_{q}_ci95", np.nan) for s in segs]
        ci = [0 if pd.isna(c) else c for c in ci]
        bars = ax.bar(x + (j - 0.5) * wdt, mean, wdt * 0.92, yerr=ci, capsize=3, color=col, label=q,
                      error_kw={"elinewidth": 1, "ecolor": "#444"})
        for b, v in zip(bars, mean):
            ax.annotate(f"{v:.0f}", (b.get_x() + b.get_width() / 2, v), xytext=(0, 3),
                        textcoords="offset points", ha="center", fontsize=7, color="#333")
    ax.set_xticks(x, [SEG_LABELS[s] for s in segs], fontsize=8)
    ax.set_ylabel("latency (ms)")
    n = int(r.n_runs)
    ci_txt = "mean ± 95 % CI" if n > 1 else "single seed, no CI"
    ax.set_title(f"Per-segment latency, {path} path, {devices} devices — {ci_txt} over {n} seed(s)\n"
                 f"offered {r.offered_msg_s_mean:.0f} msg/s ({r.offered_kbps_mean:.0f} kbps), "
                 f"window {r.window_s_mean:.0f} s per run after 20 s warm-up, "
                 f"~{r.samples_mean:.0f} samples per run", fontsize=8, loc="left")
    ax.legend(frameon=False, fontsize=8)
    ax.grid(axis="x", visible=False)
    _save(fig, os.path.join(results, "plots"), f"segments_p50_p95_{path}_d{devices}.png")


def plot_compare(results="results"):
    p = os.path.join(results, "summary_ci.csv")
    if not os.path.exists(p):
        raise SystemExit("run  python analysis/ci.py  first")
    df = pd.read_csv(p)
    out = os.path.join(results, "plots")
    base = df
    for col, val in (("partitions", 6), ("serialization", "json"), ("semantics", "at-least-once")):
        if col in base:
            base = base[base[col].astype(str) == str(val)]
    for metric, ylabel, fname in (
            ("lat_end_to_end_p95", "p95 end-to-end latency (ms)", "p95_vs_devices.png"),
            ("throughput_msg_s", "sustained throughput (msg/s)", "throughput_vs_devices.png"),
            ("lost_pct", "lost messages (%)", "loss_vs_devices.png")):
        if f"{metric}_mean" not in base:
            continue
        fig, ax = plt.subplots(figsize=(6, 3.8))
        for i, (path, g) in enumerate(base.sort_values("devices").groupby("path")):
            ax.errorbar(g.devices, g[f"{metric}_mean"], yerr=g.get(f"{metric}_ci95"), color=C[i],
                        marker="o", ms=6, capsize=4, label=f"{path} (n={int(g.n_runs.min())} seeds)")
        if metric == "lat_end_to_end_p95" and base[f"{metric}_mean"].max() > 1000:
            ax.axhline(2000, color="#888", ls="--", lw=1, label="2 s target")
        ax.set_xlabel("devices")
        ax.set_ylabel(ylabel)
        ax.set_title(f"{ylabel} vs devices — mean ± 95 % CI", fontsize=9)
        ax.legend(frameon=False, fontsize=8)
        _save(fig, out, fname)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["run", "segments", "compare"])
    ap.add_argument("run_dir", nargs="?")
    ap.add_argument("--results", default="results")
    ap.add_argument("--path", default="mqtt")
    ap.add_argument("--devices", type=int, default=100)
    a = ap.parse_args()
    if a.mode == "run":
        if not a.run_dir:
            ap.error("give logs/<RUN_ID>")
        plot_run(a.run_dir, a.results)
    elif a.mode == "segments":
        plot_segments(a.results, a.path, a.devices)
    else:
        plot_compare(a.results)
