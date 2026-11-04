"""
Mean and 95 % confidence interval over the 5 seeds of each configuration (owner: C).

Usage:  python analysis/ci.py [--results results]
Reads   results/summary.csv     Writes  results/summary_ci.csv

CI half-width = t(0.975, n-1) * s / sqrt(n)     (Student t, because n = 5 is small)
"""
import argparse
import os

import numpy as np
import pandas as pd

try:
    from scipy.stats import t as student_t
    def t975(df): return float(student_t.ppf(0.975, df))
except ImportError:                               # fallback table for small n
    _T = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}
    def t975(df): return _T.get(df, 1.96)

CONFIG = ["path", "devices", "partitions", "serialization", "semantics"]
METRICS = ["offered_msg_s", "offered_kbps", "throughput_msg_s", "samples", "window_s",
           "lost_pct", "dup_pct", "bridge_qos1_dup_pct", "not_written_pct",
           "out_of_order_pct", "late_pct", "kafka_value_bytes_mean",
           "lag_mean_records", "lag_max_records", "lag_growth_records_per_s", "lag_time_lag_p95_s"]
# + every per-segment latency percentile (lat_<segment>_p50/p95/p99), added in main()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results")
    a = ap.parse_args()
    df = pd.read_csv(os.path.join(a.results, "summary.csv"))
    cfg = [c for c in CONFIG if c in df.columns]
    lat_cols = [c for c in df.columns if c.startswith("lat_") and c.endswith(("_p50", "_p95", "_p99"))]
    rows = []
    for key, g in df.groupby(cfg, dropna=False):
        row = dict(zip(cfg, key if isinstance(key, tuple) else (key,)))
        row["n_runs"] = len(g)
        row["seeds"] = " ".join(str(s) for s in sorted(g["seed"])) if "seed" in g else ""
        for m in METRICS + lat_cols:
            if m not in g:
                continue
            v = pd.to_numeric(g[m], errors="coerce").dropna().to_numpy()
            if len(v) == 0:
                continue
            row[f"{m}_mean"] = v.mean()
            row[f"{m}_ci95"] = t975(len(v) - 1) * v.std(ddof=1) / np.sqrt(len(v)) if len(v) > 1 else np.nan
        rows.append(row)
    out = pd.DataFrame(rows)
    path = os.path.join(a.results, "summary_ci.csv")
    out.to_csv(path, index=False)
    print(out[cfg + ["n_runs"] + [c for c in out.columns if c.startswith("lat_end_to_end_p95")]].to_string(index=False))
    print(f"-> {path}")


if __name__ == "__main__":
    main()
