"""
Midterm (weeks 7-8), Project 7 bar: "100 devices end to end with per-segment latency (p50/p95)
and consumer-lag plots". One command: runs the seeds, then KPIs, CIs and all midterm plots.

Usage:
    python scripts/run_midterm.py                    # seeds 1-5, 100 devices, MQTT path (~18 min)
    python scripts/run_midterm.py --seeds 1          # quick check, one seed (~3.5 min)
"""
import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_one import ROOT, run  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3, 4, 5])
ap.add_argument("--devices", type=int, default=100)
ap.add_argument("--path", choices=["mqtt", "direct"], default="mqtt")
ap.add_argument("--duration", type=int, default=120)
a = ap.parse_args()

run_ids = [run(a.path, a.devices, s, duration=a.duration,
               run_id=f"midterm_{a.path}_d{a.devices}_p6_s{s}") for s in a.seeds]

py = sys.executable
subprocess.run([py, os.path.join("analysis", "ci.py")], cwd=ROOT, check=True)
subprocess.run([py, os.path.join("analysis", "plots.py"), "segments",
                "--path", a.path, "--devices", str(a.devices)], cwd=ROOT, check=True)

print("\nMidterm plots:")
print(f"  results/plots/segments_p50_p95_{a.path}_d{a.devices}.png   per-segment p50/p95 (all seeds)")
print(f"  results/{run_ids[0]}/lag_time.png                          consumer lag over time")
print(f"  results/{run_ids[0]}/latency_cdf.png, e2e_latency_time.png, throughput_time.png")
