"""
One measured run (owner: C). Works from PowerShell, the PyCharm terminal or any shell.
run_matrix.py (weeks 9-10) will just call run() in a loop.

Usage:
    python scripts/run_one.py <mqtt|direct> <devices> <seed> [--partitions 6] [--duration 120]
    python scripts/run_one.py mqtt 100 1

Steps: fresh stack -> create topics -> start processor + lag sampler (+ bridge)
       -> wait until the consumer group is Stable (so no message is skipped)
       -> start generator for <duration> s -> stop generator -> drain 10 s
       -> stop everything (SIGTERM, CSV logs flushed) -> kpi.py + plots.py
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # repo root
GROUP = "smartcity-processor"
# Topics the producers write to. Spec default is per domain and zone (traffic.z1 ...);
# the current code uses one topic per zone. Change here (or env TOPICS) when A/B switch.
DEFAULT_TOPICS = "telemetry-zone_1 telemetry-zone_2 telemetry-zone_3"


def dc(env, *args, check=True, capture=False):
    """docker compose --profile measure <args>, run from the repo root."""
    cmd = ["docker", "compose", "--profile", "measure", *args]
    r = subprocess.run(cmd, cwd=ROOT, env=env, text=True,
                       stdout=subprocess.PIPE if capture else None,
                       stderr=subprocess.STDOUT if capture else None)
    if check and r.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\n{r.stdout or ''}")
    return r


def kafka(env, tool, *args, check=True, capture=False):
    """Run a Kafka CLI tool inside the `kafka` container."""
    return dc(env, "exec", "-T", "kafka", f"/opt/kafka/bin/{tool}",
              "--bootstrap-server", "kafka:29092", *args, check=check, capture=capture)


def wait_for(what, test, timeout_s, every_s=1.0):
    print(f"--- waiting for {what}", flush=True)
    t_end = time.time() + timeout_s
    while time.time() < t_end:
        if test():
            return
        time.sleep(every_s)
    raise RuntimeError(f"timed out waiting for {what}")


def run(path, devices, seed, partitions=6, duration=120, run_id=None,
        serialization="json", semantics="at-least-once", analyse=True):
    if path not in ("mqtt", "direct"):
        raise ValueError("path must be mqtt or direct")
    run_id = run_id or (f"{datetime.datetime.now():%Y%m%d-%H%M%S}"
                        f"_{path}_d{devices}_p{partitions}_s{seed}")
    env = {**os.environ, "RUN_ID": run_id, "DEVICES": str(devices), "SEED": str(seed)}
    topics = os.environ.get("TOPICS", DEFAULT_TOPICS).split()
    log_dir = os.path.join(ROOT, "logs", run_id)
    os.makedirs(log_dir, exist_ok=True)
    gen = "publisher" if path == "mqtt" else "direct"

    print(f"=== run {run_id}  (path={path} devices={devices} seed={seed} "
          f"partitions={partitions} duration={duration}s)", flush=True)
    try:
        # 1) fresh stack (Kafka has no volume -> clean topics/offsets; InfluxDB/Grafana data kept)
        dc(env, "down", "--remove-orphans", check=False, capture=True)
        dc(env, "build", "-q")                     # rebuild images so code changes are included
        dc(env, "up", "-d", "mqtt", "kafka", "kafka2", "kafka3", "influxdb")
        wait_for("Kafka", lambda: kafka(env, "kafka-topics.sh", "--list",
                                        check=False, capture=True).returncode == 0, 120, 2)

        # 2) topics with the requested partitions, replication factor 3
        for t in topics:
            kafka(env, "kafka-topics.sh", "--create", "--if-not-exists", "--topic", t,
                  "--partitions", str(partitions), "--replication-factor", "3")

        # 3) consumers first, so auto_offset_reset=latest does not skip the first messages
        dc(env, "up", "-d", "processor", "lag")
        if path == "mqtt":
            dc(env, "up", "-d", "bridge")

        def group_stable():
            r = kafka(env, "kafka-consumer-groups.sh", "--describe", "--group", GROUP, "--state",
                      check=False, capture=True)
            return r.returncode == 0 and "Stable" in (r.stdout or "")
        wait_for("consumer group to be Stable", group_stable, 90)
        time.sleep(3)

        with open(os.path.join(log_dir, "meta.json"), "w") as f:
            json.dump({"run_id": run_id, "path": path, "devices": devices, "seed": seed,
                       "partitions": partitions, "duration_s": duration,
                       "serialization": serialization, "semantics": semantics,
                       "started": datetime.datetime.now().isoformat(timespec="seconds")}, f, indent=1)

        # 4) workload
        print(f"--- generator '{gen}' running for {duration}s", flush=True)
        dc(env, "up", "-d", gen)
        time.sleep(duration)

        # 5) stop generator, drain, stop the rest (SIGTERM -> CSV files flushed)
        dc(env, "stop", "-t", "10", gen)
        print("--- draining 10 s", flush=True)
        time.sleep(10)
        dc(env, "stop", "-t", "10", "bridge", "processor", "lag")
    finally:
        # always leave a clean machine, also after Ctrl+C or an error
        dc(env, "down", "--remove-orphans", check=False, capture=True)

    # 6) analysis (runs on the laptop, not in Docker)
    if analyse:
        subprocess.run([sys.executable, os.path.join("analysis", "kpi.py"), os.path.join("logs", run_id)],
                       cwd=ROOT, check=True)
        subprocess.run([sys.executable, os.path.join("analysis", "plots.py"), "run",
                        os.path.join("logs", run_id)], cwd=ROOT, check=True)
    print(f"=== done: logs/{run_id}  results/{run_id}", flush=True)
    return run_id


def main():
    ap = argparse.ArgumentParser(description="one measured run")
    ap.add_argument("path", choices=["mqtt", "direct"])
    ap.add_argument("devices", type=int)
    ap.add_argument("seed", type=int)
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--duration", type=int, default=120)
    ap.add_argument("--run-id")
    ap.add_argument("--serialization", default="json")
    ap.add_argument("--semantics", default="at-least-once")
    a = ap.parse_args()
    run(a.path, a.devices, a.seed, a.partitions, a.duration, a.run_id, a.serialization, a.semantics)


if __name__ == "__main__":
    main()
