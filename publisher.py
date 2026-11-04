import asyncio
import json
import random
import time
import argparse
import sys
from aiomqtt import Client
import config
from measurement.msglog import MsgLog  # [C-log]

LOG = MsgLog("publisher")  # [C-log]

# Rezolvare pentru eroarea `NotImplementedError` pe Windows cu Python 3.14
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

parser = argparse.ArgumentParser(description="Async Smart-City Traffic Generator")
parser.add_argument("--broker", type=str, default=config.MQTT_BROKER)
parser.add_argument("--port", type=int, default=config.MQTT_PORT)
parser.add_argument("--num-devices", type=int, default=100)
parser.add_argument("--seed", type=int, default=42)
args = parser.parse_args()

async def simulate_device(client, device_id: str, domain: str, zone_id: str, seed: int):
    rng = random.Random(seed + hash(device_id))
    seq = 0
    
    await asyncio.sleep(rng.uniform(0, 2.0))
    topic = f"smartcity/{domain}/{zone_id}/{device_id}"

    while True:
        seq += 1
        now = time.time()
        
        is_burst_time = (int(now) % 60) < 10
        in_burst_zone = (zone_id == "zone_1")
        
        payload = {
            "device_id": device_id,
            "domain": domain,
            "zone_id": zone_id,
            "seq": seq,
            "ts_device": now,
            "value": round(rng.uniform(10.0, 100.0), 2)
        }

        await client.publish(topic, json.dumps(payload), qos=config.DEFAULT_QOS)
        LOG.write(device_id, seq, zone_id, len(json.dumps(payload)), payload["ts_device"])  # [C-log]

        print(f"[SENT] {topic} -> seq:{seq} | ts:{payload['ts_device']}")
        
        sleep_time = 0.2 if (is_burst_time and in_burst_zone) else rng.uniform(1.0, 2.5)
        await asyncio.sleep(sleep_time)

async def main():
    print(f"Pornire generator: {args.num_devices} dispozitive, seed={args.seed}")
    
    # Adăugat timeout=30 pentru a preveni deconectarea rapidă
    async with Client(hostname=args.broker, port=args.port, timeout=30) as client:
        tasks = []
        for i in range(args.num_devices):
            dev_id = f"dev_{i:04d}"
            domain = config.DOMAINS[i % len(config.DOMAINS)]
            zone = config.ZONES[i % len(config.ZONES)]
            
            task = asyncio.create_task(
                simulate_device(client, dev_id, domain, zone, args.seed + i)
            )
            tasks.append(task)
            
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())