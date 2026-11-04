import asyncio
import json
import random
import time
import argparse
from kafka import KafkaProducer
import config
from measurement.msglog import MsgLog  # [C-log]

LOG = MsgLog("direct")  # [C-log]

parser = argparse.ArgumentParser(description="Direct Kafka Producer Workload")
parser.add_argument("--kafka-brokers", type=str, default=config.KAFKA_BOOTSTRAP_SERVERS[0])
parser.add_argument("--num-devices", type=int, default=100)
parser.add_argument("--seed", type=int, default=42)
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.kafka_brokers.split(','),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3
)

async def simulate_direct_device(device_id: str, domain: str, zone_id: str, seed: int):
    rng = random.Random(seed + hash(device_id))
    seq = 0
    
    await asyncio.sleep(rng.uniform(0, 2.0))

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
            "ts_bridge": None,  # Calea directă nu trece prin bridge
            "value": round(rng.uniform(10.0, 100.0), 2)
        }

        topic = f"telemetry-{zone_id}"
        producer.send(topic, key=device_id.encode('utf-8'), value=payload)
        LOG.write(device_id, seq, zone_id, len(json.dumps(payload)), payload["ts_device"])  # [C-log]
        
        sleep_time = 0.2 if (is_burst_time and in_burst_zone) else rng.uniform(1.0, 2.5)
        await asyncio.sleep(sleep_time)

async def main():
    print(f"Pornire Direct Kafka Producer: {args.num_devices} dispozitive, seed={args.seed}")
    tasks = []
    for i in range(args.num_devices):
        dev_id = f"dev_{i:04d}"
        domain = config.DOMAINS[i % len(config.DOMAINS)]
        zone = config.ZONES[i % len(config.ZONES)]
        
        task = asyncio.create_task(
            simulate_direct_device(dev_id, domain, zone, args.seed + i)
        )
        tasks.append(task)
        
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())