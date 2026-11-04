"""
3-message LogAppendTime test (roadmap, section 6, risk "Wrong timestamps").

We send 3 messages with a FAKE producer timestamp 1 hour in the past.
 - If the broker uses LogAppendTime, it overwrites that timestamp with its own
   append time  -> consumer sees timestamp_type == 1 and a timestamp ~ now.
 - If it uses CreateTime, the fake past timestamp survives -> TEST FAILS.

Run inside the compose network (see README):
  docker compose run --rm --no-deps bridge python -m measurement.check_logappendtime
"""
import argparse
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer

ap = argparse.ArgumentParser()
ap.add_argument("--brokers", default="kafka:29092,kafka2:29092,kafka3:29092")
ap.add_argument("--topic", default="c-logappendtime-test")
args = ap.parse_args()
brokers = args.brokers.split(",")

fake_ts_ms = int((time.time() - 3600) * 1000)   # 1 hour in the past
tag = uuid.uuid4().hex[:8]

producer = KafkaProducer(bootstrap_servers=brokers, acks="all")
t_send = []
for i in range(3):
    t_send.append(time.time())
    producer.send(args.topic, value=f"{tag}-{i}".encode(), timestamp_ms=fake_ts_ms).get(timeout=30)
producer.flush()

consumer = KafkaConsumer(args.topic, bootstrap_servers=brokers, auto_offset_reset="earliest",
                         enable_auto_commit=False, consumer_timeout_ms=15000,
                         group_id=f"c-ts-test-{tag}")
ok = 0
seen = 0
for msg in consumer:
    if not msg.value.decode().startswith(tag):
        continue                                   # old messages from earlier tests
    seen += 1
    i = int(msg.value.decode().split("-")[1])
    append_s = msg.timestamp / 1000.0
    delta_ms = (append_s - t_send[i]) * 1000
    is_lat = msg.timestamp_type == 1
    good = is_lat and abs(delta_ms) < 5000
    ok += good
    print(f"msg {i}: timestamp_type={msg.timestamp_type} ({'LogAppendTime' if is_lat else 'CreateTime'}), "
          f"append - send = {delta_ms:.1f} ms  -> {'OK' if good else 'FAIL'}")
    if seen == 3:
        break

print("RESULT:", "PASS - broker uses LogAppendTime" if ok == 3 else f"FAIL ({ok}/3 ok, {seen}/3 received)")
