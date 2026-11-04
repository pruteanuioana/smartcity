import json
import time
import argparse
from paho.mqtt import client as mqtt_client
from kafka import KafkaProducer
import config

parser = argparse.ArgumentParser(description="MQTT to Kafka Bridge")
parser.add_argument("--mqtt-host", type=str, default=config.MQTT_BROKER)
parser.add_argument("--kafka-brokers", type=str, default=config.KAFKA_BOOTSTRAP_SERVERS[0])
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.kafka_brokers.split(','),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # at-least-once semantics
    retries=3
)

def on_message(client, userdata, msg):
    ts_bridge = time.time()
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        data['ts_bridge'] = ts_bridge
        
        kafka_topic = f"telemetry-{data.get('zone_id', 'default')}"
        producer.send(kafka_topic, key=data['device_id'].encode('utf-8'), value=data)
    except Exception as e:
        print(f"Eroare prelucrare mesaj în bridge: {e}")

def run_bridge():
    client = mqtt_client.Client(client_id="mqtt-kafka-bridge", clean_session=False)
    client.on_message = on_message
    client.connect(args.mqtt_host, config.MQTT_PORT)
    client.subscribe("smartcity/#", qos=config.DEFAULT_QOS)
    
    print("Bridge-ul MQTT-Kafka este activ și ascultă...")
    client.loop_forever()

if __name__ == "__main__":
    run_bridge()