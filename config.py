# Setări Brokeri și Rețea
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

# Domenii și Zone definite în cerințe
DOMAINS = ["traffic", "environment", "energy"]
ZONES = ["zone_1", "zone_2", "zone_3"]

# Parametri de trafic
DEFAULT_QOS = 1
BASE_INTERVAL = 1.75  # ~0.57 msg/s per dispozitiv