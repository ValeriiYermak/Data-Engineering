from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
from kafka.errors import KafkaError, NoBrokersAvailable

TOPIC = "building_sensors_V"

sensor_id = random.randint(1000, 9999)
print(f"Sensor {sensor_id} started sending data...")

producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],  # <- Windows
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5
        )
        print("Connected to Kafka broker at localhost:9092")
    except NoBrokersAvailable:
        print("Kafka broker not available yet, retrying in 5 seconds...")
        time.sleep(5)

while True:
    data = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(25, 45), 2),
        "humidity": round(random.uniform(15, 85), 2)
    }
    try:
        producer.send(TOPIC, value=data)
        producer.flush()
        print(f"Sent: {data}")
    except KafkaError as e:
        print(f"Error sending data: {e}. Retrying in 5 seconds...")
        time.sleep(5)
    time.sleep(1)
