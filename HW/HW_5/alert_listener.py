import json
from kafka import KafkaConsumer
from colorama import init, Fore, Style

init(autoreset=True)

TOPICS = ["temperature_alerts_V", "humidity_alert_V"]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

print("Listening for alerts...\n")

for message in consumer:
    alert = message.value
    topic = message.topic

    if topic == "temperature_alerts_V":
        print(Fore.RED + f"[TEMP ALERT] Sensor {alert['sensor_id']} at {alert['timestamp']}: {alert['temperature']}°C")
    elif topic == "humidity_alert_V":
        print(Fore.CYAN + f"[HUM ALERT] Sensor {alert['sensor_id']} at {alert['timestamp']}: {alert['humidity']}%")
