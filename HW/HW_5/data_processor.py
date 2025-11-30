import json
from kafka import KafkaConsumer, KafkaProducer

# Топіки
SOURCE_TOPIC = "building_sensors_V"       # відповідає твоєму producer
TEMP_ALERT_TOPIC = "temperature_alerts_V"
HUM_ALERT_TOPIC = "humidity_alert_V"

consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Processor started listening...")

for message in consumer:
    data = message.value
    sensor_id = data["sensor_id"]
    temp = data["temperature"]
    hum = data["humidity"]

    # Перевірка температури
    if temp > 40 or temp < 25:
        alert = {
            "sensor_id": sensor_id,
            "timestamp": data["timestamp"],
            "temperature": temp,
            "message": f"Temperature out of range: {temp}°C"
        }
        producer.send(TEMP_ALERT_TOPIC, value=alert)
        print(f"[TEMP ALERT] {alert}")

    # Перевірка вологості
    if hum > 80 or hum < 20:
        alert = {
            "sensor_id": sensor_id,
            "timestamp": data["timestamp"],
            "humidity": hum,
            "message": f"Humidity out of range: {hum}%"
        }
        producer.send(HUM_ALERT_TOPIC, value=alert)
        print(f"[HUM ALERT] {alert}")
