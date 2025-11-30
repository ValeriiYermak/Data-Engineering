from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")

# Назви топіків з твоїм ім'ям
my_name = "valeriy"  # <- заміни на своє ім'я
topics = [
    f"{my_name}_building_sensors",
    f"{my_name}_temperature_alerts",
    f"{my_name}_humidity_alerts"
]

# Створення топіків
topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1) for topic in topics]
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("Topics created successfully")
except Exception as e:
    print(f"Error creating topics: {e}")

# Перевірка
for topic in admin_client.list_topics():
    if my_name in topic:
        print(topic)
