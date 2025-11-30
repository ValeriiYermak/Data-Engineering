import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col

# -----------------------
# КОНФІГУРАЦІЯ
# -----------------------

# --- Конфігурація Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_OUTPUT_TOPIC = "athlete_event_results"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# --- Облікові дані та JDBC URL ---
jdbc_host = "217.61.57.46"
jdbc_port = "3306"
jdbc_db = "olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
# Використовуйте повний JDBC URL
jdbc_url = f"jdbc:mysql://{jdbc_host}:{jdbc_port}/{jdbc_db}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
jdbc_table_events = "athlete_event_results"


try:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent
    mysql_jar_path = str(project_root / "jars" / "mysql-connector-j-8.0.32.jar")
except NameError:
    print(
        "Warning: Could not determine project root path automatically. Using default."
    )
    mysql_jar_path = r"C:\Project\MasterSc\Date_Engineering\Final_Project\Part_1\jars\mysql-connector-j-8.0.32.jar"


# ✅ ФАЙЛ ДЛЯ ЗБЕРІГАННЯ ОСТАННЬОГО ID
STATE_FILE = str(Path(__file__).resolve().parent / "producer_state.txt")
POLL_INTERVAL_SECONDS = 60
BATCH_SIZE = 100000
INCREMENTAL_COLUMN = "athlete_id"


def get_last_processed_id():
    """Читає останній оброблений ID з файлу стану."""
    try:
        with open(STATE_FILE, "r") as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0  # Почати з 0


def update_last_processed_id(max_id):
    """Оновлює останній оброблений ID у файлі стану."""
    with open(STATE_FILE, "w") as f:
        f.write(str(max_id))


def run_iterative_producer():
    # 1. Створення SparkSession
    spark = (
        SparkSession.builder.appName("IterativeMySQLKafkaProducer")
        .config("spark.jars", mysql_jar_path)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession created for Iterative Producer.")
    print(f"Using MySQL Connector JAR path: {mysql_jar_path}")

    # Перевірка наявності JAR-файлу
    if not Path(mysql_jar_path).is_file():
        raise FileNotFoundError(f"MySQL JDBC JAR not found at: {mysql_jar_path}")

    while True:
        start_time = datetime.now()
        last_id = get_last_processed_id()
        current_max_id = 0  # Ініціалізуємо для логіки обробки циклу

        if last_id == 0:
            print(
                "\n--- Starting NEW CYCLE: Reading from the beginning of the dataset. ---"
            )

        print(
            f"\n--- Starting Poll ({start_time.strftime('%H:%M:%S')}): Reading next {BATCH_SIZE} records where {INCREMENTAL_COLUMN} > {last_id} ---"
        )

        # 2. Формування інкрементального запиту з LIMIT для Batching
        # Використовуємо SELECT * з ORDER BY та LIMIT всередині підзапиту
        dbtable_query = f"(SELECT * FROM {jdbc_table_events} WHERE {INCREMENTAL_COLUMN} > {last_id} ORDER BY {INCREMENTAL_COLUMN} ASC LIMIT {BATCH_SIZE}) AS new_records"

        try:
            new_events_df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", dbtable_query)
                .option("user", jdbc_user)
                .option("password", jdbc_password)
                .load()
            )

            record_count = new_events_df.count()

            if record_count > 0:
                print(f"Found {record_count} new records. Processing...")

                # 3. Оновлення стану: Використовуємо max() для наступного циклу
                max_current_id_row = new_events_df.agg(
                    {INCREMENTAL_COLUMN: "max"}
                ).collect()[0]
                current_max_id = max_current_id_row[f"max({INCREMENTAL_COLUMN})"]

                # Переконаємось, що ми робимо інкремент лише вперед
                if current_max_id > last_id:
                    update_last_processed_id(current_max_id)
                    print(f"State updated: max_id_processed is now {current_max_id}")
                else:
                    print(
                        f"Warning: Max ID found ({current_max_id}) is not greater than last ID ({last_id}). Not updating state."
                    )

                # 4. Підготовка даних для Kafka (включно з ключовими полями)
                kafka_df = new_events_df.select(
                    col("athlete_id").cast("string").alias("key"),
                    to_json(
                        struct(
                            col("athlete_id"),
                            col("sport"),
                            col("event"),
                            col("medal"),
                            col("country_noc"),
                        )
                    ).alias("value"),
                )

                # 5. Запис у Kafka-топік (BATCH)
                (
                    kafka_df.write.format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                    .option("topic", KAFKA_OUTPUT_TOPIC)
                    .mode("append")
                    .save()
                )
                print(f"Successfully published {record_count} records to Kafka.")

                # 6. ЛОГІКА ЗАПУСКУ ЦИКЛУ (LOOPING):
                # Якщо ми прочитали менше, ніж BATCH_SIZE, це означає, що ми досягли кінця бази даних.
                if record_count < BATCH_SIZE:
                    print(
                        f"\n--- END OF DATASET REACHED. Resetting state to 0 for next iteration. ---"
                    )
                    update_last_processed_id(0)  # Скидаємо стан на 0

            else:
                # 7. Обробка порожнього результату
                if last_id > 0 and current_max_id == last_id:
                    # Якщо ми раніше читали і тепер нічого не знайшли, це кінець
                    print(
                        f"\n--- FULL DATASET PROCESSED. Resetting state to 0 for next iteration. ---"
                    )
                    update_last_processed_id(0)  # Скидаємо стан на 0
                else:
                    print("No new records found. Waiting...")

        except Exception as e:
            print(f"\n🛑 An unexpected error occurred during polling:")
            print(f"Error: {e}")
            if "ClassNotFoundException" in str(e):
                print(f"🚨 Check the path to your MySQL JAR file: {mysql_jar_path}")

        # 8. ЧАС НАСТУПНОГО ОНОВЛЕННЯ
        end_time = datetime.now()

        # Обчислюємо фактичний час, що залишився
        time_elapsed = (end_time - start_time).total_seconds()
        sleep_duration = max(0, POLL_INTERVAL_SECONDS - time_elapsed)

        next_poll_time = end_time + timedelta(seconds=sleep_duration)

        print(f"\nSleeping for {round(sleep_duration)} seconds...")
        print(f"NEXT POLL SCHEDULED FOR: {next_poll_time.strftime('%H:%M:%S')}")

        time.sleep(sleep_duration)


if __name__ == "__main__":
    try:
        run_iterative_producer()
    except KeyboardInterrupt:
        print("\nProducer stopped by user (Ctrl+C).")
    finally:
        try:
            spark = SparkSession.builder.appName("TempStopper").getOrCreate()
            spark.stop()
        except Exception:
            pass
