import os
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    avg,
    when,
    lit,
    to_json,
    struct,
    concat,
    count,
    broadcast,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
    TimestampType,
)
from datetime import datetime

# -----------------------
# КОНФІГУРАЦІЯ
# -----------------------

HADOOP_HOME_PATH = r"C:\hadoop"
if os.path.isdir(HADOOP_HOME_PATH):
    os.environ["HADOOP_HOME"] = HADOOP_HOME_PATH

PYTHON_EXECUTABLE_PATH = (
    r"C:\Project\MasterSc\Date_Engineering\.venv_spark\Scripts\python.exe"
)
if os.path.exists(PYTHON_EXECUTABLE_PATH):
    os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE_PATH
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE_PATH

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC = "athlete_event_results"
KAFKA_OUTPUT_TOPIC = "ml_model_features"

BASE_DIR = Path(__file__).resolve().parent
CHECKPOINT_LOCATION = str(BASE_DIR / "checkpoints" / "aggregator")
CHECKPOINT_LOCATION_CONSOLE = str(BASE_DIR / "checkpoints" / "aggregator_console")

# --- КОНФІГУРАЦІЯ MYSQL ---
MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_DB = "olympic_dataset"
MYSQL_USER = "neo_data_admin"
MYSQL_PASSWORD = "Proyahaxuqithab9oplp"
MYSQL_OUTPUT_TABLE = "athlete_enriched_agg"
jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"

mysql_jar_path = r"C:\Project\MasterSc\Date_Engineering\Final_Project\Part_1\jars\mysql-connector-j-8.0.32.jar"
bio_csv_path = r"C:\Project\MasterSc\Date_Engineering\Final_Project\Part_1\output\athlete_bio_filtered\athlete_bio_single_file.csv"

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

kafka_schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("country_noc", StringType(), True),
    ]
)

bio_schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("born", StringType(), True),
        StructField("height", StringType(), True),
        StructField("weight", StringType(), True),
        StructField("country", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("description", StringType(), True),
        StructField("special_notes", StringType(), True),
        StructField("height_num", DoubleType(), True),
        StructField("weight_num", DoubleType(), True),
    ]
)

# -----------------------
# ФУНКЦІЇ ДЛЯ КОНФІГУРАЦІЇ
# -----------------------


def ensure_checkpoint_directory_clean():
    """Очищення теки контрольних точок."""
    # Очищаємо обидві теки
    for cp_loc in [CHECKPOINT_LOCATION, CHECKPOINT_LOCATION_CONSOLE]:
        checkpoint_path = Path(cp_loc)
        if checkpoint_path.exists():
            print(f"Cleaning up existing checkpoint directory: {cp_loc}")
            try:
                shutil.rmtree(checkpoint_path)
                print("Cleanup successful.")
            except Exception as e:
                print(f"Warning: Could not clean up checkpoint directory. Error: {e}")
        checkpoint_path.mkdir(parents=True, exist_ok=True)
        print(f"Checkpoint directory ensured at: {cp_loc}")


def get_spark_session(app_name):
    """Створює SparkSession."""
    spark_conf = {
        "spark.jars": mysql_jar_path,
        "spark.jars.packages": KAFKA_PACKAGE,
        "spark.pyspark.python": PYTHON_EXECUTABLE_PATH,
        "spark.pyspark.driver.python": PYTHON_EXECUTABLE_PATH,
        "spark.jdbc.write.batchSize": "1000",
        "spark.jdbc.write.numPartitions": "20",
    }

    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for k, v in spark_conf.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"SparkSession '{app_name}' created successfully.")
    return spark


def fanout_batch_processor(batch_df, batch_id):
    """Обробляє кожен мікро-батч, записуючи результати до Kafka та MySQL."""
    if batch_df.isEmpty():
        return

    print(f"\n--- Batch {batch_id} Started ({datetime.now().strftime('%H:%M:%S')}) ---")

    # 1. Запис до Kafka
    kafka_output_df = batch_df.select(
        concat(col("athlete_id"), lit("_"), col("country_noc")).alias("key"),
        to_json(
            struct(
                col("athlete_id").alias("athleteId"),
                col("TotalMedals").alias("totalMedals"),
                col("AvgAge").alias("avgAge"),
                col("AvgHeight").alias("avgHeight"),
                col("AvgWeight").alias("avgWeight"),
                current_timestamp().alias("processedTime"),
            )
        ).alias("value"),
    )

    record_count = kafka_output_df.count()
    print(
        f"Batch {batch_id}: Writing {record_count} aggregated records to Kafka topic {KAFKA_OUTPUT_TOPIC}..."
    )
    try:
        (
            kafka_output_df.write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", KAFKA_OUTPUT_TOPIC)
            .mode("append")
            .save()
        )
        print(f"Batch {batch_id}: Successfully written to Kafka.")
    except Exception as e:
        print(f"Batch {batch_id}: ❌ Error writing to Kafka: {e}")

    # 2. Запис до MySQL: ВИКОРИСТАННЯ UPSERT (ON DUPLICATE KEY UPDATE)
    mysql_output_df = batch_df.select(
        col("athlete_id"),
        col("TotalMedals").alias("total_medals"),
        col("AvgAge").alias("avg_age"),
        col("AvgHeight").alias("avg_height"),
        col("AvgWeight").alias("avg_weight"),
        current_timestamp().alias("processed_at"),
    )

    print(
        f"Batch {batch_id}: Writing {record_count} records to MySQL table {MYSQL_OUTPUT_TABLE} using APPEND + UPSERT..."
    )
    try:
        (
            mysql_output_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", MYSQL_OUTPUT_TABLE)
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .mode("append")
            .option("dbtable.updateAction", "ON DUPLICATE KEY UPDATE")
            .save()
        )
        print(f"Batch {batch_id}: Successfully written to MySQL using APPEND + UPSERT.")
    except Exception as e:
        print(f"Batch {batch_id}: ❌ CRITICAL ERROR writing to MySQL: {e}")
        print(f"❌ DataFrame columns for MySQL: {mysql_output_df.columns}")

    print(f"--- Batch {batch_id} Finished ---\n")


# -----------------------
# ОСНОВНА ФУНКЦІЯ СТРИМІНГОВОГО ETL
# -----------------------
def run_streaming_etl():
    spark = get_spark_session("StreamingAggregator")

    ensure_checkpoint_directory_clean()

    # 2. Читання статичних даних з CSV
    athlete_bio_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(bio_schema)
        .load(bio_csv_path)
        .select(
            col("athlete_id").alias("athlete_id_bio"),
            col("sex"),
            col("height_num").alias("Height"),
            col("weight_num").alias("Weight"),
        )
        .withColumn("Age", lit(25))
    )

    # 3. Читання стримінгових даних з Kafka
    kafka_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 4. Десеріалізація та об'єднання
    stream_results_df = (
        kafka_stream_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), kafka_schema).alias("data"))
        .select("data.*")
    )

    joined_df = stream_results_df.join(
        broadcast(athlete_bio_df),
        col("athlete_id") == col("athlete_id_bio"),
        "inner",
    ).drop("athlete_id_bio")

    # 5. Агрегація
    aggregated_df = (
        joined_df.groupBy(col("athlete_id"), col("country_noc"))
        .agg(
            count(when(col("medal").isin("Gold", "Silver", "Bronze"), True)).alias(
                "TotalMedals"
            ),
            avg(col("Age")).alias("AvgAge"),
            avg(col("Height")).alias("AvgHeight"),
            avg(col("Weight")).alias("AvgWeight"),
        )
        .withColumn("IsMedalist", when(col("TotalMedals") > 0, 1).otherwise(0))
    )

    # 6. Запуск стримінгових запитів

    main_query = (
        aggregated_df.writeStream.outputMode("update")
        .foreachBatch(fanout_batch_processor)
        .trigger(processingTime="7 minute")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )
    print("Main stream query (Kafka/MySQL) started.")

    console_query = (
        aggregated_df.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="7 minute")
        .option("checkpointLocation", CHECKPOINT_LOCATION_CONSOLE)
        .start()
    )
    print("Console stream query started.")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_streaming_etl()
