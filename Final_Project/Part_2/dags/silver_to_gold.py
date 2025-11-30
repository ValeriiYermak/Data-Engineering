from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, when
import os


def create_spark_session(app_name):
    """Створює Spark сесію в локальному режимі"""
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[2] pyspark-shell"

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.master", "local[2]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.warehouse.dir", "/opt/airflow/data")
        .getOrCreate()
    )


def main():
    # Отримуємо абсолютний шлях до кореня проекту
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Використовуємо нову функцію для Spark сесії
    spark = create_spark_session("SilverToGold")

    print("=== Starting Silver to Gold processing ===")

    # Читання silver даних з абсолютними шляхами
    athlete_bio_path = os.path.join(PROJECT_ROOT, "data", "silver", "athlete_bio")
    athlete_events_path = os.path.join(
        PROJECT_ROOT, "data", "silver", "athlete_event_results"
    )

    athlete_bio_df = spark.read.parquet(athlete_bio_path)
    athlete_events_df = spark.read.parquet(athlete_events_path)

    print(f"Athlete bio rows: {athlete_bio_df.count()}")
    print(f"Athlete events rows: {athlete_events_df.count()}")

    # Спрощена обробка weight - беремо тільки числові значення
    athlete_bio_df = athlete_bio_df.withColumn(
        "weight_clean",
        when(col("weight").rlike(r"^\d+$"), col("weight").cast("double")).otherwise(
            None
        ),
    )

    # Обробка height
    athlete_bio_df = athlete_bio_df.withColumn(
        "height_clean", col("height").cast("double")
    )

    print("Sample data after cleaning:")
    athlete_bio_df.select("weight", "weight_clean", "height", "height_clean").show(10)

    # Об'єднання таблиць
    joined_df = athlete_bio_df.join(athlete_events_df, "athlete_id", "inner").select(
        col("sport"),
        col("medal"),
        col("sex"),
        athlete_events_df["country_noc"].alias("country_noc"),
        col("weight_clean"),
        col("height_clean"),
    )

    print(f"Joined dataset rows: {joined_df.count()}")

    # Агрегація тільки для рядків з валідними числовими значеннями
    valid_data_df = joined_df.filter(
        (col("weight_clean").isNotNull())
        & (col("height_clean").isNotNull())
        & (col("weight_clean") > 0)
        & (col("height_clean") > 0)
    )

    print(f"Rows with valid numeric data: {valid_data_df.count()}")

    # Агрегація
    gold_df = (
        valid_data_df.groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            avg("weight_clean").alias("avg_weight"),
            avg("height_clean").alias("avg_height"),
        )
        .withColumn("timestamp", current_timestamp())
        .orderBy("sport", "medal", "sex", "country_noc")
    )

    # Збереження gold даних в корені проекту
    gold_path = os.path.join(PROJECT_ROOT, "data", "gold")
    os.makedirs(gold_path, exist_ok=True)

    gold_table_path = os.path.join(gold_path, "avg_stats")
    gold_df.write.mode("overwrite").parquet(gold_table_path)

    print(f"✅ Gold layer created successfully: {gold_table_path}")
    print(f"Final aggregated rows: {gold_df.count()}")

    # Показати результати
    print("\n=== Final aggregated statistics ===")
    gold_df.show(20, truncate=False)

    # Додаткова статистика
    print("\n=== Summary Statistics ===")
    print(f"Total sports: {gold_df.select('sport').distinct().count()}")
    print(f"Total countries: {gold_df.select('country_noc').distinct().count()}")
    print(f"Medal distribution:")
    gold_df.groupBy("medal").count().orderBy("medal").show()

    spark.stop()


if __name__ == "__main__":
    main()
