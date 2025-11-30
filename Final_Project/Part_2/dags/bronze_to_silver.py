from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
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
    spark = create_spark_session("BronzeToSilver")

    # Список таблиць для обробки
    tables = ["athlete_bio", "athlete_event_results"]

    # Створення папки silver в корені проекту
    silver_path = os.path.join(PROJECT_ROOT, "data", "silver")
    os.makedirs(silver_path, exist_ok=True)

    for table_name in tables:
        print(f"\n=== Processing {table_name} from bronze to silver ===")

        # Читання bronze даних
        bronze_table_path = os.path.join(PROJECT_ROOT, "data", "bronze", table_name)
        if not os.path.exists(bronze_table_path):
            print(f"❌ Bronze path {bronze_table_path} does not exist. Skipping...")
            continue

        df = spark.read.parquet(bronze_table_path)
        initial_count = df.count()
        print(f"Loaded {initial_count} rows from bronze")

        # Визначення текстових колонок
        text_columns = []
        for field in df.schema.fields:
            if str(field.dataType) in ["StringType()", "StringType"]:
                text_columns.append(field.name)

        print(f"Text columns to clean: {text_columns}")

        # Очищення текстових колонок за допомогою вбудованих функцій Spark
        for col_name in text_columns:
            df = df.withColumn(
                col_name,
                regexp_replace(
                    regexp_replace(col(col_name), r"[^\w\s\.\,\-\'\"]", ""), r"\s+", " "
                ),
            )

        # Дедублікація
        df_cleaned = df.dropDuplicates()
        final_count = df_cleaned.count()
        removed_duplicates = initial_count - final_count

        print(f"Deduplication: {initial_count} -> {final_count} rows")
        print(f"Removed {removed_duplicates} duplicate rows")

        # Збереження у silver шарі
        silver_table_path = os.path.join(silver_path, table_name)
        df_cleaned.write.mode("overwrite").parquet(silver_table_path)

        print(f"✅ Successfully saved {final_count} rows to {silver_table_path}")

        # Показати приклад даних
        print("Sample of cleaned data:")
        df_cleaned.show(5, truncate=False)

    print("\n=== Silver layer creation completed ===")
    spark.stop()


if __name__ == "__main__":
    main()
