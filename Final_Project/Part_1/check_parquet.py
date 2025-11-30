from pyspark.sql import SparkSession

# Створюємо SparkSession
spark = (
    SparkSession.builder.appName("CheckParquetFile").master("local[*]").getOrCreate()
)

parquet_path = "Part_1/output/athlete_bio_filtered.parquet"

try:
    # Зчитуємо збережений Parquet-файл
    df_check = spark.read.parquet(parquet_path)

    print(f"=== Вміст файлу {parquet_path} ===")
    df_check.printSchema()
    df_check.show(10, truncate=False)
    print(f"Загальна кількість рядків: {df_check.count()}")

except Exception as e:
    print(f"Помилка при зчитуванні файлу Parquet. Переконайтеся, що файл існує: {e}")

spark.stop()
