from pyspark.sql import SparkSession
import requests
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


def download_csv_from_ftp(url, local_filename):
    """Завантажує CSV файл з FTP сервера"""
    print(f"Downloading from {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()

        with open(local_filename, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully: {local_filename}")
        return True
    except Exception as e:
        print(f"Failed to download file: {e}")
        return False


def main():
    # Отримуємо абсолютний шлях до кореня проекту
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    spark = create_spark_session("LandingToBronze")

    # Базова URL та список таблиць
    base_url = "https://ftp.goit.study/neoversity/"
    tables = [
        {"name": "athlete_bio", "url": f"{base_url}athlete_bio.csv"},
        {
            "name": "athlete_event_results",
            "url": f"{base_url}athlete_event_results.csv",
        },
    ]

    # Створення папок для даних в корені проекту
    landing_zone_path = os.path.join(PROJECT_ROOT, "data", "landing_zone")
    bronze_path = os.path.join(PROJECT_ROOT, "data", "bronze")

    os.makedirs(landing_zone_path, exist_ok=True)
    os.makedirs(bronze_path, exist_ok=True)

    for table in tables:
        table_name = table["name"]
        table_url = table["url"]
        local_csv_path = os.path.join(landing_zone_path, f"{table_name}.csv")

        print(f"\n=== Processing {table_name} ===")

        # Завантаження даних
        if download_csv_from_ftp(table_url, local_csv_path):
            # Читання CSV
            df = (
                spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(local_csv_path)
            )

            print(f"Schema for {table_name}:")
            df.printSchema()
            print(f"Row count: {df.count()}")

            # Збереження у parquet форматі
            table_bronze_path = os.path.join(bronze_path, table_name)
            df.write.mode("overwrite").parquet(table_bronze_path)

            print(f"Successfully saved to {table_bronze_path}")
        else:
            print(f"❌ Failed to process {table_name}")

    print("\n=== Bronze layer creation completed ===")
    spark.stop()


if __name__ == "__main__":
    main()
