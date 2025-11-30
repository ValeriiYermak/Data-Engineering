from pyspark.sql import SparkSession
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


def final_data_quality_check():
    # Отримуємо абсолютний шлях до кореня проекту
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    spark = create_spark_session("FinalDataQualityCheck")

    print("=== FINAL DATA QUALITY CHECK ===")
    print("=== BATCH DATA LAKE PIPELINE COMPLETED SUCCESSFULLY ===\n")

    # Перевірка всіх шарів з абсолютними шляхами
    layers = {
        "BRONZE": ["athlete_bio", "athlete_event_results"],
        "SILVER": ["athlete_bio", "athlete_event_results"],
        "GOLD": ["avg_stats"],
    }

    for layer, tables in layers.items():
        print(f"\n=== {layer} LAYER ===")
        for table in tables:
            path = os.path.join(PROJECT_ROOT, "data", layer.lower(), table)
            try:
                df = spark.read.parquet(path)
                count = df.count()
                print(f"✅ {table}: {count:,} rows")

                if layer == "GOLD":
                    print(f"   Schema: {len(df.columns)} columns")
                    print(f"   Sample data:")
                    df.show(3, truncate=True)

            except Exception as e:
                print(f"❌ {table}: Error - {e}")

    # Підсумкова статистика
    print("\n" + "=" * 50)
    print("=== PIPELINE SUMMARY ===")
    print("=" * 50)

    gold_path = os.path.join(PROJECT_ROOT, "data", "gold", "avg_stats")
    gold_df = spark.read.parquet(gold_path)

    print(f"🎯 GOLD LAYER FINAL STATISTICS:")
    print(f"   • Total aggregated records: {gold_df.count():,}")
    print(f"   • Unique sports: {gold_df.select('sport').distinct().count()}")
    print(f"   • Unique countries: {gold_df.select('country_noc').distinct().count()}")
    print(
        f"   • Data categories: {gold_df.select('sex').distinct().count()} gender categories"
    )

    print(f"\n📊 MEDAL DISTRIBUTION:")
    medal_stats = gold_df.groupBy("medal").count().orderBy("medal").collect()
    for row in medal_stats:
        medal = row["medal"] if row["medal"] else "No Medal"
        print(f"   • {medal}: {row['count']:,} records")

    print(f"\n🏆 TOP SPORTS BY RECORDS:")
    sport_stats = (
        gold_df.groupBy("sport")
        .count()
        .orderBy("count", ascending=False)
        .limit(5)
        .collect()
    )
    for row in sport_stats:
        print(f"   • {row['sport']}: {row['count']:,} records")

    print(f"\n✅ DATA LAKE PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"📍 Bronze: Raw data storage")
    print(f"📍 Silver: Cleaned and deduplicated data")
    print(f"📍 Gold: Aggregated analytics ready for business intelligence")

    spark.stop()


if __name__ == "__main__":
    final_data_quality_check()
