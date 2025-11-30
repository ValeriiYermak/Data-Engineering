from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = (SparkSession.builder
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "2")
         .appName("MyGoitSparkSandbox")
         .getOrCreate())

print("Spark UI URL:", spark.sparkContext.uiWebUrl)

# Завантажуємо датасет
nuek_df = (spark.read
           .option("header", "true")
           .option("inferSchema", "true")
           .csv(r"C:\Project\MasterSc\Date_Engineering\HW\data\nuek-vuh3.csv"))

nuek_repart = nuek_df.repartition(2)

nuek_processed = (nuek_repart
                  .where("final_priority < 3")
                  .select("unit_id", "final_priority")
                  .groupBy("unit_id")
                  .count())

# Ось ТУТ додано рядок
nuek_processed = nuek_processed.where("count>2")

nuek_processed.collect()

input("Press Enter to continue...5")

# Закриваємо сесію Spark
spark.stop()
