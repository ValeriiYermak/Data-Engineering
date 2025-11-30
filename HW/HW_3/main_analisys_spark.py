import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round as spark_round

os.environ["PYSPARK_PYTHON"] = r"D:\Project\MasterSc\Data Engineering\.venv_spark\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Project\MasterSc\Data Engineering\.venv_spark\Scripts\python.exe"

# 1. Створюємо SparkSession
spark = (SparkSession.builder.appName("DataAnalysisTask").getOrCreate())

# 2. Завантаження CSV-файлів
users = spark.read.option("header", True).option("inferSchema", True).csv("data/users.csv")
purchases = spark.read.option("header", True).option("inferSchema", True).csv("data/purchases.csv")
products = spark.read.option("header", True).option("inferSchema", True).csv("data/products.csv")

# 3. Очищення даних — видаляємо рядки з пропущеними значеннями
users = users.dropna()
purchases = purchases.dropna()
products = products.dropna()

# 4. Об’єднуємо всі три таблиці
df = (purchases.join(users, on="user_id", how="inner").join(products, on="product_id", how="inner"))

# 5. Розрахунок загальної суми покупок по категоріях
df = df.withColumn("total_price", col("quantity") * col("price"))

total_by_category = (df.groupBy("category").agg(spark_round(spark_sum("total_price"), 2).alias("total_sum"))
                     .orderBy(col("total_sum").desc()))

print("\n=== Загальна сума покупок за категоріями ===")
total_by_category.show(truncate=False)

# 6. Фільтр: користувачі віком 18–25
df_young = df.filter((col("age") >= 18) & (col("age") <= 25))

# 7. Сума покупок за категоріями для віку 18–25
sum_young_by_category = (df_young.groupBy("category").agg(spark_round(spark_sum("total_price"), 2).alias("sum_young")))

print("\n=== Сума покупок (18–25 років) за категоріями ===")
sum_young_by_category.show(truncate=False)

# 8. Розрахунок частки покупок кожної категорії для віку 18–25
total_young_sum = sum_young_by_category.agg(spark_sum("sum_young").alias("total_young")).collect()[0]["total_young"]

share_young = (
    sum_young_by_category
    .withColumn("share_percent", spark_round(col("sum_young") / total_young_sum * 100, 2))
    .orderBy(col("share_percent").desc())
)

print("\n=== Частка покупок (18–25 років) у % від загальних витрат ===")
share_young.show(truncate=False)

# 9. Топ-3 категорії
top3 = share_young.limit(3)

print("\n=== Топ-3 категорії (18–25 років) ===")
top3.show(truncate=False)

# Завершення
spark.stop()
