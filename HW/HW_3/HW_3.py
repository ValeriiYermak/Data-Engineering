import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round as spark_round
from tabulate import tabulate
from pyspark.sql.functions import isnan, when, count
from pyspark.sql.types import NumericType
from pandas import read_csv

# === 1. Створюємо SparkSession ===
spark = (SparkSession.builder.appName("Spark Data Analysis Task").getOrCreate())

# === 2. Завантаження CSV-файлів ===
users = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Project\MasterSc\Date_Engineering\HW\data\users.csv")
purchases = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Project\MasterSc\Date_Engineering\HW\data\purchases.csv")
products = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Project\MasterSc\Date_Engineering\HW\data\products.csv")

print()
print('================================== ДАНІ КОРИСТУВАЧІВ ================================')
users.show(5)

print()
print('================================== ДАНІ ПРО ПОКУПКИ =================================')
purchases.show(5)

print()
print('============================== ІНФОРМАЦІЯ ПРО ПРОДУКТИ ==============================')
products.show(5)


# === 2.1. Перевіряємо пропущені значення ===

def count_missing(df, name):
    cols_expr = []
    for c in df.columns:
        # якщо колонка числова, перевіряємо і isnan(), і isNull()
        if isinstance(df.schema[c].dataType, NumericType):
            cols_expr.append(count(when(col(c).isNull() | isnan(c), c)).alias(c))
        else:
            cols_expr.append(count(when(col(c).isNull(), c)).alias(c))

    missing_count = df.select(cols_expr)
    total_missing = missing_count.collect()[0].asDict()
    total_rows = df.count()

    print(f"\n🔍 Пропущені значення у DataFrame '{name}': (усього рядків: {total_rows})")
    for column, count_nulls in total_missing.items():
        if count_nulls > 0:
            print(f"  - {column}: {count_nulls}")
    if all(v == 0 for v in total_missing.values()):
        print("  ✅ Пропущених значень не знайдено!")

print()
print('============================== ПРОПУЩЕНІ ЗНАЧЕННЯ У DATAFRAME ==============================')
count_missing(users, "users")
count_missing(purchases, "purchases")
count_missing(products, "products")

# === 3. Очищення даних ===
users = users.dropna()
purchases = purchases.dropna()
products = products.dropna()

print()
print('============================== ДАНІ ПІСЛЯ ОЧИЩЕННЯ ==============================')
count_missing(users, "users")
count_missing(purchases, "purchases")
count_missing(products, "products")

# === 4. Об’єднання таблиць ===
df = (purchases.join(users, on="user_id", how="inner").join(products, on="product_id", how="inner"))

# === 5. Додаємо стовпець із загальною сумою покупки ===
df = df.withColumn("total_price", col("quantity") * col("price"))

# === 6. Загальна сума покупок по категоріях ===
total_by_category = (df.groupBy("category").agg(spark_round(spark_sum("total_price"), 2).alias("total_sum"))
                     .orderBy(col("total_sum").desc()))

# === 7. Дані по молоді 18–25 років ===
df_young = df.filter((col("age") >= 18) & (col("age") <= 25))

sum_young_by_category = (df_young.groupBy("category").agg(spark_round(spark_sum("total_price"), 2)
                                                          .alias("sum_young"))
                         .orderBy(col("sum_young").desc()))

# === 8. Частка покупок у відсотках ===
total_young_sum = sum_young_by_category.agg(spark_sum("sum_young").alias("total_young")).collect()[0]["total_young"]

share_young = (
    sum_young_by_category.withColumn("share_percent", spark_round(col("sum_young") / total_young_sum * 100, 2))
    .orderBy(col("share_percent").desc())
)

top3 = share_young.limit(3)

# === 9. Перетворення на Pandas для гарного виводу ===
total_by_category_pd = total_by_category.toPandas()
sum_young_by_category_pd = sum_young_by_category.toPandas()
share_young_pd = share_young.toPandas()
top3_pd = top3.toPandas()

# === 10. Вивід красиво через tabulate ===
print()
print('\n============================== ЗАГАЛЬНА СУМА ПОКУПОК ЗА КАТЕГОРІЯМИ ==============================')
print(tabulate(total_by_category_pd, headers='keys', tablefmt='fancy_grid', showindex=False))

print()
print('\n============================== СУМА ПОКУПОК (18–25 РОКІВ): ==============================')
print(tabulate(sum_young_by_category_pd, headers='keys', tablefmt='fancy_grid', showindex=False))

print()
print(
    '\n============================== ЧАСТКА ПОКУПОК (18–25 РОКІВ) У % ВІД ЗАГАЛЬНИХ ВИТРАТ: ==============================')
print(tabulate(share_young_pd, headers='keys', tablefmt='fancy_grid', showindex=False))

print()
print('\n============================== ТОП-3 КАТЕГОРІЇ СЕРЕД МОЛОДІ (18–25): ==============================')
print(tabulate(top3_pd, headers='keys', tablefmt='fancy_grid', showindex=False))

# === 11. Збереження результатів у файли ===
os.makedirs("output", exist_ok=True)

total_by_category_pd.to_csv("output/total_by_category.csv", index=False)
sum_young_by_category_pd.to_csv("output/sum_young_by_category.csv", index=False)
share_young_pd.to_csv("output/share_young.csv", index=False)
top3_pd.to_csv("output/top3_categories.csv", index=False)

print()
print('\n============================== РЕЗУЛЬТАТИ ЗБЕРЕЖЕНО В ПАПКУ "output/" ==============================')

# === 12. Завершення сесії ===
spark.stop()
