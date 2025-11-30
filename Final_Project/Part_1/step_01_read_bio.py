import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType

if sys.platform.startswith("win"):
    hadoop_home_path = "C:\\hadoop"
    hadoop_bin_path = os.path.join(hadoop_home_path, "bin")

    if os.path.exists(os.path.join(hadoop_bin_path, "winutils.exe")):
        os.environ["HADOOP_HOME"] = hadoop_home_path
        if hadoop_bin_path not in os.environ.get("PATH", ""):
            os.environ["PATH"] = (
                hadoop_bin_path + os.pathsep + os.environ.get("PATH", "")
            )
            print(f"✅ HADOOP_HOME встановлено. PATH оновлено: {hadoop_bin_path}")
        else:
            print(f"✅ HADOOP_HOME встановлено (PATH вже містить {hadoop_bin_path}).")
    else:
        print(f"⚠️ ПОПЕРЕДЖЕННЯ: winutils.exe не знайдено за шляхом: {hadoop_bin_path}")


# ==========================================================
# КОНФІГУРАЦІЯ ПІДКЛЮЧЕННЯ ТА ШЛЯХИ
# ==========================================================
jdbc_host = "217.61.57.46"
jdbc_port = "3306"
jdbc_db = "olympic_dataset"
jdbc_table = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
jdbc_url = f"jdbc:mysql://{jdbc_host}:{jdbc_port}/{jdbc_db}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"

current_dir = os.path.dirname(os.path.abspath(__file__))
mysql_jar_path = os.path.join(current_dir, "jars", "mysql-connector-j-8.0.32.jar")

# Перевірка чи існує файл
if not os.path.exists(mysql_jar_path):
    print(f"❌ ПОМИЛКА: MySQL connector не знайдено за шляхом: {mysql_jar_path}")
    print("Перевірте чи файл існує за цією адресою:")
    print(
        "C:\\Project\\MasterSc\\Date_Engineering\\Final_Project\\Part_1\\jars\\mysql-connector-j-8.0.32.jar"
    )
    exit(1)
else:
    print(f"✅ MySQL connector знайдено: {mysql_jar_path}")

out_path_local = os.path.join(current_dir, "output", "athlete_bio_filtered")
json_out_path_local = os.path.join(current_dir, "output", "athlete_bio_filtered_json")

# ==========================================================
# Створення SparkSession та обхід Native IO
# ==========================================================
print(
    "Налаштування SparkSession для максимальної стійкості (RawLocalFileSystem + Committer v1)..."
)

spark = (
    SparkSession.builder.appName("ReadAthleteBioStep1")
    .config("spark.jars", mysql_jar_path)
    .master("local[*]")
    # Налаштування для Spark/Hadoop
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "1")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("SparkSession створено та готово до роботи.")

# ==========================================================
# Читання таблиці athlete_bio з MySQL
# ==========================================================
print(f"Спроба зчитування з {jdbc_host}:{jdbc_port}/{jdbc_db}.{jdbc_table}")

try:
    df_bio = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", jdbc_table)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .load()
    )

    initial_count = df_bio.count()
    print(f"✅ Загальна кількість записів, зчитаних з MySQL: {initial_count}")

except Exception as e:
    print(f"❌ ПОМИЛКА при зчитуванні з MySQL: {e}")
    print("Перевірте:")
    print("1. Підключення до інтернету")
    print("2. Доступність сервера 217.61.57.46:3306")
    print("3. Коректність логіна та пароля")
    spark.stop()
    exit(1)

# ==========================================================
# Фільтрація та очищення даних
# ==========================================================
df_with_cast = df_bio.withColumn(
    "height_num", col("height").cast(DoubleType())
).withColumn("weight_num", col("weight").cast(DoubleType()))

df_filtered = df_with_cast.filter(
    col("height_num").isNotNull() & (col("height_num") > 0)
).filter(col("weight_num").isNotNull() & (col("weight_num") > 0))

df_filtered = df_filtered.withColumn("processed_ts", current_timestamp())

filtered_count = df_filtered.count()
print(f"✅ Загальна кількість відфільтрованих записів: {filtered_count}")

print("\nСхема даних:")
df_filtered.printSchema()

print("\nПерші 10 записів:")
df_filtered.show(10)

# ==========================================================
# Збереження результатів у CSV (через Pandas)
# ==========================================================
print(f"\nСпроба збереження у CSV за шляхом: {out_path_local} (через Pandas)")
try:
    # 1. Збір даних у Pandas DataFrame (виключає Hadoop I/O)
    pandas_df = df_filtered.toPandas()

    # 2. Створення вихідного каталогу
    os.makedirs(out_path_local, exist_ok=True)

    # 3. Збереження у CSV за допомогою чистого Python I/O
    output_file = os.path.join(out_path_local, "athlete_bio_single_file.csv")
    pandas_df.to_csv(output_file, index=False, header=True, sep=",")

    print(
        f"=== ✅ Відфільтровані дані успішно збережено у CSV форматі: {output_file} ==="
    )

    # Перевірка файлів
    print(f"Файли у вихідній теці {out_path_local}:")
    file_size = os.path.getsize(output_file) / (1024 * 1024)  # Розмір у MB
    print(f"  - athlete_bio_single_file.csv ({file_size:.2f} MB)")

except Exception as e:
    print(f"❌ ПОМИЛКА ПРИ ЗАПИСІ CSV (через Pandas): {e}")


# ==========================================================
# Збереження у JSON (через Pandas)
# ==========================================================
print(f"\nСпроба збереження у JSON за шляхом: {json_out_path_local} (через Pandas)")
try:
    os.makedirs(json_out_path_local, exist_ok=True)
    json_output_file = os.path.join(json_out_path_local, "athlete_bio_single_file.json")

    # Використовуємо той самий Pandas DF, якщо він вже був створений
    if "pandas_df" not in locals():
        pandas_df = df_filtered.toPandas()

    pandas_df.to_json(json_output_file, orient="records", lines=True)

    print(f"=== ✅ Дубльовано у JSON форматі: {json_output_file} ===")
except Exception as e:
    print(f"❌ ПОМИЛКА ПРИ ЗАПИСІ JSON (через Pandas): {e}")


# ==========================================================
# Фінальна інформація
# ==========================================================
print("\n" + "=" * 50)
print("РЕЗЮМЕ ОБРОБКИ:")
print(f"- Зчитано з MySQL: {initial_count} записів")
print(f"- Після фільтрації: {filtered_count} записів")
print(f"- Відфільтровано: {initial_count - filtered_count} записів")
print(f"- Дані збережено за допомогою Pandas у: {out_path_local}")
print("=" * 50)

spark.stop()
print("SparkSession зупинено.")
