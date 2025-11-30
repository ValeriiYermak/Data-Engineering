from pyspark.sql import SparkSession
from app.config import JDBC_JAR

def main():
    spark = (
        SparkSession.builder
        .appName("SparkTest")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )

    df = spark.range(10)
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()
