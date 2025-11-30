from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


spark = (SparkSession.builder
         .appName("SensorAggregation")
         .master("local[*]")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")


# Schema for the incoming data

schema = StructType([
    StructField("id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", TimestampType())
])

# Read the data from Kafka

raw_df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", "building_sensors_V")
          .option("startingOffsets", "latest")
          .load())

sensor_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str")
sensor_df = sensor_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Sliding Window agregation

agg_df = (sensor_df
          .withWatermark("timestamp", "10 seconds")
          .groupBy(window("timestamp", "1 minute", "30 seconds"))
          .agg({"temperature": "avg", "humidity": "avg"})
          .withColumnRenamed("avg(temperature)", "t_avg")
          .withColumnRenamed("avg(humidity)", "h_avg")
          .withColumn("timestamp", current_timestamp()))

# Send to Kafka Topic
agg_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "sensor_aggregated") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_agg") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
