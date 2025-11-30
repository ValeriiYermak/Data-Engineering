from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

spark = SparkSession.builder.appName("SensorAlerts").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for the aggregated data
agg_schema = StructType(
    [
        StructField(
            "window",
            StructType(
                [
                    StructField("start", TimestampType()),
                    StructField("end", TimestampType()),
                ]
            ),
        ),
        StructField("t_avg", DoubleType()),
        StructField("h_avg", DoubleType()),
        StructField("timestamp", TimestampType()),
    ]
)


# Read the data from Kafka

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor_aggregated")
    .option("startingOffsets", "latest")
    .load()
)

agg_df = (
    raw_df.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), agg_schema).alias("data"))
    .select("data.*")
)

# read the conditions of all alerts
conditions_df = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)

# Cross join the aggregated data with the alert conditions
joined_df = agg_df.crossJoin(conditions_df)
alert_df = joined_df.filter(
    ((col("min_temp") == -999) | (col("t_avg") >= col("min_temp")))
    & ((col("max_temp") == -999) | (col("t_avg") <= col("max_temp")))
    & ((col("min_humidity") == -999) | (col("h_avg") >= col("min_humidity")))
    & ((col("max_humidity") == -999) | (col("h_avg") <= col("max_humidity")))
)


# format the alert data
final_alert_df = alert_df.select(
    struct(
        struct(
            col("window.start").alias("start"), col("window.end").alias("end")
        ).alias("window"),
        col("t_avg"),
        col("h_avg"),
        col("alert_code").alias("code"),
        col("message"),
        col("timestamp"),
    ).alias("value")
).select(to_json(col("value")).alias("value"))

# Sending to Kafka
final_alert_df.writeStream.format("kafka").option(
    "kafka.bootstrap.servers", "kafka:9092"
).option("topic", "alert_topic").option(
    "checkpointLocation", "/tmp/spark_checkpoint_alerts"
).outputMode(
    "append"
).start().awaitTermination()
