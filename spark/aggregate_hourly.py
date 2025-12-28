import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUTPUT_PATH = os.getenv("SPARK_OUTPUT_PATH", "/data/traffic/curated/hourly")

spark = (
    SparkSession.builder
    .appName("traffic-hourly-aggregation")
    .enableHiveSupport()
    .getOrCreate()
)

traffic = spark.table("traffic_events")

traffic_ts = (
    traffic
    .withColumn("ts_parsed", F.to_timestamp("ts"))
    .withColumn("vehicle_count", F.coalesce(F.col("vehicle_count"), F.lit(0)))
    .filter(F.col("ts_parsed").isNotNull())
)

hourly = (
    traffic_ts
    .withColumn("hour_ts", F.date_trunc("hour", F.col("ts_parsed")))
    .groupBy("road_id", "hour_ts")
    .agg(
        F.avg("speed_kmh").alias("avg_speed"),
        F.sum("vehicle_count").alias("vehicle_count")
    )
    .withColumn(
        "congestion_index",
        F.col("vehicle_count") / (F.col("avg_speed") + F.lit(1.0))
    )
)

hourly.write.mode("overwrite").format("parquet").save(OUTPUT_PATH)

spark.stop()
