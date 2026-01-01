import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import socketserver

if not hasattr(socketserver, "UnixStreamServer"):
    socketserver.UnixStreamServer = socketserver.TCPServer
    socketserver.UnixStreamRequestHandler = socketserver.StreamRequestHandler



OUTPUT_PATH = os.getenv("OUTPUT_PATH", "D:/proje4/output/hourly")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "traffic_db")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "12345")
MYSQL_JDBC_JAR = os.getenv("MYSQL_JDBC_JAR", "D:/mysql-connector-j-8.0.33.jar")
WRITE_MODE = os.getenv("WRITE_MODE", "append")

if not Path(MYSQL_JDBC_JAR).exists():
    raise FileNotFoundError(
        f"MySQL JDBC driver not found: {MYSQL_JDBC_JAR}. "
        "Set MYSQL_JDBC_JAR to the connector .jar path."
    )

jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&serverTimezone=UTC"

spark = (
    SparkSession.builder
    .appName("traffic-hourly-aggregation")
    .config("spark.jars", MYSQL_JDBC_JAR)
    .config("spark.sql.warehouse.dir", "D:/proje4/output/spark_warehouse")
    .getOrCreate()
)

traffic = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "traffic_events")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
)


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

(
    hourly
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "traffic_hourly_stats")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .mode(WRITE_MODE)
    .save()
)

spark.stop()
