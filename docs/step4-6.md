# Steps 4-6: Hive + Spark

## Step 4 - Hive external table (traffic_events)

Note: Hive does not allow a column and a partition to share the same name. Because `region` is a partition column, it is omitted from the column list below and comes from the partition path.

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_events (
  event_id STRING,
  source STRING,
  city STRING,
  road_id STRING,
  ts STRING,
  speed_kmh DOUBLE,
  vehicle_count INT,
  latitude DOUBLE,
  longitude DOUBLE,
  ingest_ts STRING
)
PARTITIONED BY (
  dt STRING,
  hour STRING,
  region STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/traffic/raw';

-- After new partitions arrive:
-- MSCK REPAIR TABLE traffic_events;

## Step 5 - Hive analytics queries

1) Average speed by hour of day

SELECT
  hour,
  AVG(speed_kmh) AS avg_speed
FROM traffic_events
GROUP BY hour
ORDER BY hour;

2) Roads with highest congestion frequency

-- Define congestion as avg speed < 20 km/h per road/hour
WITH road_hour AS (
  SELECT
    road_id,
    dt,
    hour,
    AVG(speed_kmh) AS avg_speed
  FROM traffic_events
  GROUP BY road_id, dt, hour
)
SELECT
  road_id,
  COUNT(*) AS congested_hours
FROM road_hour
WHERE avg_speed < 20
GROUP BY road_id
ORDER BY congested_hours DESC;

3) Weekday vs weekend traffic comparison

SELECT
  CASE
    WHEN date_format(to_date(ts), 'u') IN ('6','7') THEN 'weekend'
    ELSE 'weekday'
  END AS day_type,
  AVG(speed_kmh) AS avg_speed,
  AVG(vehicle_count) AS avg_vehicle_count
FROM traffic_events
GROUP BY
  CASE
    WHEN date_format(to_date(ts), 'u') IN ('6','7') THEN 'weekend'
    ELSE 'weekday'
  END;

## Step 6 - Spark aggregation job (PySpark)

The job reads from Hive `traffic_events`, aggregates hourly stats per road, and writes a curated output. Congestion index logic:

congestion_index = total_vehicle_count / (avg_speed + 1)

This grows when volume is high and speed is low; the +1 avoids division by zero.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("traffic-hourly-aggregation")
    .enableHiveSupport()
    .getOrCreate()
)

# Read from Hive table
traffic = spark.table("traffic_events")

# Parse timestamp and normalize vehicle_count
traffic_ts = (
    traffic
    .withColumn("ts_parsed", F.to_timestamp("ts"))
    .withColumn("vehicle_count", F.coalesce(F.col("vehicle_count"), F.lit(0)))
)

# Hourly rollup per road
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

# Output schema: road_id, hour_ts, avg_speed, vehicle_count, congestion_index
hourly.write.mode("overwrite").format("parquet").save("/data/traffic/curated/hourly")

spark.stop()
```
