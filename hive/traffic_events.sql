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
