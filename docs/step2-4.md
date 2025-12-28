# Steps 2-4: Storage, NiFi, Hive

## Step 2 - HDFS storage design
Partitioned by date (dt), hour, and region.

Example layout:

- /data/traffic/raw/dt=YYYY-MM-DD/hour=HH/region=XXX/part-000.json

Notes:
- dt from event timestamp (ts) in UTC or agreed timezone.
- hour is zero-padded 00-23.
- region is a normalized string (e.g., district code).

## Step 3 - Apache NiFi flow
Processors in order:

1) InvokeHTTP (API source)
- Purpose: simulate traffic API ingestion
- HTTP Method: GET
- Remote URL: https://example-traffic-api.local/events
- SSL Context Service: (if HTTPS required)
- Output Response Regardless: true
- Always Output Response: true
- Retry Count: 3
- Connection Timeout: 10 sec
- Read Timeout: 30 sec
- Response Body Attribute: (blank, keep in flowfile content)

2) InvokeHTTP (Sensor source)
- Purpose: simulate sensor gateway ingestion
- HTTP Method: GET
- Remote URL: https://example-sensor-gateway.local/events
- Output Response Regardless: true
- Always Output Response: true
- Retry Count: 3
- Connection Timeout: 10 sec
- Read Timeout: 30 sec

3) InvokeHTTP (GPS source)
- Purpose: simulate public transport GPS ingestion
- HTTP Method: GET
- Remote URL: https://example-gps-service.local/events
- Output Response Regardless: true
- Always Output Response: true
- Retry Count: 3
- Connection Timeout: 10 sec
- Read Timeout: 30 sec

4) JoltTransformJSON
- Jolt Spec: see below (canonical schema mapping)
- Jolt Transformation DSL: Chain
- Custom Transformation: (none)

5) UpdateAttribute
- Purpose: derive partition values
- Attributes:
  - dt = ${ts:toDate("yyyy-MM-dd'T'HH:mm:ssX"):format("yyyy-MM-dd")}
  - hour = ${ts:toDate("yyyy-MM-dd'T'HH:mm:ssX"):format("HH")}
  - region = ${region:trim():toLower()}

6) PutHDFS
- Hadoop Configuration Resources: /etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
- Directory: /data/traffic/raw/dt=${dt}/hour=${hour}/region=${region}
- Conflict Resolution Strategy: replace
- Record Writer: (none, writing raw JSON)
- Compression: none (enable gzip in prod if needed)

### Sample JOLT spec

[
  {
    "operation": "shift",
    "spec": {
      "id": "event_id",
      "source": "source",
      "city": "city",
      "region": "region",
      "roadId": "road_id",
      "timestamp": "ts",
      "speedKmh": "speed_kmh",
      "vehicleCount": "vehicle_count",
      "lat": "latitude",
      "lon": "longitude",
      "ingestTs": "ingest_ts"
    }
  },
  {
    "operation": "modify-overwrite-beta",
    "spec": {
      "speed_kmh": "=toDouble",
      "vehicle_count": "=toInteger"
    }
  }
]

### Example input JSON

{
  "id": "9b2c4a7e-4df8-4a6f-a2d2-6f3e2f5a91f4",
  "source": "api",
  "city": "Istanbul",
  "region": "BESIKTAS",
  "roadId": "D-100",
  "timestamp": "2025-01-10T08:15:42Z",
  "speedKmh": 42.7,
  "vehicleCount": 18,
  "lat": 41.0438,
  "lon": 29.0094,
  "ingestTs": "2025-01-10T08:15:45Z"
}

### Example output JSON (canonical)

{
  "event_id": "9b2c4a7e-4df8-4a6f-a2d2-6f3e2f5a91f4",
  "source": "api",
  "city": "Istanbul",
  "region": "BESIKTAS",
  "road_id": "D-100",
  "ts": "2025-01-10T08:15:42Z",
  "speed_kmh": 42.7,
  "vehicle_count": 18,
  "latitude": 41.0438,
  "longitude": 29.0094,
  "ingest_ts": "2025-01-10T08:15:45Z"
}

## Step 4 - Hive external table

-- External table over raw JSON in HDFS
-- Region is a partition column (the raw JSON may still include region; extra fields are ignored by JSON SerDe)

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

-- After loading new partitions:
-- MSCK REPAIR TABLE traffic_events;
