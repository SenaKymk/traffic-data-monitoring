# NiFi Flow: Traffic Ingestion

## HDFS directory layout

- /data/traffic/raw/dt=YYYY-MM-DD/hour=HH/region=XXX/part-000.json

## Processors and configuration

1) InvokeHTTP (API source)
- HTTP Method: GET
- Remote URL: https://example-traffic-api.local/events
- Output Response Regardless: true
- Always Output Response: true
- Retry Count: 3
- Connection Timeout: 10 sec
- Read Timeout: 30 sec

2) InvokeHTTP (Sensor source)
- HTTP Method: GET
- Remote URL: https://example-sensor-gateway.local/events
- Output Response Regardless: true
- Always Output Response: true
- Retry Count: 3
- Connection Timeout: 10 sec
- Read Timeout: 30 sec

3) InvokeHTTP (GPS source)
- HTTP Method: GET
- Remote URL: https://example-gps-service.local/events
- Output Response Regardless: true
- Always Output Response: true
- Retry Count: 3
- Connection Timeout: 10 sec
- Read Timeout: 30 sec

4) JoltTransformJSON
- Jolt Spec: nifi/jolt_spec.json
- Jolt Transformation DSL: Chain

5) UpdateAttribute
- dt = ${ts:toDate("yyyy-MM-dd'T'HH:mm:ssX"):format("yyyy-MM-dd")}
- hour = ${ts:toDate("yyyy-MM-dd'T'HH:mm:ssX"):format("HH")}
- region = ${region:trim():toLower()}

6) PutHDFS
- Hadoop Configuration Resources: /etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
- Directory: /data/traffic/raw/dt=${dt}/hour=${hour}/region=${region}
- Conflict Resolution Strategy: replace
- Compression: none (enable gzip for production)

## Sample payloads

- Input: nifi/sample_input.json
- Output: nifi/sample_output.json
