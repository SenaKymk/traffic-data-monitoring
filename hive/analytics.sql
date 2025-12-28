-- 1) Average speed by hour of day
SELECT
  hour,
  AVG(speed_kmh) AS avg_speed
FROM traffic_events
GROUP BY hour
ORDER BY hour;

-- 2) Roads with highest congestion frequency
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

-- 3) Weekday vs weekend comparison
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
