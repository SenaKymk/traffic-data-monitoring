import os
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.models import TrafficHourlyStat

from pyspark.sql import SparkSession

SPARK_OUTPUT_PATH = os.getenv("SPARK_OUTPUT_PATH", "/data/traffic/curated/hourly")

spark = (
    SparkSession.builder
    .appName("traffic-hourly-load")
    .getOrCreate()
)

hourly = spark.read.parquet(SPARK_OUTPUT_PATH)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://traffic_user:traffic_pass@localhost:5432/traffic_db"
)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

def _row_to_payload(row):
    if row["avg_speed"] is None or row["hour_ts"] is None or row["road_id"] is None:
        return None
    return {
        "road_id": row["road_id"],
        "hour_ts": row["hour_ts"],
        "avg_speed": float(row["avg_speed"]) if row["avg_speed"] is not None else None,
        "vehicle_count": int(row["vehicle_count"]) if row["vehicle_count"] is not None else 0,
        "congestion_index": float(row["congestion_index"]) if row["congestion_index"] is not None else None,
    }

def _bulk_insert_partition(rows_iter):
    # Create engine/session per partition to avoid driver-side connections.
    engine = create_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True
    )
    SessionLocal = sessionmaker(bind=engine)

    batch = []
    with SessionLocal() as session:
        try:
            for row in rows_iter:
                payload = _row_to_payload(row)
                if payload is None:
                    continue
                batch.append(payload)
                if len(batch) >= BATCH_SIZE:
                    session.bulk_insert_mappings(TrafficHourlyStat, batch)
                    session.commit()
                    batch.clear()

            if batch:
                session.bulk_insert_mappings(TrafficHourlyStat, batch)
                session.commit()
        except Exception:
            session.rollback()
            raise

    engine.dispose()

# Stream inserts per partition to avoid collecting to the driver.
(
    hourly
    .select("road_id", "hour_ts", "avg_speed", "vehicle_count", "congestion_index")
    .rdd
    .foreachPartition(_bulk_insert_partition)
)

spark.stop()
