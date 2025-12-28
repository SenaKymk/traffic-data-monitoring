# Steps 7-9: PostgreSQL + ORM + Load

## Step 7 - SQLAlchemy ORM models + DB setup

```python
# src/models.py
from sqlalchemy import Column, String, Float, Integer, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Road(Base):
    __tablename__ = "roads"

    road_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    stats = relationship("TrafficHourlyStat", back_populates="road")

class TrafficHourlyStat(Base):
    __tablename__ = "traffic_hourly_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    road_id = Column(String, ForeignKey("roads.road_id"), nullable=False)
    hour_ts = Column(DateTime, nullable=False)
    avg_speed = Column(Float, nullable=False)
    vehicle_count = Column(Integer, nullable=False)
    congestion_index = Column(Float, nullable=False)

    road = relationship("Road", back_populates="stats")
```

```python
# src/db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://traffic_user:traffic_pass@localhost:5432/traffic_db")

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(bind=engine)
```

```python
# src/create_tables.py
from models import Base
from db import engine

if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    print("Tables created")
```

## Step 8 - Spark -> ORM data load (bulk insert, scalable)

```python
# src/load_hourly_stats.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import TrafficHourlyStat

from pyspark.sql import SparkSession

SPARK_OUTPUT_PATH = os.getenv("SPARK_OUTPUT_PATH", "/data/traffic/curated/hourly")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://traffic_user:traffic_pass@localhost:5432/traffic_db"
)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

spark = (
    SparkSession.builder
    .appName("traffic-hourly-load")
    .getOrCreate()
)

hourly = spark.read.parquet(SPARK_OUTPUT_PATH)

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

(
    hourly
    .select("road_id", "hour_ts", "avg_speed", "vehicle_count", "congestion_index")
    .rdd
    .foreachPartition(_bulk_insert_partition)
)

spark.stop()
```

## Step 9 - ORM analytic queries

```python
# src/analytics.py
from sqlalchemy import func, case, cast, Date
from sqlalchemy.orm import Session
from db import engine
from models import TrafficHourlyStat, Road

# Average speed by hour
with Session(engine) as session:
    avg_by_hour = (
        session.query(
            func.date_part('hour', TrafficHourlyStat.hour_ts).label('hour'),
            func.avg(TrafficHourlyStat.avg_speed).label('avg_speed')
        )
        .group_by('hour')
        .order_by('hour')
        .all()
    )

# Top congested roads (highest avg congestion index)
with Session(engine) as session:
    top_congested = (
        session.query(
            TrafficHourlyStat.road_id,
            func.avg(TrafficHourlyStat.congestion_index).label('avg_congestion')
        )
        .group_by(TrafficHourlyStat.road_id)
        .order_by(func.avg(TrafficHourlyStat.congestion_index).desc())
        .limit(10)
        .all()
    )

# Weekday vs weekend comparison
with Session(engine) as session:
    day_type = case(
        (func.date_part('dow', TrafficHourlyStat.hour_ts).in_([0, 6]), 'weekend'),
        else_='weekday'
    )

    weekday_weekend = (
        session.query(
            day_type.label('day_type'),
            func.avg(TrafficHourlyStat.avg_speed).label('avg_speed'),
            func.avg(TrafficHourlyStat.vehicle_count).label('avg_vehicle_count')
        )
        .group_by('day_type')
        .all()
    )
```
