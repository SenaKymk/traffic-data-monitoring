import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from sqlalchemy.orm import Session
from src.db import engine
from src.models import Road, TrafficHourlyStat

HOURS_BACK = int(os.getenv("HOURS_BACK", "72"))
SEED = int(os.getenv("SEED", "42"))

random.seed(SEED)

if __name__ == "__main__":
    end_ts = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_ts = end_ts - timedelta(hours=HOURS_BACK)

    with Session(engine) as session:
        roads = session.query(Road).all()
        if not roads:
            raise SystemExit("No roads found. Run seed_roads.py first.")

        session.query(TrafficHourlyStat).filter(
            TrafficHourlyStat.hour_ts >= start_ts,
            TrafficHourlyStat.hour_ts <= end_ts
        ).delete(synchronize_session=False)

        payload = []
        current = start_ts
        while current <= end_ts:
            for road in roads:
                avg_speed = random.uniform(10.0, 80.0)
                vehicle_count = random.randint(0, 120)
                congestion_index = vehicle_count / (avg_speed + 1.0)

                payload.append({
                    "road_id": road.road_id,
                    "hour_ts": current,
                    "avg_speed": avg_speed,
                    "vehicle_count": vehicle_count,
                    "congestion_index": congestion_index,
                })

            current += timedelta(hours=1)

        session.bulk_insert_mappings(TrafficHourlyStat, payload)
        session.commit()
        print(f"Inserted {len(payload)} hourly stats")
