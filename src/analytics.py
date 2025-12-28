import sys
from pathlib import Path

from sqlalchemy import func, case
from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.db import engine
from src.models import TrafficHourlyStat

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
    print("Average speed by hour:", avg_by_hour)

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
    print("Top congested roads:", top_congested)

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
    print("Weekday vs weekend:", weekday_weekend)
