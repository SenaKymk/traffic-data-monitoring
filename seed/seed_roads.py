import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from sqlalchemy.orm import Session
from src.db import engine
from src.models import Road

SEED_ROADS = [
    {"road_id": "D-100", "name": "D-100", "latitude": 41.0438, "longitude": 29.0094},
    {"road_id": "TEM", "name": "TEM Motorway", "latitude": 41.0927, "longitude": 29.0527},
    {"road_id": "E-5", "name": "E-5", "latitude": 41.0049, "longitude": 28.9784},
    {"road_id": "O-7", "name": "O-7 North", "latitude": 41.1625, "longitude": 28.8458},
    {"road_id": "BOSPH", "name": "Bosphorus Bridge", "latitude": 41.0451, "longitude": 29.0340},
]

if __name__ == "__main__":
    with Session(engine) as session:
        existing = {r.road_id for r in session.query(Road.road_id).all()}
        payload = [r for r in SEED_ROADS if r["road_id"] not in existing]

        if payload:
            session.bulk_insert_mappings(Road, payload)
            session.commit()
            print(f"Inserted {len(payload)} roads")
        else:
            print("No new roads to insert")
