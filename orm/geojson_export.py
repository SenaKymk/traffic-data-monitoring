import json
import os
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.db import engine
from src.models import Road, TrafficHourlyStat

OUTPUT_PATH = os.getenv("GEOJSON_OUTPUT_PATH", "output/roads.geojson")

def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)

def validate_geojson(geojson_obj):
    errors = []

    if not isinstance(geojson_obj, dict):
        return ["Root must be a JSON object"]

    if geojson_obj.get("type") != "FeatureCollection":
        errors.append("Root type must be FeatureCollection")

    features = geojson_obj.get("features")
    if not isinstance(features, list):
        errors.append("features must be a list")
        return errors

    for idx, feature in enumerate(features):
        if not isinstance(feature, dict):
            errors.append(f"Feature[{idx}] must be an object")
            continue

        if feature.get("type") != "Feature":
            errors.append(f"Feature[{idx}].type must be Feature")

        geometry = feature.get("geometry")
        if not isinstance(geometry, dict) or geometry.get("type") != "Point":
            errors.append(f"Feature[{idx}].geometry must be a Point")
        else:
            coords = geometry.get("coordinates")
            if (
                not isinstance(coords, list)
                or len(coords) != 2
                or not _is_number(coords[0])
                or not _is_number(coords[1])
            ):
                errors.append(f"Feature[{idx}].geometry.coordinates must be [lon, lat] numbers")

        props = feature.get("properties")
        if not isinstance(props, dict):
            errors.append(f"Feature[{idx}].properties must be an object")
        else:
            if "road_id" not in props or not isinstance(props["road_id"], str):
                errors.append(f"Feature[{idx}].properties.road_id must be a string")
            for key in ("avg_speed", "congestion_index"):
                if key in props and props[key] is not None and not _is_number(props[key]):
                    errors.append(f"Feature[{idx}].properties.{key} must be a number or null")

    return errors

with Session(engine) as session:
    rows = (
        session.query(
            Road.road_id,
            Road.latitude,
            Road.longitude,
            func.avg(TrafficHourlyStat.avg_speed).label("avg_speed"),
            func.avg(TrafficHourlyStat.congestion_index).label("congestion_index")
        )
        .join(TrafficHourlyStat, TrafficHourlyStat.road_id == Road.road_id)
        .group_by(Road.road_id, Road.latitude, Road.longitude)
        .all()
    )

features = []
for r in rows:
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [float(r.longitude), float(r.latitude)]
        },
        "properties": {
            "road_id": r.road_id,
            "avg_speed": float(r.avg_speed) if r.avg_speed is not None else None,
            "congestion_index": float(r.congestion_index) if r.congestion_index is not None else None
        }
    }
    features.append(feature)

geojson = {
    "type": "FeatureCollection",
    "features": features
}

validation_errors = validate_geojson(geojson)
if validation_errors:
    error_text = "\n".join(validation_errors)
    raise ValueError(f"GeoJSON validation failed:\n{error_text}")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
with open(OUTPUT_PATH, "w", encoding="ascii") as f:
    json.dump(geojson, f, ensure_ascii=True)

print(f"GeoJSON exported to {OUTPUT_PATH}")
