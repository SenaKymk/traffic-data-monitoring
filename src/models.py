from sqlalchemy import (
    Column,
    String,
    Float,
    Integer,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
    CheckConstraint,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Road(Base):
    __tablename__ = "roads"

    road_id = Column(String(64), primary_key=True)
    name = Column(String(255), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    stats = relationship("TrafficHourlyStat", back_populates="road")

class TrafficEvent(Base):
    __tablename__ = "traffic_events"
    __table_args__ = (
        Index("ix_traffic_events_road_id", "road_id"),
        Index("ix_traffic_events_ts", "ts"),
        CheckConstraint("speed_kmh >= 0", name="ck_event_speed_nonneg"),
        CheckConstraint("vehicle_count >= 0", name="ck_event_vehicle_count_nonneg"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    road_id = Column(String(64), nullable=False)
    ts = Column(String(32), nullable=False)
    speed_kmh = Column(Float, nullable=False)
    vehicle_count = Column(Integer, nullable=True)

class TrafficHourlyStat(Base):
    __tablename__ = "traffic_hourly_stats"
    __table_args__ = (
        UniqueConstraint("road_id", "hour_ts", name="uq_road_hour"),
        Index("ix_traffic_hourly_stats_road_id", "road_id"),
        Index("ix_traffic_hourly_stats_hour_ts", "hour_ts"),
        CheckConstraint("avg_speed >= 0", name="ck_avg_speed_nonneg"),
        CheckConstraint("vehicle_count >= 0", name="ck_vehicle_count_nonneg"),
        CheckConstraint("congestion_index >= 0", name="ck_congestion_index_nonneg"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    road_id = Column(String(64), ForeignKey("roads.road_id", ondelete="CASCADE"), nullable=False)
    hour_ts = Column(DateTime, nullable=False)
    avg_speed = Column(Float, nullable=False)
    vehicle_count = Column(Integer, nullable=False)
    congestion_index = Column(Float, nullable=False)

    road = relationship("Road", back_populates="stats")
