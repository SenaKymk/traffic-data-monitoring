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
