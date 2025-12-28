import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.models import Base
from src.db import engine

if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    print("Tables created")
