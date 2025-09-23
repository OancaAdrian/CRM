import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv("DATABASE_URL") or "postgresql://user:pass@localhost:5432/dbname"

# Engine + session factory
engine: Engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# FastAPI dependency (generator)
def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()
