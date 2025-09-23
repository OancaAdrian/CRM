import os
import socket
from time import sleep
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# Configuration
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

# Ensure SQLAlchemy uses the correct dialect prefix
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Engine with resilience settings
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=2,
    max_overflow=3,
    connect_args={"connect_timeout": 10},
    future=True,
)

app = FastAPI(title="CRM API")

# Serve static files and index
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
def root_index():
    return FileResponse("static/index.html")


class DBTest(BaseModel):
    db: int


@app.on_event("startup")
def startup_check_db():
    # Simple retry loop to make logs clearer on ephemeral DB startup delays
    for attempt in range(3):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("DB reachable at startup")
            return
        except OperationalError as e:
            print(f"DB startup check failed (attempt {attempt+1}): {e}")
            sleep(2)
    print("DB unreachable after retries")


@app.get("/test-db", response_model=DBTest)
def test_db():
    try:
        with engine.connect() as conn:
            r = conn.execute(text("SELECT 1")).scalar_one_or_none()
            if r is None:
                raise HTTPException(status_code=503, detail="DB query returned no rows")
            return {"db": 1}
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/firme")
def list_firme(
    q: str = Query(..., description="CUI to look up"),
    limit: int = Query(50, ge=1, le=500),
) -> List[Dict[str, Any]]:
    if not q:
        raise HTTPException(status_code=400, detail="Missing query parameter `q`")

    # Use explicit column names that match the DB schema (denumire, cui).
    stmt = text(
        """
        SELECT denumire AS name, cui
        FROM firms
        WHERE cui = :cui
        LIMIT :limit
        """
    )

    try:
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"cui": q, "limit": limit}).mappings().all()
            results = []
            for r in rows:
                rec = dict(r)
                # Provide an `id` field for frontends that expect it (temporary/stable = cui)
                if "id" not in rec:
                    rec["id"] = rec.get("cui")
                results.append(rec)
            return results
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="unhealthy")
