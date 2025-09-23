import os
import socket
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# Configuration
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

# Ensure SQLAlchemy uses the correct dialect prefix (postgresql)
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


class DBTest(BaseModel):
    db: int


@app.on_event("startup")
def startup_check_db():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        app.logger = getattr(app, "logger", None)
        # Log reachable state to Render logs
        print("DB reachable at startup")
    except OperationalError as e:
        # Print a startup warning but allow app to boot (endpoints should handle DB errors)
        print("DB startup check failed:", str(e))


@app.get("/test-db", response_model=DBTest)
def test_db():
    """
    Simple DB sanity endpoint: returns {"db":1} when a DB query succeeds.
    """
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


@app.get("/resolve-db")
def resolve_db():
    """
    Debug endpoint that returns the IPv4/IPv6 addresses the container sees for the DB host.
    Temporary: remove after debugging.
    """
    host = os.environ.get("DB_HOST_OVERRIDE") or "db.mvlhhotwozhbnspgqjxz.supabase.co"
    try:
        infos = socket.getaddrinfo(host, None)
        ipv4s = sorted({ai[4][0] for ai in infos if ai[0] == socket.AF_INET})
        ipv6s = sorted({ai[4][0] for ai in infos if ai[0] == socket.AF_INET6})
        return {"host": host, "ipv4": ipv4s, "ipv6": ipv6s}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/firme")
def list_firme(
    q: str = Query(..., description="CUI to look up"),
    limit: int = Query(50, ge=1, le=500),
) -> List[Dict[str, Any]]:
    """
    Lookup firms by CUI.
    Maps database column `denumire` to `name` so frontends expecting `name` still work.
    """
    if not q:
        raise HTTPException(status_code=400, detail="Missing query parameter `q`")

    # Use explicit column names that match the actual DB schema (denumire, cui).
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
            # If frontend expects an `id` field, provide one derived from `cui` (temporary).
            results = []
            for r in rows:
                rec = dict(r)
                if "id" not in rec:
                    rec["id"] = rec.get("cui")  # temporary stable identifier
                results.append(rec)
            return results
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except SQLAlchemyError as e:
        # Surface SQL errors for debugging (replace with safer logging in prod)
        raise HTTPException(status_code=500, detail=str(e))


# Optional: health check
@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="unhealthy")
