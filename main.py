import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn.error")

# Read DATABASE_URL from env
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL is not set")
    raise RuntimeError("DATABASE_URL environment variable is required")

# Create SQLAlchemy engine with sensible defaults for pooled connections
# pool_pre_ping helps recover broken connections in cloud environments
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    future=True,
)

app = FastAPI(title="CRM API")

# CORS - adjust origins to your frontend domains
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change to specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def on_startup():
    logger.info("Starting application")
    # quick DB smoke-test during startup (non-fatal)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("DB reachable at startup")
    except Exception as e:
        logger.warning("DB startup check failed: %s", e)

@app.get("/test-db")
def test_db():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar_one()
        return {"db": result}
    except OperationalError as oe:
        logger.exception("OperationalError in /test-db")
        raise HTTPException(status_code=503, detail=str(oe))
    except Exception as e:
        logger.exception("Error in /test-db")
        raise HTTPException(status_code=500, detail=str(e))

# Example minimal /firme endpoint using query param 'q' and 'limit'
@app.get("/firme")
def list_firme(q: str, limit: int = 50):
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=422, detail="limit must be between 1 and 200")
    try:
        with engine.connect() as conn:
            # Replace the SQL below with your real query; this is a placeholder
            stmt = text("SELECT id, name, cui FROM firms WHERE cui = :cui LIMIT :limit")
            rows = conn.execute(stmt, {"cui": q, "limit": limit}).mappings().all()
            return {"results": [dict(row) for row in rows]}
    except Exception as e:
        logger.exception("Error in /firme")
        raise HTTPException(status_code=500, detail=str(e))

# Add other endpoints (activitati, agenda, import) below following the same pattern.
# Ensure heavy startup work (data imports, seeding) is not executed on import time.
# ---- temporary endpoint to resolve DB host from within Render container ----
import socket
from fastapi import HTTPException

@app.get("/resolve-db")
def resolve_db():
    host = "db.mvlhhotwozhbnspgqjxz.supabase.co"
    try:
        infos = socket.getaddrinfo(host, None)
        ipv4s = sorted({ai[4][0] for ai in infos if ai[0] == socket.AF_INET})
        ipv6s = sorted({ai[4][0] for ai in infos if ai[0] == socket.AF_INET6})
        return {"host": host, "ipv4": ipv4s, "ipv6": ipv6s}
    except Exception as e:
        # Return error text so we can see it in the response and logs
        raise HTTPException(status_code=500, detail=str(e))
# ---- end temporary endpoint ----
