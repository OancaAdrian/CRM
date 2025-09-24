import os
import logging
from time import sleep
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crm")

# Configuration
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=2,
    max_overflow=3,
    connect_args={"connect_timeout": 10},
    future=True,
)

app = FastAPI(title="CRM API with tolerant search")

# Serve static UI if present
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
def root_index():
    index_path = "static/index.html"
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"service": "CRM API", "status": "running"}


class DBTest(BaseModel):
    db: int


@app.on_event("startup")
def startup_check_db():
    for attempt in range(3):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("DB reachable at startup")
            return
        except OperationalError as e:
            logger.warning("DB startup check failed (attempt %d): %s", attempt + 1, e)
            sleep(2)
    logger.error("DB unreachable after retries")


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


def has_column(conn, table_name: str, column_name: str) -> bool:
    q = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :table AND column_name = :column
        LIMIT 1
        """
    )
    return conn.execute(q, {"table": table_name, "column": column_name}).scalar_one_or_none() is not None


@app.get("/search")
def search_firms(
    q: str = Query(..., description="CUI or company name"),
    limit: int = Query(20, ge=1, le=200),
) -> List[Dict[str, Any]]:
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Query parameter `q` required")
    q = q.strip()

    stmt_exact = text(
        """
        SELECT denumire AS name, cui, judet, cifra_de_afaceri_neta AS cifra_afaceri
        FROM firms
        WHERE cui = :q
        LIMIT :limit
        """
    )

    stmt_ilike = text(
        """
        SELECT denumire AS name, cui, judet, cifra_de_afaceri_neta AS cifra_afaceri
        FROM firms
        WHERE lower(denumire) ILIKE lower(:pattern)
        ORDER BY lower(denumire) = lower(:q) DESC
        LIMIT :limit
        """
    )

    stmt_similarity = text(
        """
        SELECT denumire AS name, cui, judet, cifra_de_afaceri_neta AS cifra_afaceri,
               similarity(lower(denumire), lower(:q)) AS sim
        FROM firms
        WHERE similarity(lower(denumire), lower(:q)) >= :threshold
        ORDER BY sim DESC
        LIMIT :limit
        """
    )

    pattern = f"%{q}%"
    threshold = 0.45

    try:
        with engine.connect() as conn:
            rows = conn.execute(stmt_exact, {"q": q, "limit": limit}).mappings().all()
            if rows:
                return [dict(r) for r in rows]

            rows = conn.execute(stmt_ilike, {"pattern": pattern, "q": q, "limit": limit}).mappings().all()
            if rows:
                return [dict(r) for r in rows]

            try:
                rows = conn.execute(stmt_similarity, {"q": q, "threshold": threshold, "limit": limit}).mappings().all()
                return [dict(r) for r in rows]
            except Exception:
                logger.debug("similarity fallback failed or pg_trgm missing")
                return []
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/firma/{cui}/detalii")
def firm_details(cui: str):
    try:
        with engine.connect() as conn:
            include_caen_descr = has_column(conn, "firms", "caen_descriere")
            if include_caen_descr:
                stmt = text(
                    """
                    SELECT
                      denumire AS name,
                      cui,
                      judet,
                      localitate,
                      cifra_de_afaceri_neta AS cifra_afaceri,
                      profitul_net AS profit,
                      caen AS cod_caen,
                      NULLIF(NULLIF(caen_descriere, ''), '') AS caen_descriere,
                      numar_mediu_de_salariati AS angajati,
                      numar_licente AS licente
                    FROM firms
                    WHERE cui = :cui
                    LIMIT 1
                    """
                )
            else:
                stmt = text(
                    """
                    SELECT
                      denumire AS name,
                      cui,
                      judet,
                      localitate,
                      cifra_de_afaceri_neta AS cifra_afaceri,
                      profitul_net AS profit,
                      caen AS cod_caen,
                      NULL::text AS caen_descriere,
                      numar_mediu_de_salariati AS angajati,
                      numar_licente AS licente
                    FROM firms
                    WHERE cui = :cui
                    LIMIT 1
                    """
                )
            row = conn.execute(stmt, {"cui": cui}).mappings().first()
            if not row:
                raise HTTPException(status_code=404, detail="Firma nu a fost găsită")
            return dict(row)
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/firme/raw/{cui}")
def firm_raw(cui: str):
    stmt = text("SELECT * FROM firms WHERE cui = :cui LIMIT 1")
    try:
        with engine.connect() as conn:
            row = conn.execute(stmt, {"cui": cui}).mappings().first()
            if not row:
                raise HTTPException(status_code=404, detail="Firma not found")
            return dict(row)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# CAEN endpoints
@app.get("/caen/{grupa}")
def get_caen(grupa: str):
    stmt = text("SELECT grupa, denumire, nace, diviziune FROM public.caen_codes WHERE grupa = :grupa LIMIT 1")
    with engine.connect() as conn:
        row = conn.execute(stmt, {"grupa": grupa}).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="CAEN not found")
        return dict(row)


@app.get("/caen")
def search_caen(q: str = Query("", description="Search CAEN description"), limit: int = 50):
    if not q:
        stmt = text("SELECT grupa, denumire FROM public.caen_codes ORDER BY grupa LIMIT :limit")
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"limit": limit}).mappings().all()
            return [dict(r) for r in rows]
    pattern = f"%{q}%"
    stmt = text(
        """
      SELECT grupa, denumire
      FROM public.caen_codes
      WHERE lower(denumire) ILIKE lower(:pattern)
      ORDER BY lower(denumire) = lower(:q) DESC
      LIMIT :limit
    """
    )
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"pattern": pattern, "q": q, "limit": limit}).mappings().all()
        return [dict(r) for r in rows]


@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="unhealthy")
