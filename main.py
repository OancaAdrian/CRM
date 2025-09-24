# main.py
import os
import logging
from time import sleep
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError, IntegrityError
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlparse, urlunparse

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crm-main")

# ---------- Configuration ----------
DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_LOCAL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
parsed = urlparse(DATABASE_URL)
query = parsed.query or ""
if "sslmode=" not in query:
    query = (query + "&" if query else "") + "sslmode=require"
parsed = parsed._replace(query=query)
DATABASE_URL = urlunparse(parsed)

# ---------- Engine / Session ----------
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args={"sslmode": "require"},
    future=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# ---------- App and static ----------
app = FastAPI(title="CRM API")
STATIC_DIR = "static" if os.path.isdir("static") else ("Static" if os.path.isdir("Static") else None)
if STATIC_DIR:
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def root_index():
    if STATIC_DIR:
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))
    return {"message": "No static site found. Visit /docs for API docs."}


# ---------- Startup DB check ----------
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


# ---------- Helpers ----------
def safe_iso(dt):
    if dt is None:
        return None
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)


def norm_number(s):
    if s is None:
        return None
    try:
        s_str = str(s).strip()
        cleaned = s_str.replace(".", "").replace(",", "").replace(" ", "")
        if cleaned == "":
            return None
        return int(cleaned)
    except Exception:
        try:
            return float(str(s).replace(",", "."))
        except Exception:
            return None


# ---------- Health / test ----------
@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="unhealthy")


# ---------- Search endpoints ----------
@app.get("/firme")
def list_firme(q: str = Query(..., description="CUI to look up"), limit: int = Query(50, ge=1, le=500)) -> List[Dict[str, Any]]:
    if not q:
        raise HTTPException(status_code=400, detail="Missing query parameter `q`")
    stmt = text(
        """
        SELECT denumire AS name, cui, judet, cifra_de_afaceri_neta
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
                if "id" not in rec:
                    rec["id"] = rec.get("cui")
                rec["cifra_afaceri"] = norm_number(rec.get("cifra_de_afaceri_neta"))
                rec["profit_net"] = None
                rec["angajati"] = None
                rec["licente"] = None
                results.append(rec)
            return results
    except OperationalError as e:
        logger.exception("OperationalError in /firme")
        raise HTTPException(status_code=503, detail=str(e))
    except SQLAlchemyError as e:
        logger.exception("SQLAlchemyError in /firme")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search")
def search_compat(q: str = Query(..., description="CUI or name to search"), limit: int = Query(20, ge=1, le=500)):
    stmt = text(
        """
        SELECT denumire AS name, cui, judet, cifra_de_afaceri_neta
        FROM firms
        WHERE cui = :cui OR denumire ILIKE :like
        LIMIT :limit
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"cui": q, "like": f"%{q}%", "limit": limit}).mappings().all()
            results = []
            for r in rows:
                rec = dict(r)
                if "id" not in rec:
                    rec["id"] = rec.get("cui")
                rec["cifra_afaceri"] = norm_number(rec.get("cifra_de_afaceri_neta"))
                rec["profit_net"] = None
                rec["angajati"] = None
                rec["licente"] = None
                results.append(rec)
            return JSONResponse(content=results)
    except OperationalError as e:
        logger.exception("OperationalError in /search")
        raise HTTPException(status_code=503, detail="Database unavailable")
    except Exception as e:
        logger.exception("Unexpected error in /search: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------- Firm details ----------
@app.get("/api/firms/{firm_id}")
def get_firm(firm_id: str):
    try:
        with engine.connect() as conn:
            firm = conn.execute(
                text(
                    """
                    SELECT
                      denumire AS name,
                      cui,
                      cod_inmatriculare,
                      data_inmatriculare,
                      euid,
                      forma_juridica,
                      tara,
                      judet,
                      localitate,
                      adr_den_strada,
                      adr_nr_strada,
                      adr_bloc,
                      adr_scara,
                      adr_etaj,
                      adr_apartament,
                      adr_cod_postal,
                      caen,
                      numar_licente,
                      telefon,
                      manager_de_transport,
                      cifra_de_afaceri_neta,
                      profitul_net,
                      numar_mediu_de_salariati,
                      an,
                      actualizat_la
                    FROM firms
                    WHERE cui = :cui
                    LIMIT 1
                    """
                ),
                {"cui": firm_id},
            ).mappings().first()

            if not firm:
                col_exists = conn.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='firms' AND column_name='id' LIMIT 1"
                    )
                ).first()
                if col_exists and firm_id.isdigit():
                    firm = conn.execute(
                        text(
                            """
                            SELECT
                              denumire AS name,
                              cui,
                              cod_inmatriculare,
                              data_inmatriculare,
                              euid,
                              forma_juridica,
                              tara,
                              judet,
                              localitate,
                              adr_den_strada,
                              adr_nr_strada,
                              adr_bloc,
                              adr_scara,
                              adr_etaj,
                              adr_apartament,
                              adr_cod_postal,
                              caen,
                              numar_licente,
                              telefon,
                              manager_de_transport,
                              cifra_de_afaceri_neta,
                              profitul_net,
                              numar_mediu_de_salariati,
                              an,
                              actualizat_la
                            FROM firms
                            WHERE id = :id
                            LIMIT 1
                            """
                        ),
                        {"id": int(firm_id)},
                    ).mappings().first()

            if not firm:
                raise HTTPException(status_code=404, detail="Firm not found")

            activities = conn.execute(
                text(
                    "SELECT id, activity_type_id, comment, score, scheduled_date, created_at "
                    "FROM public.activities WHERE cui = :cui ORDER BY created_at DESC LIMIT 200"
                ),
                {"cui": firm["cui"]},
            ).mappings().all()

            acts = []
            for a in activities:
                acts.append({
                    "id": a.get("id"),
                    "type_id": a.get("activity_type_id"),
                    "comment": a.get("comment"),
                    "score": a.get("score"),
                    "scheduled_date": safe_iso(a.get("scheduled_date")),
                    "created_at": safe_iso(a.get("created_at")),
                })

            # optional CAEN description
            caen_desc = None
            try:
                if firm.get("caen"):
                    cd = conn.execute(text("SELECT denumire FROM public.caen_codes WHERE clasa = :cl ORDER BY id LIMIT 1"), {"cl": firm["caen"]}).scalar_one_or_none()
                    if cd:
                        caen_desc = cd
            except Exception:
                caen_desc = None

            resp = {
                "id": firm.get("cui"),
                "cui": firm.get("cui"),
                "name": firm.get("name"),
                "judet": firm.get("judet"),
                "localitate": firm.get("localitate"),
                "caen": firm.get("caen"),
                "caen_description": caen_desc,
                "cifra_afaceri": norm_number(firm.get("cifra_de_afaceri_neta")),
                "profit_net": norm_number(firm.get("profitul_net")),
                "angajati": norm_number(firm.get("numar_mediu_de_salariati")),
                "licente": norm_number(firm.get("numar_licente")),
                "an": firm.get("an"),
                "actualizat_la": firm.get("actualizat_la"),
                "raw": dict(firm),
                "activities": acts,
            }
            return JSONResponse(content=resp)
    except Exception as e:
        logger.exception("get_firm failed for %s: %s", firm_id, e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/firma/{firm_id}/detalii")
def firma_detalii_compat(firm_id: str):
    return get_firm(firm_id)


# ---------- Create activity ----------
class ActivityIn(BaseModel):
    firm_id: str
    activity_type_id: int | None = None
    comment: str
    score: int | None = None
    scheduled_date: str | None = None


@app.post("/api/activities", status_code=201)
def create_activity(payload: ActivityIn):
    cui = (payload.firm_id or "").strip()
    comment = (payload.comment or "").strip()
    if not cui or not comment:
        raise HTTPException(status_code=400, detail="firm_id and comment required")
    try:
        with engine.begin() as conn:
            existing = conn.execute(
                text(
                    "SELECT id, cui, activity_type_id, comment, score, scheduled_date, created_at "
                    "FROM public.activities WHERE cui = :cui AND comment = :comment LIMIT 1"
                ),
                {"cui": cui, "comment": comment},
            ).mappings().first()
            if existing:
                return JSONResponse(
                    status_code=409,
                    content={
                        "id": existing.get("id"),
                        "cui": existing.get("cui"),
                        "activity_type_id": existing.get("activity_type_id"),
                        "comment": existing.get("comment"),
                        "score": existing.get("score"),
                        "scheduled_date": safe_iso(existing.get("scheduled_date")),
                        "created_at": safe_iso(existing.get("created_at")),
                    },
                )
            res = conn.execute(
                text(
                    "INSERT INTO public.activities (cui, activity_type_id, comment, score, scheduled_date, created_at) "
                    "VALUES (:cui, :type_id, :comment, :score, :scheduled_date, now()) RETURNING id, created_at"
                ),
                {
                    "cui": cui,
                    "type_id": payload.activity_type_id,
                    "comment": comment,
                    "score": payload.score,
                    "scheduled_date": payload.scheduled_date,
                },
            ).mappings().first()
            return {
                "id": res.get("id"),
                "cui": cui,
                "activity_type_id": payload.activity_type_id,
                "comment": comment,
                "score": payload.score,
                "scheduled_date": payload.scheduled_date,
                "created_at": safe_iso(res.get("created_at")),
            }
    except IntegrityError:
        logger.exception("IntegrityError creating activity")
        raise HTTPException(status_code=409, detail="Activity already exists")
    except Exception:
        logger.exception("Unexpected error creating activity")
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------- CAEN import endpoint ----------
@app.post("/api/caen/import")
async def import_caen(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Expected CSV")
    content = await file.read()
    text_csv = content.decode("utf-8-sig", errors="replace")
    import io, csv
    reader = csv.DictReader(io.StringIO(text_csv), delimiter=",")
    inserted = 0
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS public.caen_codes (
                  id serial PRIMARY KEY,
                  diviziune varchar(20),
                  grupa varchar(20),
                  clasa varchar(40),
                  denumire text,
                  nivel smallint
                )
                """
                )
            )
            for row in reader:
                div = row.get("diviziune") or row.get("div") or None
                grupa = row.get("grupa") or row.get("grup") or None
                clasa = row.get("clasa") or row.get("cod") or None
                den = row.get("denumire") or row.get("descriere") or None
                nivel = row.get("nivel") or None
                conn.execute(
                    text(
                        """
                    INSERT INTO public.caen_codes (diviziune, grupa, clasa, denumire, nivel)
                    VALUES (:div, :grupa, :clasa, :den, :nivel)
                    ON CONFLICT DO NOTHING
                    """
                    ),
                    {"div": div, "grupa": grupa, "clasa": clasa, "den": den, "nivel": nivel},
                )
                inserted += 1
        return {"imported_count": inserted}
    except Exception:
        logger.exception("CAEN import failed")
        raise HTTPException(status_code=500, detail="Import failed")


# ---------- CAEN desc endpoint (simple lookup) ----------
@app.get("/api/caen/desc")
def caen_desc(code: str = Query(...)):
    try:
        with engine.connect() as conn:
            desc = conn.execute(text("SELECT denumire FROM public.caen_codes WHERE clasa = :cl ORDER BY id LIMIT 1"), {"cl": code}).scalar_one_or_none()
            return {"clasa": code, "denumire": desc}
    except Exception:
        logger.exception("CAEN desc lookup failed for %s", code)
        raise HTTPException(status_code=500, detail="CAEN lookup failed")


# ---------- SPA catch-all ----------
if STATIC_DIR:
    @app.get("/{path:path}", include_in_schema=False)
    def spa_catch_all(path: str, request: Request):
        p = request.url.path
        if p.startswith("/api") or p.startswith("/static") or p.startswith("/firme") or p.startswith("/search") or p.startswith("/firma"):
            raise HTTPException(status_code=404, detail="Not Found")
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))
