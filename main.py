# main.py
import os
import logging
from time import sleep
from typing import List, Dict, Any
from datetime import date, timedelta, datetime

from fastapi import FastAPI, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError, IntegrityError
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlparse, urlunparse

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crm-main")

# config
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

# engine/session
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args={"sslmode": "require"},
    future=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# app + static
app = FastAPI(title="CRM API")
STATIC_DIR = "static" if os.path.isdir("static") else ("Static" if os.path.isdir("Static") else None)
if STATIC_DIR:
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def root_index():
    if STATIC_DIR:
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))
    return {"message": "No static site found. Visit /docs for API docs."}


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


@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="unhealthy")


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
    except OperationalError:
        raise HTTPException(status_code=503, detail="Database unavailable")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")


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

    def score_to_offset_days(score: int | None) -> int | None:
        if score is None:
            return None
        try:
            s = int(score)
        except Exception:
            return None
        if s == 1: return 1
        if s == 2: return 3
        if s == 3: return 5
        if s == 4: return 10
        if s == 5: return 30
        if s == 6: return 90
        if s == 7: return 150
        if s == 8: return 270
        if s == 9: return 365
        if s == 10: return int(1.5 * 365)
        if s == 11: return 2 * 365
        if s == 12: return int(2.5 * 365)
        if s == 13: return 3 * 365
        if s == 14: return int(3.5 * 365)
        if s == 15: return 4 * 365
        if s == 16: return 5 * 365
        if s == 17: return 6 * 365
        if s == 18: return 7 * 365
        if s == 19: return 8 * 365
        if s == 20: return None
        return None

    try:
        with engine.begin() as conn:
            existing = conn.execute(
                text(
                    "SELECT id FROM public.activities WHERE cui = :cui AND comment = :comment LIMIT 1"
                ),
                {"cui": cui, "comment": comment},
            ).mappings().first()
            if existing:
                raise HTTPException(status_code=409, detail="Activity already exists")

            if payload.scheduled_date:
                sched_dt = payload.scheduled_date
            else:
                days = score_to_offset_days(payload.score)
                if days is None:
                    sched_dt = None
                else:
                    sched_date = (date.today() + timedelta(days=days))
                    sched_dt = sched_date.isoformat()

            res = conn.execute(
                text(
                    "INSERT INTO public.activities (cui, activity_type_id, comment, score, scheduled_date, created_at) "
                    "VALUES (:cui, :type_id, :comment, :score, :scheduled_date, now()) RETURNING id, created_at, scheduled_date"
                ),
                {
                    "cui": cui,
                    "type_id": payload.activity_type_id,
                    "comment": comment,
                    "score": payload.score,
                    "scheduled_date": sched_dt,
                },
            ).mappings().first()

            return {
                "id": res.get("id"),
                "cui": cui,
                "activity_type_id": payload.activity_type_id,
                "comment": comment,
                "score": payload.score,
                "scheduled_date": safe_iso(res.get("scheduled_date")) if res.get("scheduled_date") else sched_dt,
                "created_at": safe_iso(res.get("created_at")),
            }
    except IntegrityError:
        raise HTTPException(status_code=409, detail="Activity already exists")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error creating activity")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/agenda")
def get_agenda(day: str | None = Query(None, description="ISO date YYYY-MM-DD. Defaults to today")):
    try:
        if day:
            try:
                target = datetime.strptime(day, "%Y-%m-%d").date()
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid date format, expected YYYY-MM-DD")
        else:
            target = date.today()

        with engine.connect() as conn:
            today_rows = conn.execute(
                text(
                    """
                    SELECT a.id, a.cui, a.activity_type_id, a.comment, a.score, a.scheduled_date, a.created_at,
                           f.denumire AS firm_name
                    FROM public.activities a
                    LEFT JOIN public.firms f ON f.cui = a.cui
                    WHERE a.scheduled_date = :target
                    ORDER BY a.scheduled_date, a.created_at DESC
                    """
                ),
                {"target": target.isoformat()},
            ).mappings().all()

            overdue_rows = conn.execute(
                text(
                    """
                    SELECT a.id, a.cui, a.activity_type_id, a.comment, a.score, a.scheduled_date, a.created_at,
                           f.denumire AS firm_name
                    FROM public.activities a
                    LEFT JOIN public.firms f ON f.cui = a.cui
                    WHERE a.scheduled_date < :target
                    ORDER BY a.scheduled_date ASC, a.created_at DESC
                    """
                ),
                {"target": target.isoformat()},
            ).mappings().all()

            def normalize(rows):
                out = []
                for r in rows:
                    out.append({
                        "id": r.get("id"),
                        "cui": r.get("cui"),
                        "firm_name": r.get("firm_name") or None,
                        "type_id": r.get("activity_type_id"),
                        "comment": r.get("comment"),
                        "score": r.get("score"),
                        "scheduled_date": safe_iso(r.get("scheduled_date")),
                        "created_at": safe_iso(r.get("created_at")),
                    })
                return out

            return JSONResponse(content={
                "date": target.isoformat(),
                "today": normalize(today_rows),
                "overdue": normalize(overdue_rows),
            })
    except Exception as e:
        logger.exception("get_agenda failed: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")
