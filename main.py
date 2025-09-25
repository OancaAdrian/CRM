# main.py
import os
import logging
from time import sleep
from typing import List, Dict, Any
from datetime import date, timedelta, datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, IntegrityError
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlparse, urlunparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crm-main")

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

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args={"sslmode": "require"},
    future=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

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
                # ensure contacts table exists
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.contacts (
                  id serial PRIMARY KEY,
                  firm_cui varchar NOT NULL,
                  name text NOT NULL,
                  phone text,
                  email text,
                  role text,
                  created_at timestamp default now()
                );
                """))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_contacts_firm_cui ON public.contacts (firm_cui);"))
                # ensure activity_types table and seed expected rows
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.activity_types (
                  id integer PRIMARY KEY,
                  name text
                );
                """))
                conn.execute(text("""
                INSERT INTO public.activity_types (id, name)
                VALUES (7, 'vizita'), (8, 'intalnire')
                ON CONFLICT (id) DO NOTHING;
                """))
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
        logger.exception("search_compat failed")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/firms/{firm_id}")
def get_firm(firm_id: str):
    try:
        with engine.connect() as conn:
            # Use LEFT JOIN to bring caen description directly, trimming firms.caen on join
            stmt = text(
                """
                SELECT
                  f.denumire AS name,
                  f.cui,
                  f.cod_inmatriculare,
                  f.data_inmatriculare,
                  f.euid,
                  f.forma_juridica,
                  f.tara,
                  f.judet,
                  f.localitate,
                  f.adr_den_strada,
                  f.adr_nr_strada,
                  f.adr_bloc,
                  f.adr_scara,
                  f.adr_etaj,
                  f.adr_apartament,
                  f.adr_cod_postal,
                  f.caen,
                  f.numar_licente,
                  f.telefon,
                  f.manager_de_transport,
                  f.cifra_de_afaceri_neta,
                  f.profitul_brut,
                  f.numar_mediu_de_salariati,
                  f.an,
                  f.actualizat_la,
                  c.descriere AS caen_description
                FROM public.firms f
                LEFT JOIN public.caen_codes c ON c.clasa = trim(f.caen)
                WHERE f.cui = :cui
                LIMIT 1
                """
            )
            firm = conn.execute(stmt, {"cui": firm_id}).mappings().first()

            # fallback: if not found via cui, try id
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
                              f.denumire AS name,
                              f.cui,
                              f.cod_inmatriculare,
                              f.data_inmatriculare,
                              f.euid,
                              f.forma_juridica,
                              f.tara,
                              f.judet,
                              f.localitate,
                              f.adr_den_strada,
                              f.adr_nr_strada,
                              f.adr_bloc,
                              f.adr_scara,
                              f.adr_etaj,
                              f.adr_apartament,
                              f.adr_cod_postal,
                              f.caen,
                              f.numar_licente,
                              f.telefon,
                              f.manager_de_transport,
                              f.cifra_de_afaceri_neta,
                              f.profitul_brut,
                              f.numar_mediu_de_salariati,
                              f.an,
                              f.actualizat_la,
                              c.descriere AS caen_description
                            FROM public.firms f
                            LEFT JOIN public.caen_codes c ON c.clasa = trim(f.caen)
                            WHERE f.id = :id
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

            # load contacts for this firm
            contacts = []
            try:
                crows = conn.execute(
                    text("SELECT id, name, phone, email, role, created_at FROM public.contacts WHERE firm_cui = :cui ORDER BY created_at DESC"),
                    {"cui": firm["cui"]}
                ).mappings().all()
                for c in crows:
                    contacts.append({
                        "id": c.get("id"),
                        "name": c.get("name"),
                        "phone": c.get("phone"),
                        "email": c.get("email"),
                        "role": c.get("role"),
                        "created_at": safe_iso(c.get("created_at")),
                    })
            except Exception:
                contacts = []

            # ensure caen_description is a plain string (normalize)
            caen_desc = firm.get("caen_description")
            if isinstance(caen_desc, str):
                caen_desc = caen_desc.strip()
            else:
                caen_desc = caen_desc if caen_desc is not None else None

            resp = {
                "id": firm.get("cui"),
                "cui": firm.get("cui"),
                "name": firm.get("name"),
                "judet": firm.get("judet"),
                "localitate": firm.get("localitate"),
                "caen": firm.get("caen"),
                "caen_description": caen_desc,
                "cifra_afaceri": norm_number(firm.get("cifra_de_afaceri_neta")),
                "profit_net": norm_number(firm.get("profitul_brut") or firm.get("profitul_net")),
                "angajati": norm_number(firm.get("numar_mediu_de_salariati")),
                "licente": norm_number(firm.get("numar_licente")),
                "an": firm.get("an"),
                "actualizat_la": firm.get("actualizat_la"),
                "raw": dict(firm),
                "activities": acts,
                "contacts": contacts,
            }
            return JSONResponse(content=resp)
    except Exception as e:
        logger.exception("get_firm failed for %s: %s", firm_id, e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/firma/{firm_id}/detalii")
def firma_detalii_compat(firm_id: str):
    return get_firm(firm_id)


class ActivityIn(BaseModel):
    firm_id: str
    activity_type_id: int | None = None
    comment: str
    score: int | None = None
    scheduled_date: str | None = None


def score_to_offset_days(score: int | None) -> int | None:
    if score is None:
        return None
    try:
        s = int(score)
    except Exception:
        return None
    mapping = {
        1: 1, 2: 3, 3: 5, 4: 10, 5: 30,
        6: 90, 7: 150, 8: 270, 9: 365,
        10: int(1.5 * 365), 11: 2 * 365, 12: int(2.5 * 365),
        13: 3 * 365, 14: int(3.5 * 365), 15: 4 * 365,
        16: 5 * 365, 17: 6 * 365, 18: 7 * 365,
        19: 8 * 365, 20: None
    }
    return mapping.get(s)


@app.post("/api/activities")
def create_or_update_activity(payload: ActivityIn):
    cui = (payload.firm_id or "").strip()
    comment = (payload.comment or "").strip()
    if not cui or not comment:
        raise HTTPException(status_code=400, detail="firm_id and comment required")

    target_day_date = None
    if payload.scheduled_date and payload.scheduled_date.strip():
        try:
            target_day_date = datetime.strptime(payload.scheduled_date.strip(), "%Y-%m-%d").date()
        except Exception:
            raise HTTPException(status_code=400, detail="scheduled_date must be YYYY-MM-DD")
    else:
        days = score_to_offset_days(payload.score)
        if days is not None:
            target_day_date = (date.today() + timedelta(days=days))
        else:
            target_day_date = None

    try:
        with engine.begin() as conn:
            if payload.activity_type_id is not None:
                at_exists = conn.execute(
                    text("SELECT 1 FROM public.activity_types WHERE id = :id LIMIT 1"),
                    {"id": payload.activity_type_id}
                ).scalar_one_or_none()
                if not at_exists:
                    try:
                        conn.execute(text("INSERT INTO public.activity_types (id, name) VALUES (7, 'vizita') ON CONFLICT (id) DO NOTHING"))
                        conn.execute(text("INSERT INTO public.activity_types (id, name) VALUES (8, 'intalnire') ON CONFLICT (id) DO NOTHING"))
                    except Exception:
                        pass
                    at_exists = conn.execute(
                        text("SELECT 1 FROM public.activity_types WHERE id = :id LIMIT 1"),
                        {"id": payload.activity_type_id}
                    ).scalar_one_or_none()
                    if not at_exists:
                        payload.activity_type_id = None

            if target_day_date is not None:
                existing = conn.execute(
                    text(
                        """
                        SELECT id
                        FROM public.activities
                        WHERE cui = :cui
                          AND comment = :comment
                          AND scheduled_date::date = :target_day
                        LIMIT 1
                        """
                    ),
                    {"cui": cui, "comment": comment, "target_day": target_day_date},
                ).mappings().first()
            else:
                existing = conn.execute(
                    text(
                        """
                        SELECT id
                        FROM public.activities
                        WHERE cui = :cui
                          AND comment = :comment
                          AND created_at::date = CURRENT_DATE
                        LIMIT 1
                        """
                    ),
                    {"cui": cui, "comment": comment},
                ).mappings().first()

            if existing:
                updated = conn.execute(
                    text(
                        """
                        UPDATE public.activities
                        SET activity_type_id = :type_id,
                            score = :score,
                            scheduled_date = :scheduled_date,
                            comment = :comment,
                            created_at = now()
                        WHERE id = :id
                        RETURNING id, created_at, scheduled_date
                        """
                    ),
                    {
                        "id": existing["id"],
                        "type_id": payload.activity_type_id,
                        "score": payload.score,
                        "scheduled_date": target_day_date,
                        "comment": comment,
                    },
                ).mappings().first()

                return JSONResponse(
                    status_code=200,
                    content={
                        "id": updated.get("id"),
                        "cui": cui,
                        "activity_type_id": payload.activity_type_id,
                        "comment": comment,
                        "score": payload.score,
                        "scheduled_date": safe_iso(updated.get("scheduled_date")) if updated.get("scheduled_date") else (target_day_date.isoformat() if target_day_date else None),
                        "created_at": safe_iso(updated.get("created_at")),
                        "updated": True,
                    },
                )
            else:
                res = conn.execute(
                    text(
                        """
                        INSERT INTO public.activities (cui, activity_type_id, comment, score, scheduled_date, created_at)
                        VALUES (:cui, :type_id, :comment, :score, :scheduled_date, now())
                        RETURNING id, created_at, scheduled_date
                        """
                    ),
                    {
                        "cui": cui,
                        "type_id": payload.activity_type_id,
                        "comment": comment,
                        "score": payload.score,
                        "scheduled_date": target_day_date,
                    },
                ).mappings().first()

                return JSONResponse(
                    status_code=201,
                    content={
                        "id": res.get("id"),
                        "cui": cui,
                        "activity_type_id": payload.activity_type_id,
                        "comment": comment,
                        "score": payload.score,
                        "scheduled_date": safe_iso(res.get("scheduled_date")) if res.get("scheduled_date") else (target_day_date.isoformat() if target_day_date else None),
                        "created_at": safe_iso(res.get("created_at")),
                        "updated": False,
                    },
                )
    except IntegrityError as e:
        logger.exception("create_or_update_activity integrity error: %s", e)
        raise HTTPException(status_code=400, detail="Database integrity error")
    except Exception as e:
        logger.exception("create_or_update_activity failed: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


# contacts endpoints
class ContactIn(BaseModel):
    firm_cui: str
    name: str
    phone: str | None = None
    email: str | None = None
    role: str | None = None


@app.post("/api/contacts", status_code=201)
def create_contact(payload: ContactIn):
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.contacts (
                  id serial PRIMARY KEY,
                  firm_cui varchar NOT NULL,
                  name text NOT NULL,
                  phone text,
                  email text,
                  role text,
                  created_at timestamp default now()
                );
            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_contacts_firm_cui ON public.contacts (firm_cui);"))

            res = conn.execute(
                text(
                    "INSERT INTO public.contacts (firm_cui, name, phone, email, role) "
                    "VALUES (:cui, :name, :phone, :email, :role) RETURNING id, created_at"
                ),
                {
                    "cui": payload.firm_cui,
                    "name": payload.name,
                    "phone": payload.phone,
                    "email": payload.email,
                    "role": payload.role,
                },
            ).mappings().first()
            return {
                "id": res["id"],
                "firm_cui": payload.firm_cui,
                "name": payload.name,
                "phone": payload.phone,
                "email": payload.email,
                "role": payload.role,
                "created_at": safe_iso(res["created_at"]),
            }
    except Exception as e:
        logger.exception("create_contact failed: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/firms/{firm_id}/contacts")
def list_contacts(firm_id: str):
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name, phone, email, role, created_at FROM public.contacts WHERE firm_cui = :cui ORDER BY created_at DESC"),
                {"cui": firm_id}
            ).mappings().all()
            return [dict(r) for r in rows]
    except Exception as e:
        logger.exception("list_contacts failed: %s", e)
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
