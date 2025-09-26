# main.py
import os
import logging
from time import sleep
from datetime import date, timedelta, datetime

from datetime import date, timedelta, datetime
from urllib.parse import urlparse, urlunparse

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from pydantic import BaseModel

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, IntegrityError
from sqlalchemy.orm import sessionmaker


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
                # helper tables (non-destructive)
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
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.activity_types (
                  id integer PRIMARY KEY,
                  name text
                );
                """))
                conn.execute(text("""
                INSERT INTO public.activity_types (id, name)
                VALUES (1,'contact'),(2,'oferta'),(3,'contract'),(4,'contact in vederea livrarii'),
                       (5,'livrare'),(6,'feedback livrare'),(7,'vizita'),(8,'intalnire')
                ON CONFLICT (id) DO NOTHING;
                """))
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.activities (
                  id serial PRIMARY KEY,
                  cui varchar NOT NULL,
                  activity_type_id integer,
                  comment text,
                  score integer,
                  scheduled_date date,
                  created_at timestamp default now()
                );
                """))
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.caen_codes (
                  id serial PRIMARY KEY,
                  sectiune text,
                  diviziune text,
                  grupa text,
                  clasa text,
                  descriere text,
                  created_at timestamp
                );
                """))
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.licente (
                  cui text PRIMARY KEY,
                  licente integer
                );
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


# tolerant helpers for licente table column names (each uses its own connection)
def detect_licente_columns():
    try:
        with engine.connect() as conn:
            cols = conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='licente'"
            )).scalars().all()
    except Exception:
        return {"cui": None, "licente": None}
    colmap = {"cui": None, "licente": None}
    for c in cols:
        low = c.lower()
        if any(x in low for x in ("cui", "codcui", "cod_cui", "cod fiscal", "codfiscal")) and colmap["cui"] is None:
            colmap["cui"] = c
        if any(x in low for x in ("licen", "license", "licente", "nr_licente", "numar_licente")) and colmap["licente"] is None:
            colmap["licente"] = c
    return colmap



def get_licente_for_cui(cui, colmap):
    if not colmap:
        return None
    cui_col = colmap.get("cui")
    lic_col = colmap.get("licente")
    if not lic_col:
        return None
    try:
        with engine.connect() as conn:
            if cui_col:
                # exact match, case/space tolerant
                stmt = text(f'SELECT "{lic_col}" FROM public.licente WHERE trim(lower("{cui_col}"::text)) = trim(lower(:cui)) LIMIT 1')
                row = conn.execute(stmt, {"cui": cui}).first()
                if row and row[0] is not None:
                    return row[0]
                # fallback: partial match
                stmt2 = text(f'SELECT "{lic_col}" FROM public.licente WHERE "{cui_col}"::text ILIKE :like LIMIT 1')
                row2 = conn.execute(stmt2, {"like": f"%{cui}%"}).first()
                return row2[0] if row2 else None
            else:
                # no cui column, return first lic value if any
                stmt3 = text(f'SELECT "{lic_col}" FROM public.licente LIMIT 1')
                r3 = conn.execute(stmt3).first()
                return r3[0] if r3 else None
    except Exception:
        return None

@app.get("/api/_debug_detect_cols")
def _debug_detect_cols():
    try:
        with engine.connect() as conn:
            firms = conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='firms' ORDER BY ordinal_position"
            )).scalars().all()
            lic = conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='licente' ORDER BY ordinal_position"
            )).scalars().all()
            # quick counts to see if tables are populated
            firms_count = conn.execute(text("SELECT count(*) FROM public.firms")).scalar()
            lic_count = conn.execute(text("SELECT count(*) FROM public.licente")).scalar()
        return JSONResponse(content={
            "firms_columns": firms,
            "licente_columns": lic,
            "firms_count": firms_count,
            "licente_count": lic_count
        })
    except Exception as e:
        logger.exception("debug_detect_cols failed: %s", e)
        raise HTTPException(status_code=500, detail=f"debug_detect_cols failure: {e}")

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
    q_like = f"%{q}%"
    stmt = text("SELECT f.* FROM public.firms f WHERE f.cui = :cui OR f.denumire ILIKE :like LIMIT :limit")
    try:
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"cui": q, "like": q_like, "limit": limit}).mappings().all()
    except OperationalError:
        raise HTTPException(status_code=503, detail="Database unavailable")
    except Exception:
        logger.exception("search_compat query failed")
        raise HTTPException(status_code=500, detail="Internal server error")

    colmap = detect_licente_columns()
    results = []
    for r in rows:
        rec = dict(r)
        result = {}
        result["id"] = rec.get("cui") or rec.get("id")
        result["cui"] = rec.get("cui") or rec.get("id")
        result["name"] = rec.get("denumire") or rec.get("name") or None
        result["judet"] = rec.get("judet") or None
        result["localitate"] = rec.get("localitate") or None
        result["caen"] = rec.get("caen") or rec.get("cod_caen") or None
        cifra = rec.get("cifra_de_afaceri_neta") or rec.get("cifra_de_afaceri") or rec.get("cifra_afaceri")
        result["cifra_afaceri"] = norm_number(cifra)
        lic_val = rec.get("numar_licente") or rec.get("licente")
        if (lic_val is None or lic_val == "") and colmap.get("cui") and colmap.get("licente"):
            lic_val = get_licente_for_cui(result["cui"], colmap)
        result["licente"] = norm_number(lic_val)
        result["profit"] = norm_number(rec.get("profitul_brut") or rec.get("profit_net") or rec.get("profit"))
        result["angajati"] = norm_number(rec.get("numar_mediu_de_salariati") or rec.get("angajati"))
        result["raw"] = rec
        results.append(result)
    return JSONResponse(content=results)


@app.get("/api/firms/{firm_id}")
def get_firm(firm_id: str):
    # 1) fetch firm row (separate connection)
    try:
        with engine.connect() as conn:
            firm_row = conn.execute(text("SELECT f.* FROM public.firms f WHERE f.cui = :cui LIMIT 1"), {"cui": firm_id}).mappings().first()
    except Exception:
        logger.exception("get_firm: error fetching firm row")
        raise HTTPException(status_code=500, detail="Internal server error")

    # fallback by numeric id if present (separate conn)
    if not firm_row:
        try:
            with engine.connect() as conn:
                col_exists = conn.execute(text(
                    "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='firms' AND column_name='id' LIMIT 1"
                )).first()
            if col_exists and firm_id.isdigit():
                with engine.connect() as conn:
                    firm_row = conn.execute(text("SELECT f.* FROM public.firms f WHERE f.id = :id LIMIT 1"), {"id": int(firm_id)}).mappings().first()
        except Exception:
            logger.exception("get_firm: fallback id lookup failed")
            firm_row = None

    if not firm_row:
        raise HTTPException(status_code=404, detail="Firm not found")

    firm = dict(firm_row)

    # 2) caen_description lookup (separate connection)
    caen_val = firm.get("caen") or firm.get("cod_caen")
    caen_desc = None
    if caen_val:
        try:
            with engine.connect() as conn:
                cd = conn.execute(text("SELECT descriere FROM public.caen_codes WHERE clasa = trim(:caen) LIMIT 1"), {"caen": str(caen_val)}).scalar_one_or_none()
                if cd:
                    caen_desc = cd.strip() if isinstance(cd, str) else cd
        except Exception:
            caen_desc = None

    # 3) licente (detect + lookup, separate connection inside helper)
    colmap = detect_licente_columns()
    lic_val = firm.get("numar_licente") or firm.get("licente")
    if (lic_val is None or lic_val == "") and colmap.get("cui") and colmap.get("licente"):
        lic_val = get_licente_for_cui(firm.get("cui") or firm.get("id"), colmap)

    # 4) activities (separate connection)
    acts = []
    try:
        with engine.connect() as conn:
            activities = conn.execute(
                text("SELECT id, activity_type_id, comment, score, scheduled_date, created_at FROM public.activities WHERE cui = :cui ORDER BY created_at DESC LIMIT 200"),
                {"cui": firm.get("cui") or firm.get("id")},
            ).mappings().all()
        for a in activities:
            acts.append({
                "id": a.get("id"),
                "type_id": a.get("activity_type_id"),
                "comment": a.get("comment"),
                "score": a.get("score"),
                "scheduled_date": safe_iso(a.get("scheduled_date")),
                "created_at": safe_iso(a.get("created_at")),
            })
    except Exception:
        logger.exception("get_firm: activities lookup failed")
        acts = []

    # 5) contacts (separate connection)
    contacts = []
    try:
        with engine.connect() as conn:
            crows = conn.execute(
                text("SELECT id, name, phone, email, role, created_at FROM public.contacts WHERE firm_cui = :cui ORDER BY created_at DESC"),
                {"cui": firm.get("cui") or firm.get("id")}
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
        logger.exception("get_firm: contacts lookup failed")
        contacts = []

    resp = {
        "id": firm.get("cui") or firm.get("id"),
        "cui": firm.get("cui") or firm.get("id"),
        "name": firm.get("denumire") or firm.get("name"),
        "judet": firm.get("judet"),
        "localitate": firm.get("localitate"),
        "caen": caen_val,
        "caen_description": caen_desc,
        "cifra_afaceri": norm_number(firm.get("cifra_de_afaceri_neta") or firm.get("cifra_de_afaceri") or firm.get("cifra_afaceri")),
        "profit": norm_number(firm.get("profitul_brut") or firm.get("profit_net") or firm.get("profit")),
        "angajati": norm_number(firm.get("numar_mediu_de_salariati") or firm.get("angajati")),
        "licente": norm_number(lic_val),
        "an": firm.get("an"),
        "actualizat_la": firm.get("actualizat_la"),
        "raw": firm,
        "activities": acts,
        "contacts": contacts,
    }
    resp["profit_net"] = resp.get("profit")

    return JSONResponse(content=resp)


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
            # asigurăm existența tabelei (no-op dacă există)
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



@app.get("/firma/{firm_id}/detalii")
def firma_detalii_compat(firm_id: str):
    return get_firm(firm_id)


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

        # detect available name column on firms table
        firm_name_cols = []
        try:
            with engine.connect() as conn:
                cols = conn.execute(text(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='firms'"
                )).scalars().all()
            for c in cols:
                low = c.lower()
                if low in ("denumire", "name", "denumire_firma", "company", "firm_name"):
                    firm_name_cols.append(c)
            # fallback: if no known column found, try first textual column candidate
            if not firm_name_cols:
                for c in cols:
                    low = c.lower()
                    if "name" in low or "denum" in low or "denumire" in low:
                        firm_name_cols.append(c)
                        break
        except Exception:
            firm_name_cols = []

        # choose the SQL expression for firm name safely
        if firm_name_cols:
            # use first detected column, coalesce just in case
            fn = firm_name_cols[0]
            firm_name_expr = f"COALESCE(f.{fn})"
        else:
            # no firm name column detected — return NULL as firm_name
            firm_name_expr = "NULL AS firm_name"

        # join clause: join on cui only (safe)
        join_clause = "LEFT JOIN public.firms f ON f.cui = a.cui"

        qry_today = f"""
            SELECT a.id, a.cui, a.activity_type_id, a.comment, a.score, a.scheduled_date, a.created_at,
                   {firm_name_expr} AS firm_name
            FROM public.activities a
            {join_clause}
            WHERE a.scheduled_date = :target
            ORDER BY a.scheduled_date, a.created_at DESC
        """

        qry_overdue = f"""
            SELECT a.id, a.cui, a.activity_type_id, a.comment, a.score, a.scheduled_date, a.created_at,
                   {firm_name_expr} AS firm_name
            FROM public.activities a
            {join_clause}
            WHERE a.scheduled_date < :target
            ORDER BY a.scheduled_date ASC, a.created_at DESC
        """

        with engine.connect() as conn:
            today_rows = conn.execute(text(qry_today), {"target": target.isoformat()}).mappings().all()
            overdue_rows = conn.execute(text(qry_overdue), {"target": target.isoformat()}).mappings().all()

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

@app.post("/api/suggest_contacts", status_code=201)
def suggest_contacts_for_tomorrow(limit: int = 5):
    from datetime import date, timedelta

    today = date.today()
    target = (today + timedelta(days=1)).isoformat()

    # detect columns in firms
    with engine.connect() as conn:
        cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='firms' ORDER BY ordinal_position"
        )).scalars().all()

    # detect cifra de afaceri column
    ca_cols = [c for c in cols if any(x in c.lower() for x in (
        "cifra", "cifra_de_afaceri", "cifra_afaceri", "cifra_de_afaceri_neta", "cifra_de_afaceri_net"))]
    ca_col = ca_cols[0] if ca_cols else None

    # detect licente source table/cols
    licemap = detect_licente_columns()  # returns {"cui": colname, "licente": colname} or None
    lic_col = licemap.get("licente") if licemap else None
    lic_cui_col = licemap.get("cui") if licemap else None

    if not lic_col and not ca_col:
        logger.error("suggest_contacts: no ca_col and no lic_col detected; firms columns: %s; licente cols: %s", cols, licemap)
        raise HTTPException(status_code=500, detail="Could not detect cifra_afaceri or licente columns in DB. Check DB schema.")

    # detect firm name column (fallback to denumire or first textual candidate)
    fn = None
    for c in cols:
        low = c.lower()
        if low in ("denumire", "name", "denumire_firma", "company", "firm_name"):
            fn = c
            break
    if not fn:
        for c in cols:
            low = c.lower()
            if "name" in low or "denum" in low or "denumire" in low:
                fn = c
                break

    # safe selectors for numeric casting (or defaults)
    ca_sel = f'COALESCE(NULLIF(trim("{ca_col}"::text), \'\'), \'0\')::numeric' if ca_col else '0'
    # lic_sel will come from aggregated licente table, so we prepare join expression
    name_select = f'COALESCE(filtered."{fn}") AS denumire' if fn else "NULL AS denumire"

    # Build query: left join aggregate of licente (group by cui) to firms
    lic_agg_join = ""
    if lic_col and lic_cui_col:
        lic_agg_join = f"""
        LEFT JOIN (
          SELECT "{lic_cui_col}"::text AS lic_cui, SUM(COALESCE(NULLIF(trim("{lic_col}"::text), ''), '0')::int) AS lic_count
          FROM public.licente
          GROUP BY "{lic_cui_col}"::text
        ) l ON l.lic_cui = f.cui::text
        """
        lic_count_expr = "COALESCE(l.lic_count, 0)"
    else:
        lic_count_expr = "0"

    # Exclude firms from judet Constanta (case-insensitive)
    qry = f"""
    WITH candidate_firms AS (
      SELECT f.*, {lic_count_expr} AS lic_count, {ca_sel} AS cifra_val
      FROM public.firms f
      {lic_agg_join}
    ),
    filtered AS (
      SELECT cf.*
      FROM candidate_firms cf
      WHERE NOT EXISTS (
        SELECT 1 FROM public.activities a
        WHERE a.cui = cf.cui
          AND a.scheduled_date >= :today
      )
      AND NOT EXISTS (
        SELECT 1 FROM public.activities a2
        WHERE a2.cui = cf.cui
          AND a2.score = 20
      )
      AND lower(coalesce(cf.judet::text, '')) != 'constanta'
    )
    SELECT filtered.cui, {name_select}, filtered.lic_count, filtered.cifra_val
    FROM filtered
    ORDER BY filtered.lic_count DESC, filtered.cifra_val DESC
    LIMIT :limit
    """

    with engine.begin() as conn:
        rows = conn.execute(text(qry), {"today": today.isoformat(), "limit": limit}).mappings().all()

        inserted = []
        for r in rows:
            cui = r.get("cui")
            # final defensive check: exclude if any score=20 or scheduled >= today (race-safe) or judet Constanta
            exists_block = conn.execute(text(
                "SELECT 1 FROM public.activities WHERE cui = :cui AND (scheduled_date >= :today OR score = 20) LIMIT 1"
            ), {"cui": cui, "today": today.isoformat()}).first()
            if exists_block:
                continue
            # extra runtime check for judet just in case
            if str(r.get("cifra_val")) is None:
                pass
            judet_val = None
            try:
                # try to fetch judet for the cui to be extra-safe
                j = conn.execute(text("SELECT judet FROM public.firms WHERE cui = :cui LIMIT 1"), {"cui": cui}).scalar()
                judet_val = (j or "").lower()
            except Exception:
                judet_val = None
            if judet_val == "constanta":
                continue

            ins = conn.execute(text(
                "INSERT INTO public.activities (cui, activity_type_id, comment, score, scheduled_date, created_at) "
                "VALUES (:cui, :atype, :comment, :score, :scheduled, now()) RETURNING id, scheduled_date"
            ), {
                "cui": cui,
                "atype": 1,
                "comment": "Auto-suggest",
                "score": 1,
                "scheduled": target,
            }).mappings().first()

            inserted.append({
                "id": ins["id"],
                "cui": cui,
                "scheduled_date": safe_iso(ins["scheduled_date"]),
                "name": r.get("denumire"),
                "licente": int(r.get("lic_count") or 0),
                "cifra_afaceri": int(r.get("cifra_val") or 0),
            })

    return JSONResponse(content={
        "date": target,
        "created": inserted,
        "requested_limit": limit,
        "candidates_checked": len(rows),
    })



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
                existing = conn.execute(text("""
                    SELECT id FROM public.activities
                    WHERE cui = :cui AND comment = :comment AND scheduled_date::date = :target_day LIMIT 1
                """), {"cui": cui, "comment": comment, "target_day": target_day_date}).mappings().first()
            else:
                existing = conn.execute(text("""
                    SELECT id FROM public.activities
                    WHERE cui = :cui AND comment = :comment AND created_at::date = CURRENT_DATE LIMIT 1
                """), {"cui": cui, "comment": comment}).mappings().first()

            if existing:
                updated = conn.execute(text("""
                    UPDATE public.activities
                    SET activity_type_id = :type_id, score = :score, scheduled_date = :scheduled_date, comment = :comment, created_at = now()
                    WHERE id = :id
                    RETURNING id, created_at, scheduled_date
                """), {"id": existing["id"], "type_id": payload.activity_type_id, "score": payload.score, "scheduled_date": target_day_date, "comment": comment}).mappings().first()

                return JSONResponse(status_code=200, content={
                    "id": updated.get("id"),
                    "cui": cui,
                    "activity_type_id": payload.activity_type_id,
                    "comment": comment,
                    "score": payload.score,
                    "scheduled_date": safe_iso(updated.get("scheduled_date")) if updated.get("scheduled_date") else (target_day_date.isoformat() if target_day_date else None),
                    "created_at": safe_iso(updated.get("created_at")),
                    "updated": True,
                })
            else:
                res = conn.execute(text("""
                    INSERT INTO public.activities (cui, activity_type_id, comment, score, scheduled_date, created_at)
                    VALUES (:cui, :type_id, :comment, :score, :scheduled_date, now())
                    RETURNING id, created_at, scheduled_date
                """), {"cui": cui, "type_id": payload.activity_type_id, "comment": comment, "score": payload.score, "scheduled_date": target_day_date}).mappings().first()

                return JSONResponse(status_code=201, content={
                    "id": res.get("id"),
                    "cui": cui,
                    "activity_type_id": payload.activity_type_id,
                    "comment": comment,
                    "score": payload.score,
                    "scheduled_date": safe_iso(res.get("scheduled_date")) if res.get("scheduled_date") else (target_day_date.isoformat() if target_day_date else None),
                    "created_at": safe_iso(res.get("created_at")),
                    "updated": False,
                })
    except IntegrityError as e:
        logger.exception("create_or_update_activity integrity error: %s", e)
        raise HTTPException(status_code=400, detail="Database integrity error")
    except Exception as e:
        logger.exception("create_or_update_activity failed: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")
