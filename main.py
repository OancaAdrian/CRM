# main.py
import os
import re
import logging
from time import sleep
from datetime import date, datetime, timedelta
from urllib.parse import urlparse, urlunparse

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Depends
from auth_middleware import require_app_password

app = FastAPI()


from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, IntegrityError
from sqlalchemy.orm import sessionmaker
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crm-main")

from dotenv import load_dotenv
load_dotenv()


# ---------- DATABASE URL ----------
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
# simple app password middleware (blocks POST/PUT/PATCH/DELETE to /api/* and /admin/* unless x-app-password matches)
from fastapi import Request
from fastapi.responses import JSONResponse

APP_PASSWORD = os.environ.get("APP_PASSWORD", "5864")

@app.middleware("http")
async def require_app_password_middleware(request: Request, call_next):
    try:
        path = request.url.path or ""
        method = request.method or ""
        # apply only to write methods under /api or admin endpoints
        write_methods = ("POST", "PUT", "PATCH", "DELETE")
        protected = (path.startswith("/api/") or path.startswith("/admin/")) and method in write_methods
        if not protected:
            return await call_next(request)
        # check header first
        pw = request.headers.get("x-app-password")
        if not pw:
            # try JSON body for clients that send password in body
            try:
                body = await request.json()
                pw = body.get("password")
            except Exception:
                pw = None
        if pw != APP_PASSWORD:
            return JSONResponse(status_code=401, content={"detail": "Missing or invalid app password"})
        return await call_next(request)
    except Exception:
        return JSONResponse(status_code=500, content={"detail": "internal error"})


# Dev CORS (tighten in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# static serve detection
PROJECT_ROOT = os.path.dirname(__file__)
_candidate = os.path.join(PROJECT_ROOT, "web", "dist")
_static_candidate = os.path.join(PROJECT_ROOT, "static")
if os.path.isdir(_candidate) and os.path.isfile(os.path.join(_candidate, "index.html")):
    STATIC_DIR = _candidate
elif os.path.isdir(_static_candidate) and os.path.isfile(os.path.join(_static_candidate, "index.html")):
    STATIC_DIR = _static_candidate
else:
    STATIC_DIR = None
if STATIC_DIR:
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# helpers
def safe_iso(val):
    if val is None: return None
    try: return val.isoformat()
    except Exception: return str(val)

def norm_number(s):
    if s is None: return None
    try:
        if isinstance(s, Decimal):
            return float(s)
        s_str = str(s).strip()
        cleaned = s_str.replace(".", "").replace(",", "").replace(" ", "")
        if cleaned == "": return None
        return int(cleaned)
    except Exception:
        try: return float(str(s).replace(",", "."))
        except Exception: return None

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
    if not colmap: return None
    cui_col = colmap.get("cui"); lic_col = colmap.get("licente")
    if not lic_col: return None
    try:
        with engine.connect() as conn:
            if cui_col:
                stmt = text(f'SELECT "{lic_col}" FROM public.licente WHERE trim(lower("{cui_col}"::text)) = trim(lower(:cui)) LIMIT 1')
                row = conn.execute(stmt, {"cui": cui}).first()
                if row and row[0] is not None: return row[0]
                stmt2 = text(f'SELECT "{lic_col}" FROM public.licente WHERE "{cui_col}"::text ILIKE :like LIMIT 1')
                row2 = conn.execute(stmt2, {"like": f"%{cui}%"}).first()
                return row2[0] if row2 else None
            else:
                stmt3 = text(f'SELECT "{lic_col}" FROM public.licente LIMIT 1')
                r3 = conn.execute(stmt3).first()
                return r3[0] if r3 else None
    except Exception:
        return None

# suggested_top and reprogram options management (extended to include suggested_by_caen)
def ensure_suggested_and_reprogram_tables():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.suggested_top (
          id serial PRIMARY KEY,
          rank integer NOT NULL,
          cui text NOT NULL,
          denumire text,
          licente integer DEFAULT 0,
          cifra_afaceri numeric DEFAULT 0,
          used boolean DEFAULT false,
          created_at timestamp default now()
        );
        """))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_suggested_top_cui ON public.suggested_top(cui);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_suggested_top_used_rank ON public.suggested_top(used, rank);"))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.reprogram_options (
          id serial PRIMARY KEY,
          label text NOT NULL,
          days integer NULL
        );
        """))
        # populate default options if empty
        existing = conn.execute(text("SELECT 1 FROM public.reprogram_options LIMIT 1")).first()
        if not existing:
            opts = [
                ("1 zi", 1), ("2 zile", 2), ("3 zile", 3), ("4 zile", 4), ("5 zile", 5),
                ("1 saptamana", 7), ("2 saptamani", 14), ("3 saptamani", 21),
                ("1 luna", 30), ("2 luni", 60), ("3 luni", 90), ("6 luni", 180),
                ("9 luni", 270), ("1 an", 365), ("1 an si jumatate", 548),
                ("2 ani", 730), ("3 ani", 1095), ("4 ani", 1460), ("5 ani", 1825),
                ("Nu programa", None)
            ]
            for label, days in opts:
                conn.execute(text("INSERT INTO public.reprogram_options (label, days) VALUES (:label, :days)"), {"label": label, "days": days})

    # ensure suggested_by_caen table exists
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.suggested_by_caen (
          id serial PRIMARY KEY,
          rank integer,
          cui text,
          denumire text,
          caen varchar,
          cifra_de_afaceri numeric DEFAULT 0,
          numar_licente integer DEFAULT 0,
          source text DEFAULT 'caen',
          created_at timestamptz default now()
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_suggested_by_caen_cui ON public.suggested_by_caen(cui);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_suggested_by_caen_caen ON public.suggested_by_caen(caen);"))

def rebuild_top20(limit=20):
    """
    Build top20 of candidate firms into public.suggested_top.
    Criteria: licente DESC, cifra_afaceri DESC.
    Exclude firms that have any activity (ever) and exclude firms from judet Constanta.
    Clean denumire field to remove trailing județ and normalize name.
    """
    ensure_suggested_and_reprogram_tables()
    try:
        with engine.connect() as conn:
            cols = conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='firms' ORDER BY ordinal_position"
            )).scalars().all()
    except Exception:
        cols = []

    # detect possible columns
    ca_col = next((c for c in cols if any(x in c.lower() for x in ("cifra", "cifra_de_afaceri", "cifra_afaceri", "cifra_de_afaceri_neta"))), None)
    fn = next((c for c in cols if c.lower() in ("denumire", "name", "denumire_firma", "company", "firm_name")), None)
    if not fn:
        fn = next((c for c in cols if "name" in c.lower() or "denum" in c.lower()), None)

    try:
        with engine.connect() as conn:
            lic_cols = conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='licente'"
            )).scalars().all()
    except Exception:
        lic_cols = []
    lic_col = next((c for c in lic_cols if any(x in c.lower() for x in ("licen", "license", "licente"))), None)
    lic_cui_col = next((c for c in lic_cols if any(x in c.lower() for x in ("cui", "codcui", "cod_cui"))), None)

    ca_sel = f'COALESCE(NULLIF(trim("{ca_col}"::text), \'\'), \'0\')::numeric' if ca_col else '0'
    # alias the name column to denumire_src to avoid collisions with f.*
    name_select = f'COALESCE(f."{fn}", \'\') AS denumire_src' if fn else "'' AS denumire_src"

    lic_agg_join = ""
    lic_count_expr = "0"
    if lic_col and lic_cui_col:
        lic_agg_join = f'''
        LEFT JOIN (
          SELECT "{lic_cui_col}"::text AS lic_cui,
                 SUM(COALESCE(NULLIF(trim("{lic_col}"::text), ''), '0')::int) AS lic_count
          FROM public.licente
          GROUP BY "{lic_cui_col}"::text
        ) l ON l.lic_cui = f.cui::text
        '''
        lic_count_expr = "COALESCE(l.lic_count, 0)"

    # Exclude firms where judet contains 'constan' (covers Constanța / Constanta)
    qry = f"""
    WITH candidate_firms AS (
      SELECT f.*, {lic_count_expr} AS lic_count, {ca_sel} AS cifra_val, {name_select}
      FROM public.firms f
      {lic_agg_join}
      WHERE NOT EXISTS (SELECT 1 FROM public.activities a WHERE a.cui::text = f.cui::text)
        AND lower(COALESCE(f.judet, '')) NOT LIKE '%constan%'
    )
    SELECT cf.cui, COALESCE(NULLIF(cf.denumire_src, ''), '') AS denumire, cf.lic_count, cf.cifra_val
    FROM candidate_firms cf
    ORDER BY cf.lic_count DESC, cf.cifra_val DESC
    LIMIT :limit
    """

    with engine.begin() as conn:
        rows = conn.execute(text(qry), {"limit": limit}).mappings().all()
        # refresh suggested_top
        conn.execute(text("TRUNCATE public.suggested_top RESTART IDENTITY;"))
        rank = 1
        for r in rows:
            raw_name = r.get("denumire") or ""
            # remove trailing patterns like "· Județ: Constanta" or "| Județ: Constanta" or ", Județ: Constanta"
            denumire_clean = re.sub(r'\s*[·\|\-,]\s*Județ\s*:.*$', '', raw_name, flags=re.IGNORECASE).strip()
            # additional trim: remove trailing comma/pipe/dot fragments
            denumire_clean = re.sub(r'[\|\-:,\.]+\s*$', '', denumire_clean).strip()
            if not denumire_clean:
                denumire_clean = r.get("cui") or ""
            conn.execute(text("""
                INSERT INTO public.suggested_top (rank, cui, denumire, licente, cifra_afaceri, used)
                VALUES (:rank, :cui, :denumire, :lic, :cifra, false)
            """), {
                "rank": rank,
                "cui": r.get("cui"),
                "denumire": denumire_clean,
                "lic": int(r.get("lic_count") or 0),
                "cifra": float(r.get("cifra_val") or 0)
            })
            rank += 1
    return {"inserted": len(rows)}

def rebuild_top20_caen(limit=20):
    """
    Rebuild suggested_by_caen restricted to target counties and deduplicated by CUI.
    Counties: Galati, Braila, Tulcea, Vaslui, Vrancea, Ialomita.
    """
    ensure_suggested_and_reprogram_tables()

    target_judete = ["galati","brăila","braila","tulcea","vaslui","vrancea","ialomiţa","ialomita"]
    # detect columns
    try:
        with engine.connect() as conn:
            fcols = conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='firms' ORDER BY ordinal_position"
            )).scalars().all()
    except Exception:
        fcols = []

    ca_col = next((c for c in fcols if any(x in c.lower() for x in ("cifra","cifra_de_afaceri","cifra_afaceri","cifra_de_afaceri_neta"))), None)
    name_col = next((c for c in fcols if c.lower() in ("denumire","name","denumire_firma","company","firm_name")), None)
    judet_col = next((c for c in fcols if "judet" in c.lower() or "județ" in c.lower() or "jud." in c.lower()), "judet")
    lic_col = next((c for c in fcols if any(x in c.lower() for x in ("numar_licente","numar_licen","licente","nr_licente"))), None)

    name_select = f'COALESCE(f."{name_col}", \'\') AS denumire_src' if name_col else "'' AS denumire_src"
    cifra_select = f'COALESCE(NULLIF(trim(f."{ca_col}"::text), \'\'), \'0\')::numeric AS cifra_val' if ca_col else "0 AS cifra_val"
    # safe join to relevant_caen but restrict by judet values (normalized)
    candidates_q = f"""
    WITH raw_candidates AS (
      SELECT DISTINCT f.cui::text AS cui, {name_select}, {cifra_select}, COALESCE(lower(trim(f."{judet_col}"::text)), '') AS judet_norm
      FROM public.firms f
      JOIN public.relevant_caen r ON trim(f.caen::text) = trim(r.caen_code)
      WHERE NOT EXISTS (SELECT 1 FROM public.activities a WHERE a.cui::text = f.cui::text)
    )
    SELECT cui, COALESCE(NULLIF(denumire_src,''), '') AS denumire, cifra_val, judet_norm
    FROM raw_candidates
    WHERE judet_norm IN ({', '.join([':j' + str(i) for i in range(len(target_judete))])})
    ORDER BY cifra_val DESC
    LIMIT :limit
    """

    params = {f"j{i}": target_judete[i] for i in range(len(target_judete))}
    params["limit"] = limit

    with engine.begin() as conn:
        rows = conn.execute(text(candidates_q), params).mappings().all()
        # refresh target table
        conn.execute(text("TRUNCATE public.suggested_by_caen RESTART IDENTITY;"))

        rank = 1
        seen = set()
        for r in rows:
            cui = r.get("cui")
            if not cui or cui in seen:
                continue
            seen.add(cui)

            raw_name = r.get("denumire") or ""
            denumire_clean = re.sub(r'\s*[·\|\-,]\s*Județ\s*:.*$', '', raw_name, flags=re.IGNORECASE).strip()
            denumire_clean = re.sub(r'[\|\-:,\.]+\s*$', '', denumire_clean).strip()
            if not denumire_clean:
                denumire_clean = cui

            # defensive per-row select for caen, cifra and licente
            select_fields = ["f.cui::text as cui", "f.caen::text as caen"]
            if ca_col:
                select_fields.append(f'COALESCE(NULLIF(trim(f."{ca_col}"::text), \'\'), \'0\')::numeric as cifra_de_afaceri')
            else:
                select_fields.append("0 as cifra_de_afaceri")
            if lic_col:
                select_fields.append(f'COALESCE(NULLIF(trim(f.\"{lic_col}\"::text), \'\'), \'0\')::int as numar_licente')
            else:
                select_fields.append("0 as numar_licente")

            select_sql = "SELECT " + ", ".join(select_fields) + " FROM public.firms f WHERE f.cui::text = :cui LIMIT 1"

            try:
                firm_vals = conn.execute(text(select_sql), {"cui": cui}).mappings().first()
            except Exception:
                firm_vals = None

            caen_val = firm_vals.get("caen") if firm_vals else None
            cifra_val = firm_vals.get("cifra_de_afaceri") if firm_vals else 0
            lic_val = firm_vals.get("numar_licente") if firm_vals else 0

            try:
                conn.execute(text("""
                    INSERT INTO public.suggested_by_caen (rank, cui, denumire, caen, cifra_de_afaceri, numar_licente, source, created_at)
                    VALUES (:rank, :cui, :denumire, :caen, :cifra, :lic, 'caen', now())
                """), {
                    "rank": rank,
                    "cui": cui,
                    "denumire": denumire_clean,
                    "caen": caen_val,
                    "cifra": cifra_val,
                    "lic": int(lic_val or 0)
                })
            except Exception:
                logger.exception("insert suggested_by_caen failed for %s", cui)
                # try minimal insert to keep pipeline moving
                try:
                    conn.execute(text("""
                        INSERT INTO public.suggested_by_caen (rank, cui, denumire, source, created_at)
                        VALUES (:rank, :cui, :denumire, 'caen', now())
                    """), {"rank": rank, "cui": cui, "denumire": denumire_clean})
                except Exception:
                    logger.exception("fallback insert also failed for %s", cui)
            rank += 1

    return {"inserted": len(seen)}

def take_next_suggestions(n=5):
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT rank,cui,denumire,licente,cifra_afaceri FROM public.suggested_top WHERE used = false ORDER BY rank LIMIT :n"
        ), {"n": n}).mappings().all()
    out = []
    for r in rows:
        item = dict(r)
        ca = item.get("cifra_afaceri")
        if ca is not None:
            try: item["cifra_afaceri"] = float(ca)
            except Exception: item["cifra_afaceri"] = str(ca)
        out.append(item)
    return out

def take_top_caen(n=5):
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT rank, cui, denumire, caen, cifra_de_afaceri, numar_licente FROM public.suggested_by_caen ORDER BY rank LIMIT :n"
        ), {"n": n}).mappings().all()
    out = []
    for r in rows:
        item = dict(r)
        ca = item.get("cifra_de_afaceri")
        if ca is not None:
            try: item["cifra_de_afaceri"] = float(ca)
            except Exception: item["cifra_de_afaceri"] = str(ca)
        out.append(item)
    return out

def mark_suggestions_used(cuis):
    if not cuis: return {"marked": 0}
    with engine.begin() as conn:
        conn.execute(text("UPDATE public.suggested_top SET used = true WHERE cui = ANY(:arr)"), {"arr": cuis})
    # rebuild both tops after marking used to keep consistency
    try:
        rebuild_top20(20)
    except Exception:
        logger.exception("rebuild_top20 failed after mark_suggestions_used")
    try:
        rebuild_top20_caen(20)
    except Exception:
        logger.exception("rebuild_top20_caen failed after mark_suggestions_used")
    return {"marked": len(cuis)}

# startup
@app.on_event("startup")
def startup_check_db():
    for attempt in range(3):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

                # activity_types
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

                # ensure suggested + reprogram tables and populate options
                ensure_suggested_and_reprogram_tables()

                # activities table: keep score for compatibility, add reprogram fields (nullable)
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.activities (
                  id serial PRIMARY KEY,
                  cui varchar NOT NULL,
                  activity_type_id integer,
                  comment text,
                  score integer,
                  reprogram_id integer NULL,
                  reprogram_label text NULL,
                  reprogram_days integer NULL,
                  scheduled_date date,
                  completed boolean DEFAULT false,
                  created_at timestamp default now()
                );
                """))

                # contacts
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
            logger.info("DB reachable at startup")
            try:
                rebuild_top20(20)
                rebuild_top20_caen(20)
                logger.info("rebuild_top20 and rebuild_top20_caen executed at startup")
            except Exception:
                logger.exception("rebuilds failed during startup")
            sleep(0.1)
            return
        except OperationalError as e:
            logger.warning("DB startup check failed (attempt %d): %s", attempt + 1, e)
            sleep(2)
    logger.error("DB unreachable after retries")

# SPA root
@app.get("/", include_in_schema=False)
def root_index():
    index_path = os.path.join(STATIC_DIR or "", "index.html") if STATIC_DIR else None
    if index_path and os.path.isfile(index_path):
        return FileResponse(index_path, media_type="text/html")
    return HTMLResponse(content="<!doctype html><html><body><h2>Frontend not found</h2><p>Place build in web/dist or static.</p></body></html>", status_code=200)

# health
@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="unhealthy")

# AGENDA: scheduled (exclude completed) + suggested (combined top5 from suggested_top and top5 from suggested_by_caen)
@app.get("/api/agenda")
def api_agenda(day: str = Query(None), cui: str = Query(None)):
    try:
        if not day: target = date.today()
        else: target = datetime.fromisoformat(day).date()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid day")

    try:
        # detect firm name column defensively
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
            if not firm_name_cols:
                for c in cols:
                    low = c.lower()
                    if "name" in low or "denum" in low or "denumire" in low:
                        firm_name_cols.append(c); break
        except Exception:
            firm_name_cols = []

        if firm_name_cols:
            fn = firm_name_cols[0]
            firm_name_expr = f"COALESCE(NULLIF(f.\"{fn}\"::text, ''), f.cui::text) AS firm_name"
        else:
            firm_name_expr = "f.cui::text AS firm_name"

        with engine.connect() as conn:
            params = {"day": target}
            cui_clause = "AND a.cui = :cui" if cui else ""
            if cui: params["cui"] = cui

            # scheduled: exclude completed activities; include only scheduled_date == target
            scheduled_q = text(f"""
                SELECT a.id, a.cui, a.activity_type_id, a.comment, a.score, a.reprogram_id, a.reprogram_label, a.reprogram_days, a.scheduled_date, a.completed, a.created_at,
                       {firm_name_expr}
                FROM public.activities a
                LEFT JOIN public.firms f ON f.cui::text = a.cui::text
                WHERE a.scheduled_date = :day AND COALESCE(a.completed, false) = false {cui_clause}
                ORDER BY a.created_at DESC LIMIT 500
            """)
            scheduled_rows = conn.execute(scheduled_q, {"day": target, **({"cui": cui} if cui else {})}).mappings().all()

            # overdue: scheduled_date < target and not completed
            overdue_q = text(f"""
                SELECT a.id, a.cui, a.activity_type_id, a.comment, a.score, a.reprogram_id, a.reprogram_label, a.reprogram_days, a.scheduled_date, a.completed, a.created_at,
                       {firm_name_expr}
                FROM public.activities a
                LEFT JOIN public.firms f ON f.cui::text = a.cui::text
                WHERE a.scheduled_date < :day AND COALESCE(a.completed, false) = false {cui_clause}
                ORDER BY a.scheduled_date DESC LIMIT 200
            """)
            overdue_rows = conn.execute(overdue_q, {"day": target, **({"cui": cui} if cui else {})}).mappings().all()

            # nearby: future scheduled (next 7 days) and not completed
            params_nb = {"day": target, "day_end": target + timedelta(days=7)}
            if cui: params_nb["cui"] = cui
            nearby_q = text(f"""
                SELECT a.id, a.cui, a.activity_type_id, a.comment, a.score, a.reprogram_id, a.reprogram_label, a.reprogram_days, a.scheduled_date, a.completed, a.created_at,
                       {firm_name_expr}
                FROM public.activities a
                LEFT JOIN public.firms f ON f.cui::text = a.cui::text
                WHERE a.scheduled_date > :day AND a.scheduled_date <= :day_end AND COALESCE(a.completed, false) = false {cui_clause}
                ORDER BY a.scheduled_date ASC LIMIT 500
            """)
            nearby_rows = conn.execute(nearby_q, params_nb).mappings().all()

        def row_to_obj(r):
            sd = r.get("scheduled_date"); ca = r.get("created_at")
            return {
                "id": r.get("id"),
                "cui": r.get("cui"),
                "firm_name": r.get("firm_name") if r.get("firm_name") is not None else r.get("cui"),
                "type_id": r.get("activity_type_id"),
                "comment": r.get("comment"),
                "programare_id": r.get("reprogram_id"),
                "programare_label": r.get("reprogram_label"),
                "programare_days": r.get("reprogram_days"),
                "score": r.get("score"),  # kept for compatibility
                "scheduled_date": sd.isoformat() if sd is not None else None,
                "completed": bool(r.get("completed")),
                "created_at": ca.isoformat() if ca is not None else None,
            }

        suggested = take_next_suggestions(5)
        suggested_caen = take_top_caen(5)

        # combine suggested lists into single suggested array (first licenses-based then caen-based, dedup by cui)
        combined = []
        seen = set()
        for s in suggested:
            cui = s.get("cui")
            if cui and cui not in seen:
                seen.add(cui)
                entry = {"source": "licenses", **s}
                combined.append(entry)
        for s in suggested_caen:
            cui = s.get("cui")
            if cui and cui not in seen:
                seen.add(cui)
                # adapt field names to match existing suggested structure
                entry = {
                    "source": "caen",
                    "rank": s.get("rank"),
                    "cui": s.get("cui"),
                    "denumire": s.get("denumire"),
                    "licente": s.get("numar_licente") if s.get("numar_licente") is not None else 0,
                    "cifra_afaceri": s.get("cifra_de_afaceri")
                }
                combined.append(entry)

        return JSONResponse(content={
            "date": target.isoformat(),
            "scheduled": [row_to_obj(r) for r in scheduled_rows],
            "overdue": [row_to_obj(r) for r in overdue_rows],
            "nearby": [row_to_obj(r) for r in nearby_rows],
            "suggested": combined,              # up to 10 (5+5) combined and deduped
            "suggested_caen": suggested_caen   # separate list if frontend prefers it
        })
    except Exception:
        logger.exception("api_agenda failed")
        raise HTTPException(status_code=500, detail="internal error")

# expose reprogram options to frontend
@app.get("/api/reprogram_options")
def api_reprogram_options():
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT id, label, days FROM public.reprogram_options ORDER BY id")).mappings().all()
        out = [{"id": r.get("id"), "label": r.get("label"), "days": r.get("days")} for r in rows]
        return JSONResponse(content=out)
    except Exception:
        logger.exception("api_reprogram_options failed")
        return JSONResponse(content=[])

# search kept as previously implemented (defensive)
@app.get("/search")
def api_search(q: str = Query(...), limit: int = Query(10, ge=1, le=100)):
    try:
        like = f"%{q.strip()}%"

        # detect licente columns
        try:
            with engine.connect() as conn:
                lic_cols = conn.execute(text(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='licente'"
                )).scalars().all()
        except Exception:
            lic_cols = []

        lic_cui_col = next((c for c in lic_cols if any(x in c.lower() for x in ("cui", "codcui", "cod_cui", "firm_cui"))), None)
        lic_count_col = next((c for c in lic_cols if any(x in c.lower() for x in ("licen", "license", "licente", "nr_licente", "numar_licente"))), None)

        if lic_cui_col and lic_count_col:
            lic_sub = (
                f'LEFT JOIN ('
                f'  SELECT "{lic_cui_col}"::text AS lic_cui, '
                f'         SUM(COALESCE(NULLIF(trim("{lic_count_col}"::text), \'\'), \'0\')::int) AS lic_count '
                f'  FROM public.licente '
                f'  GROUP BY "{lic_cui_col}"::text'
                f') l ON l.lic_cui = f.cui::text'
            )
        else:
            lic_sub = "LEFT JOIN (SELECT ''::text AS lic_cui, 0 AS lic_count LIMIT 0) l ON false"

        # detect firms columns
        try:
            with engine.connect() as conn:
                fcols = conn.execute(text(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='firms'"
                )).scalars().all()
        except Exception:
            fcols = []

        firm_name_col = None
        for c in fcols:
            low = c.lower()
            if low in ("denumire", "name", "denumire_firma", "company", "firm_name"):
                firm_name_col = c; break
        if not firm_name_col:
            for c in fcols:
                low = c.lower()
                if "name" in low or "denum" in low or "denumire" in low:
                    firm_name_col = c; break

        ca_col = None
        for c in fcols:
            low = c.lower()
            if any(x in low for x in ("cifra", "cifra_de_afaceri", "cifra_afaceri", "cifra_de_afaceri_neta")):
                ca_col = c; break

        if firm_name_col:
            name_expr = f'COALESCE(NULLIF(f."{firm_name_col}", \'\'), f.cui::text)'
            name_filter = f'COALESCE(f."{firm_name_col}", \'\') ILIKE :like'
        else:
            name_expr = "f.cui::text"
            name_filter = "false"

        if ca_col:
            ca_expr = f'COALESCE(NULLIF(f."{ca_col}"::text, \'\'), \'\')'
        else:
            ca_expr = "''"

        sql = (
            "SELECT f.cui, "
            f"       {name_expr} AS name, "
            "       COALESCE(f.judet, '') AS judet, "
            f"       {ca_expr} AS cifra_afaceri_raw, "
            "       COALESCE(l.lic_count, 0) AS licente "
            "FROM public.firms f "
            + lic_sub + " "
            "WHERE (f.cui::text ILIKE :like OR COALESCE(f.denumire,'') ILIKE :like OR " + name_filter + ") "
            "LIMIT :limit"
        )

        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"like": like, "limit": limit}).mappings().all()

        out = []
        for r in rows:
            out.append({
                "cui": r.get("cui"),
                "name": (r.get("name") or '').strip(),
                "judet": r.get("judet"),
                "cifra_afaceri": norm_number(r.get("cifra_afaceri_raw")),
                "licente": int(r.get("licente") or 0)
            })
        return JSONResponse(content=out)
    except Exception:
        logger.exception("api_search failed")
        raise HTTPException(status_code=500, detail="search failed")

@app.get("/api/suggested_next")
def api_suggested_next(n: int = Query(5, ge=1, le=20)):
    try:
        return JSONResponse(content=take_next_suggestions(n))
    except Exception:
        logger.exception("api_suggested_next failed")
        return JSONResponse(content=[])

@app.post("/api/suggested_mark_used")
def api_suggested_mark_used(cuis: list[str] = Body(...)):
    try:
        return JSONResponse(content=mark_suggestions_used(cuis))
    except Exception:
        logger.exception("api_suggested_mark_used failed")
        raise HTTPException(status_code=500, detail="failed")

@app.post("/admin/rebuild_top20")
def admin_rebuild(limit: int = 20):
    try:
        return JSONResponse(content=rebuild_top20(limit))
    except Exception:
        logger.exception("admin rebuild failed")
        raise HTTPException(status_code=500, detail="rebuild failed")

@app.get("/api/firms/{firm_id}")
def get_firm(firm_id: str):
    try:
        with engine.connect() as conn:
            firm_row = conn.execute(text("SELECT f.* FROM public.firms f WHERE f.cui = :cui LIMIT 1"), {"cui": firm_id}).mappings().first()
    except Exception:
        logger.exception("get_firm failed")
        raise HTTPException(status_code=500, detail="internal error")
    if not firm_row: raise HTTPException(status_code=404, detail="Firm not found")
    firm = dict(firm_row)
    name = firm.get("denumire") or firm.get("name")
    acts = []; contacts = []
    try:
        with engine.connect() as conn:
            activities = conn.execute(text("SELECT id, activity_type_id, comment, score, reprogram_id, reprogram_label, reprogram_days, scheduled_date, completed, created_at FROM public.activities WHERE cui = :cui ORDER BY created_at DESC LIMIT 200"), {"cui": firm.get("cui")}).mappings().all()
            types = {r["id"]: r["name"] for r in conn.execute(text("SELECT id, name FROM public.activity_types")).mappings().all()}
        for a in activities:
            acts.append({
                "id": a.get("id"),
                "type_id": a.get("activity_type_id"),
                "type_name": types.get(a.get("activity_type_id")),
                "comment": a.get("comment"),
                "programare_id": a.get("reprogram_id"),
                "programare_label": a.get("reprogram_label"),
                "programare_days": a.get("reprogram_days"),
                "score": a.get("score"),
                "scheduled_date": safe_iso(a.get("scheduled_date")),
                "completed": bool(a.get("completed")),
                "created_at": safe_iso(a.get("created_at"))
            })
    except Exception:
        acts = []
    try:
        with engine.connect() as conn:
            crows = conn.execute(text("SELECT id, name, phone, email, role, created_at FROM public.contacts WHERE firm_cui = :cui ORDER BY created_at DESC"), {"cui": firm.get("cui")}).mappings().all()
        for c in crows:
            contacts.append({"id": c.get("id"), "name": c.get("name"), "phone": c.get("phone"), "email": c.get("email"), "role": c.get("role"), "created_at": safe_iso(c.get("created_at"))})
    except Exception:
        contacts = []
    resp = {
        "id": firm.get("cui"),
        "cui": firm.get("cui"),
        "name": name,
        "judet": firm.get("judet"),
        "localitate": firm.get("localitate"),
        "caen": firm.get("caen") or firm.get("cod_caen"),
        "caen_description": None,
        "cifra_afaceri": norm_number(firm.get("cifra_de_afaceri_neta") or firm.get("cifra_de_afaceri") or firm.get("cifra_afaceri")),
        "profit": norm_number(firm.get("profitul_brut") or firm.get("profit_net") or firm.get("profit")),
        "angajati": norm_number(firm.get("numar_mediu_de_salariati") or firm.get("angajati")),
        "licente": None,
        "raw": firm,
        "activities": acts,
        "contacts": contacts
    }
    try:
        caen_val = resp.get("caen")
        if caen_val:
            with engine.connect() as conn:
                cd = conn.execute(text("SELECT descriere FROM public.caen_codes WHERE clasa = trim(:caen) LIMIT 1"), {"caen": str(caen_val)}).scalar_one_or_none()
            if cd: resp["caen_description"] = cd.strip() if isinstance(cd, str) else cd
    except Exception: pass
    try:
        colmap = detect_licente_columns()
        lic_val = firm.get("numar_licente") or firm.get("licente")
        if (lic_val is None or lic_val == "") and colmap.get("cui") and colmap.get("licente"):
            lic_val = get_licente_for_cui(firm.get("cui"), colmap)
        resp["licente"] = norm_number(lic_val)
    except Exception: pass
    resp["profit_net"] = resp.get("profit")
    return JSONResponse(content=resp)

@app.get("/api/firms/{firm_id}/contacts")
def get_firm_contacts(firm_id: str):
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT id, name, phone, email, role, created_at FROM public.contacts WHERE firm_cui = :cui ORDER BY created_at DESC"), {"cui": firm_id}).mappings().all()
        out = []
        for r in rows:
            out.append({"id": r.get("id"), "name": r.get("name"), "phone": r.get("phone"), "email": r.get("email"), "role": r.get("role"), "created_at": safe_iso(r.get("created_at"))})
        return JSONResponse(content=out)
    except Exception:
        logger.exception("get_firm_contacts failed")
        raise HTTPException(status_code=500, detail="cannot load contacts")

# Activities / contacts creation and marking completed
class ActivityIn(BaseModel):
    firm_id: str
    activity_type_id: int | None = None
    comment: str
    # keep score for backwards compatibility; prefer programare_id
    score: int | None = None
    programare_id: int | None = None
    scheduled_date: str | None = None
    completed: bool | None = None

def _lookup_reprogram_option(conn, reprogram_id):
    if reprogram_id is None:
        return None, None
    row = conn.execute(text("SELECT id, label, days FROM public.reprogram_options WHERE id = :id LIMIT 1"), {"id": reprogram_id}).mappings().first()
    if not row:
        return None, None
    return row.get("label"), row.get("days")

@app.post("/api/activities")
def create_or_update_activity(payload: dict = Body(...)):
    try:
        if not isinstance(payload, dict):
            payload = payload.dict()
    except Exception:
        payload = dict(payload)

    cui = (payload.get("firm_id") or payload.get("firmId") or "").strip()
    comment = (payload.get("comment") or "").strip()
    if not cui or not comment:
        raise HTTPException(status_code=400, detail="firm_id and comment required")

    prog_id = payload.get("programare_id") or payload.get("programareId") or payload.get("programare")
    prog_days_override = payload.get("programare_days") or payload.get("programareDays") or payload.get("days")

    try:
        with engine.begin() as conn:
            prog_label = None
            prog_days = None

            if prog_days_override is not None:
                try:
                    prog_days = int(prog_days_override)
                    prog_label = f"manual ({prog_days})"
                except Exception:
                    prog_days = None
            elif prog_id is not None:
                try:
                    pid = int(prog_id)
                    plabel, pdays = _lookup_reprogram_option(conn, pid)
                    prog_label, prog_days = plabel, pdays
                except Exception:
                    prog_label, prog_days = None, None

            scheduled = None
            if prog_days is not None:
                try:
                    scheduled = (datetime.utcnow().date() + timedelta(days=int(prog_days)))
                except Exception:
                    scheduled = None

            sdate_from_client = payload.get("scheduled_date")
            if sdate_from_client and scheduled is None:
                try:
                    scheduled = datetime.fromisoformat(sdate_from_client).date()
                except Exception:
                    scheduled = None

            reprogram_id_to_store = None
            try:
                reprogram_id_to_store = int(prog_id) if prog_id not in (None, "") else None
            except Exception:
                reprogram_id_to_store = None

            res = conn.execute(text("""
                INSERT INTO public.activities (cui, activity_type_id, comment, score, reprogram_id, reprogram_label, reprogram_days, scheduled_date, completed, created_at)
                VALUES (:cui, :atype, :comment, :score, :rid, :rlabel, :rdays, :sdate, :completed, now())
                RETURNING id, created_at, scheduled_date
            """), {
                "cui": cui,
                "atype": payload.get("activity_type_id") or payload.get("activityTypeId") or None,
                "comment": comment,
                "score": payload.get("score") or None,
                "rid": reprogram_id_to_store,
                "rlabel": prog_label,
                "rdays": prog_days,
                "sdate": scheduled,
                "completed": bool(payload.get("completed", False))
            }).mappings().first()

            conn.execute(text("UPDATE public.suggested_top SET used = true WHERE cui = :cui"), {"cui": cui})

        rebuild_top20(20)
        rebuild_top20_caen(20)

        return JSONResponse(status_code=201, content={
            "id": res.get("id"),
            "cui": cui,
            "scheduled_date": safe_iso(res.get("scheduled_date")),
            "created_at": safe_iso(res.get("created_at"))
        })

    except IntegrityError:
        raise HTTPException(status_code=400, detail="Database integrity error")
    except Exception:
        logger.exception("create_or_update_activity failed")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/activities/{activity_id}/complete")
def mark_activity_completed(activity_id: int):
    try:
        with engine.begin() as conn:
            r = conn.execute(text("UPDATE public.activities SET completed = true WHERE id = :id RETURNING id"), {"id": activity_id}).first()
            if not r:
                raise HTTPException(status_code=404, detail="Activity not found")
        return {"updated": 1}
    except Exception:
        logger.exception("mark_activity_completed failed")
        raise HTTPException(status_code=500, detail="failed")

class ContactIn(BaseModel):
    firm_cui: str
    name: str
    phone: str | None = None
    email: str | None = None
    role: str | None = None

@app.post("/api/contacts", status_code=201)
def create_contact(payload: ContactIn = Body(...)):
    try:
        with engine.begin() as conn:
            res = conn.execute(text(
                "INSERT INTO public.contacts (firm_cui, name, phone, email, role) VALUES (:cui, :name, :phone, :email, :role) RETURNING id, created_at"
            ), {"cui": payload.firm_cui, "name": payload.name, "phone": payload.phone, "email": payload.email, "role": payload.role}).mappings().first()
        return JSONResponse(content={"id": res["id"], "firm_cui": payload.firm_cui, "name": payload.name, "created_at": safe_iso(res["created_at"])}, status_code=201)
    except Exception:
        logger.exception("create_contact failed")
        raise HTTPException(status_code=500, detail="cannot create contact")

@app.post("/api/firms/{firm_id}/contacts")
def create_contact_for_firm(firm_id: str, contact: ContactIn):
    try:
        with engine.begin() as conn:
            res = conn.execute(text(
                "INSERT INTO public.contacts (firm_cui, name, phone, email, role) VALUES (:cui, :name, :phone, :email, :role) RETURNING id, created_at"
            ), {"cui": firm_id, "name": contact.name, "phone": contact.phone, "email": contact.email, "role": contact.role}).mappings().first()
        return JSONResponse(content={"id": res["id"], "created_at": safe_iso(res["created_at"])}, status_code=201)
    except Exception:
        logger.exception("create_contact_for_firm failed")
        raise HTTPException(status_code=500, detail="cannot create contact")
