# main.py
import os
import re
import logging
from time import sleep
from datetime import date, datetime, timedelta
from urllib.parse import urlparse, urlunparse

from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Depends

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
    ensure_suggested_and_reprogram_tables()
    try:
        with engine.connect() as conn:
            cols = conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='firms' ORDER BY ordinal_position"
            )).scalars().all()
    except Exception:
        cols = []

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
        conn.execute(text("TRUNCATE public.suggested_top RESTART IDENTITY;"))
        rank = 1
        for r in rows:
            raw_name = r.get("denumire") or ""
            denumire_clean = re.sub(r'\s*[·\|\-,]\s*Județ\s*:.*$', '', raw_name, flags=re.IGNORECASE).strip()
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
    ensure_suggested_and_reprogram_tables()

    target_judete = ["galati","brăila","braila","tulcea","vaslui","vrancea","ialomiţa","ialomita"]
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

            select_fields = ["f.cui::text as cui", "f.caen::text as caen"]
            if ca_col:
                select_fields.append(f'COALESCE(NULLIF(trim(f."{ca_col}"::text), \'\'), \'\')::numeric as cifra_de_afaceri')
            else:
                select_fields.append("0 as cifra_de_afaceri")
            if lic_col:
                select_fields.append(f'COALESCE(NULLIF(trim(f."{lic_col}"::text), \'\'), \'\')::int as numar_licente')
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

                ensure_suggested_and_reprogram_tables()

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

# Remaining routes unchanged (agenda, reprogram_options, search, suggested endpoints, firms, contacts, activities handlers)
# ... (rest of routes unchanged, already present in file)
# Note: keep the rest of your route implementations below exactly as before.
