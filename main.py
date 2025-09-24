# main.py
import os
import io
import csv
import datetime
import logging
import traceback

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Date, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError, OperationalError

# ---------- Config & logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crm-main")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ---------- Database ----------
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

class Activity(Base):
    __tablename__ = "activities"
    id = Column(Integer, primary_key=True, index=True)
    cui = Column(String(50), nullable=False)
    activity_type_id = Column(Integer, nullable=True)
    comment = Column(Text, nullable=False)
    score = Column(Integer, nullable=True)
    scheduled_date = Column(Date, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.datetime.utcnow)

class Firm(Base):
    __tablename__ = "firms"
    id = Column(Integer, primary_key=True, index=True)
    cui = Column(String(50), unique=True, index=True)
    name = Column(String(255))
    judet = Column(String(100))
    localitate = Column(String(200))
    cifra_afaceri = Column(String(100))
    profit_net = Column(String(100))
    angajati = Column(String(50))
    licente = Column(String(50))
    caen = Column(String(20))

try:
    Base.metadata.create_all(bind=engine)
except OperationalError as e:
    logger.warning("DB tables creation skipped or failed: %s", e)

# ---------- FastAPI app ----------
app = FastAPI(title="CRM API", version="0.1.0")

# Allow CORS for local/dev. In production narrow this.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static frontend if folder exists
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "frontend_build")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="static")
else:
    # keep root redirect to docs if no static bundle
    @app.get("/", include_in_schema=False)
    def _root():
        return RedirectResponse(url="/docs")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Schemas ----------
class ActivityIn(BaseModel):
    firm_id: str
    activity_type_id: int | None = None
    comment: str
    score: int | None = None
    scheduled_date: str | None = None

# ---------- Helpers ----------
def safe_isoformat(dt):
    if dt is None:
        return None
    if hasattr(dt, "isoformat"):
        try:
            return dt.isoformat()
        except Exception:
            return str(dt)
    return str(dt)

# ---------- Routes ----------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/api/activities", status_code=201)
def create_activity(payload: ActivityIn, db=Depends(get_db)):
    cui = (payload.firm_id or "").strip()
    comment = (payload.comment or "").strip()
    if not cui or not comment:
        raise HTTPException(status_code=400, detail="firm_id and comment required")

    # idempotent check: same cui + comment
    existing = db.execute(
        text("SELECT id, cui, activity_type_id, comment, score, scheduled_date, created_at "
             "FROM public.activities WHERE cui = :cui AND comment = :comment LIMIT 1"),
        {"cui": cui, "comment": comment}
    ).first()
    if existing:
        row = existing
        return JSONResponse(status_code=409, content={
            "id": row.id,
            "cui": row.cui,
            "activity_type_id": row.activity_type_id,
            "comment": row.comment,
            "score": row.score,
            "scheduled_date": safe_isoformat(row.scheduled_date),
            "created_at": safe_isoformat(row.created_at),
        })

    try:
        ins = Activity(
            cui=cui,
            activity_type_id=payload.activity_type_id,
            comment=comment,
            score=payload.score,
            scheduled_date=(datetime.date.fromisoformat(payload.scheduled_date) if payload.scheduled_date else None)
        )
        db.add(ins)
        db.commit()
        db.refresh(ins)
        return {
            "id": ins.id,
            "cui": ins.cui,
            "activity_type_id": ins.activity_type_id,
            "comment": ins.comment,
            "score": ins.score,
            "scheduled_date": safe_isoformat(ins.scheduled_date),
            "created_at": safe_isoformat(ins.created_at),
        }
    except IntegrityError:
        db.rollback()
        logger.exception("IntegrityError on insert")
        existing2 = db.execute(
            text("SELECT id, cui, activity_type_id, comment, score, scheduled_date, created_at "
                 "FROM public.activities WHERE cui = :cui AND comment = :comment LIMIT 1"),
            {"cui": cui, "comment": comment}
        ).first()
        if existing2:
            return JSONResponse(status_code=409, content={
                "id": existing2.id,
                "cui": existing2.cui,
                "activity_type_id": existing2.activity_type_id,
                "comment": existing2.comment,
                "score": existing2.score,
                "scheduled_date": safe_isoformat(existing2.scheduled_date),
                "created_at": safe_isoformat(existing2.created_at),
            })
        raise HTTPException(status_code=500, detail="Database error")

@app.get("/api/firms/{firm_id}")
def get_firm(firm_id: str, db=Depends(get_db)):
    try:
        firm = db.query(Firm).filter((Firm.cui == firm_id) | (Firm.id == firm_id)).first()
        if not firm:
            raise HTTPException(status_code=404, detail="Firm not found")

        rows = db.execute(
            text("SELECT id, cui, activity_type_id, comment, score, scheduled_date, created_at "
                 "FROM public.activities WHERE cui = :cui ORDER BY created_at DESC LIMIT 200"),
            {"cui": firm.cui}
        ).all()

        acts = []
        for a in rows:
            try:
                acts.append({
                    "id": getattr(a, "id", None),
                    "type_id": getattr(a, "activity_type_id", None),
                    "comment": getattr(a, "comment", None),
                    "score": getattr(a, "score", None),
                    "scheduled_date": safe_isoformat(getattr(a, "scheduled_date", None)),
                    "created_at": safe_isoformat(getattr(a, "created_at", None))
                })
            except Exception:
                logger.exception("Failed to serialize activity id=%s", getattr(a, "id", None))
                continue

        return {
            "id": firm.id,
            "cui": firm.cui,
            "name": firm.name,
            "judet": firm.judet,
            "localitate": firm.localitate,
            "cifra_afaceri": firm.cifra_afaceri,
            "profit_net": firm.profit_net,
            "angajati": firm.angajati,
            "licente": firm.licente,
            "caen": firm.caen,
            "activities": acts
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in get_firm for %s", firm_id)
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")

@app.post("/api/caen/import")
async def import_caen(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Expected CSV")

    content = await file.read()
    text_csv = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text_csv), delimiter=",")

    inserted = 0
    with engine.begin() as conn:
        for row in reader:
            div = row.get("diviziune") or row.get("div") or None
            grupa = row.get("grupa") or row.get("grup") or None
            clasa = row.get("clasa") or row.get("cod") or None
            den = row.get("denumire") or row.get("descriere") or None
            nivel = row.get("nivel") or None
            conn.execute(text("""
                INSERT INTO public.caen_codes (diviziune, grupa, clasa, denumire, nivel)
                VALUES (:div, :grupa, :clasa, :den)
                ON CONFLICT DO NOTHING
            """), {"div": div, "grupa": grupa, "clasa": clasa, "den": den, "nivel": nivel})
            inserted += 1

    return {"imported_count": inserted}

# ---------- Startup check ----------
@app.on_event("startup")
def startup_checks():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("DB OK")
    except Exception:
        logger.exception("DB connection failed at startup")
