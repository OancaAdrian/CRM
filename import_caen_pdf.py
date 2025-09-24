#!/usr/bin/env python3
"""
import_caen_pdf.py

- Extrage structura CAEN dintr-un PDF (diviziune, grupa, clasa, denumire).
- Scrie un CSV de verificare (caen_extracted.csv).
- Normalizeaza denumirile eliminand diacritice in denumire_plain.
- Opțional: importa CSV-ul in Postgres (creaza tabela daca nu exista).
Usage:
  python import_caen_pdf.py            # doar extracție -> caen_extracted.csv
  python import_caen_pdf.py --import   # face și importul în DB (vezi WARNING)
  python import_caen_pdf.py --pdf path/to/file.pdf --csv out.csv --import
"""
from __future__ import annotations
import os
import re
import sys
import csv
import argparse
import unicodedata
from typing import List, Tuple, Optional

try:
    import pdfplumber
except Exception as e:
    print("Missing pdfplumber. Install with: pip install pdfplumber")
    raise

try:
    from sqlalchemy import create_engine, text
except Exception:
    print("Missing sqlalchemy or DB driver. Install with: pip install sqlalchemy psycopg2-binary")
    raise

# ----- Config / Defaults -----
DEFAULT_PDF = "CAEN-Rev.3_structura-completa.pdf"
DEFAULT_CSV = "caen_extracted.csv"

# ----- Utilities -----
def remove_diacritics(s: Optional[str]) -> str:
    if not s:
        return ""
    nf = unicodedata.normalize("NFKD", s)
    no_diac = "".join(ch for ch in nf if not unicodedata.combining(ch))
    # collapse whitespace and lowercase
    return re.sub(r"\s+", " ", no_diac).strip().lower()

def normalize_line(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

# Regexes for detection
CLASS_RE = re.compile(r'^\s*([0-9]{4})\s+(.+)$')   # 4-digit class + desc
GROUP_RE = re.compile(r'^\s*([0-9]{3})\s+(.+)$')   # 3-digit group + desc
DIV_RE   = re.compile(r'^\s*([0-9]{2})\s*$')       # 2-digit division alone

# ----- PDF extraction -----
def extract_rows_from_pdf(pdf_path: str) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], int]]:
    """
    Returns list of tuples: (diviziune, grupa, clasa, denumire, nivel)
    nivel: 1=diviziune, 2=grupă, 3=clasă
    """
    rows: List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], int]] = []
    current_div: Optional[str] = None
    current_group: Optional[str] = None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
            for ln in lines:
                ln = normalize_line(ln)
                # class (4 digits)
                m = CLASS_RE.match(ln)
                if m:
                    cl = m.group(1)
                    desc = normalize_line(m.group(2))
                    rows.append((current_div, current_group, cl, desc, 3))
                    continue
                # group (3 digits)
                m = GROUP_RE.match(ln)
                if m:
                    grp = m.group(1)
                    desc = normalize_line(m.group(2))
                    current_group = grp
                    rows.append((current_div, grp, None, desc, 2))
                    continue
                # division (2 digits alone)
                m = DIV_RE.match(ln)
                if m:
                    div = m.group(1)
                    current_div = div
                    rows.append((current_div, None, None, None, 1))
                    continue
                # fallback: continuation of previous description -> append to last row
                if rows and ln:
                    last_div, last_grp, last_cl, last_desc, last_lvl = rows[-1]
                    new_desc = (last_desc or "") + " " + ln
                    rows[-1] = (last_div, last_grp, last_cl, normalize_line(new_desc), last_lvl)
    # dedupe/clean: keep best description for same (grupa,clasa)
    seen = {}
    cleaned = []
    for d, g, c, desc, lvl in rows:
        key = (g or "", c or "")
        if key in seen:
            # prefer longer desc
            if desc and len(desc) > len(seen[key][3] or ""):
                # update in cleaned
                idx = seen[key][0]
                cleaned[idx] = (d, g, c, desc, lvl)
                seen[key] = (idx, (d, g, c, desc, lvl))
            continue
        seen[key] = (len(cleaned), (d, g, c, desc, lvl))
        cleaned.append((d, g, c, desc, lvl))
    return cleaned

# ----- CSV write -----
def write_csv(rows, csv_path: str):
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["diviziune", "grupa", "clasa", "denumire", "denumire_plain", "nivel"])
        for d, g, c, desc, lvl in rows:
            desc = desc or ""
            plain = remove_diacritics(desc)
            w.writerow([d or "", g or "", c or "", desc, plain, lvl])

# ----- DB helpers -----
def get_engine_from_env() -> object:
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL environment variable is not set")
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    engine = create_engine(DATABASE_URL, future=True)
    return engine

def ensure_table(engine):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.caen_codes (
      id SERIAL PRIMARY KEY,
      diviziune VARCHAR(4),
      grupa VARCHAR(4),
      clasa VARCHAR(4),
      denumire TEXT,
      denumire_plain TEXT,
      nivel SMALLINT,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
    );
    """
    idx_sql = """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_caen_grupa_clasa ON public.caen_codes (grupa, clasa);
    CREATE INDEX IF NOT EXISTS idx_caen_grupa ON public.caen_codes (grupa);
    CREATE INDEX IF NOT EXISTS idx_caen_denumire_plain ON public.caen_codes (lower(denumire_plain));
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        # create indexes; some DB UIs don't support multiple statements, but SQLAlchemy's execute does.
        conn.execute(text(idx_sql))

def import_csv_to_db(engine, csv_path: str, truncate=False) -> int:
    inserted = 0
    if truncate:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE public.caen_codes;"))
    insert_stmt = text("""
    INSERT INTO public.caen_codes (diviziune, grupa, clasa, denumire, denumire_plain, nivel)
    VALUES (:diviziune, :grupa, :clasa, :denumire, :denumire_plain, :nivel)
    ON CONFLICT (grupa, clasa) DO UPDATE
      SET denumire = COALESCE(NULLIF(EXCLUDED.denumire, ''), public.caen_codes.denumire),
          denumire_plain = COALESCE(NULLIF(EXCLUDED.denumire_plain, ''), public.caen_codes.denumire_plain),
          diviziune = COALESCE(NULLIF(EXCLUDED.diviziune, ''), public.caen_codes.diviziune),
          nivel = EXCLUDED.nivel
    """)
    with engine.begin() as conn, open(csv_path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter=";")
        for r in rdr:
            # sanitize
            div = r.get("diviziune") or None
            grp = (r.get("grupa") or "").strip() or None
            cl = (r.get("clasa") or "").strip() or None
            den = r.get("denumire") or ""
            den_plain = r.get("denumire_plain") or remove_diacritics(den)
            lvl = int(r.get("nivel") or 2)
            conn.execute(insert_stmt, {
                "diviziune": div,
                "grupa": grp,
                "clasa": cl,
                "denumire": den,
                "denumire_plain": den_plain,
                "nivel": lvl
            })
            inserted += 1
    return inserted

# ----- CLI -----
def main():
    ap = argparse.ArgumentParser(description="Extract CAEN from PDF, generate CSV and optionally import to Postgres")
    ap.add_argument("--pdf", default=DEFAULT_PDF, help="PDF input path")
    ap.add_argument("--csv", default=DEFAULT_CSV, help="CSV output path")
    ap.add_argument("--import", dest="do_import", action="store_true", help="Also import CSV into DB")
    ap.add_argument("--truncate", action="store_true", help="Truncate target table before import (use with care)")
    args = ap.parse_args()

    if not os.path.exists(args.pdf):
        print(f"PDF not found: {args.pdf}")
        sys.exit(1)

    print(f"Extracting rows from: {args.pdf} ...")
    rows = extract_rows_from_pdf(args.pdf)
    print(f"Extracted {len(rows)} candidate rows. Writing CSV to {args.csv} ...")
    write_csv(rows, args.csv)
    print("Wrote CSV. PLEASE REVIEW the CSV before importing.")

    if args.do_import:
        print("Import flag present. Proceeding to import into DB...")
        engine = get_engine_from_env()
        print("Ensuring target table exists and indexes are present...")
        ensure_table(engine)
        print("Importing CSV -> DB. This will run INSERT ... ON CONFLICT DO UPDATE.")
        cnt = import_csv_to_db(engine, args.csv, truncate=args.truncate)
        print(f"Imported/updated {cnt} rows into public.caen_codes.")
    else:
        print("Import not performed. Re-run with --import to import into DB after manual CSV review.")

if __name__ == "__main__":
    main()
