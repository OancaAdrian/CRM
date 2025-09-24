# -*- coding: utf-8 -*-
import os, csv
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, future=True)

insert_stmt = text("""
INSERT INTO public.caen_codes (grupa, denumire, nace, diviziune)
VALUES (:grupa, :denumire, :nace, :diviziune)
ON CONFLICT (grupa) DO UPDATE
  SET denumire = EXCLUDED.denumire,
      nace = EXCLUDED.nace,
      diviziune = EXCLUDED.diviziune
""")

csv_path = "coduri_caen.csv"
if not os.path.exists(csv_path):
    raise SystemExit(f"CSV not found: {csv_path}")

count = 0
with engine.begin() as conn:
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';', quotechar='"')
        for i, row in enumerate(reader, start=1):
            grupa = (row.get("GRUPA") or "").strip()
            den = (row.get("DENUMIRE") or "").strip()
            nace = (row.get("NACE") or "").strip() or None
            div = (row.get("DIVIZIUNE") or "").strip() or None
            if not grupa or not den:
                continue
            conn.execute(insert_stmt, {"grupa": grupa, "denumire": den, "nace": nace, "diviziune": div})
            count += 1

print(f"Imported/updated {count} CAEN rows.")