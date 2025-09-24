# import_caen.py
import csv
import os
from sqlalchemy import create_engine, text
from urllib.parse import unquote_plus

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# Normalize postgres:// -> postgresql:// if needed
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, future=True)

csv_path = "coduri_caen.csv"  # deja în proiect

insert_stmt = text("""
INSERT INTO public.caen_codes (grupa, denumire, nace, diviziune)
VALUES (:grupa, :denumire, :nace, :diviziune)
ON CONFLICT (grupa) DO UPDATE
  SET denumire = EXCLUDED.denumire,
      nace = EXCLUDED.nace,
      diviziune = EXCLUDED.diviziune
""")

with engine.begin() as conn:
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';', quotechar='"')
        count = 0
        for row in reader:
            grupa = row.get("GRUPA") or row.get("Grupa") or row.get("grupa")
            denumire = row.get("DENUMIRE") or row.get("Denumire") or row.get("denumire")
            nace = row.get("NACE") or row.get("Nace") or row.get("nace") or None
            diviziune = row.get("DIVIZIUNE") or row.get("Diviziune") or row.get("diviziune") or None

            if grupa and denumire:
                conn.execute(insert_stmt, {"grupa": grupa.strip(), "denumire": denumire.strip(), "nace": (nace or "").strip() or None, "diviziune": (diviziune or "").strip() or None})
                count += 1

print(f"Imported/updated {count} CAEN rows.")
