# check_schema.py
import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_LOCAL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL not set in environment")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

e = create_engine(DATABASE_URL, future=True)
with e.connect() as conn:
    cols = conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'firms' "
            "ORDER BY ordinal_position"
        )
    ).all()
    print("firms columns:", [c[0] for c in cols])

    # sample one firm
    sample = conn.execute(text("SELECT * FROM firms LIMIT 1")).mappings().first()
    print("sample firm row:", sample)

