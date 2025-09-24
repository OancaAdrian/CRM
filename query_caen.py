from sqlalchemy import create_engine, text
import os
url = os.environ.get("DATABASE_URL")
if not url:
    raise SystemExit("DATABASE_URL not set")
if url.startswith("postgres://"): url = url.replace("postgres://", "postgresql://", 1)
e = create_engine(url, future=True)
with e.connect() as c:
    rows = c.execute(text("SELECT grupa, denumire FROM public.caen_codes WHERE grupa LIKE '451%' ORDER BY grupa LIMIT 50")).all()
    for r in rows:
        print(r)
