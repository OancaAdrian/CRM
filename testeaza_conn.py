# testeaza_conn.py
from sqlalchemy import create_engine, text
import os, sys
url = os.environ.get("DATABASE_URL")
if not url:
    print("DATABASE_URL missing"); sys.exit(1)
if url.startswith("postgres://"):
    url = url.replace("postgres://", "postgresql://", 1)
engine = create_engine(url, future=True, connect_args={"connect_timeout":5})
try:
    with engine.connect() as conn:
        r = conn.execute(text("SELECT 1")).scalar_one()
        print("OK, SELECT 1 ->", r)
except Exception as e:
    print("Connection failed:", e)
