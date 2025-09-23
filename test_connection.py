from sqlalchemy import create_engine, text
import os, sys

url = os.getenv("DATABASE_URL")
if not url:
    print("DATABASE_URL not set")
    sys.exit(1)

try:
    engine = create_engine(url)
    with engine.connect() as conn:
        cur = conn.execute(text("SELECT current_user;"))
        user = cur.fetchone()[0]
        print("connected as:", user)
except Exception as e:
    print("connection error:", e)
    sys.exit(2)
