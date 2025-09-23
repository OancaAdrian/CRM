from database import SessionLocal
from models import Firm

db = SessionLocal()
print("count:", db.query(Firm).count())
for f in db.query(Firm).limit(50):
    print(f.cui, "-", f.denumire)
db.close()
