# create_tables.py
from database import engine, Base
import models

if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    print("✅ Tabelele au fost create în baza de date.")
