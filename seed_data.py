# seed_data.py
from database import SessionLocal
from models import Firm, Financial, ActivityType

def seed():
    db = SessionLocal()
    try:
        # Firme de test
        f1 = Firm(cui="RO123456", denumire="Arabesque SRL", adr_judet="GALATI", adr_localitate="Galati", numar_licente=5)
        f2 = Firm(cui="RO654321", denumire="Construct Galati SA", adr_judet="GALATI", adr_localitate="Galati", numar_licente=2)

        # Date financiare test
        fin1 = Financial = None
        try:
            from models import Financial
            fin1 = Financial(cui="RO123456", an=2024, cifra_afaceri=10000000, profitul_net=1200000)
            fin2 = Financial(cui="RO654321", an=2024, cifra_afaceri=5000000, profitul_net=300000)
        except Exception:
            fin1 = None
            fin2 = None

        # Activity types
        t1 = ActivityType(name="contact")
        t2 = ActivityType(name="oferta")

        # insert if not exists
        existing = {r.cui for r in db.query(Firm.cui).all()}
        to_add = []
        if "RO123456" not in existing:
            to_add.append(f1)
        if "RO654321" not in existing:
            to_add.append(f2)

        # ActivityType dedupe
        existing_types = {r.name for r in db.query(ActivityType).all()}
        if "contact" not in existing_types:
            db.add(t1)
        if "oferta" not in existing_types:
            db.add(t2)

        if fin1 and fin2:
            db.add_all([fin1, fin2])

        if to_add:
            db.add_all(to_add)

        db.commit()
        print("✅ Datele de test au fost inserate (dacă nu existau).")
    except Exception as e:
        db.rollback()
        print("❌ Eroare la seed:", e)
    finally:
        db.close()

if __name__ == "__main__":
    seed()
