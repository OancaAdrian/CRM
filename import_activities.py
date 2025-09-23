# import_activities.py
import csv, sys, datetime
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Activity, ActivityType

def get_or_create_activity_type(db: Session, name: str):
    if not name:
        return None
    obj = db.query(ActivityType).filter(ActivityType.name == name).first()
    if obj:
        return obj
    obj = ActivityType(name=name)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def import_csv_for_cui(path_csv, cui, delimiter=',', date_format="%Y-%m-%d", type_column="type", comment_column="comment", score_column="score", date_column="date"):
    db = SessionLocal()
    created = 0
    errors = []
    with open(path_csv, newline='', encoding='utf-8-sig') as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        for i, row in enumerate(reader, start=1):
            try:
                type_val = row.get(type_column) or row.get("activity_type") or None
                comment_val = row.get(comment_column) or row.get("comment") or None
                score_val = row.get(score_column) or None
                date_val = row.get(date_column) or None

                at = get_or_create_activity_type(db, type_val) if type_val else None

                score_int = None
                if score_val:
                    try:
                        score_int = int(''.join(ch for ch in str(score_val) if ch.isdigit()))
                    except:
                        score_int = None

                created_at = None
                if date_val:
                    for fmt in (date_format, "%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"):
                        try:
                            created_at = datetime.datetime.strptime(str(date_val).strip(), fmt)
                            break
                        except:
                            continue

                a = Activity(cui=cui, activity_type_id=at.id if at else None, comment=comment_val, score=score_int)
                if created_at:
                    a.created_at = created_at
                db.add(a)
                created += 1
            except Exception as ex:
                errors.append((i, str(ex), row))
        try:
            db.commit()
        except Exception as ex:
            db.rollback()
            print("Commit error:", ex)
            return
    print("Created:", created, "Errors:", len(errors))
    for e in errors[:10]:
        print(e)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python import_activities.py path.csv CUI")
        sys.exit(1)
    import_csv_for_cui(sys.argv[1], sys.argv[2])
