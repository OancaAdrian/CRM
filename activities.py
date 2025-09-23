from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict
import csv, io, datetime
from database import get_db
from models import Activity, ActivityType

router = APIRouter(prefix="/activitati", tags=["activitati"])

def get_or_create_activity_type(db: Session, name: Optional[str]) -> Optional[ActivityType]:
    if not name:
        return None
    name = name.strip()
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

@router.get("/firma/{cui}", response_model=List[Dict])
def list_activities_for_firm(cui: str, limit: int = Query(50, ge=1, le=1000), db: Session = Depends(get_db)):
    qs = db.query(Activity).filter(Activity.cui == cui).order_by(Activity.created_at.desc()).limit(limit).all()
    return [
        {
            "id": a.id,
            "cui": a.cui,
            "type": a.activity_type.name if a.activity_type else None,
            "comment": a.comment,
            "score": a.score,
            "created_at": a.created_at.isoformat() if a.created_at else None
        }
        for a in qs
    ]

@router.post("/firma/{cui}/create", response_model=Dict)
def create_activity_for_firm(
    cui: str,
    type_name: Optional[str] = Form(None),
    comment: Optional[str] = Form(None),
    score: Optional[int] = Form(None),
    db: Session = Depends(get_db),
):
    at = get_or_create_activity_type(db, type_name) if type_name else None
    a = Activity(cui=cui, activity_type_id=at.id if at else None, comment=comment, score=score)
    db.add(a)
    db.commit()
    db.refresh(a)
    return {
        "id": a.id,
        "cui": a.cui,
        "type": at.name if at else None,
        "comment": a.comment,
        "score": a.score,
        "created_at": a.created_at.isoformat() if a.created_at else None
    }

@router.post("/firma/{cui}/import_csv", response_model=Dict)
async def import_activities_csv(
    cui: str,
    file: UploadFile = File(...),
    delimiter: Optional[str] = Form(","),
    date_format: Optional[str] = Form("%Y-%m-%d"),
    type_column: Optional[str] = Form("type"),
    comment_column: Optional[str] = Form("comment"),
    score_column: Optional[str] = Form("score"),
    date_column: Optional[str] = Form("date"),
    db: Session = Depends(get_db),
):
    content = await file.read()
    try:
        text_io = io.StringIO(content.decode('utf-8-sig'))
    except UnicodeDecodeError:
        text_io = io.StringIO(content.decode('latin-1'))
    reader = csv.DictReader(text_io, delimiter=delimiter)
    created = 0
    errors = []
    for i, row in enumerate(reader, start=1):
        try:
            type_val = row.get(type_column) or row.get("activity_type") or row.get("tip") or None
            comment_val = row.get(comment_column) or row.get("comment") or row.get("descriere") or None
            score_val = row.get(score_column) or row.get("score") or None
            date_val = row.get(date_column) or row.get("date") or None

            at = get_or_create_activity_type(db, type_val) if type_val else None

            score_int = None
            if score_val:
                try:
                    score_int = int(''.join(ch for ch in str(score_val) if ch.isdigit()))
                except Exception:
                    score_int = None

            created_at = None
            if date_val:
                for fmt in (date_format, "%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d"):
                    try:
                        created_at = datetime.datetime.strptime(str(date_val).strip(), fmt)
                        break
                    except Exception:
                        continue

            a = Activity(
                cui=cui,
                activity_type_id=at.id if at else None,
                comment=comment_val,
                score=score_int
            )
            if created_at:
                a.created_at = created_at
            db.add(a)
            created += 1
        except Exception as ex:
            errors.append({"row": i, "error": str(ex), "row_data": row})
    try:
        db.commit()
    except Exception as ex:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB commit error: {ex}")
    return {"created": created, "errors": errors}
