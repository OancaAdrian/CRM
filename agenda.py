from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Dict
from datetime import datetime, date
from database import get_db
from models import Activity

router = APIRouter(tags=["agenda"])

def row_to_activity_dict(a: Activity) -> Dict:
    return {
        "id": a.id,
        "cui": a.cui,
        "type": a.activity_type.name if a.activity_type else None,
        "comentariu": a.comment,
        "scor": a.score,
        "data": a.created_at.date().isoformat() if a.created_at else None
    }

@router.get("/agenda", response_model=List[Dict])
def get_agenda(cui: str, data: str, db: Session = Depends(get_db)):
    try:
        d = datetime.fromisoformat(data).date()
    except Exception:
        raise HTTPException(status_code=400, detail="data must be ISO date YYYY-MM-DD")

    start = datetime.combine(d, datetime.min.time())
    end = datetime.combine(d, datetime.max.time())

    rows = db.query(Activity).filter(
        Activity.cui == cui,
        Activity.created_at >= start,
        Activity.created_at <= end
    ).order_by(Activity.created_at.desc()).all()
    return [row_to_activity_dict(a) for a in rows]

@router.post("/agenda", response_model=Dict)
def post_agenda(payload: Dict, db: Session = Depends(get_db)):
    cui = payload.get("cui")
    data_str = payload.get("data")
    comentariu = payload.get("comentariu")
    scor = payload.get("scor")

    if not cui or not data_str:
        raise HTTPException(status_code=400, detail="cui and data required")

    try:
        dt = datetime.fromisoformat(data_str)
    except Exception:
        try:
            d = date.fromisoformat(data_str)
            dt = datetime.combine(d, datetime.min.time())
        except Exception:
            raise HTTPException(status_code=400, detail="data must be ISO date YYYY-MM-DD or datetime")

    a = Activity(cui=cui, comment=comentariu, score=int(scor) if scor is not None else None)
    a.created_at = dt
    db.add(a)
    db.commit()
    db.refresh(a)
    return {"status": "ok", "id": a.id}
