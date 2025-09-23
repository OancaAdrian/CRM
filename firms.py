from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from typing import List
from sqlalchemy.orm import Session
from database import get_db

router = APIRouter()

@router.get("/firme")
def cauta_firma(q: str = Query(...), limit: int = Query(50, ge=1, le=200), db: Session = Depends(get_db)):
    q_clean = q.strip().upper()
    params = {"q": q_clean, "ro_q": "RO" + q_clean, "like": f"%{q_clean}%", "limit": limit}

    sql = text("""
        SELECT
            f.cui,
            f.denumire,
            f.judet         AS adr_judet,
            f.localitate    AS adr_localitate,
            f.adr_den_strada,
            f.adr_nr_strada,
            f.telefon,
            fa.an           AS financial_an,
            fa.cifra_afaceri AS cifra_afaceri,
            fa.profitul_net AS profitul_net
        FROM public.firms f
        LEFT JOIN (
            SELECT DISTINCT ON (cui) cui, an, cifra_afaceri, profitul_net
            FROM public.financials_annual
            ORDER BY cui, an DESC
        ) fa ON fa.cui = f.cui
        WHERE
            trim(upper(replace(f.cui, ' ', ''))) = :q
            OR trim(upper(replace(f.cui, ' ', ''))) = :ro_q
            OR trim(upper(f.denumire)) ILIKE :like
        ORDER BY f.denumire
        LIMIT :limit
    """)
    try:
        rows = db.execute(sql, params).fetchall()
        result = []
        for r in rows:
            row = dict(r._mapping)
            # convert numeric-like fields if present
            for k in ("financial_an", "cifra_afaceri", "profitul_net"):
                if row.get(k) is not None:
                    try:
                        row[k] = int(row[k])
                    except Exception:
                        pass
            result.append(row)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
