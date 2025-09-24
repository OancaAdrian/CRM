# app/routers/search.py
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from typing import List

router = APIRouter()

@router.get("/search")
def search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=200)):
    # detect exact CUI numeric lookup
    if q.isdigit():
        stmt = text("""
            SELECT denumire AS name, cui, judet, cifra_de_afaceri_neta AS cifra_afaceri
            FROM public.firms
            WHERE cui = :q
            LIMIT :limit
        """)
        params = {"q": q, "limit": limit}
    else:
        stmt = text("""
            SELECT denumire AS name, cui, judet, cifra_de_afaceri_neta AS cifra_afaceri,
                   similarity(lower(denumire), lower(:q)) AS sim
            FROM public.firms
            WHERE lower(denumire) ILIKE '%' || lower(:q) || '%'
            ORDER BY sim DESC
            LIMIT :limit
        """)
        params = {"q": q, "limit": limit}

    # import engine from your app (adjust path as needed)
    from app.db import engine
    with engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()
    return rows
