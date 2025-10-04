# main.py — FastAPI minimal, compatible, and safe rewrite
# Save as main.py, replace placeholder DB functions with real logic, then redeploy.

import os
import logging
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, Query, Header, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger("uvicorn.error")
app = FastAPI(title="CRM API - Compatibility Layer")

# Simple CORS so browsers can call the API during testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Configuration
APP_PASSWORD = os.environ.get("APP_PASSWORD", "5864")  # optional server-side check

# --- Middleware: request/response logging
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"INCOMING {request.method} {request.url.path}?{request.url.query}")
    try:
        response = await call_next(request)
    finally:
        logger.info(f"HANDLED {request.method} {request.url.path}")
    return response

# --- Log registered routes at startup (diagnostic)
@app.on_event("startup")
async def _log_routes():
    try:
        routes = sorted([r.path for r in app.routes])
        logger.info("REGISTERED ROUTES: %s", ", ".join(routes))
    except Exception as e:
        logger.exception("Failed to list routes: %s", e)

# --- Health
@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})

# --- Root: serve a minimal HTML (keeps SPA fallback behavior safe)
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse("<html><body><h1>CRM API</h1></body></html>")

# --- Helper: placeholder DB/query functions (replace with real implementation)
def query_agenda_from_db(day: Optional[str]) -> Dict[str, Any]:
    # TODO: Replace with real DB logic. Returning empty structure as fallback.
    return {"scheduled": [], "overdue": [], "nearby": [], "day": day}

def query_search_from_db(q: str, limit: int = 10) -> List[Dict[str, Any]]:
    # TODO: Replace with real search logic. Return empty list as fallback.
    return []

def query_firm_from_db(cui: str) -> Optional[Dict[str, Any]]:
    # TODO: Replace with real lookup. Return None if not found.
    return {"id": cui, "cui": cui, "name": f"Firmă {cui}", "judet": "", "licente": 0}

# --- Legacy redirects / compatibility endpoints
@app.get("/search")
async def legacy_search_redirect(q: str = "", limit: int = 10):
    # Keep compatibility for old frontends that call /search
    return RedirectResponse(url=f"/api/search?q={q}&limit={limit}", status_code=307)

@app.get("/api/agenda2/day/{day}")
async def legacy_agenda2(day: str):
    # Redirect old agenda2 -> canonical /api/agenda?day=
    return RedirectResponse(url=f"/api/agenda?day={day}", status_code=307)

# --- API: search (compatible stub)
@app.get("/api/search")
async def api_search(q: str = Query(...), limit: int = Query(10)):
    results = query_search_from_db(q, limit)
    return JSONResponse(status_code=200, content=results)

# --- API: agenda (returns 200 + empty arrays when no data)
@app.get("/api/agenda")
async def api_agenda(
    day: Optional[str] = Query(None),
    x_app_password: Optional[str] = Header(None),
):
    # If you protect API with an app password, optionally enforce it here
    if APP_PASSWORD and x_app_password and APP_PASSWORD != x_app_password:
        raise HTTPException(status_code=401, detail="Unauthorized")

    data = query_agenda_from_db(day)
    if data is None:
        data = {"scheduled": [], "overdue": [], "nearby": [], "day": day}
    return JSONResponse(status_code=200, content=data)

# --- API: firm detail (compatibility)
@app.get("/api/firms/{cui}")
async def api_firm(cui: str, x_app_password: Optional[str] = Header(None)):
    # optional password check
    if APP_PASSWORD and x_app_password and APP_PASSWORD != x_app_password:
        raise HTTPException(status_code=401, detail="Unauthorized")

    firm = query_firm_from_db(cui)
    if not firm:
        raise HTTPException(status_code=404, detail="Not Found")
    return JSONResponse(status_code=200, content=firm)

# --- Optional: catch-all for API routes under /api that are not found
@app.get("/api/{full_path:path}")
async def api_not_found(full_path: str):
    return JSONResponse(status_code=404, content={"detail": "Not Found"})

# --- Notes
# - Replace placeholder query_* functions with real DB access.
# - Ensure your start command on Render points to this module (example):
#     gunicorn -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:$PORT
#   or:
#     uvicorn main:app --host 0.0.0.0 --port $PORT
# - Keep logging enabled while you test to correlate incoming requests with platform logs.
