from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from firms import router as firms_router
from activities import router as activities_router
from agenda import router as agenda_router
from models import Base
from database import engine

app = FastAPI(title="CRM Site API")

# Enable CORS for development; restrict origins in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(firms_router)
app.include_router(activities_router)
app.include_router(agenda_router)

# Optional: create tables in dev (uncomment if you want SQLAlchemy to create missing tables locally)
# Base.metadata.create_all(bind=engine)
