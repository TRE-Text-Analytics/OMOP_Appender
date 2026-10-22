"""
OMOP Appender - Backend

This file owns only three things:
  1. Creating the FastAPI application instance
  2. Registering feature routers
  3. Mounting static files

All feature logic lives under features/<feature_name>/.
To add a new feature, create a features/<name>/router.py and
add an app.include_router() call below.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from features.omop_appender.router import router as omop_appender_router

app = FastAPI(title="OMOP Appender", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Feature routers
# ---------------------------------------------------------------------------
# Each feature registers its own routes via an APIRouter.
# Add new features here as the project grows.

app.include_router(omop_appender_router)
# ---------------------------------------------------------------------------
# Static files (frontend SPA)
# ---------------------------------------------------------------------------
# Uncomment the dockerised path when running in a container.
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
