#!/usr/bin/env python3
import time
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from core.core_analyzer import load_semantic_model  

# DB + environment
from config.settings import init_environment, Base, engine

# Routers
from routes.ingest_routes import router as ingest_router
from routes.report_routes import router as report_router

# Initialize folders, environment, and DB
init_environment()
load_semantic_model("/app/semantic_model")
# -------------------- FastAPI App --------------------
app = FastAPI(
    title="Analyzer with Screenshot Integration",
    version="0.6",
)

# -------------------- CORS Configuration --------------------
# Allow frontend (React) to connect — adjust origins for production
origins = [
    "http://localhost:5173",  # Vite dev server
    "http://127.0.0.1:5173",
    "http://localhost:3000",  # Common React dev port
    "http://127.0.0.1:3000",
    "*"  # You can replace "*" with specific domain for production
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allow GET, POST, PUT, DELETE, etc.
    allow_headers=["*"],
)

# -------------------- Include Routers --------------------
app.include_router(ingest_router)
app.include_router(report_router)

# -------------------- Health Check --------------------
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}

# -------------------- Startup Events --------------------
@app.on_event("startup")
def on_startup():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)

    # Log all active routes on startup
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if path and methods:
            logging.info(f"  {sorted(methods)} {path}")
