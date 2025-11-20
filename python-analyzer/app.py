#!/usr/bin/env python3
"""
FastAPI app for the Python analyzer service.
"""
# Load environment variables from .env file (for standalone mode)
try:
    from dotenv import load_dotenv
    import os
    # Try to load .env from project root or python-analyzer directory
    env_paths = [
        os.path.join(os.path.dirname(__file__), "..", ".env"),  # Project root
        os.path.join(os.path.dirname(__file__), ".env"),  # python-analyzer directory
    ]
    for env_path in env_paths:
        if os.path.exists(env_path):
            load_dotenv(env_path)
            print(f"[env:loaded] {env_path}", flush=True)
            break
except ImportError:
    pass  # python-dotenv not installed, use environment variables only

import time
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware  

# DB + environment
from config.settings import init_environment, Base, engine
from models.database_init import init_database, seed_taxonomy
from utils.progress_manager import get_progress_manager

# Routers
from routes.ingest_routes import router as ingest_router
from routes.report_routes import router as report_router
from routes.progress_routes import router as progress_router

# -------------------------------------------------------------------------
# Init environment & FastAPI app
# -------------------------------------------------------------------------
init_environment()
try:
    init_database()
    seed_taxonomy()
    logging.info("Database schema ensured and taxonomy seeded")
except Exception as exc:
    logging.exception("[startup] database bootstrap failed: %s", exc)

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown."""
    # Startup
    logging.info("[startup] Python Analyzer starting up...")
    # Don't block startup - let the background thread handle spaCy model loading
    # The server should start accepting requests immediately
    logging.info("[startup] Python Analyzer ready to accept requests (spaCy model loading in background)")
    yield
    # Shutdown (if needed)
    logging.info("[shutdown] Python Analyzer shutting down...")

app = FastAPI(title="python-analyzer", lifespan=lifespan)

# Allow cors for dev & local testing
origins = ["*"]
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
app.include_router(progress_router)


# -------------------- Health Check --------------------
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}


@app.get("/health/db")
def health_db():
    """Check if database is accessible and responsive"""
    try:
        from models.database_models import Hit
        from config.settings import SessionLocal
        db = SessionLocal()
        try:
            # Test database connection with a simple query
            result = db.query(Hit).first()
            db.close()
            return {
                "db_ok": True,
                "example_hit_present": result is not None
            }
        except Exception as e:
            logging.exception("[health_db] error querying db: %s", e)
            return {"db_ok": False, "error": str(e)}
    except Exception as e:
        logging.exception("[health_db] unexpected error: %s", e)
        return {"db_ok": False, "error": str(e)}


@app.get("/health/spacy")
def health_spacy():
    """Diagnostic endpoint to check spaCy model status"""
    try:
        from core.core_analyzer import get_spacy_status
        return get_spacy_status()
    except Exception as e:
        logging.exception("[health_spacy] error: %s", e)
        return {"error": str(e)}


@app.get("/health/metrics")
def health_metrics():
    """Get system metrics and monitoring data"""
    try:
        from core.core_analyzer import export_metrics, hit_queue, _screenshot_queue
        from libs.dlq import dlq
        metrics = export_metrics()
        dlq_stats = dlq.stats()
        
        # Get queue sizes safely
        hit_qsize = 0
        screenshot_qsize = 0
        try:
            hit_qsize = hit_queue.qsize()
        except:
            pass
        try:
            if _screenshot_queue:
                screenshot_qsize = _screenshot_queue.qsize()
        except:
            pass
        
        return {
            "metrics": metrics,
            "dlq": dlq_stats,
            "queues": {
                "hit_queue_size": hit_qsize,
                "screenshot_queue_size": screenshot_qsize,
            }
        }
    except Exception as e:
        logging.exception(f"[health_metrics] error: {e}")
        return {"error": str(e)}


@app.get("/health/dlq")
def health_dlq():
    """Get Dead Letter Queue status and stats"""
    try:
        from libs.dlq import dlq
        return dlq.stats()
    except Exception as e:
        logging.exception(f"[health_dlq] error: {e}")
        return {"error": str(e)}


# -------------------- App startup tasks --------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)

    # Log all active routes on startup
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if path and methods:
            logging.info(f"  {sorted(methods)} {path}")
    
    logging.info("Analyzer started: database initialized, progress tracking enabled")
