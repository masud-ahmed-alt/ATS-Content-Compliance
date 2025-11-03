import os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from contextlib import contextmanager

# ---------------- ENV CONFIG ----------------
DATA_DIR = "/data"
UPI_MAP = os.path.join(DATA_DIR, "upi_map.json")
DOMAIN_STATS = os.path.join(DATA_DIR, "domain_stats.json")
PW_DOMAINS = os.path.join(DATA_DIR, "playwright_domains.txt")

# Keyword & external integrations
KEYWORDS_FILE = os.environ.get("KEYWORDS_FILE", "/app/keywords/keywords.yml")
OPENSEARCH_HOST = os.environ.get("OPENSEARCH_HOST", "http://localhost:9200")

# Matching / escalation thresholds
FUZZ_THRESHOLD = int(os.environ.get("FUZZ_THRESHOLD", "90"))
JS_ESCALATE_THRESHOLD = int(os.environ.get("JS_ESCALATE_THRESHOLD", "2"))

# Renderer endpoints
RENDERER_URL = os.environ.get("RENDERER_URL", "http://localhost:9000/render").rstrip("/")
RENDERER_SS = os.environ.get("RENDERER_SS", "http://localhost:9000/render-and-screenshot").rstrip("/")

# OCR / Image limits (used by libs/ocr_qr.py)
MAX_IMG_BYTES = int(os.environ.get("MAX_IMG_BYTES", "800000"))  # ~0.8MB per image cap
MAX_IMGS = int(os.environ.get("MAX_IMGS", "3"))                 # max images to fetch per page

# ---------------- POSTGRES CONFIG ----------------
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "admin")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "analyzerdb")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ---------------- SQLALCHEMY INIT ----------------
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
    future=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

@contextmanager
def db_session():
    """Context manager for raw DB usage."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db():
    """FastAPI dependency-style generator."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------- ENV INITIALIZATION ----------------
def init_environment():
    """Prepare runtime directories & support JSON data files."""
    os.makedirs(DATA_DIR, exist_ok=True)

    # JSON state files
    for p, init in [(UPI_MAP, {}), (DOMAIN_STATS, {})]:
        if not os.path.exists(p):
            with open(p, "w", encoding="utf-8") as f:
                json.dump(init, f)

    # Policy domains
    if not os.path.exists(PW_DOMAINS):
        with open(PW_DOMAINS, "w", encoding="utf-8") as f:
            f.write("")
