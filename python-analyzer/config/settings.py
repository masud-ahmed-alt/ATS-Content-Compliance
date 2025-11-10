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

# ---------------- MINIO CONFIG ----------------
from datetime import timedelta
from minio import Minio
from xml.etree.ElementTree import Element, SubElement, tostring

# MinIO environment variables
MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT", "minio:7000")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET       = os.getenv("MINIO_BUCKET", "analyzer-html")
MINIO_USE_SSL      = os.getenv("MINIO_USE_SSL", "false").lower() in ("1", "true", "yes")
MINIO_EXPIRY_DAYS  = int(os.getenv("MINIO_EXPIRY_DAYS", "5"))

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_USE_SSL,
)

# Ensure bucket exists
try:
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        print(f"[minio:init] created bucket: {MINIO_BUCKET}", flush=True)
    else:
        print(f"[minio:init] using existing bucket: {MINIO_BUCKET}", flush=True)
except Exception as e:
    print(f"[minio:init:error] {e}", flush=True)

# Lifecycle policy for auto-deletion after N days
def ensure_minio_lifecycle(bucket: str, days: int):
    """Ensure bucket auto-deletes files older than N days."""
    try:
        rule = Element("LifecycleConfiguration")
        rule1 = SubElement(rule, "Rule")
        SubElement(rule1, "ID").text = "AutoDeleteHTML"
        SubElement(rule1, "Status").text = "Enabled"
        exp = SubElement(rule1, "Expiration")
        SubElement(exp, "Days").text = str(days)
        xml_config = tostring(rule, encoding="utf-8", method="xml")
        minio_client.set_bucket_lifecycle(bucket, xml_config)
        print(f"[minio:lifecycle] auto delete after {days} days", flush=True)
    except Exception as e:
        print(f"[minio:lifecycle:error] {e}", flush=True)

# Apply lifecycle rule
ensure_minio_lifecycle(MINIO_BUCKET, MINIO_EXPIRY_DAYS)
