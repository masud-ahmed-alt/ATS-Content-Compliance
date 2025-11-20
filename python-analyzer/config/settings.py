"""
Settings module - backward compatibility wrapper.
This module maintains the old interface while using the new configuration system.
"""
import os
import json
import logging
from typing import Optional
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from contextlib import contextmanager

# Import new configuration system
from libs.common.config import (
    get_config, AppConfig,
    DatabaseConfig, RedisConfig, MinioConfig, OpenSearchConfig
)

logger = logging.getLogger(__name__)

# ✅ OPTIMIZATION: Import Redis for caching
try:
    import redis
    _HAS_REDIS = True
except ImportError:
    _HAS_REDIS = False

# ✅ OPTIMIZATION: Import OpenSearch
try:
    from opensearchpy import OpenSearch
    _HAS_OPENSEARCH = True
except ImportError:
    _HAS_OPENSEARCH = False

# Get global configuration
_config: Optional[AppConfig] = None

def _get_config() -> AppConfig:
    """Get or load configuration."""
    global _config
    if _config is None:
        _config = get_config()
    return _config

# ---------------- DATABASE INITIALIZATION ----------------
# Initialize database engine using new config system
_engine = None
_SessionLocal = None
Base = declarative_base()

def _init_database():
    """Initialize database engine."""
    global _engine, _SessionLocal
    
    if _engine is not None:
        return
    
    config = _get_config()
    db_config = config.database
    
    _engine = create_engine(
        db_config.url,
        pool_pre_ping=True,
        pool_size=db_config.pool_size,
        max_overflow=db_config.max_overflow,
        pool_recycle=db_config.pool_recycle,
        pool_timeout=db_config.pool_timeout,
        echo_pool=os.environ.get("ECHO_POOL", "False") == "True",
        connect_args={
            "connect_timeout": db_config.connect_timeout,
            "application_name": f"analyzer-{config.service.instance_id}",
        },
        future=True,
    )
    
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)

# Initialize on import
_init_database()

engine = _engine
SessionLocal = _SessionLocal

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

# ---------------- REDIS CLIENT INITIALIZATION ----------------
redis_client: Optional[redis.Redis] = None

def _init_redis():
    """Initialize Redis client."""
    global redis_client, _HAS_REDIS
    
    if not _HAS_REDIS:
        return
    
    try:
        config = _get_config()
        redis_config = config.redis
        
        redis_client = redis.from_url(
            redis_config.url,
            decode_responses=redis_config.decode_responses,
            socket_connect_timeout=redis_config.socket_connect_timeout,
            socket_keepalive=redis_config.socket_keepalive,
            health_check_interval=redis_config.health_check_interval,
        )
        redis_client.ping()
        _HAS_REDIS = True
        logger.info(f"✅ Redis connected: {redis_config.url}")
    except Exception as e:
        logger.warning(f"⚠️ Redis unavailable: {e}. Running without caching.")
        _HAS_REDIS = False
        redis_client = None
    except NameError:
        # redis module not available
        logger.warning("⚠️ Redis module not installed. Install with: pip install redis")
        _HAS_REDIS = False
        redis_client = None

_init_redis()

# ---------------- OPENSEARCH CLIENT INITIALIZATION ----------------
opensearch_client = None

def _init_opensearch():
    """Initialize OpenSearch client."""
    global opensearch_client, _HAS_OPENSEARCH
    
    if not _HAS_OPENSEARCH:
        return
    
    try:
        config = _get_config()
        opensearch_config = config.opensearch
        
        opensearch_client = OpenSearch(
            hosts=[opensearch_config.host],
            use_ssl=opensearch_config.use_ssl,
            verify_certs=opensearch_config.verify_certs,
            ssl_show_warn=opensearch_config.ssl_show_warn,
            timeout=opensearch_config.timeout,
        )
        opensearch_client.cluster.health()
        _HAS_OPENSEARCH = True
        logger.info(f"✅ OpenSearch connected: {opensearch_config.host}")
    except Exception as e:
        logger.warning(f"⚠️ OpenSearch unavailable: {e}")
        _HAS_OPENSEARCH = False
        opensearch_client = None
    except NameError:
        # OpenSearch module not available
        logger.warning("⚠️ OpenSearch module not installed. Install with: pip install opensearch-py")
        _HAS_OPENSEARCH = False
        opensearch_client = None

_init_opensearch()

# ---------------- MINIO CLIENT INITIALIZATION ----------------
from libs.storage.minio_client import MinioStorageClient

minio_client: Optional[MinioStorageClient] = None

def _init_minio():
    """Initialize MinIO client."""
    global minio_client
    
    try:
        config = _get_config()
        minio_config = config.minio
        
        minio_client = MinioStorageClient(minio_config)
        logger.info(f"✅ MinIO connected: {minio_config.endpoint}/{minio_config.bucket}")
    except Exception as e:
        logger.warning(f"⚠️ MinIO unavailable: {e}")
        minio_client = None

_init_minio()


# ---------------- CACHE MANAGER ----------------
class CacheManager:
    """Redis-based caching layer with graceful degradation."""
    
    DEFAULT_TTL = 300  # 5 minutes
    
    def __init__(self, ttl_seconds: int = DEFAULT_TTL):
        self.ttl = ttl_seconds
        self.enabled = redis_client is not None
    
    def get(self, key: str):
        """Retrieve from cache. Returns None if miss or error."""
        if not self.enabled:
            return None
        try:
            data = redis_client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.warning(f"Cache GET error: {e}")
            return None
    
    def set(self, key: str, value: dict, ttl: int = None):
        """Store in cache with TTL."""
        if not self.enabled:
            return False
        try:
            redis_client.setex(
                key,
                ttl or self.ttl,
                json.dumps(value, default=str)
            )
            return True
        except Exception as e:
            logger.warning(f"Cache SET error: {e}")
            return False
    
    def delete(self, key: str):
        """Remove from cache."""
        if not self.enabled:
            return False
        try:
            redis_client.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Cache DELETE error: {e}")
            return False
    
    def clear_pattern(self, pattern: str):
        """Clear all keys matching pattern."""
        if not self.enabled:
            return 0
        try:
            keys = redis_client.keys(pattern)
            if keys:
                return redis_client.delete(*keys)
            return 0
        except Exception as e:
            logger.warning(f"Cache CLEAR error: {e}")
            return 0


# Global cache instance
config = _get_config()
cache = CacheManager(ttl_seconds=config.cache_ttl)

# ---------------- BACKWARD COMPATIBILITY: Export config values ----------------
# These are computed properties that forward to the new config system
def _get_config_value(name: str):
    """Get configuration value by name."""
    config = _get_config()
    
    mapping = {
        'DATABASE_URL': lambda: config.database.url,
        'REDIS_URL': lambda: config.redis.url,
        'MINIO_ENDPOINT': lambda: config.minio.endpoint,
        'MINIO_BUCKET': lambda: config.minio.bucket,
        'MINIO_ACCESS_KEY': lambda: config.minio.access_key,
        'MINIO_SECRET_KEY': lambda: config.minio.secret_key,
        'MINIO_USE_SSL': lambda: config.minio.use_ssl,
        'MINIO_EXPIRY_DAYS': lambda: config.minio.expiry_days,
        'OPENSEARCH_HOST': lambda: config.opensearch.host,
        'RENDERER_URL': lambda: config.renderer.url,
        'RENDERER_SS': lambda: config.renderer.screenshot_endpoint,
        'KEYWORDS_FILE': lambda: config.service.keywords_file,
        'DATA_DIR': lambda: config.service.data_dir,
        'FUZZ_THRESHOLD': lambda: config.fuzz_threshold,
        'JS_ESCALATE_THRESHOLD': lambda: config.js_escalate_threshold,
        'MAX_IMG_BYTES': lambda: config.max_img_bytes,
        'MAX_IMGS': lambda: config.max_imgs,
        'ENABLE_SPACY_VALIDATION': lambda: config.enable_spacy_validation,
        'USE_SPACY': lambda: config.use_spacy,
        'SPACY_MODEL_NAME': lambda: config.spacy_model_name,
        'SPACY_THRESHOLD': lambda: config.spacy_threshold,
        'CACHE_TTL': lambda: config.cache_ttl,
        'POSTGRES_USER': lambda: config.database.user,
        'POSTGRES_PASSWORD': lambda: config.database.password,
        'POSTGRES_DB': lambda: config.database.database,
        'POSTGRES_HOST': lambda: config.database.host,
        'POSTGRES_PORT': lambda: config.database.port,
    }
    
    if name in mapping:
        return mapping[name]()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


# Create module-level constants for backward compatibility
# Using __getattr__ for dynamic attribute access
def __getattr__(name: str):
    """Dynamic attribute access for backward compatibility."""
    try:
        return _get_config_value(name)
    except (KeyError, AttributeError):
        # Also support legacy path variables
        config = _get_config()
        data_dir = config.service.data_dir
        
        path_mapping = {
            'UPI_MAP': os.path.join(data_dir, "upi_map.json"),
            'DOMAIN_STATS': os.path.join(data_dir, "domain_stats.json"),
            'PW_DOMAINS': os.path.join(data_dir, "playwright_domains.txt"),
        }
        
        if name in path_mapping:
            return path_mapping[name]
        
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

# ---------------- ENV INITIALIZATION ----------------
def init_environment():
    """Prepare runtime directories & support JSON data files."""
    config = _get_config()
    data_dir = config.service.data_dir
    
    os.makedirs(data_dir, exist_ok=True)

    # JSON state files
    upi_map = os.path.join(data_dir, "upi_map.json")
    domain_stats = os.path.join(data_dir, "domain_stats.json")
    pw_domains = os.path.join(data_dir, "playwright_domains.txt")
    
    for p, init in [(upi_map, {}), (domain_stats, {})]:
        if not os.path.exists(p):
            with open(p, "w", encoding="utf-8") as f:
                json.dump(init, f)

    # Policy domains
    if not os.path.exists(pw_domains):
        with open(pw_domains, "w", encoding="utf-8") as f:
            f.write("")


# ============================================================================
# ✅ OPTIMIZATION: Health Check Functions
# ============================================================================

def check_database_health():
    """Quick database connectivity check."""
    try:
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": "error", "error": str(e)}


def check_cache_health():
    """Quick cache connectivity check."""
    if not cache.enabled:
        return {"status": "ok", "cache": "disabled"}
    try:
        redis_client.ping()
        info = redis_client.info("stats")
        return {
            "status": "ok",
            "cache": "connected",
            "memory_mb": round(info.get("used_memory", 0) / 1024 / 1024, 2),
        }
    except Exception as e:
        return {"status": "error", "cache": "error", "error": str(e)}


def check_opensearch_health():
    """Quick OpenSearch connectivity check."""
    if opensearch_client is None:
        return {"status": "ok", "opensearch": "disabled"}
    try:
        health = opensearch_client.cluster.health()
        return {
            "status": "ok",
            "opensearch": "connected",
            "cluster_status": health.get("status", "unknown"),
        }
    except Exception as e:
        return {"status": "error", "opensearch": "error", "error": str(e)}
