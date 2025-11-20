"""
Centralized configuration management with validation.
"""
import os
import logging
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration with validation."""
    host: str = "postgres"
    port: int = 5432
    user: str = "postgres"
    password: str = "admin"
    database: str = "analyzerdb"
    pool_size: Optional[int] = None
    max_overflow: Optional[int] = None
    pool_timeout: int = 30
    pool_recycle: int = 1800
    connect_timeout: int = 10
    
    @property
    def url(self) -> str:
        """Build database URL."""
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Load from environment variables."""
        from .resource_pools import calculate_pool_size
        
        pool_size, max_overflow = calculate_pool_size(
            multiplier=15, max_size=120,
            overflow_multiplier=10, max_overflow=80
        )
        
        return cls(
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", "admin"),
            database=os.environ.get("POSTGRES_DB", "analyzerdb"),
            pool_size=int(os.environ.get("DB_POOL_SIZE", str(pool_size))),
            max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", str(max_overflow))),
            pool_timeout=int(os.environ.get("DB_POOL_TIMEOUT", "30")),
            pool_recycle=int(os.environ.get("DB_POOL_RECYCLE", "1800")),
            connect_timeout=int(os.environ.get("DB_CONNECT_TIMEOUT", "10")),
        )


@dataclass
class RedisConfig:
    """Redis configuration with validation."""
    url: str = "redis://redis:6379/0"
    socket_connect_timeout: int = 5
    socket_keepalive: bool = True
    health_check_interval: int = 30
    decode_responses: bool = True
    
    @classmethod
    def from_env(cls) -> 'RedisConfig':
        """Load from environment variables."""
        return cls(
            url=os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            socket_connect_timeout=int(os.environ.get("REDIS_CONNECT_TIMEOUT", "5")),
            socket_keepalive=os.environ.get("REDIS_KEEPALIVE", "true").lower() in ("1", "true", "yes"),
            health_check_interval=int(os.environ.get("REDIS_HEALTH_CHECK_INTERVAL", "30")),
            decode_responses=True,
        )


@dataclass
class MinioConfig:
    """MinIO configuration with validation."""
    endpoint: str = "minio:7000"
    access_key: str = "admin"
    secret_key: str = "minioadmin"
    bucket: str = "analyzer-html"
    use_ssl: bool = False
    expiry_days: int = 5
    
    @property
    def secure(self) -> bool:
        """Determine if SSL should be used."""
        return self.use_ssl or self.endpoint.startswith("https")
    
    @classmethod
    def from_env(cls, prefix: str = "MINIO") -> 'MinioConfig':
        """Load from environment variables with optional prefix."""
        return cls(
            endpoint=os.environ.get(f"{prefix}_ENDPOINT", "minio:7000"),
            access_key=os.environ.get(f"{prefix}_ACCESS_KEY", "admin"),
            secret_key=os.environ.get(f"{prefix}_SECRET_KEY", "minioadmin"),
            bucket=os.environ.get(f"{prefix}_BUCKET", "analyzer-html"),
            use_ssl=os.environ.get(f"{prefix}_USE_SSL", "false").lower() in ("1", "true", "yes"),
            expiry_days=int(os.environ.get(f"{prefix}_EXPIRY_DAYS", "5")),
        )


@dataclass
class OpenSearchConfig:
    """OpenSearch configuration with validation."""
    host: str = "http://localhost:9200"
    verify_certs: bool = False
    ssl_show_warn: bool = False
    timeout: int = 10
    use_ssl: bool = False
    
    @classmethod
    def from_env(cls) -> 'OpenSearchConfig':
        """Load from environment variables."""
        return cls(
            host=os.environ.get("OPENSEARCH_HOST", "http://localhost:9200"),
            verify_certs=os.environ.get("OPENSEARCH_VERIFY_CERTS", "false").lower() in ("1", "true", "yes"),
            ssl_show_warn=os.environ.get("OPENSEARCH_SSL_SHOW_WARN", "false").lower() in ("1", "true", "yes"),
            timeout=int(os.environ.get("OPENSEARCH_TIMEOUT", "10")),
            use_ssl=os.environ.get("OPENSEARCH_USE_SSL", "false").lower() in ("1", "true", "yes"),
        )


@dataclass
class RendererConfig:
    """Renderer service configuration."""
    url: str = "http://localhost:9000"
    timeout: int = 60
    concurrency: int = 4
    goto_timeout: int = 120000
    wait_until: str = "load"
    screenshot_endpoint: str = "http://localhost:9000/render-and-screenshot"
    
    @classmethod
    def from_env(cls) -> 'RendererConfig':
        """Load from environment variables."""
        from .resource_pools import calculate_worker_count
        
        default_concurrency = calculate_worker_count(multiplier=2, max_workers=8, min_workers=2)
        base_url = os.environ.get("RENDERER_URL", "http://localhost:9000").rstrip("/")
        renderer_ss = os.environ.get("RENDERER_SS", f"{base_url}/render-and-screenshot")
        
        return cls(
            url=base_url,
            timeout=int(os.environ.get("RENDERER_TIMEOUT", "60")),
            concurrency=int(os.environ.get("RENDERER_CONCURRENCY", str(default_concurrency))),
            goto_timeout=int(os.environ.get("RENDERER_GOTO_TIMEOUT", "120000")),
            wait_until=os.environ.get("RENDERER_WAIT_UNTIL", "load"),
            screenshot_endpoint=renderer_ss,
        )


@dataclass
class ServiceConfig:
    """Service-level configuration."""
    data_dir: str = "/data"
    keywords_file: str = "/app/keywords/keywords.yml"
    instance_id: str = "1"
    
    @classmethod
    def from_env(cls) -> 'ServiceConfig':
        """Load from environment variables."""
        data_dir = os.environ.get("DATA_DIR", "/data")
        
        # Support both absolute and relative paths
        if not os.path.isabs(data_dir):
            base_dir = Path(__file__).parent.parent.parent
            data_dir = str(base_dir / data_dir)
        
        # Find keywords file
        default_keywords = "/app/keywords/keywords.yml"
        if not os.path.exists(default_keywords):
            base_dir = Path(__file__).parent.parent.parent.parent
            default_keywords = str(base_dir / "keywords" / "keywords.yml")
            if not os.path.exists(default_keywords):
                default_keywords = str(base_dir / "keywords" / "enhanced_keywords.yml")
        
        return cls(
            data_dir=data_dir,
            keywords_file=os.environ.get("KEYWORDS_FILE", default_keywords),
            instance_id=os.environ.get("INSTANCE_ID", "1"),
        )


@dataclass
class AppConfig:
    """Main application configuration aggregating all sub-configs."""
    database: DatabaseConfig = field(default_factory=DatabaseConfig.from_env)
    redis: RedisConfig = field(default_factory=RedisConfig.from_env)
    minio: MinioConfig = field(default_factory=MinioConfig.from_env)
    opensearch: OpenSearchConfig = field(default_factory=OpenSearchConfig.from_env)
    renderer: RendererConfig = field(default_factory=RendererConfig.from_env)
    service: ServiceConfig = field(default_factory=ServiceConfig.from_env)
    
    # Matching thresholds
    fuzz_threshold: int = 90
    js_escalate_threshold: int = 2
    
    # OCR/Image limits
    max_img_bytes: int = 800000
    max_imgs: int = 3
    
    # spaCy NLP settings
    enable_spacy_validation: bool = False
    use_spacy: bool = True
    spacy_model_name: str = "en_core_web_sm"
    spacy_threshold: float = 0.60
    autoload_spacy: bool = True
    
    # Cache settings
    cache_ttl: int = 300
    
    @classmethod
    def from_env(cls) -> 'AppConfig':
        """Load complete configuration from environment."""
        config = cls()
        
        # Load sub-configs
        config.database = DatabaseConfig.from_env()
        config.redis = RedisConfig.from_env()
        config.minio = MinioConfig.from_env()
        config.opensearch = OpenSearchConfig.from_env()
        config.renderer = RendererConfig.from_env()
        config.service = ServiceConfig.from_env()
        
        # Load other settings
        config.fuzz_threshold = int(os.environ.get("FUZZ_THRESHOLD", "90"))
        config.js_escalate_threshold = int(os.environ.get("JS_ESCALATE_THRESHOLD", "2"))
        config.max_img_bytes = int(os.environ.get("MAX_IMG_BYTES", "800000"))
        config.max_imgs = int(os.environ.get("MAX_IMGS", "3"))
        
        config.enable_spacy_validation = os.environ.get("ENABLE_SPACY_VALIDATION", "false").lower() in ("1", "true", "yes")
        config.use_spacy = os.environ.get("USE_SPACY", "true").lower() in ("1", "true", "yes")
        config.spacy_model_name = os.environ.get("SPACY_MODEL_NAME", "en_core_web_sm")
        config.spacy_threshold = float(os.environ.get("SPACY_THRESHOLD", "0.60"))
        config.autoload_spacy = os.environ.get("AUTOLOAD_SPACY", "true").lower() in ("1", "true", "yes")
        
        config.cache_ttl = int(os.environ.get("CACHE_TTL", "300"))
        
        return config


# Global config instance (singleton pattern)
_app_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get or create global application configuration."""
    global _app_config
    if _app_config is None:
        _app_config = AppConfig.from_env()
    return _app_config


def reload_config() -> AppConfig:
    """Reload configuration from environment."""
    global _app_config
    _app_config = AppConfig.from_env()
    return _app_config

