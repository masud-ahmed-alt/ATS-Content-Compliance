"""
database_models.py — SQLAlchemy ORM models aligned with BCNF database_schema.sql

Provides complete ORM coverage for:
- Crawl session tracking (crawl_sessions)
- Content storage (page_content)
- Product classification (product_categories, products)
- Keyword matching (keyword_matches, merchant_products)
- UPI/payment extraction (payment_providers, upi_handles, merchants)
- Screenshots and audit logging
"""

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, Float, JSON,
    ForeignKey, UniqueConstraint, Index, BigInteger, Enum, func
)
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

from config.settings import Base


class CrawlSession(Base):
    """Tracks bulk crawl operations, progress, and completion status."""
    __tablename__ = "crawl_sessions"

    id = Column(Integer, primary_key=True)
    session_id = Column(String(255), unique=True, nullable=False, index=True)
    status = Column(String(50), nullable=False, default="running")  # running, completed, failed
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    total_pages = Column(Integer, default=0)
    pages_processed = Column(Integer, default=0)
    pages_failed = Column(Integer, default=0)
    total_matches = Column(Integer, default=0)
    categories_found = Column(JSON, default=dict)  # {"category": count}
    error_message = Column(Text, nullable=True)
    metadata_json = Column(JSON, default=dict)

    page_contents = relationship("PageContent", back_populates="crawl_session")
    keyword_matches = relationship("KeywordMatch", back_populates="crawl_session")

    __table_args__ = (
        Index("idx_session_status", "session_id", "status"),
        Index("idx_session_started", "started_at"),
    )


class PageContent(Base):
    """Stores raw HTML content, metadata, and rendering status."""
    __tablename__ = "page_content"

    id = Column(Integer, primary_key=True)
    session_id = Column(String(255), ForeignKey("crawl_sessions.session_id"), nullable=False, index=True)
    crawl_session = relationship("CrawlSession", back_populates="page_contents")

    url = Column(Text, nullable=False, index=True)
    main_url = Column(Text, nullable=False, index=True)
    http_status = Column(Integer, nullable=True)
    content_type = Column(String(100), nullable=True)
    html_content = Column(Text, nullable=True)
    fetch_duration_ms = Column(Integer, nullable=True)
    is_javascript_heavy = Column(Boolean, default=False)
    fetched_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    keyword_matches = relationship("KeywordMatch", back_populates="page_content")
    screenshots = relationship("Screenshot", back_populates="page_content")

    __table_args__ = (
        Index("idx_page_session_url", "session_id", "url"),
        Index("idx_page_main_url", "main_url"),
    )


class ProductCategory(Base):
    """Product categories (8 main categories for compliance)."""
    __tablename__ = "product_categories"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    risk_level = Column(String(50), nullable=True)  # HIGH, MEDIUM, LOW

    products = relationship("Product", back_populates="category")
    keyword_matches = relationship("KeywordMatch", back_populates="category")

    __table_args__ = (
        Index("idx_category_name", "name"),
    )


class Product(Base):
    """Individual products within categories (49 total)."""
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey("product_categories.id"), nullable=False, index=True)
    category = relationship("ProductCategory", back_populates="products")

    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    risk_score = Column(Float, nullable=True)  # 5.0 to 10.0
    keywords = Column(JSON, default=list)  # ["keyword1", "keyword2", ...]
    detection_patterns = Column(JSON, default=dict)  # {"regex": [...], "fuzzy": [...]}

    keyword_matches = relationship("KeywordMatch", back_populates="product")
    merchant_products = relationship("MerchantProduct", back_populates="product")

    __table_args__ = (
        Index("idx_product_category", "category_id"),
        Index("idx_product_name", "name"),
    )


class KeywordMatch(Base):
    """Detected keyword matches with confidence scoring."""
    __tablename__ = "keyword_matches"

    id = Column(Integer, primary_key=True)
    session_id = Column(String(255), ForeignKey("crawl_sessions.session_id"), nullable=False, index=True)
    crawl_session = relationship("CrawlSession", back_populates="keyword_matches")

    page_id = Column(Integer, ForeignKey("page_content.id"), nullable=False, index=True)
    page_content = relationship("PageContent", back_populates="keyword_matches")

    category_id = Column(Integer, ForeignKey("product_categories.id"), nullable=True)
    category = relationship("ProductCategory", back_populates="keyword_matches")

    product_id = Column(Integer, ForeignKey("products.id"), nullable=True)
    product = relationship("Product", back_populates="keyword_matches")

    matched_keyword = Column(String(255), nullable=False)
    match_snippet = Column(Text, nullable=False)  # Contextual excerpt
    source = Column(String(50), nullable=False)  # regex, fuzzy, alias, qr, ocr, semantic
    confidence_score = Column(Float, nullable=True)  # 0.0 to 1.0
    matched_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_match_session", "session_id"),
        Index("idx_match_page", "page_id"),
        Index("idx_match_category", "category_id"),
        Index("idx_match_keyword", "matched_keyword"),
    )


class PaymentProvider(Base):
    """Payment service providers (8+ UPI/payment PSPs)."""
    __tablename__ = "payment_providers"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    code = Column(String(50), unique=True, nullable=False)  # upi, paytm, ybl, etc.
    description = Column(Text, nullable=True)
    risk_level = Column(String(50), nullable=True)

    upi_handles = relationship("UPIHandle", back_populates="provider")


class UPIHandle(Base):
    """Extracted UPI and payment handles with detection method."""
    __tablename__ = "upi_handles"

    id = Column(Integer, primary_key=True)
    session_id = Column(String(255), ForeignKey("crawl_sessions.session_id"), nullable=False, index=True)
    provider_id = Column(Integer, ForeignKey("payment_providers.id"), nullable=True)
    provider = relationship("PaymentProvider", back_populates="upi_handles")

    handle = Column(String(255), nullable=False, index=True)
    main_url = Column(Text, nullable=False)
    detection_method = Column(String(50), nullable=False)  # regex, ocr, qr
    occurrence_count = Column(Integer, default=1)
    first_detected_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_upi_session", "session_id"),
        Index("idx_upi_handle", "handle"),
        UniqueConstraint("session_id", "handle", name="uq_upi_session_handle"),
    )


class Merchant(Base):
    """Domain-level aggregation and risk scoring."""
    __tablename__ = "merchants"

    id = Column(Integer, primary_key=True)
    domain = Column(String(255), unique=True, nullable=False, index=True)
    main_url = Column(Text, nullable=False)
    total_hits = Column(Integer, default=0)
    total_pages_crawled = Column(Integer, default=0)
    risk_score = Column(Float, default=0.0)  # Aggregate risk
    categories_found = Column(JSON, default=list)
    upi_count = Column(Integer, default=0)
    first_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    products = relationship("MerchantProduct", back_populates="merchant")

    __table_args__ = (
        Index("idx_merchant_domain", "domain"),
        Index("idx_merchant_risk", "risk_score"),
    )


class MerchantProduct(Base):
    """Normalized M2M relationship between merchants and detected products."""
    __tablename__ = "merchant_products"

    id = Column(Integer, primary_key=True)
    merchant_id = Column(Integer, ForeignKey("merchants.id"), nullable=False)
    merchant = relationship("Merchant", back_populates="products")

    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    product = relationship("Product", back_populates="merchant_products")

    occurrence_count = Column(Integer, default=1)
    last_detected_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("merchant_id", "product_id", name="uq_merchant_product"),
        Index("idx_merchant_product", "merchant_id", "product_id"),
    )


class Screenshot(Base):
    """Image storage with OCR text extraction and metadata."""
    __tablename__ = "screenshots"

    id = Column(Integer, primary_key=True)
    page_id = Column(Integer, ForeignKey("page_content.id"), nullable=False)
    page_content = relationship("PageContent", back_populates="screenshots")

    storage_path = Column(Text, nullable=False)  # MinIO or S3 path
    ocr_text = Column(Text, nullable=True)
    thumbnail_url = Column(Text, nullable=True)
    captured_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_screenshot_page", "page_id"),
    )


class AuditLog(Base):
    """Comprehensive audit trail for compliance."""
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True)
    session_id = Column(String(255), ForeignKey("crawl_sessions.session_id"), nullable=True, index=True)
    severity = Column(String(50), nullable=False)  # INFO, WARNING, ERROR, CRITICAL
    action = Column(String(255), nullable=False)
    details = Column(JSON, default=dict)
    logged_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_audit_severity", "severity"),
        Index("idx_audit_session", "session_id"),
    )


class ComplianceReport(Base):
    """Cached report data for fast export and retrieval."""
    __tablename__ = "compliance_reports"

    id = Column(Integer, primary_key=True)
    main_url = Column(Text, unique=True, nullable=False, index=True)
    session_id = Column(String(255), ForeignKey("crawl_sessions.session_id"), nullable=True)
    total_hits = Column(Integer, default=0)
    category_breakdown = Column(JSON, default=dict)  # {"category": count}
    merchant_risk_summary = Column(JSON, default=dict)
    upi_map = Column(JSON, default=dict)
    generated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    exported_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_report_url", "main_url"),
    )


class ServiceMetric(Base):
    """Performance monitoring and metrics time-series."""
    __tablename__ = "service_metrics"

    id = Column(Integer, primary_key=True)
    metric_name = Column(String(255), nullable=False, index=True)
    metric_value = Column(Float, nullable=False)
    tags = Column(JSON, default=dict)  # {"service": "analyzer", "endpoint": "/ingest"}
    recorded_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_metric_name_time", "metric_name", "recorded_at"),
    )
