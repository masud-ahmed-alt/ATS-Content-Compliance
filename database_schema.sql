-- COMPLIANCE CRAWLER SYSTEM - NORMALIZED DATABASE SCHEMA
-- Designed for BCNF (Boyce-Codd Normal Form)
-- PostgreSQL 14+

-- ============================================================================
-- 1. CRAWL MANAGEMENT TABLES
-- ============================================================================

CREATE TABLE crawl_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    total_urls INTEGER NOT NULL,
    processed_urls INTEGER NOT NULL DEFAULT 0,
    failed_urls INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    CONSTRAINT valid_processed CHECK (processed_urls <= total_urls)
);

CREATE INDEX idx_crawl_sessions_status ON crawl_sessions(status);
CREATE INDEX idx_crawl_sessions_created_at ON crawl_sessions(created_at DESC);

-- ============================================================================
-- 2. PAGE CONTENT STORAGE
-- ============================================================================

CREATE TABLE page_content (
    page_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES crawl_sessions(session_id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    domain VARCHAR(255) NOT NULL,
    http_status_code INTEGER,
    content_type VARCHAR(100),
    html_content BYTEA NOT NULL,
    html_size_bytes INTEGER NOT NULL,
    fetch_duration_ms INTEGER NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_javascript_heavy BOOLEAN DEFAULT FALSE,
    is_rendered BOOLEAN DEFAULT FALSE,
    renderer_duration_ms INTEGER,
    CONSTRAINT valid_html_size CHECK (html_size_bytes > 0)
);

CREATE INDEX idx_page_content_session_id ON page_content(session_id);
CREATE INDEX idx_page_content_domain ON page_content(domain);
CREATE INDEX idx_page_content_js_heavy ON page_content(is_javascript_heavy);
CREATE INDEX idx_page_content_rendered ON page_content(is_rendered);

-- ============================================================================
-- 3. PRODUCT & SERVICE CATEGORIES (NORMALIZED)
-- ============================================================================

CREATE TABLE product_categories (
    category_id SMALLINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    category_description TEXT NOT NULL,
    risk_level VARCHAR(20) NOT NULL CHECK (risk_level IN ('critical', 'high', 'medium', 'low')),
    keywords_pattern TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id SMALLINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    category_id SMALLINT NOT NULL REFERENCES product_categories(category_id),
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT NOT NULL,
    risk_score NUMERIC(3,1) NOT NULL CHECK (risk_score >= 0 AND risk_score <= 10),
    detection_pattern TEXT,
    common_keywords TEXT ARRAY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (category_id, product_name)
);

CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_risk_score ON products(risk_score DESC);

-- ============================================================================
-- 4. KEYWORD MATCHING & ANALYSIS
-- ============================================================================

CREATE TABLE keyword_matches (
    match_id BIGSERIAL PRIMARY KEY,
    page_id UUID NOT NULL REFERENCES page_content(page_id) ON DELETE CASCADE,
    product_id SMALLINT NOT NULL REFERENCES products(product_id),
    category_id SMALLINT NOT NULL REFERENCES product_categories(category_id),
    match_type VARCHAR(50) NOT NULL CHECK (match_type IN ('regex', 'fuzzy', 'nlp', 'qr_code')),
    matched_text TEXT NOT NULL,
    confidence_score NUMERIC(3,2) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    context_snippet TEXT,
    context_position INTEGER,
    matched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_keyword_matches_page_id ON keyword_matches(page_id);
CREATE INDEX idx_keyword_matches_product_id ON keyword_matches(product_id);
CREATE INDEX idx_keyword_matches_category_id ON keyword_matches(category_id);
CREATE INDEX idx_keyword_matches_confidence ON keyword_matches(confidence_score DESC);
CREATE INDEX idx_keyword_matches_match_type ON keyword_matches(match_type);

-- ============================================================================
-- 5. PAYMENT HANDLE DETECTION & NORMALIZATION
-- ============================================================================

CREATE TABLE payment_providers (
    provider_id SMALLINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    provider_name VARCHAR(100) NOT NULL UNIQUE,
    handle_pattern VARCHAR(100) NOT NULL,
    risk_level VARCHAR(20) NOT NULL CHECK (risk_level IN ('critical', 'high', 'medium', 'low'))
);

INSERT INTO payment_providers (provider_name, handle_pattern, risk_level) VALUES
    ('Paytm', '@paytm', 'high'),
    ('Google Pay', '@okaxis', 'medium'),
    ('WhatsApp Pay', '@okhdfcbank', 'medium'),
    ('PhonePe', '@ybl', 'high'),
    ('ICICI Bank', '@icici', 'medium'),
    ('HDFC Bank', '@okhdfcbank', 'medium'),
    ('Axis Bank', '@okaxis', 'medium'),
    ('SBI', '@oksbi', 'medium');

CREATE TABLE upi_handles (
    handle_id BIGSERIAL PRIMARY KEY,
    page_id UUID NOT NULL REFERENCES page_content(page_id) ON DELETE CASCADE,
    provider_id SMALLINT NOT NULL REFERENCES payment_providers(provider_id),
    upi_handle VARCHAR(100) NOT NULL,
    detection_method VARCHAR(50) NOT NULL CHECK (detection_method IN ('text', 'qr_code', 'image_ocr')),
    is_suspicious BOOLEAN DEFAULT FALSE,
    suspicion_reason TEXT,
    confidence_score NUMERIC(3,2) NOT NULL,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_upi_handles_page_id ON upi_handles(page_id);
CREATE INDEX idx_upi_handles_provider_id ON upi_handles(provider_id);
CREATE INDEX idx_upi_handles_suspicious ON upi_handles(is_suspicious);

-- ============================================================================
-- 6. MERCHANT & DOMAIN ANALYSIS
-- ============================================================================

CREATE TABLE merchants (
    merchant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain VARCHAR(255) NOT NULL UNIQUE,
    merchant_name VARCHAR(255),
    total_pages_analyzed INTEGER NOT NULL DEFAULT 0,
    suspicious_pages INTEGER NOT NULL DEFAULT 0,
    risk_score NUMERIC(3,1) NOT NULL DEFAULT 0,
    requires_manual_review BOOLEAN DEFAULT FALSE,
    last_analyzed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_merchants_domain ON merchants(domain);
CREATE INDEX idx_merchants_risk_score ON merchants(risk_score DESC);
CREATE INDEX idx_merchants_requires_review ON merchants(requires_manual_review);

-- ============================================================================
-- 7. MERCHANT PRODUCT MAPPING
-- ============================================================================

CREATE TABLE merchant_products (
    mapping_id BIGSERIAL PRIMARY KEY,
    merchant_id UUID NOT NULL REFERENCES merchants(merchant_id) ON DELETE CASCADE,
    product_id SMALLINT NOT NULL REFERENCES products(product_id),
    category_id SMALLINT NOT NULL REFERENCES product_categories(category_id),
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    first_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    confidence_score NUMERIC(3,2) NOT NULL,
    UNIQUE (merchant_id, product_id)
);

CREATE INDEX idx_merchant_products_merchant_id ON merchant_products(merchant_id);
CREATE INDEX idx_merchant_products_product_id ON merchant_products(product_id);
CREATE INDEX idx_merchant_products_category_id ON merchant_products(category_id);

-- ============================================================================
-- 8. SCREENSHOT MANAGEMENT
-- ============================================================================

CREATE TABLE screenshots (
    screenshot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    page_id UUID NOT NULL REFERENCES page_content(page_id) ON DELETE CASCADE,
    merchant_id UUID REFERENCES merchants(merchant_id) ON DELETE SET NULL,
    screenshot_path TEXT NOT NULL,
    thumbnail_path TEXT,
    screenshot_size_bytes INTEGER NOT NULL,
    width INTEGER,
    height INTEGER,
    is_processed BOOLEAN DEFAULT FALSE,
    ocr_text TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_screenshots_page_id ON screenshots(page_id);
CREATE INDEX idx_screenshots_merchant_id ON screenshots(merchant_id);
CREATE INDEX idx_screenshots_processed ON screenshots(is_processed);

-- ============================================================================
-- 9. AUDIT & COMPLIANCE LOGGING
-- ============================================================================

CREATE TABLE audit_logs (
    log_id BIGSERIAL PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES crawl_sessions(session_id) ON DELETE CASCADE,
    log_level VARCHAR(20) NOT NULL CHECK (log_level IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    log_message TEXT NOT NULL,
    error_details TEXT,
    affected_url TEXT,
    affected_page_id UUID REFERENCES page_content(page_id),
    service_name VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_logs_session_id ON audit_logs(session_id);
CREATE INDEX idx_audit_logs_log_level ON audit_logs(log_level);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at DESC);
CREATE INDEX idx_audit_logs_service_name ON audit_logs(service_name);

-- ============================================================================
-- 10. REPORT CACHE & AGGREGATION
-- ============================================================================

CREATE TABLE compliance_reports (
    report_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES crawl_sessions(session_id) ON DELETE CASCADE,
    report_type VARCHAR(50) NOT NULL CHECK (report_type IN ('summary', 'detailed', 'merchant', 'category')),
    merchant_id UUID REFERENCES merchants(merchant_id),
    category_id SMALLINT REFERENCES product_categories(category_id),
    total_matches INTEGER NOT NULL,
    critical_matches INTEGER NOT NULL,
    high_risk_matches INTEGER NOT NULL,
    report_data JSONB NOT NULL,
    generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    exported_at TIMESTAMP
);

CREATE INDEX idx_compliance_reports_session_id ON compliance_reports(session_id);
CREATE INDEX idx_compliance_reports_merchant_id ON compliance_reports(merchant_id);
CREATE INDEX idx_compliance_reports_generated_at ON compliance_reports(generated_at DESC);

-- ============================================================================
-- 11. PERFORMANCE METRICS & MONITORING
-- ============================================================================

CREATE TABLE service_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    service_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC NOT NULL,
    tags JSONB,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_service_metrics_service_name ON service_metrics(service_name);
CREATE INDEX idx_service_metrics_recorded_at ON service_metrics(recorded_at DESC);

-- ============================================================================
-- VIEWS FOR REPORTING
-- ============================================================================

CREATE VIEW vw_merchant_risk_summary AS
SELECT
    m.merchant_id,
    m.domain,
    m.merchant_name,
    m.total_pages_analyzed,
    m.suspicious_pages,
    COUNT(DISTINCT mp.product_id) as unique_products_detected,
    COUNT(DISTINCT mp.category_id) as unique_categories,
    ROUND(m.risk_score::NUMERIC, 2) as risk_score,
    m.requires_manual_review,
    m.last_analyzed_at
FROM merchants m
LEFT JOIN merchant_products mp ON m.merchant_id = mp.merchant_id
GROUP BY m.merchant_id, m.domain, m.merchant_name, m.total_pages_analyzed,
         m.suspicious_pages, m.risk_score, m.requires_manual_review, m.last_analyzed_at;

CREATE VIEW vw_category_statistics AS
SELECT
    pc.category_id,
    pc.category_name,
    pc.risk_level,
    COUNT(DISTINCT p.product_id) as total_products,
    COUNT(DISTINCT km.page_id) as total_pages_with_matches,
    COUNT(DISTINCT km.match_id) as total_matches,
    ROUND(AVG(km.confidence_score)::NUMERIC, 3) as avg_confidence_score
FROM product_categories pc
LEFT JOIN products p ON pc.category_id = p.category_id
LEFT JOIN keyword_matches km ON p.product_id = km.product_id
GROUP BY pc.category_id, pc.category_name, pc.risk_level;

-- ============================================================================
-- CONSTRAINTS & TRIGGERS
-- ============================================================================

CREATE OR REPLACE FUNCTION update_merchant_metrics()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE merchants
    SET
        total_pages_analyzed = total_pages_analyzed + 1,
        last_analyzed_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE merchant_id = NEW.merchant_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_merchant_on_page_analysis
AFTER INSERT ON merchant_products
FOR EACH ROW
EXECUTE FUNCTION update_merchant_metrics();

-- ============================================================================
-- COMMENT ON TABLES FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE crawl_sessions IS 'Tracks bulk crawl operations with status, counts, and timestamps';
COMMENT ON TABLE page_content IS 'Stores raw HTML content and metadata for each fetched page';
COMMENT ON TABLE keyword_matches IS 'Records detected keyword matches with confidence scores and context';
COMMENT ON TABLE upi_handles IS 'Payment handle detection normalized by provider';
COMMENT ON TABLE merchants IS 'Domain-level aggregation for risk scoring and reporting';
COMMENT ON TABLE screenshots IS 'Screenshot storage with OCR text extraction capability';
COMMENT ON TABLE audit_logs IS 'Comprehensive audit trail for compliance and debugging';
COMMENT ON TABLE compliance_reports IS 'Generated reports cached for export and historical tracking';

