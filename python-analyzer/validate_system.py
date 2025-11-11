#!/usr/bin/env python3
"""
validate_system.py — Quick validation script to verify system fixes are in place

Run: python validate_system.py

Checks:
- Database models importable
- Progress manager initializes
- WebSocket routes registered
- Database schema would be created
- Taxonomy data available
"""

import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def validate_imports():
    """Check all new modules import without errors."""
    logger.info("[validate] Checking imports...")
    try:
        from models.database_models import (
            CrawlSession, PageContent, ProductCategory, Product,
            KeywordMatch, PaymentProvider, UPIHandle, Merchant,
            MerchantProduct, Screenshot, AuditLog, ComplianceReport,
            ServiceMetric, Base
        )
        logger.info("✓ Database models imported successfully")
    except ImportError as e:
        logger.error(f"✗ Failed to import database models: {e}")
        return False

    try:
        from models.database_init import (
            init_database, seed_taxonomy, PRODUCT_TAXONOMY, UPI_PROVIDERS
        )
        logger.info("✓ Database init module imported successfully")
    except ImportError as e:
        logger.error(f"✗ Failed to import database_init: {e}")
        return False

    try:
        from utils.progress_manager import (
            IngestProgress, ProgressManager, get_progress_manager
        )
        logger.info("✓ Progress manager imported successfully")
    except ImportError as e:
        logger.error(f"✗ Failed to import progress_manager: {e}")
        return False

    try:
        from routes.progress_routes import router as progress_router
        logger.info("✓ Progress routes imported successfully")
    except ImportError as e:
        logger.error(f"✗ Failed to import progress_routes: {e}")
        return False

    return True


def validate_progress_manager():
    """Check progress manager functionality."""
    logger.info("[validate] Checking progress manager...")
    try:
        from utils.progress_manager import get_progress_manager
        import asyncio
        
        async def test():
            mgr = get_progress_manager()
            await mgr.create_session("test-session-1", 100)
            session = await mgr.get_session("test-session-1")
            if session is None:
                return False
            await mgr.add_match("test-session-1", "test_category", "test_keyword")
            state = session.to_dict()
            if state["total_matches"] != 1:
                return False
            await mgr.complete_session("test-session-1", success=True)
            return state["status"] == "completed"
        
        result = asyncio.run(test())
        if result:
            logger.info("✓ Progress manager functionality OK")
            return True
        else:
            logger.error("✗ Progress manager state management failed")
            return False
    except Exception as e:
        logger.error(f"✗ Progress manager test failed: {e}")
        return False


def validate_taxonomy():
    """Check product taxonomy data."""
    logger.info("[validate] Checking product taxonomy...")
    try:
        from models.database_init import PRODUCT_TAXONOMY, UPI_PROVIDERS
        
        if len(PRODUCT_TAXONOMY) != 8:
            logger.error(f"✗ Expected 8 categories, got {len(PRODUCT_TAXONOMY)}")
            return False
        
        total_products = sum(len(cat["products"]) for cat in PRODUCT_TAXONOMY.values())
        if total_products != 49:
            logger.error(f"✗ Expected 49 products, got {total_products}")
            return False
        
        if len(UPI_PROVIDERS) < 6:
            logger.error(f"✗ Expected 6+ UPI providers, got {len(UPI_PROVIDERS)}")
            return False
        
        logger.info(f"✓ Taxonomy OK: {len(PRODUCT_TAXONOMY)} categories, {total_products} products, {len(UPI_PROVIDERS)} UPI providers")
        return True
    except Exception as e:
        logger.error(f"✗ Taxonomy check failed: {e}")
        return False


def validate_orm_models():
    """Check ORM model structure."""
    logger.info("[validate] Checking ORM models...")
    try:
        from models.database_models import (
            CrawlSession, KeywordMatch, UPIHandle, ProductCategory, Product
        )
        from sqlalchemy.orm import class_mapper
        
        # Check primary keys and columns
        checks = [
            (CrawlSession, ["id", "session_id", "status", "started_at"]),
            (KeywordMatch, ["id", "matched_keyword", "source", "confidence_score"]),
            (UPIHandle, ["id", "handle", "provider_id", "detection_method"]),
            (ProductCategory, ["id", "name", "risk_level"]),
            (Product, ["id", "name", "risk_score", "keywords"]),
        ]
        
        for model, expected_cols in checks:
            mapper = class_mapper(model)
            actual_cols = [c.key for c in mapper.columns]
            for col in expected_cols:
                if col not in actual_cols:
                    logger.error(f"✗ {model.__name__} missing column: {col}")
                    return False
        
        logger.info("✓ ORM model structure OK")
        return True
    except Exception as e:
        logger.error(f"✗ ORM model check failed: {e}")
        return False


def validate_fastapi_routes():
    """Check FastAPI routes are defined."""
    logger.info("[validate] Checking FastAPI routes...")
    try:
        from routes.progress_routes import router as progress_router
        
        routes_found = {}
        for route in progress_router.routes:
            path = getattr(route, "path", None)
            methods = getattr(route, "methods", None)
            if path:
                routes_found[path] = methods or []
        
        expected = [
            "/ws/{session_id}",
            "/status/{session_id}",
            "/start/{session_id}",
            "/end/{session_id}",
        ]
        
        for route_path in expected:
            if route_path not in routes_found:
                logger.error(f"✗ Missing route: {route_path}")
                return False
        
        logger.info(f"✓ FastAPI routes OK: {len(routes_found)} routes found")
        return True
    except Exception as e:
        logger.error(f"✗ Route check failed: {e}")
        return False


def main():
    """Run all validations."""
    logger.info("=" * 60)
    logger.info("SYSTEM VALIDATION")
    logger.info("=" * 60)
    
    checks = [
        ("Imports", validate_imports),
        ("ORM Models", validate_orm_models),
        ("Product Taxonomy", validate_taxonomy),
        ("Progress Manager", validate_progress_manager),
        ("FastAPI Routes", validate_fastapi_routes),
    ]
    
    results = {}
    for name, check_fn in checks:
        try:
            results[name] = check_fn()
        except Exception as e:
            logger.error(f"✗ {name} check crashed: {e}")
            results[name] = False
    
    logger.info("=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status}: {name}")
    
    logger.info("=" * 60)
    logger.info(f"RESULT: {passed}/{total} checks passed")
    logger.info("=" * 60)
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
