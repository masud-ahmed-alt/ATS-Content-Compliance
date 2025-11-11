"""
database_init.py — Initialize database schema and seed product taxonomy on startup

Features:
- Applies full BCNF schema if not already present
- Seeds product_categories and products from configuration
- Provides convenient session helpers
"""

import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import inspect, text
from config.settings import engine, SessionLocal, Base
from models.database_models import (
    ProductCategory, Product, PaymentProvider, CrawlSession, AuditLog
)

logger = logging.getLogger(__name__)

# Product taxonomy data (matches enhanced_keywords.yml structure)
PRODUCT_TAXONOMY = {
    "Controlled Narcotics": {
        "description": "High-risk substances and related paraphernalia.",
        "risk_level": "HIGH",
        "products": [
            {"name": "Cannabis/CBD Products", "risk_score": 9.5, "keywords": ["cannabis", "weed", "CBD oil", "edibles", "THC"]},
            {"name": "Opioids & Heroin", "risk_score": 10.0, "keywords": ["heroin", "fentanyl", "opioid", "buy oxy", "painkillers"]},
            {"name": "Cocaine & Stimulants", "risk_score": 10.0, "keywords": ["cocaine", "speed", "meth", "buy coke", "crack"]},
            {"name": "Kratom & Khat", "risk_score": 7.5, "keywords": ["kratom", "mitragyna", "khat", "buy kratom", "natural opiate"]},
            {"name": "Unauthorized Prescription Drugs", "risk_score": 8.5, "keywords": ["no rx", "no prescription", "buy xanax"]},
            {"name": "Psychedelics & Hallucinogens", "risk_score": 9.0, "keywords": ["psilocybin", "LSD", "DMT", "buy shrooms"]},
            {"name": "Novel Psychoactive Substances", "risk_score": 8.5, "keywords": ["spice", "bath salts", "designer drug"]},
        ]
    },
    "Anabolic Steroids": {
        "description": "Steroid and performance-enhancing drug sales.",
        "risk_level": "HIGH",
        "products": [
            {"name": "Injectable Steroids", "risk_score": 9.5, "keywords": ["testosterone", "trenbolone", "nandrolone"]},
            {"name": "Oral Steroids", "risk_score": 9.0, "keywords": ["dianabol", "winstrol", "anavar"]},
            {"name": "Post Cycle Therapy", "risk_score": 8.0, "keywords": ["PCT", "clomid", "tamoxifen"]},
            {"name": "Growth Hormones", "risk_score": 8.5, "keywords": ["HGH", "IGF-1", "peptides"]},
            {"name": "Fat Burners", "risk_score": 8.5, "keywords": ["clenbuterol", "ephedrine", "DNP"]},
            {"name": "Veterinary Steroids", "risk_score": 9.0, "keywords": ["veterinary steroid", "animal grade"]},
            {"name": "Counterfeit Steroids", "risk_score": 9.5, "keywords": ["fake steroids", "counterfeit"]},
        ]
    },
    "Gambling": {
        "description": "Illegal or unlicensed betting platforms.",
        "risk_level": "HIGH",
        "products": [
            {"name": "International Betting", "risk_score": 8.0, "keywords": ["betting", "sportsbook", "1xbet"]},
            {"name": "Cricket Betting", "risk_score": 8.5, "keywords": ["IPL", "cricket bet", "satta"]},
            {"name": "Casino Platforms", "risk_score": 7.5, "keywords": ["online casino", "slots", "roulette"]},
            {"name": "Crypto Betting", "risk_score": 7.5, "keywords": ["crypto casino", "bitcoin bet"]},
            {"name": "Fantasy Sports", "risk_score": 6.5, "keywords": ["fantasy sports", "DFS"]},
            {"name": "Satta/Matka", "risk_score": 8.5, "keywords": ["satta king", "matka"]},
            {"name": "Horse Racing", "risk_score": 7.0, "keywords": ["horse racing", "pari-mutuel"]},
        ]
    },
    "Weapons & Explosives": {
        "description": "Illicit weapons, ammunition, and components.",
        "risk_level": "CRITICAL",
        "products": [
            {"name": "Illegal Firearms", "risk_score": 10.0, "keywords": ["buy gun", "illegal firearm"]},
            {"name": "Ammunition", "risk_score": 10.0, "keywords": ["ammunition", "explosives"]},
            {"name": "Bladed Weapons", "risk_score": 8.0, "keywords": ["switchblade", "illegal knife"]},
            {"name": "3D Printed Weapons", "risk_score": 9.5, "keywords": ["3d gun", "ghost gun"]},
            {"name": "Chemical Weapons", "risk_score": 10.0, "keywords": ["chemical agent", "toxin", "poison"]},
            {"name": "Armor Piercing", "risk_score": 9.5, "keywords": ["armor piercing", "AP rounds"]},
            {"name": "Martial Arts Weapons", "risk_score": 7.5, "keywords": ["nunchaku", "brass knuckles"]},
        ]
    },
    "Adult Services": {
        "description": "Commercial sexual services.",
        "risk_level": "HIGH",
        "products": [
            {"name": "Escort Services", "risk_score": 9.5, "keywords": ["escort", "call girl", "companion"]},
            {"name": "Adult Content", "risk_score": 8.0, "keywords": ["porn", "adult videos", "xxx"]},
            {"name": "Massage Services", "risk_score": 8.5, "keywords": ["sensual massage", "escort massage"]},
            {"name": "Dating Platforms", "risk_score": 7.5, "keywords": ["sugar dating", "paid dating"]},
            {"name": "Live Performance", "risk_score": 8.0, "keywords": ["webcam", "camgirl", "live adult"]},
            {"name": "Novelty Sales", "risk_score": 5.5, "keywords": ["sex toy", "adult shop"]},
            {"name": "Trafficking Indicators", "risk_score": 10.0, "keywords": ["trafficking", "forced", "coercion"]},
        ]
    },
    "Counterfeit & Fraud": {
        "description": "Fake goods and fraudulent services.",
        "risk_level": "HIGH",
        "products": [
            {"name": "Counterfeit Luxury", "risk_score": 7.5, "keywords": ["replica", "fake gucci"]},
            {"name": "Fake IDs", "risk_score": 9.5, "keywords": ["fake id", "forged passport"]},
            {"name": "Electronics", "risk_score": 8.0, "keywords": ["fake iphone", "counterfeit"]},
            {"name": "Fraudulent Services", "risk_score": 8.5, "keywords": ["scam", "ponzi", "fraud"]},
            {"name": "Food/Cosmetics", "risk_score": 7.0, "keywords": ["fake perfume", "adulterated"]},
            {"name": "Fake Pharma", "risk_score": 9.0, "keywords": ["fake medicine", "counterfeit"]},
            {"name": "Ticket Fraud", "risk_score": 7.5, "keywords": ["fake ticket", "credential fraud"]},
        ]
    },
    "Cryptocurrency Fraud": {
        "description": "Crypto-based fraud and laundering.",
        "risk_level": "HIGH",
        "products": [
            {"name": "Illicit Wallets", "risk_score": 8.0, "keywords": ["crypto mixer", "tumbler"]},
            {"name": "Pump & Dump", "risk_score": 8.0, "keywords": ["pump and dump", "moon token"]},
            {"name": "Ransomware Services", "risk_score": 9.5, "keywords": ["ransomware payment", "btc transfer"]},
            {"name": "Money Laundering", "risk_score": 8.5, "keywords": ["no KYC", "dark exchange"]},
            {"name": "ICO Fraud", "risk_score": 8.0, "keywords": ["ICO scam", "rug pull"]},
            {"name": "Illegal Remittance", "risk_score": 7.5, "keywords": ["hawala", "underground bank"]},
            {"name": "Carding", "risk_score": 9.0, "keywords": ["cc dump", "stolen cards", "carding"]},
        ]
    },
    "Pharmaceuticals": {
        "description": "Pharmaceuticals marketed for misuse.",
        "risk_level": "MEDIUM",
        "products": [
            {"name": "Benzodiazepines", "risk_score": 7.5, "keywords": ["xanax", "valium", "benzo"]},
            {"name": "ADHD Meds", "risk_score": 7.0, "keywords": ["adderall", "ritalin", "methylphenidate"]},
            {"name": "ED Drugs", "risk_score": 6.5, "keywords": ["viagra", "cialis"]},
            {"name": "Painkillers", "risk_score": 8.0, "keywords": ["oxycodone", "tramadol", "narcotic"]},
            {"name": "Sleep Aids", "risk_score": 7.0, "keywords": ["ambien", "zopiclone", "zolpidem"]},
            {"name": "Cough Syrup", "risk_score": 6.5, "keywords": ["DXM", "dextromethorphan"]},
            {"name": "Anesthetics", "risk_score": 8.5, "keywords": ["ketamine", "propofol"]},
        ]
    },
}

UPI_PROVIDERS = [
    {"name": "Google Pay", "code": "gpay", "risk_level": "LOW"},
    {"name": "PhonePe", "code": "ybl", "risk_level": "LOW"},
    {"name": "Paytm", "code": "paytm", "risk_level": "MEDIUM"},
    {"name": "ICICI Bank UPI", "code": "oksbi", "risk_level": "LOW"},
    {"name": "HDFC Bank UPI", "code": "okhdfcbank", "risk_level": "LOW"},
    {"name": "Axis Bank UPI", "code": "okaxis", "risk_level": "LOW"},
    {"name": "Unregistered", "code": "unregistered", "risk_level": "HIGH"},
]


def init_database():
    """Initialize database schema on application startup."""
    try:
        # Create all tables from ORM models
        Base.metadata.create_all(bind=engine)
        logger.info("Database schema initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database schema: {e}")
        raise


def seed_taxonomy():
    """Seed product categories and products into database."""
    db = SessionLocal()
    try:
        # Check if taxonomy already seeded
        existing_count = db.query(ProductCategory).count()
        if existing_count > 0:
            logger.info(f"Taxonomy already seeded ({existing_count} categories)")
            return

        # Seed categories and products
        for cat_name, cat_data in PRODUCT_TAXONOMY.items():
            category = ProductCategory(
                name=cat_name,
                description=cat_data.get("description", ""),
                risk_level=cat_data.get("risk_level", "MEDIUM")
            )
            db.add(category)
            db.flush()

            for prod_data in cat_data.get("products", []):
                product = Product(
                    category_id=category.id,
                    name=prod_data.get("name"),
                    risk_score=prod_data.get("risk_score", 5.0),
                    keywords=prod_data.get("keywords", []),
                    detection_patterns={
                        "keywords": prod_data.get("keywords", [])
                    }
                )
                db.add(product)

        # Seed payment providers
        existing_providers = db.query(PaymentProvider).count()
        if existing_providers == 0:
            for prov in UPI_PROVIDERS:
                provider = PaymentProvider(
                    name=prov["name"],
                    code=prov["code"],
                    risk_level=prov.get("risk_level", "MEDIUM")
                )
                db.add(provider)

        db.commit()
        logger.info(f"Taxonomy seeded: {len(PRODUCT_TAXONOMY)} categories, {sum(len(v['products']) for v in PRODUCT_TAXONOMY.values())} products")
    except Exception as e:
        logger.error(f"Error seeding taxonomy: {e}")
        db.rollback()
    finally:
        db.close()


def log_audit(session_id: str, severity: str, action: str, details: dict = None):
    """Log an audit event."""
    db = SessionLocal()
    try:
        audit = AuditLog(
            session_id=session_id,
            severity=severity,
            action=action,
            details=details or {}
        )
        db.add(audit)
        db.commit()
    except Exception as e:
        logger.error(f"Error logging audit: {e}")
        db.rollback()
    finally:
        db.close()


@contextmanager
def get_db_session() -> Generator:
    """Context manager for database sessions."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        logger.error(f"Database session error: {e}")
        db.rollback()
        raise
    finally:
        db.close()
