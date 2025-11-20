# models/result_model.py

from sqlalchemy import Column, Integer, String, Text, JSON, BigInteger
from config.settings import Base
import time


class Result(Base):
    """
    Results table: Stores ALL matches (before spaCy validation) as master data.
    One row per main_url containing all matches found before validation.
    This is the master data showing what was found before filtering.
    """
    __tablename__ = "results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, nullable=False, index=True)  # Index for faster lookups
    main_url = Column(String, nullable=False, unique=True, index=True)  # Unique: one row per main_url
    sub_urls = Column(JSON, default=list)
    keyword_match = Column(JSON, default=list)
    categories = Column(JSON, default=list)
    word_count_raw_data = Column(Integer, default=0)
    word_count_cleaned_data = Column(Integer, default=0)
    raw_data = Column(Text, nullable=True)
    cleaned_data = Column(Text, nullable=True)
    timestamp = Column(Integer, default=lambda: int(time.time()))

class Hit(Base):
    """
    Hits table: Stores ONLY validated matches (after spaCy validation).
    Individual hits that passed spaCy validation threshold.
    Multiple rows per main_url (one per validated match).
    """
    __tablename__ = "hits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, nullable=False, index=True)             # Task identifier
    main_url = Column(Text, nullable=False, index=True)               # Main/master URL
    sub_url = Column(Text, nullable=False, index=True)                # Subpage where match occurred
    category = Column(String(100), nullable=False)        # e.g., payments, crypto, etc.
    matched_keyword = Column(String(255), nullable=False) # Keyword or pattern term
    snippet = Column(Text, nullable=False)                # Contextual text snippet
    screenshot_path = Column(Text, nullable=True)        # Path to cropped screenshot
    timestamp = Column(BigInteger, nullable=False)        # Unix epoch time
    source = Column(String(50), nullable=False)           # regex / alias / fuzzy / qr / context
    confident_score = Column(Integer, nullable=True)        # Confidence score from spaCy validation

    def __repr__(self):
        return f"<Hit(task={self.task_id}, keyword={self.matched_keyword}, source={self.source})>"


