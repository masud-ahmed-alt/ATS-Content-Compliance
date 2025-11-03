# models/result_model.py

from sqlalchemy import Column, Integer, String, Text, JSON, BigInteger
from config.settings import Base
import time


class Result(Base):
    __tablename__ = "results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, nullable=False)
    main_url = Column(String, nullable=False)
    sub_urls = Column(JSON, default=list)
    keyword_match = Column(JSON, default=list)
    categories = Column(JSON, default=list)
    word_count_raw_data = Column(Integer, default=0)
    word_count_cleaned_data = Column(Integer, default=0)
    raw_data = Column(Text, nullable=True)
    cleaned_data = Column(Text, nullable=True)
    timestamp = Column(Integer, default=lambda: int(time.time()))

class Hit(Base):
    __tablename__ = "hits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, nullable=False)             # Task identifier
    main_url = Column(Text, nullable=False)               # Main/master URL
    sub_url = Column(Text, nullable=False)                # Subpage where match occurred
    category = Column(String(100), nullable=False)        # e.g., payments, crypto, etc.
    matched_keyword = Column(String(255), nullable=False) # Keyword or pattern term
    snippet = Column(Text, nullable=False)                # Contextual text snippet
    screenshot_path = Column(Text, nullable=True)        # Path to cropped screenshot
    timestamp = Column(BigInteger, nullable=False)        # Unix epoch time
    source = Column(String(50), nullable=False)           # regex / alias / fuzzy / qr / context

    def __repr__(self):
        return f"<Hit(task={self.task_id}, keyword={self.matched_keyword}, source={self.source})>"


