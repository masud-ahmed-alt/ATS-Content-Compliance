#!/usr/bin/env python3
"""
opensearch_indexer.py - Index analysis results to OpenSearch for dashboards and search

Provides functions to index keyword matches, results, and analytics to OpenSearch
for real-time dashboards and reporting.
"""

import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class OpenSearchIndexer:
    """Client for indexing analysis results to OpenSearch"""
    
    INDEX_PREFIX = "analysis"
    
    def __init__(self, opensearch_client):
        """
        Initialize indexer with OpenSearch client.
        
        Args:
            opensearch_client: Connected OpenSearch client from config.settings
        """
        self.client = opensearch_client
    
    def index_keyword_match(self, match_data: Dict[str, Any]) -> bool:
        """
        Index a single keyword match to OpenSearch.
        
        Args:
            match_data: Dictionary containing:
                - session_id: Session identifier
                - url: Matched URL
                - keyword: Matched keyword
                - category: Product category
                - product: Product name
                - confidence: Confidence score (0-1)
                - source: Detection method (regex, fuzzy, ocr, qr, semantic)
                - snippet: Contextual excerpt
                
        Returns:
            True if successfully indexed, False otherwise
        """
        try:
            if not self.client:
                logger.warning("[opensearch] Client not available")
                return False
            
            index_name = f"{self.INDEX_PREFIX}-matches"
            
            doc = {
                "session_id": match_data.get("session_id"),
                "url": match_data.get("url"),
                "keyword": match_data.get("keyword"),
                "category": match_data.get("category"),
                "product": match_data.get("product"),
                "confidence": float(match_data.get("confidence", 0)),
                "source": match_data.get("source"),
                "snippet": match_data.get("snippet", ""),
                "timestamp": datetime.utcnow().isoformat(),
                "detected_at": datetime.utcnow().isoformat()
            }
            
            response = self.client.index(
                index=index_name,
                body=doc,
                refresh=False  # Batch refreshes for performance
            )
            
            logger.debug(f"[opensearch] Indexed keyword match: {match_data.get('keyword')} -> {response}")
            return True
            
        except Exception as e:
            logger.error(f"[opensearch] Failed to index keyword match: {e}")
            return False
    
    def index_session_result(self, result_data: Dict[str, Any]) -> bool:
        """
        Index a complete session result to OpenSearch.
        
        Args:
            result_data: Dictionary containing:
                - session_id: Session identifier
                - main_url: Main crawled URL
                - total_pages: Total pages processed
                - total_matches: Total matches found
                - categories: List of detected categories
                - keywords: List of detected keywords
                - status: Session status (completed, failed)
                - timestamp: Session start time
                
        Returns:
            True if successfully indexed, False otherwise
        """
        try:
            if not self.client:
                logger.warning("[opensearch] Client not available")
                return False
            
            index_name = f"{self.INDEX_PREFIX}-results"
            
            doc = {
                "session_id": result_data.get("session_id"),
                "main_url": result_data.get("main_url"),
                "total_pages": int(result_data.get("total_pages", 0)),
                "total_matches": int(result_data.get("total_matches", 0)),
                "categories": result_data.get("categories", []),
                "keywords": result_data.get("keywords", []),
                "status": result_data.get("status", "completed"),
                "timestamp": datetime.utcnow().isoformat(),
                "indexed_at": datetime.utcnow().isoformat()
            }
            
            response = self.client.index(
                index=index_name,
                id=result_data.get("session_id"),  # Use session_id as doc ID for updates
                body=doc,
                refresh=False
            )
            
            logger.info(f"[opensearch] Indexed session result: {result_data.get('session_id')}")
            return True
            
        except Exception as e:
            logger.error(f"[opensearch] Failed to index session result: {e}")
            return False
    
    def bulk_index_matches(self, matches: List[Dict[str, Any]]) -> int:
        """
        Bulk index multiple keyword matches for better performance.
        
        Args:
            matches: List of match dictionaries (same format as index_keyword_match)
            
        Returns:
            Number of successfully indexed matches
        """
        if not self.client or not matches:
            return 0
        
        try:
            index_name = f"{self.INDEX_PREFIX}-matches"
            
            bulk_body = []
            for match in matches:
                # Add metadata action
                bulk_body.append({
                    "index": {
                        "_index": index_name
                    }
                })
                # Add document
                bulk_body.append({
                    "session_id": match.get("session_id"),
                    "url": match.get("url"),
                    "keyword": match.get("keyword"),
                    "category": match.get("category"),
                    "product": match.get("product"),
                    "confidence": float(match.get("confidence", 0)),
                    "source": match.get("source"),
                    "snippet": match.get("snippet", ""),
                    "timestamp": datetime.utcnow().isoformat()
                })
            
            response = self.client.bulk(body=bulk_body, refresh=False)
            
            errors = response.get("errors", False)
            if errors:
                logger.warning(f"[opensearch] Bulk indexing had errors: {response}")
            else:
                logger.info(f"[opensearch] Bulk indexed {len(matches)} matches")
            
            return len(matches) if not errors else 0
            
        except Exception as e:
            logger.error(f"[opensearch] Failed to bulk index matches: {e}")
            return 0
    
    def search_matches(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search for keyword matches in OpenSearch.
        
        Args:
            query: Search query (keyword, URL, category, etc.)
            limit: Maximum results to return
            
        Returns:
            List of matching documents
        """
        if not self.client:
            logger.warning("[opensearch] Client not available")
            return []
        
        try:
            index_name = f"{self.INDEX_PREFIX}-matches"
            
            body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["keyword^2", "category", "product", "url"]
                    }
                },
                "size": limit
            }
            
            response = self.client.search(index=index_name, body=body)
            
            hits = response.get("hits", {}).get("hits", [])
            results = [hit["_source"] for hit in hits]
            
            logger.debug(f"[opensearch] Search found {len(results)} results for '{query}'")
            return results
            
        except Exception as e:
            logger.error(f"[opensearch] Search failed: {e}")
            return []
    
    def create_indices_if_not_exist(self) -> bool:
        """
        Create OpenSearch indices with proper mappings if they don't exist.
        
        Returns:
            True if indices exist or were successfully created
        """
        if not self.client:
            logger.warning("[opensearch] Client not available")
            return False
        
        indices = {
            f"{self.INDEX_PREFIX}-matches": {
                "mappings": {
                    "properties": {
                        "session_id": {"type": "keyword"},
                        "url": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "keyword": {"type": "keyword"},
                        "category": {"type": "keyword"},
                        "product": {"type": "keyword"},
                        "confidence": {"type": "float"},
                        "source": {"type": "keyword"},
                        "snippet": {"type": "text"},
                        "timestamp": {"type": "date"},
                        "detected_at": {"type": "date"}
                    }
                }
            },
            f"{self.INDEX_PREFIX}-results": {
                "mappings": {
                    "properties": {
                        "session_id": {"type": "keyword"},
                        "main_url": {"type": "text"},
                        "total_pages": {"type": "integer"},
                        "total_matches": {"type": "integer"},
                        "categories": {"type": "keyword"},
                        "keywords": {"type": "keyword"},
                        "status": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "indexed_at": {"type": "date"}
                    }
                }
            }
        }
        
        try:
            for index_name, index_config in indices.items():
                if not self.client.indices.exists(index=index_name):
                    self.client.indices.create(index=index_name, body=index_config)
                    logger.info(f"[opensearch] Created index: {index_name}")
                else:
                    logger.debug(f"[opensearch] Index already exists: {index_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"[opensearch] Failed to create indices: {e}")
            return False
