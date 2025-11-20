"""
Dead Letter Queue (DLQ) for failed hits and screenshots.
Prevents data loss when queues overflow or operations fail.
"""

import json
import time
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict
from config.settings import redis_client

logger = logging.getLogger(__name__)

@dataclass
class FailedHit:
    """Represents a hit that failed to be queued or persisted."""
    task_id: str
    main_url: str
    sub_url: str
    category: str
    matched_keyword: str
    snippet: str
    timestamp: int
    source: str
    confident_score: int
    error: str
    retry_count: int = 0
    created_at: float = 0.0

@dataclass
class FailedScreenshot:
    """Represents a screenshot job that failed."""
    sub_url: str
    keyword: str
    main_url: str
    task_id: str
    error: str
    retry_count: int = 0
    created_at: float = 0.0


class DeadLetterQueue:
    """Redis-based Dead Letter Queue for failed operations."""
    
    HIT_QUEUE_KEY = "dlq:hits"
    SCREENSHOT_QUEUE_KEY = "dlq:screenshots"
    MAX_RETRIES = 5
    TTL_DAYS = 30
    
    def __init__(self):
        self.enabled = redis_client is not None
        if not self.enabled:
            logger.warning("[dlq] Redis unavailable - DLQ disabled. Data loss may occur on queue overflow.")
    
    def enqueue_hit(self, hit: FailedHit) -> bool:
        """Add a failed hit to DLQ."""
        if not self.enabled:
            logger.error(f"[dlq:hit:lost] {hit.sub_url} - {hit.matched_keyword} (Redis unavailable)")
            return False
        
        try:
            hit.created_at = time.time()
            data = json.dumps(asdict(hit), default=str)
            redis_client.lpush(self.HIT_QUEUE_KEY, data)
            redis_client.expire(self.HIT_QUEUE_KEY, self.TTL_DAYS * 24 * 3600)
            logger.warning(f"[dlq:hit:enqueued] {hit.sub_url} - {hit.matched_keyword} (retry_count={hit.retry_count})")
            return True
        except Exception as e:
            logger.error(f"[dlq:hit:error] Failed to enqueue: {e}")
            return False
    
    def enqueue_screenshot(self, screenshot: FailedScreenshot) -> bool:
        """Add a failed screenshot to DLQ."""
        if not self.enabled:
            logger.error(f"[dlq:screenshot:lost] {screenshot.sub_url} - {screenshot.keyword} (Redis unavailable)")
            return False
        
        try:
            screenshot.created_at = time.time()
            data = json.dumps(asdict(screenshot), default=str)
            redis_client.lpush(self.SCREENSHOT_QUEUE_KEY, data)
            redis_client.expire(self.SCREENSHOT_QUEUE_KEY, self.TTL_DAYS * 24 * 3600)
            logger.warning(f"[dlq:screenshot:enqueued] {screenshot.sub_url} - {screenshot.keyword} (retry_count={screenshot.retry_count})")
            return True
        except Exception as e:
            logger.error(f"[dlq:screenshot:error] Failed to enqueue: {e}")
            return False
    
    def dequeue_hit(self) -> Optional[FailedHit]:
        """Retrieve next failed hit from DLQ."""
        if not self.enabled:
            return None
        
        try:
            data = redis_client.rpop(self.HIT_QUEUE_KEY)
            if not data:
                return None
            hit_dict = json.loads(data)
            return FailedHit(**hit_dict)
        except Exception as e:
            logger.error(f"[dlq:hit:dequeue:error] {e}")
            return None
    
    def dequeue_screenshot(self) -> Optional[FailedScreenshot]:
        """Retrieve next failed screenshot from DLQ."""
        if not self.enabled:
            return None
        
        try:
            data = redis_client.rpop(self.SCREENSHOT_QUEUE_KEY)
            if not data:
                return None
            screenshot_dict = json.loads(data)
            return FailedScreenshot(**screenshot_dict)
        except Exception as e:
            logger.error(f"[dlq:screenshot:dequeue:error] {e}")
            return None
    
    def stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        if not self.enabled:
            return {"enabled": False, "hit_queue_size": 0, "screenshot_queue_size": 0}
        
        try:
            hit_size = redis_client.llen(self.HIT_QUEUE_KEY)
            screenshot_size = redis_client.llen(self.SCREENSHOT_QUEUE_KEY)
            return {
                "enabled": True,
                "hit_queue_size": hit_size,
                "screenshot_queue_size": screenshot_size,
            }
        except Exception as e:
            logger.error(f"[dlq:stats:error] {e}")
            return {"enabled": False, "error": str(e)}


# Global DLQ instance
dlq = DeadLetterQueue()

