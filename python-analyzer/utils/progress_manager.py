"""
progress_manager.py — Real-time ingest progress tracking with WebSocket/SSE support

Provides:
- Session-level progress tracking (pages processed, matches found, etc.)
- In-memory state management for fast updates
- Subscribe/publish pattern for real-time delivery
- Broadcast to connected clients
"""

import logging
import asyncio
import json
from typing import Dict, List, Callable, Optional, Set
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


class IngestProgress:
    """Per-session progress state."""

    def __init__(self, session_id: str, total_urls: int = 0):
        self.session_id = session_id
        self.total_urls = total_urls
        self.urls_processed = 0
        self.urls_failed = 0
        self.total_matches = 0
        self.categories_found: Dict[str, int] = defaultdict(int)
        self.upi_handles_found: List[str] = []
        self.keywords_matched: Dict[str, int] = defaultdict(int)
        self.status = "running"  # running, completed, failed
        self.error_message: Optional[str] = None
        self.started_at = datetime.utcnow()
        self.completed_at: Optional[datetime] = None
        self.current_batch_size = 0
        self.current_batch_matches = 0

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        elapsed = (
            (self.completed_at or datetime.utcnow()) - self.started_at
        ).total_seconds()
        return {
            "session_id": self.session_id,
            "status": self.status,
            "urls_processed": self.urls_processed,
            "urls_total": self.total_urls,
            "urls_failed": self.urls_failed,
            "percentage": (self.urls_processed / self.total_urls * 100) if self.total_urls > 0 else 0,
            "total_matches": self.total_matches,
            "categories": dict(self.categories_found),
            "upi_count": len(self.upi_handles_found),
            "upi_samples": self.upi_handles_found[-5:] if self.upi_handles_found else [],
            "keywords_top": dict(sorted(self.keywords_matched.items(), key=lambda x: -x[1])[:10]),
            "elapsed_seconds": elapsed,
            "current_batch": {
                "size": self.current_batch_size,
                "matches": self.current_batch_matches,
            },
            "error": self.error_message,
            "timestamp": datetime.utcnow().isoformat(),
        }


class ProgressManager:
    """Manages progress tracking for all active ingest sessions."""

    def __init__(self):
        self._sessions: Dict[str, IngestProgress] = {}
        self._subscribers: Dict[str, Set[Callable]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def create_session(self, session_id: str, total_urls: int = 0):
        """Create and track a new ingest session."""
        async with self._lock:
            progress = IngestProgress(session_id, total_urls)
            self._sessions[session_id] = progress
            logger.info(f"[progress:create] session={session_id} total_urls={total_urls}")
            await self._broadcast(session_id, progress.to_dict())

    async def get_session(self, session_id: str) -> Optional[IngestProgress]:
        """Get progress for a session."""
        async with self._lock:
            return self._sessions.get(session_id)

    async def update_urls_processed(self, session_id: str, count: int):
        """Update processed URL count."""
        async with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].urls_processed = count
                await self._broadcast(session_id, self._sessions[session_id].to_dict())

    async def update_batch(self, session_id: str, batch_size: int, matches_in_batch: int):
        """Update current batch being processed."""
        async with self._lock:
            if session_id in self._sessions:
                p = self._sessions[session_id]
                p.current_batch_size = batch_size
                p.current_batch_matches = matches_in_batch
                await self._broadcast(session_id, p.to_dict())

    async def add_match(self, session_id: str, category: str, keyword: str):
        """Record a keyword match."""
        async with self._lock:
            if session_id in self._sessions:
                p = self._sessions[session_id]
                p.total_matches += 1
                p.categories_found[category] += 1
                p.keywords_matched[keyword] += 1
                await self._broadcast(session_id, p.to_dict())

    async def add_upi_handle(self, session_id: str, handle: str):
        """Record a detected UPI handle."""
        async with self._lock:
            if session_id in self._sessions:
                p = self._sessions[session_id]
                if handle not in p.upi_handles_found:
                    p.upi_handles_found.append(handle)
                await self._broadcast(session_id, p.to_dict())

    async def complete_session(self, session_id: str, success: bool = True, error_msg: str = None):
        """Mark session as completed."""
        async with self._lock:
            if session_id in self._sessions:
                p = self._sessions[session_id]
                p.status = "completed" if success else "failed"
                p.error_message = error_msg
                p.completed_at = datetime.utcnow()
                await self._broadcast(session_id, p.to_dict())

    async def subscribe(self, session_id: str, callback: Callable):
        """Subscribe to progress updates for a session."""
        async with self._lock:
            self._subscribers[session_id].add(callback)
            logger.info(f"[progress:subscribe] session={session_id} subscribers={len(self._subscribers[session_id])}")

    async def unsubscribe(self, session_id: str, callback: Callable):
        """Unsubscribe from progress updates."""
        async with self._lock:
            self._subscribers[session_id].discard(callback)
            if not self._subscribers[session_id]:
                del self._subscribers[session_id]
                logger.info(f"[progress:unsubscribe] session={session_id}")

    async def _broadcast(self, session_id: str, state_dict: dict):
        """Broadcast state to all subscribers."""
        if session_id in self._subscribers:
            for callback in self._subscribers[session_id]:
                try:
                    await callback(state_dict)
                except Exception as e:
                    logger.error(f"[progress:broadcast_error] {e}")


# Global instance
_progress_manager: Optional[ProgressManager] = None


def get_progress_manager() -> ProgressManager:
    """Get or create global progress manager."""
    global _progress_manager
    if _progress_manager is None:
        _progress_manager = ProgressManager()
    return _progress_manager
