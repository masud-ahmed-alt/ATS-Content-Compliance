"""
Metrics and monitoring for the analyzer.
Tracks queue sizes, errors, and performance metrics.
"""

import time
import threading
from typing import Dict, Any
from collections import defaultdict
from config.settings import redis_client

_metrics_lock = threading.Lock()
_metrics: Dict[str, Any] = {
    "hits_dropped": 0,
    "screenshots_dropped": 0,
    "queue_overflow_count": 0,
    "screenshot_failures": 0,
    "renderer_timeouts": 0,
    "db_timeouts": 0,
    "minio_errors": 0,
    "total_hits_processed": 0,
    "total_screenshots_processed": 0,
    "last_reset": time.time(),
}

def increment_metric(name: str, value: int = 1):
    """Increment a metric counter."""
    with _metrics_lock:
        _metrics[name] = _metrics.get(name, 0) + value

def set_metric(name: str, value: Any):
    """Set a metric value."""
    with _metrics_lock:
        _metrics[name] = value

def get_metric(name: str, default: Any = 0) -> Any:
    """Get a metric value."""
    with _metrics_lock:
        return _metrics.get(name, default)

def get_all_metrics() -> Dict[str, Any]:
    """Get all metrics."""
    with _metrics_lock:
        return _metrics.copy()

def reset_metrics():
    """Reset all metrics (except counters that should persist)."""
    with _metrics_lock:
        _metrics["last_reset"] = time.time()
        # Keep cumulative counters, reset others
        for key in list(_metrics.keys()):
            if key not in ["total_hits_processed", "total_screenshots_processed", "last_reset"]:
                _metrics[key] = 0

def export_metrics() -> Dict[str, Any]:
    """Export metrics in a format suitable for monitoring."""
    all_metrics = get_all_metrics()
    
    # Calculate rates if possible
    uptime = time.time() - all_metrics.get("last_reset", time.time())
    if uptime > 0:
        all_metrics["hits_per_second"] = all_metrics.get("total_hits_processed", 0) / uptime
        all_metrics["screenshots_per_second"] = all_metrics.get("total_screenshots_processed", 0) / uptime
    
    return all_metrics

