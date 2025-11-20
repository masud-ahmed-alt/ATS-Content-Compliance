"""
Centralized resource pool size calculations.
"""
import os
from typing import Tuple


def calculate_pool_size(
    multiplier: int = 8,
    max_size: int = 64,
    overflow_multiplier: int = None,
    max_overflow: int = None
) -> Tuple[int, int]:
    """
    Calculate resource pool size based on CPU cores.
    
    Args:
        multiplier: Multiplier for pool size (default: 8x CPU cores)
        max_size: Maximum pool size
        overflow_multiplier: Multiplier for overflow (default: multiplier * 0.75)
        max_overflow: Maximum overflow size
        
    Returns:
        Tuple of (pool_size, max_overflow)
    """
    cpu_count = os.cpu_count() or 4
    
    pool_size = min(cpu_count * multiplier, max_size)
    
    if overflow_multiplier is None:
        overflow_multiplier = int(multiplier * 0.75)
    if max_overflow is None:
        max_overflow = int(max_size * 0.75)
    
    overflow_size = min(cpu_count * overflow_multiplier, max_overflow)
    
    return pool_size, overflow_size


def calculate_queue_size(multiplier: int = 500, max_size: int = 4000) -> int:
    """Calculate queue size based on CPU cores."""
    cpu_count = os.cpu_count() or 4
    return min(cpu_count * multiplier, max_size)


def calculate_worker_count(
    multiplier: int = 8,
    max_workers: int = 50,
    min_workers: int = 1
) -> int:
    """Calculate worker thread/process count based on CPU cores."""
    cpu_count = os.cpu_count() or 4
    return max(min_workers, min(cpu_count * multiplier, max_workers))

