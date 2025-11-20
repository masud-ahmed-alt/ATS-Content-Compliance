"""
Standardized retry logic with exponential backoff.
"""
import time
import logging
import random
from dataclasses import dataclass
from typing import Callable, TypeVar, Optional
from .exceptions import RetryableError

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple = (RetryableError, TimeoutError, ConnectionError, OSError)


def retry_with_backoff(
    func: Callable[[], T],
    config: Optional[RetryConfig] = None,
    operation: str = None
) -> T:
    """
    Retry a function with exponential backoff.
    
    Args:
        func: Function to retry (no arguments)
        config: Retry configuration
        operation: Description of operation for logging
        
    Returns:
        Function result
        
    Raises:
        Last exception if all retries exhausted
    """
    if config is None:
        config = RetryConfig()
    
    last_exception = None
    
    for attempt in range(config.max_retries + 1):
        try:
            return func()
        except config.retryable_exceptions as e:
            last_exception = e
            
            if attempt < config.max_retries:
                delay = min(
                    config.initial_delay * (config.exponential_base ** attempt),
                    config.max_delay
                )
                
                if config.jitter:
                    delay = delay * (0.5 + random.random() * 0.5)
                
                op_desc = operation or getattr(func, '__name__', 'operation')
                logger.warning(
                    f"[retry] {op_desc} failed (attempt {attempt + 1}/{config.max_retries + 1}): {e}. "
                    f"Retrying in {delay:.2f}s..."
                )
                
                time.sleep(delay)
            else:
                op_desc = operation or getattr(func, '__name__', 'operation')
                logger.error(
                    f"[retry] {op_desc} failed after {config.max_retries + 1} attempts: {e}"
                )
        except Exception as e:
            # Non-retryable exception, re-raise immediately
            op_desc = operation or getattr(func, '__name__', 'operation')
            logger.error(f"[retry] {op_desc} failed with non-retryable error: {e}")
            raise
    
    # All retries exhausted
    if last_exception:
        raise last_exception
    else:
        raise RuntimeError("Retry exhausted without exception")

