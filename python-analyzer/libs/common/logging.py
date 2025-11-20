"""
Standardized logging setup.
"""
import logging
import sys
from typing import Optional


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    service_name: str = "analyzer"
) -> None:
    """
    Setup standardized logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        format_string: Custom format string
        service_name: Service name for log messages
    """
    if format_string is None:
        format_string = (
            f"[{service_name}] %(asctime)s - %(name)s - %(levelname)s - "
            "%(message)s"
        )
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ],
        force=True  # Override existing configuration
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger with standardized name."""
    return logging.getLogger(name)

