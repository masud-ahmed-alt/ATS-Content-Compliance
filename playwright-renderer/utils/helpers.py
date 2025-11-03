
from urllib.parse import urlparse

def safe_name(url: str) -> str:
    """Generate filesystem-safe name for URL."""
    u = urlparse(url)
    host = (u.netloc or "unknown").replace(".", "_").replace(":", "_")
    return host[:80]