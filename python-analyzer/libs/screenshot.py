"""
Screenshot capture using renderer service.
Refactored to use common utilities.
"""
import os
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from libs.common.config import get_config
from libs.common.retry import retry_with_backoff, RetryConfig
from libs.common.resource_pools import calculate_pool_size
from libs.common.exceptions import RetryableError, TimeoutError

logger = logging.getLogger(__name__)

# Initialize session with dynamic pool sizing
_pool_connections, _pool_maxsize = calculate_pool_size(
    multiplier=5, max_size=50,
    overflow_multiplier=10, max_overflow=100
)

_screenshot_session = requests.Session()
_adapter = HTTPAdapter(
    pool_connections=_pool_connections,
    pool_maxsize=_pool_maxsize,
    max_retries=Retry(total=2, backoff_factor=0.3)
)
_screenshot_session.mount("http://", _adapter)
_screenshot_session.mount("https://", _adapter)


def capture_screenshot(url: str, keyword: str):
    """
    Sends a POST request to the renderer to capture a screenshot
    only for matched keyword area.
    Refactored to use centralized config and retry utilities.
    """
    config = get_config()
    renderer_config = config.renderer
    
    endpoint = renderer_config.screenshot_endpoint
    timeout = int(os.environ.get("SCREENSHOT_TIMEOUT", "90"))

    json_payload = {
        "url": url,
        "keyword": keyword,
        "max_matches": 5,
        "upload": False
    }

    logger.debug(f"[screenshot] POST {endpoint} | payload={json_payload}")

    def _capture():
        try:
            resp = _screenshot_session.post(
                endpoint,
                json=json_payload,
                timeout=timeout
            )
            resp.raise_for_status()
            data = resp.json()

            if not data.get("matches"):
                logger.debug(f"[screenshot:no-match] {url} {keyword}")

            return data
        except requests.exceptions.Timeout as e:
            raise TimeoutError(f"Screenshot timeout after {timeout}s", service="renderer", timeout=timeout)
        except requests.exceptions.RequestException as e:
            raise RetryableError(f"Screenshot request failed: {e}", service="renderer")

    try:
        retry_config = RetryConfig(max_retries=3, initial_delay=1.0)
        return retry_with_backoff(
            _capture,
            config=retry_config,
            operation=f"screenshot capture for {url}"
        )
    except Exception as e:
        logger.error(f"[screenshot:fatal] {keyword}: {e}")
        return None
