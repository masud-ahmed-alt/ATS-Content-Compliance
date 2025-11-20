#!/usr/bin/env python3
"""
renderer_integration.py - Integration with Playwright Renderer service

Provides functions to render JS-heavy pages and capture screenshots via the 
Playwright Renderer microservice.
"""

import asyncio
import logging
import requests
from typing import Optional, Dict, Any
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


class RendererClient:
    """Client for interacting with Playwright Renderer service"""
    
    def __init__(self, renderer_url: str, timeout: int = None):
        """
        Initialize renderer client.
        
        Args:
            renderer_url: Base URL of renderer service (e.g., http://renderer:9000)
            timeout: Request timeout in seconds (defaults to 60, configurable via RENDERER_TIMEOUT env var)
        """
        import os
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        self.renderer_url = renderer_url.rstrip("/")
        # Default timeout is 60 seconds, but can be overridden
        self.timeout = timeout or int(os.environ.get("RENDERER_TIMEOUT", "60"))
        
        # Dynamic connection pool: Scale based on available CPU cores
        import os
        _cpu_count = os.cpu_count() or 4
        _pool_connections = min(_cpu_count * 5, 50)  # 5x CPU cores, max 50
        _pool_maxsize = min(_cpu_count * 10, 100)    # 10x CPU cores, max 100
        
        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=_pool_connections,
            pool_maxsize=_pool_maxsize,
            max_retries=Retry(total=2, backoff_factor=0.3)
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def render_html(self, url: str) -> Optional[str]:
        """
        Render a URL and return its HTML content (POST to /render endpoint).
        
        Used for JS-heavy pages where initial fetch doesn't have full content.
        
        Args:
            url: URL to render
            
        Returns:
            Rendered HTML content or None if rendering failed
        """
        try:
            render_url = f"{self.renderer_url}/render"
            logger.debug(f"[renderer] Requesting HTML render: {url}")
            
            # POST request with JSON payload
            resp = self.session.post(
                render_url,
                json={"url": url},
                timeout=self.timeout,
                headers={"Content-Type": "application/json"}
            )
            resp.raise_for_status()
            
            data = resp.json()
            
            if not data.get("ok", False):
                error = data.get("error", "unknown error")
                logger.warning(f"[renderer] Render failed for {url}: {error}")
                return None
            
            html_content = data.get("content")
            
            if html_content:
                logger.info(f"[renderer] Successfully rendered {url} ({len(html_content)} bytes)")
                return html_content
            else:
                logger.warning(f"[renderer] No content returned for {url}")
                return None
                
        except requests.Timeout:
            logger.error(f"[renderer] Timeout rendering {url} (timeout={self.timeout}s)")
            from libs.metrics import increment_metric
            increment_metric("renderer_timeouts")
            return None
        except requests.RequestException as e:
            logger.error(f"[renderer] Failed to render {url}: {e}")
            from libs.metrics import increment_metric
            increment_metric("renderer_timeouts")
            return None
        except Exception as e:
            logger.exception(f"[renderer] Unexpected error rendering {url}: {e}")
            from libs.metrics import increment_metric
            increment_metric("renderer_timeouts")
            return None
    
    def render_and_screenshot(self, url: str, keyword: str, max_matches: int = 5) -> Optional[Dict[str, Any]]:
        """
        Render a URL and capture screenshots of matching keywords.
        
        Used for visual evidence extraction and validation.
        
        Args:
            url: URL to render and screenshot
            keyword: Keyword to highlight in screenshots
            max_matches: Maximum screenshots to capture
            
        Returns:
            Dictionary with screenshots and metadata or None if failed
        """
        try:
            params = {
                "url": url,
                "keyword": keyword,
                "max_matches": max_matches
            }
            render_url = f"{self.renderer_url}/render-and-screenshot?{urlencode(params)}"
            logger.debug(f"[renderer] Requesting screenshot for keyword '{keyword}' on {url}")
            
            resp = self.session.get(render_url, timeout=self.timeout)
            resp.raise_for_status()
            
            data = resp.json()
            screenshots = data.get("screenshots", [])
            
            if screenshots:
                logger.info(f"[renderer] Captured {len(screenshots)} screenshots of '{keyword}' on {url}")
                return data
            else:
                logger.warning(f"[renderer] No screenshots captured for '{keyword}' on {url}")
                return None
                
        except requests.Timeout:
            logger.error(f"[renderer] Timeout capturing screenshots for {url}")
            return None
        except requests.RequestException as e:
            logger.error(f"[renderer] Failed to capture screenshots for {url}: {e}")
            return None
        except Exception as e:
            logger.exception(f"[renderer] Unexpected error capturing screenshots: {e}")
            return None
    
    def close(self):
        """Close the session"""
        self.session.close()


def create_renderer_client(renderer_url: str, timeout: int = None) -> RendererClient:
    """
    Factory function to create renderer client.
    
    Args:
        renderer_url: Base URL of renderer service
        timeout: Request timeout in seconds (defaults to 60, configurable via RENDERER_TIMEOUT env var)
    """
    return RendererClient(renderer_url, timeout)
