#!/usr/bin/env python3
import time
import requests
from typing import List, Dict, Any
from config.settings import RENDERER_SS


class ScreenshotResult:
    """Wrapper for Playwright renderer screenshot results."""

    def __init__(self, url: str = "", keyword: str = "", matches: List[Dict[str, Any]] = None, total: int = 0, error: str = ""):
        self.url = url
        self.keyword = keyword
        self.matches = matches or []
        self.total = total
        self.error = error

    @property
    def first_url(self) -> str:
        """Returns the first screenshot URL, if available."""
        if self.matches and isinstance(self.matches, list):
            for m in self.matches:
                if m.get("screenshot_url"):
                    return m["screenshot_url"]
        return ""

    def __repr__(self):
        return f"<ScreenshotResult keyword='{self.keyword}' total={self.total}>"


def capture_screenshot(url: str, keyword: str = None) -> ScreenshotResult:
    """
    Calls the Playwright Renderer (/render-and-screenshot)
    and wraps the result in a ScreenshotResult object.
    """
    endpoint = RENDERER_SS
    params = {"url": url}
    if keyword:
        params["keyword"] = keyword

    print(f"[screenshot] Endpoint: {endpoint} | params={params}", flush=True)

    for attempt in range(3):
        try:
            resp = requests.get(endpoint, params=params, timeout=90)
            print(f"[screenshot] Renderer HTTP {resp.status_code} (try {attempt + 1})", flush=True)

            if resp.status_code != 200:
                time.sleep(2)
                continue

            data = resp.json()
            matches = data.get("matches", [])
            total = len(matches)

            if total == 0:
                print(f"[screenshot] No visual matches for {keyword}", flush=True)
            else:
                print(f"[screenshot] {total} cropped match(es) for {keyword}", flush=True)

            # ✅ Return structured result with .first_url property
            return ScreenshotResult(
                url=url,
                keyword=keyword or "",
                matches=matches,
                total=total,
            )

        except Exception as e:
            print(f"[screenshot:error] {keyword}: {e}", flush=True)
            time.sleep(2)

    # Return a clean error wrapper
    return ScreenshotResult(
        url=url,
        keyword=keyword or "",
        matches=[],
        total=0,
        error="Renderer unreachable after retries"
    )
