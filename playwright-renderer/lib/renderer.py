# lib/renderer.py
import asyncio
import os
import time
import io
from typing import Optional, Dict, Any, List

from playwright.async_api import async_playwright, Browser, Page
from minio import Minio
from minio.error import S3Error

# Globals
_browser: Optional[Browser] = None
_playwright = None
# concurrency limit (tune with env var)
MAX_CONCURRENCY = int(os.environ.get("RENDERER_CONCURRENCY", "4"))
_semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

# MinIO env config
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "screenshots")

# Allow skipping stealth or other advanced behaviours
STEALTH = os.environ.get("RENDERER_STEALTH", "false").lower() in ("1", "true", "yes")

# Small JS used to find text matches in page and return bounding rects
# Modified to accept a single array argument [keyword, maxMatches] for Playwright evaluate()
_FIND_RECTS_JS = """
(function(args) {
  const keyword = args[0];
  const maxMatches = args[1];
  if (!keyword) return [];
  const kw = keyword.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&');
  const re = new RegExp(kw, 'gi');
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
  const rects = [];
  let node;
  while (node = walker.nextNode()) {
    const text = node.nodeValue;
    if (!text) continue;
    let match;
    while ((match = re.exec(text)) !== null) {
      const start = match.index;
      const end = start + match[0].length;
      try {
        const range = document.createRange();
        range.setStart(node, start);
        range.setEnd(node, end);
        const clientRects = Array.from(range.getClientRects()).map(r => ({
          x: r.x, y: r.y, width: r.width, height: r.height
        }));
        clientRects.forEach(cr => rects.push(cr));
        range.detach && range.detach();
      } catch (err) {
        // ignore ranges we can't read (cross-node complexities)
      }
      if (rects.length >= maxMatches) return rects;
    }
  }
  return rects;
})
"""

async def init_browser() -> None:
    """
    Initialize Playwright browser singleton.
    """
    global _browser, _playwright
    if _browser is not None:
        return

    _playwright = await async_playwright().start()
    # Use Chromium by default
    _browser = await _playwright.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
    # Create bucket in MinIO if needed (lazy)
    return

async def shutdown_browser() -> None:
    global _browser, _playwright
    try:
        if _browser:
            await _browser.close()
            _browser = None
        if _playwright:
            await _playwright.stop()
            _playwright = None
    except Exception:
        pass

async def upload_to_minio(png_bytes: bytes, object_name: str) -> Dict[str, Any]:
    """
    Uploads bytes to MinIO. Returns dictionary with bucket and object info.
    If MINIO_ENDPOINT is not set, returns empty dict.
    """
    if not MINIO_ENDPOINT:
        return {"ok": False, "error": "minio not configured"}

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_ENDPOINT.startswith("https")
    )

    # ensure bucket exists
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
    except Exception as e:
        return {"ok": False, "error": f"minio bucket error: {e}"}

    try:
        client.put_object(MINIO_BUCKET, object_name, io.BytesIO(png_bytes), length=len(png_bytes))
        url = f"{MINIO_ENDPOINT.rstrip('/')}/{MINIO_BUCKET}/{object_name}"
        return {"ok": True, "bucket": MINIO_BUCKET, "object": object_name, "url": url}
    except S3Error as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def render_html(url: str) -> Dict[str, Any]:
    """
    Render the page at URL and return its HTML content (no screenshot).
    Used for JS-heavy pages where initial fetch doesn't have full content.
    """
    await init_browser()
    start = time.time()

    async with _semaphore:
        if _browser is None:
            return {"ok": False, "error": "browser not initialized"}

        # create context and page
        ctx = await _browser.new_context(viewport={"width": 1280, "height": 900}, locale="en-US")
        page: Page = await ctx.new_page()
        try:
            goto_timeout = int(os.environ.get("RENDERER_GOTO_TIMEOUT", "120000"))
            wait_until = os.environ.get("RENDERER_WAIT_UNTIL", "load")
            await page.goto(url, wait_until=wait_until, timeout=goto_timeout)
            await asyncio.sleep(0.5)

            # get rendered HTML content
            html_content = await page.content()
            elapsed = int((time.time() - start) * 1000)

            return {
                "ok": True,
                "url": url,
                "content": html_content,
                "time_ms": elapsed,
            }
        except Exception as e:
            print(f"[renderer:error] render_html {url} -> {e}", flush=True)
            return {"ok": False, "error": str(e)}
        finally:
            try:
                await ctx.close()
            except Exception:
                pass


async def render_and_screenshot(url: str, keyword: Optional[str], max_matches: int = 5) -> Dict[str, Any]:
    """
    Render the page at URL, find bounding rects for keyword, return screenshot bytes and boxes.
    """
    await init_browser()
    start = time.time()

    async with _semaphore:
        if _browser is None:
            return {"ok": False, "error": "browser not initialized"}

        # create context and page
        ctx = await _browser.new_context(viewport={"width": 1280, "height": 900}, locale="en-US")
        page: Page = await ctx.new_page()
        try:
            # navigate
            # Configurable timeout (default 120 seconds for slow-loading pages)
            goto_timeout = int(os.environ.get("RENDERER_GOTO_TIMEOUT", "120000"))
            # Use "load" instead of "networkidle" for pages with continuous network activity
            # "load" waits for the load event, which is more reliable for slow pages
            wait_until = os.environ.get("RENDERER_WAIT_UNTIL", "load")
            await page.goto(url, wait_until=wait_until, timeout=goto_timeout)
            # small delay to allow dynamic content to settle (tweak if needed)
            await asyncio.sleep(0.5)  # Increased from 0.3s to 0.5s for better content rendering

            # find rects for keyword
            boxes = []
            if keyword:
                try:
                    # Playwright evaluate: pass arguments as a list
                    # The JS function receives the list as a single argument
                    boxes = await page.evaluate(
                        _FIND_RECTS_JS,
                        [keyword, max_matches]
                    )
                except Exception as e:
                    print(f"[renderer:boxes:error] {url} -> {e}", flush=True)

            # take full page screenshot (png bytes)
            screenshot_bytes = await page.screenshot(full_page=True)
            elapsed = int((time.time() - start) * 1000)

            return {
                "ok": True,
                "url": url,
                "keyword": keyword,
                "matches": len(boxes),
                "boxes": boxes,
                "time_ms": elapsed,
                "screenshot": screenshot_bytes,
            }
        except Exception as e:
            print(f"[renderer:error] render_and_screenshot {url} -> {e}", flush=True)
            return {"ok": False, "error": str(e)}
        finally:
            try:
                await ctx.close()
            except Exception:
                pass
