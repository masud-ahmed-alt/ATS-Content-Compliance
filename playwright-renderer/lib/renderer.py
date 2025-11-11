#!/usr/bin/env python3
import os, traceback
from datetime import datetime
from PIL import Image
from playwright.sync_api import sync_playwright, TimeoutError
from utils.helpers import safe_name
from io import BytesIO
from minio import Minio

from config.settings import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
)

# ==========================================================
# 🔹 Initialize MinIO client
# ==========================================================
client = Minio(
    MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_ENDPOINT.startswith("https"),
)

try:
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"[INIT] Created MinIO bucket '{MINIO_BUCKET}'", flush=True)
    else:
        print(f"[INIT] Using MinIO bucket '{MINIO_BUCKET}'", flush=True)
except Exception as e:
    print(f"[ERROR] MinIO init failed: {e}", flush=True)


# ==========================================================
# 🔹 Utility: Upload cropped screenshot to MinIO
# ==========================================================
def _upload_crop_to_minio(url: str, keyword: str, img: Image.Image) -> str | None:
    try:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        safe = safe_name(url)
        fname = f"{safe}_{keyword}_{ts}_{abs(hash(url))}.png"

        buf = BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        client.put_object(
            MINIO_BUCKET,
            fname,
            buf,
            length=len(buf.getvalue()),
            content_type="image/png",
        )

        return f"{MINIO_ENDPOINT}/{MINIO_BUCKET}/{fname}"
    except Exception as e:
        print(f"[minio:upload:error] {url} -> {e}", flush=True)
        return None


# ==========================================================
# 🔹 Screenshot Renderer (with stability and retry)
# ==========================================================
def render_and_screenshot(url: str, keyword: str, max_matches: int = 5) -> dict:
    """
    Opens the page with Playwright and captures cropped screenshots of
    keyword occurrences. Includes stability waits and retry logic.
    """
    print(f"[TASK] Screenshot for {url} | keyword='{keyword}'", flush=True)
    if not keyword:
        return {"url": url, "error": "Keyword required"}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-setuid-sandbox",
                    "--disable-software-rasterizer",
                ],
            )
            ctx = browser.new_context(viewport={"width": 1280, "height": 900})
            page = ctx.new_page()

            try:
                # ✅ Use 'networkidle' for more reliable loading
                page.goto(url, wait_until="networkidle", timeout=45000)
                page.wait_for_timeout(1500)

                dpr = page.evaluate("window.devicePixelRatio || 1")

                # ✅ Retry logic for JS evaluation (if navigation happens)
                rects = None
                for attempt in range(3):
                    try:
                        rects = page.evaluate(
                            """
                            ({kw, maxMatches}) => {
                                const results = [];
                                const keyword = kw.toLowerCase();
                                const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
                                let node;
                                while ((node = walker.nextNode())) {
                                    const txt = node.textContent || "";
                                    if (txt.toLowerCase().includes(keyword)) {
                                        const el = node.parentElement;
                                        if (el) {
                                            const r = el.getBoundingClientRect();
                                            if (r.width > 5 && r.height > 5)
                                                results.push({
                                                    rect: r,
                                                    snippet: txt.trim().slice(0,150)
                                                });
                                        }
                                        if (results.length >= maxMatches) break;
                                    }
                                }
                                return results;
                            }
                            """,
                            {"kw": keyword, "maxMatches": max_matches},
                        )
                        break
                    except Exception as e:
                        print(f"[WARN] Retry {attempt+1}/3 evaluating keyword search: {e}", flush=True)
                        page.wait_for_timeout(1000)

                if not rects:
                    print(f"[INFO] No matches for '{keyword}' on {url}", flush=True)
                    return {
                        "url": url,
                        "keyword": keyword,
                        "matches": [],
                        "total": 0,
                        "message": "Keyword not found",
                    }

                # ✅ Capture full page
                img_bytes = page.screenshot(full_page=True)
                im = Image.open(BytesIO(img_bytes))

                matches_info = []
                for i, r in enumerate(rects, start=1):
                    rect = r["rect"]
                    x = max(int(rect["left"] * dpr - 10), 0)
                    y = max(int(rect["top"] * dpr - 10), 0)
                    w = min(int(rect["width"] * dpr + 20), im.width - x)
                    h = min(int(rect["height"] * dpr + 20), im.height - y)

                    crop = im.crop((x, y, x + w, y + h))
                    uploaded_path = _upload_crop_to_minio(url, keyword, crop)

                    matches_info.append({
                        "snippet": r["snippet"],
                        "screenshot_url": uploaded_path,
                    })

                print(f"[INFO] Uploaded {len(matches_info)} cropped screenshot(s) for '{keyword}'", flush=True)
                return {
                    "url": url,
                    "keyword": keyword,
                    "matches": matches_info,
                    "total": len(matches_info),
                }

            except TimeoutError:
                print(f"[WARN] Timeout loading {url}", flush=True)
                return {"url": url, "keyword": keyword, "error": "Timeout while loading page"}

            except Exception as e:
                print(f"[ERROR] Screenshot failed: {e}\n{traceback.format_exc()}", flush=True)
                return {"url": url, "keyword": keyword, "error": str(e)}

            finally:
                browser.close()

    except Exception as e:
        print(f"[FATAL] Playwright launch failed: {e}\n{traceback.format_exc()}", flush=True)
        return {"url": url, "keyword": keyword, "error": "Playwright launch failed"}


# ==========================================================
# 🔹 HTML Renderer (with stability and retry)
# ==========================================================
def render_html(url: str, timeout: int = 30000) -> dict:
    """
    Fully render page HTML for JS-heavy sites with robust retry and stability logic.
    """
    print(f"[TASK] Rendering HTML for {url}", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-setuid-sandbox",
                "--disable-software-rasterizer",
            ],
        )
        ctx = browser.new_context(viewport={"width": 1280, "height": 900})
        page = ctx.new_page()

        try:
            # ✅ Wait for all requests to finish for stable DOM
            page.goto(url, wait_until="networkidle", timeout=timeout)
            page.wait_for_timeout(1500)

            html = None
            for attempt in range(3):
                try:
                    html = page.content()
                    break
                except Exception as e:
                    print(f"[WARN] Retry {attempt+1}/3 fetching HTML: {e}", flush=True)
                    page.wait_for_timeout(1000)

            if not html:
                raise RuntimeError("Failed to get stable HTML after retries")

            print(f"[INFO] Rendered {url} ({len(html)} chars)", flush=True)
            return {"url": url, "html": html, "error": None}

        except TimeoutError:
            print(f"[WARN] Timeout loading {url}", flush=True)
            return {"url": url, "html": "", "error": "Timeout while loading page"}

        except Exception as e:
            print(f"[ERROR] Render failed: {e}\n{traceback.format_exc()}", flush=True)
            return {"url": url, "html": "", "error": str(e)}

        finally:
            browser.close()
