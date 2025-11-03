#!/usr/bin/env python3
from fastapi import FastAPI, Query
from utils.load_keywords import load_keywords
from lib.renderer import render_and_screenshot, render_html
from playwright.sync_api import sync_playwright

app = FastAPI(title="Playwright Renderer (Optimized)")

def ensure_browser():
    """Launch a Playwright browser safely."""
    return sync_playwright().start().chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--disable-setuid-sandbox",
            "--disable-software-rasterizer",
        ],
    )


# ---------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------

@app.get("/render")
def render_endpoint(url: str = Query(..., description="URL to render HTML content")):
    """Render HTML content (used by analyzer for JS-heavy pages)."""
    return render_html(url)

@app.get("/render-and-screenshot")
def render_and_screenshot_api(
    url: str = Query(..., description="Target webpage URL"),
    keyword: str = Query(..., description="Keyword to capture"),
    max_matches: int = Query(5, description="Maximum cropped screenshots per page")
):
    """Expose render_and_screenshot() from lib/renderer.py via API."""
    return render_and_screenshot(url, keyword, max_matches)


