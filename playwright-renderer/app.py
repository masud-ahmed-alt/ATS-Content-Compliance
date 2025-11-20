# app.py
import base64
import os
import uuid
from typing import Optional

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator
from urllib.parse import urlparse

from lib.renderer import (
    init_browser,
    shutdown_browser,
    render_html,
    render_and_screenshot,
    upload_to_minio,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown."""
    # Startup
    await init_browser()
    yield
    # Shutdown
    await shutdown_browser()

app = FastAPI(title="Playwright Async Renderer", lifespan=lifespan)

# default upload behaviour if not specified in request (env var "true"/"1"/"yes")
MINIO_UPLOAD_DEFAULT = os.environ.get("MINIO_UPLOAD_DEFAULT", "false").lower() in ("1", "true", "yes")


class RenderRequest(BaseModel):
    url: str  # Changed from HttpUrl to str to handle URLs with query parameters
    keyword: Optional[str] = None
    # If present, overrides MINIO_UPLOAD_DEFAULT
    upload: Optional[bool] = None
    # optional max matches to return
    max_matches: Optional[int] = 20
    
    @field_validator('url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate that the URL is well-formed"""
        if not v:
            raise ValueError("URL cannot be empty")
        try:
            parsed = urlparse(v)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError(f"Invalid URL format: {v}")
            return v
        except Exception as e:
            raise ValueError(f"Invalid URL: {v} - {e}")


# Lifespan events are now handled in the lifespan context manager above


@app.post("/render")
async def api_render_html(req: RenderRequest):
    """
    Render a URL and return its HTML content (no screenshot).
    Used for JS-heavy pages where initial fetch doesn't have full content.
    """
    try:
        result = await render_html(str(req.url))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not result.get("ok", False):
        raise HTTPException(status_code=500, detail=result.get("error", "unknown error"))

    return result


@app.post("/render-and-screenshot")
async def api_render(req: RenderRequest):
    # call renderer
    try:
        result = await render_and_screenshot(str(req.url), keyword=req.keyword, max_matches=req.max_matches)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not result.get("ok", False):
        raise HTTPException(status_code=500, detail=result.get("error", "unknown error"))

    # screenshot bytes may be included
    screenshot_bytes = result.get("screenshot")
    if screenshot_bytes:
        result["screenshot_b64"] = base64.b64encode(screenshot_bytes).decode()
        # drop raw bytes to keep JSON friendly
        del result["screenshot"]

    # optionally upload to MinIO if requested or default enabled
    should_upload = req.upload if req.upload is not None else MINIO_UPLOAD_DEFAULT
    if should_upload and screenshot_bytes:
        object_name = f"screenshots/{uuid.uuid4().hex}.png"
        upload_info = await upload_to_minio(screenshot_bytes, object_name)
        result["minio"] = upload_info

    return result
