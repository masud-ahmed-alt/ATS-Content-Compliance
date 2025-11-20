#!/usr/bin/env python3

import gzip
import json
import base64
import asyncio
import logging
import os
import zipfile
import io
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect
from core.core_analyzer import process_ingest_payload

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# FastAPI Wrapper Endpoint
# -------------------------------------------------------------------------
async def handle_ingest(request: Request):
    """
    Handle the /ingest endpoint (generic ingestion).
    Accepts JSON or gzipped JSON and delegates to process_ingest_payload().
    """
    try:
        raw = await request.body()

        # Handle gzip compression
        if request.headers.get("content-encoding") == "gzip":
            try:
                raw = gzip.decompress(raw)
            except Exception as e:
                logger.error(f"[ingest] Failed to decompress gzip: {e}")
                return {"error": "invalid gzip payload", "status": "failed"}, 400

        # Parse JSON payload
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError as e:
            logger.error(f"[ingest] Invalid JSON payload: {e}")
            return {"error": "invalid json"}

        return await process_ingest_payload(payload)

    except Exception as e:
        logger.exception(f"[ingest] Unhandled exception: {e}")
        return {"error": str(e), "status": "failed"}, 500


# -------------------------------------------------------------------------
# Webhook Handler for Go Fetcher Batch Delivery
# -------------------------------------------------------------------------
def _inflate_archive(payload: dict) -> list[dict]:
    """
    Expand a base64-encoded ZIP archive of HTML pages into the legacy pages list format.
    """
    archive_b64 = payload.get("archive_base64")
    if not archive_b64:
        return []

    try:
        archive_bytes = base64.b64decode(archive_b64)
    except Exception as exc:
        logger.error(f"[webhook] Failed to base64 decode archive: {exc}")
        return []

    metadata = payload.get("metadata") or payload.get("archive_metadata") or []
    pages: list[dict] = []

    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
            for meta in metadata:
                url = meta.get("url")
                if not url:
                    continue

                has_html = meta.get("has_html", True)
                file_name = meta.get("file_name") or meta.get("filename")
                html_content = ""

                if has_html and file_name:
                    try:
                        html_bytes = zf.read(file_name)
                        html_content = html_bytes.decode("utf-8", errors="ignore")
                    except KeyError:
                        logger.warning(f"[webhook] Missing file {file_name} in archive for {url}")
                    except Exception as exc:
                        logger.error(f"[webhook] Failed to extract {file_name}: {exc}")

                pages.append({
                    "url": url,
                    "final_url": meta.get("final_url") or url,
                    "html": html_content,
                    "content_type": meta.get("content_type") or "text/html",
                    "error": meta.get("error"),
                })
    except zipfile.BadZipFile as exc:
        logger.error(f"[webhook] Invalid ZIP archive: {exc}")
        return []
    except Exception as exc:
        logger.exception(f"[webhook] Unexpected archive inflate error: {exc}")
        return []

    return pages


async def handle_webhook_task_done(request: Request):
    """
    Webhook endpoint called by Go Fetcher to deliver crawled page batches.

    Receives batches of pages from the crawler and queues them for analysis.

    Payload format:
    {
        "request_id": "uuid-string",
        "batch_id": "uuid-string",
        "pages": [
            {
                "url": "https://example.com",
                "html_content": "<html>...</html>",
                "http_status": 200,
                "is_javascript_heavy": false,
                "fetch_duration_ms": 1500
            },
            ...
        ]
    }

    This handler **schedules processing as a background task** and returns
    a 202 Accepted response immediately so the caller (go-fetcher) does not
    time out while heavy analysis runs.
    """
    try:
        # Read request body with timeout to prevent hanging on large payloads
        # Increased timeout to match go-fetcher's timeout (180s) with buffer
        read_timeout = float(os.environ.get("WEBHOOK_READ_TIMEOUT", "200.0"))
        try:
            raw = await asyncio.wait_for(request.body(), timeout=read_timeout)
        except asyncio.TimeoutError:
            logger.warning(f"[webhook] Timeout reading request body (exceeded {read_timeout}s)")
            return JSONResponse(
                status_code=408, 
                content={"error": f"request body read timeout (exceeded {read_timeout}s)", "status": "failed"}
            )
        except ClientDisconnect:
            # Client disconnected before body was fully read - this is common with large payloads
            # Log but don't treat as a critical error since go-fetcher will retry
            logger.warning("[webhook] Client disconnected while reading request body - will retry")
            return JSONResponse(
                status_code=499,  # 499 Client Closed Request (non-standard but appropriate)
                content={"error": "client disconnected", "status": "retry"}
            )

        # Handle gzip compression
        if request.headers.get("content-encoding") == "gzip":
            try:
                raw = gzip.decompress(raw)
            except Exception as e:
                logger.error(f"[webhook] Failed to decompress gzip: {e}")
                return JSONResponse(status_code=400, content={"error": "invalid gzip payload", "status": "failed"})

        # Parse JSON payload
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError as e:
            logger.error(f"[webhook] Invalid JSON payload: {e}")
            return JSONResponse(status_code=400, content={"error": "invalid json", "status": "failed"})

        # Validate required fields
        request_id = payload.get("request_id") or payload.get("RequestID")
        batch_id = payload.get("batch_id") or payload.get("BatchID")
        pages = payload.get("pages", []) or payload.get("Pages", [])
        
        # If no pages array but archive_base64 exists, extract pages from archive
        if not pages and payload.get("archive_base64"):
            logger.info(f"[webhook] Extracting pages from archive_base64 for batch {batch_id}")
            pages = _inflate_archive(payload)
        
        # If pages array exists but items have wrong format, normalize them
        if pages and len(pages) > 0:
            normalized_pages = []
            pages_without_html = 0
            for idx, page in enumerate(pages):
                # Normalize page format: handle both go-fetcher format and legacy format
                html_content = (
                    page.get("html") or 
                    page.get("HTML") or 
                    page.get("html_content") or 
                    page.get("htmlContent") or 
                    page.get("body") or  # Sometimes HTML is in 'body' field
                    ""
                )
                normalized = {
                    "url": page.get("url") or page.get("URL"),
                    "final_url": page.get("final_url") or page.get("finalUrl") or page.get("url") or page.get("URL"),
                    "html": html_content,
                    "content_type": page.get("content_type") or page.get("contentType") or page.get("ContentType") or "text/html",
                    "error": page.get("error") or page.get("Error"),
                }
                if normalized["url"]:
                    normalized_pages.append(normalized)
                    if not html_content:
                        pages_without_html += 1
                        # Log first few pages without HTML for debugging
                        if pages_without_html <= 3:
                            logger.warning(
                                f"[webhook] Page {idx+1}/{len(pages)} missing HTML: url={normalized['url']}, "
                                f"available_keys={list(page.keys())}, "
                                f"has_error={'error' in page or 'Error' in page}"
                            )
            
            if pages_without_html > 0:
                logger.warning(
                    f"[webhook] Batch {batch_id}: {pages_without_html}/{len(pages)} pages missing HTML content. "
                    f"This may indicate pages need JavaScript rendering or fetch errors occurred."
                )
            pages = normalized_pages

        if not request_id or not batch_id:
            logger.warning(f"[webhook] Missing request_id or batch_id in payload")
            return JSONResponse(status_code=400, content={"error": "missing request_id or batch_id", "status": "failed"})

        if not isinstance(pages, list) or len(pages) == 0:
            logger.warning(f"[webhook] Empty or invalid pages array for batch {batch_id}")
            return JSONResponse(status_code=400, content={"error": "empty pages array", "status": "failed"})

        # Build the analysis payload used by process_ingest_payload
        analysis_payload = {
            "request_id": request_id,
            "batch_id": batch_id,
            "pages": pages,
            "main_url": payload.get("main_url"),
            "batch_num": payload.get("batch_num", 1),
            "is_complete": payload.get("is_complete", True),
            "total_pages": payload.get("total_pages"),
            "stats": payload.get("stats"),
            "source": "go-fetcher",  # Mark as coming from fetcher
        }

        # Schedule background processing (do not await heavy work)
        # Wrap in exception handler to prevent silent failures
        async def _safe_process_with_error_handling():
            """Wrapper to ensure exceptions in background task are logged"""
            try:
                result = await process_ingest_payload(analysis_payload)
                logger.info(
                    f"[webhook:background:complete] batch {batch_id} processed successfully: {result.get('status', 'unknown')}"
                )
                return result
            except Exception as e:
                logger.exception(
                    f"[webhook:background:error] Failed to process batch {batch_id} (request_id: {request_id}): {e}"
                )
                # Don't re-raise - this is a background task and we don't want to crash the server
                return {"error": str(e), "status": "failed", "batch_id": batch_id}
        
        loop = asyncio.get_running_loop()
        task = loop.create_task(_safe_process_with_error_handling())
        # Store task reference to prevent garbage collection before completion
        # Add done callback for additional logging
        def _task_done_callback(t):
            if t.exception():
                logger.error(f"[webhook:background:task:error] Task for batch {batch_id} raised exception: {t.exception()}")
        
        task.add_done_callback(_task_done_callback)

        logger.info(
            f"[webhook] Accepted batch {batch_id} with {len(pages)} pages (request_id: {request_id})"
        )

        # Return 202 Accepted immediately
        return JSONResponse(status_code=202, content={
            "status": "accepted",
            "batch_id": batch_id,
            "pages_received": len(pages),
            "request_id": request_id
        })

    except ClientDisconnect:
        # Handle ClientDisconnect at the top level as well (in case it happens elsewhere)
        logger.warning("[webhook] Client disconnected - request will be retried by go-fetcher")
        return JSONResponse(
            status_code=499,
            content={"error": "client disconnected", "status": "retry"}
        )
    except Exception as e:
        logger.exception(f"[webhook] Unhandled exception scheduling background task: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "status": "failed"})
