#!/usr/bin/env python3

import gzip
import json
from fastapi import Request
from core.core_analyzer import process_ingest_payload


# -------------------------------------------------------------------------
# FastAPI Wrapper Endpoint
# -------------------------------------------------------------------------
async def handle_ingest(request: Request):
    """
    Receives gzipped or regular JSON and delegates to process_ingest_payload().
    """
    raw = await request.body()
    if request.headers.get("content-encoding") == "gzip":
        try:
            raw = gzip.decompress(raw)
        except Exception:
            return {"error": "invalid gzip payload"}

    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return {"error": "invalid json"}

    return await process_ingest_payload(payload)
