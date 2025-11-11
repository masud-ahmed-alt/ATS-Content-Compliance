"""
ingest_controller_v2.py — UPDATED ingest controller with progress tracking integration

This is a guide showing how to update ingest_controller.py to use the new progress manager
and persist data to the full BCNF schema.
"""

import gzip
import json
import uuid
import logging
from fastapi import Request
from core.core_analyzer import process_ingest_payload
from utils.progress_manager import get_progress_manager
from models.database_init import get_db_session, log_audit
from models.database_models import CrawlSession, KeywordMatch, UPIHandle, ProductCategory

logger = logging.getLogger(__name__)


async def handle_ingest(request: Request):
    """
    Enhanced ingest handler with progress tracking and database persistence.
    
    Expected Payload:
    {
        "session_id": "optional-session-id",  # auto-generated if not provided
        "urls": ["https://example.com", ...],
        "total_urls": 100  # hint for progress
    }
    """
    raw = await request.body()
    if request.headers.get("content-encoding") == "gzip":
        try:
            raw = gzip.decompress(raw)
        except Exception as e:
            logger.error(f"[ingest:gzip_error] {e}")
            return {"error": "invalid gzip payload"}

    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError as e:
        logger.error(f"[ingest:json_error] {e}")
        return {"error": "invalid json"}

    # Extract or generate session_id
    session_id = payload.get("session_id") or str(uuid.uuid4())
    total_urls = payload.get("total_urls", len(payload.get("urls", [])))
    
    logger.info(f"[ingest:start] session={session_id} total_urls={total_urls}")

    # Initialize progress tracking
    progress_mgr = get_progress_manager()
    await progress_mgr.create_session(session_id, total_urls)
    
    # Create database record for this crawl session
    db = None
    crawl_session_id = None
    try:
        with get_db_session() as db:
            crawl = CrawlSession(
                session_id=session_id,
                status="running",
                total_pages=total_urls,
                metadata_json={"payload_keys": list(payload.keys())}
            )
            db.add(crawl)
            db.flush()
            crawl_session_id = crawl.id
            
        log_audit(session_id, "INFO", "Crawl session started", {"total_urls": total_urls})
    except Exception as e:
        logger.error(f"[ingest:db_error] {e}")
        await progress_mgr.complete_session(session_id, success=False, error_msg=str(e))
        return {"error": "database error", "session_id": session_id}

    try:
        # Process payload (this is where the analyzer runs)
        # We'll pass progress_mgr and session_id so core_analyzer can report back
        result = await process_ingest_payload_with_progress(
            payload, 
            session_id, 
            progress_mgr
        )

        # Update database record with results
        with get_db_session() as db:
            crawl = db.query(CrawlSession).filter_by(session_id=session_id).first()
            if crawl:
                crawl.status = "completed"
                crawl.pages_processed = result.get("pages_processed", 0)
                crawl.total_matches = result.get("total_matches", 0)
                crawl.categories_found = result.get("categories_found", {})
                
            log_audit(session_id, "INFO", "Crawl completed", result)

        # Mark progress as complete
        await progress_mgr.complete_session(session_id, success=True)
        
        logger.info(f"[ingest:complete] session={session_id}")
        return {
            "session_id": session_id,
            "status": "completed",
            "result": result
        }

    except Exception as e:
        logger.error(f"[ingest:error] session={session_id} error={e}", exc_info=True)
        
        # Update database with error
        with get_db_session() as db:
            crawl = db.query(CrawlSession).filter_by(session_id=session_id).first()
            if crawl:
                crawl.status = "failed"
                crawl.error_message = str(e)
                
            log_audit(session_id, "ERROR", "Crawl failed", {"error": str(e)})

        # Update progress
        await progress_mgr.complete_session(session_id, success=False, error_msg=str(e))
        
        return {
            "error": str(e),
            "session_id": session_id,
            "status": "failed"
        }


async def process_ingest_payload_with_progress(payload, session_id, progress_mgr):
    """
    Wrapper around core analyzer that tracks progress and persists results.
    
    This function:
    1. Calls process_ingest_payload() from core_analyzer
    2. For each match detected, updates progress and stores in DB
    3. Returns aggregated results
    """
    
    # Collect results
    total_matches = 0
    categories_found = {}
    pages_processed = 0
    upi_handles_found = []
    
    urls = payload.get("urls", [])
    
    # Process each URL (or batch, depending on your analyzer implementation)
    for idx, url in enumerate(urls):
        try:
            # Process this URL through the analyzer
            # (This is where your existing core_analyzer logic runs)
            
            # Simulate processing (replace with actual analyzer call)
            page_matches = await analyze_page(url, session_id)
            
            pages_processed += 1
            
            # Update progress
            await progress_mgr.update_urls_processed(session_id, pages_processed)
            
            # Store matches in database
            for match in page_matches:
                with get_db_session() as db:
                    keyword_match = KeywordMatch(
                        session_id=session_id,
                        matched_keyword=match.get("keyword"),
                        match_snippet=match.get("snippet", ""),
                        source=match.get("source", "regex"),  # regex, fuzzy, qr, ocr
                        confidence_score=match.get("confidence", 1.0),
                        category_id=match.get("category_id"),
                        product_id=match.get("product_id"),
                    )
                    db.add(keyword_match)
                
                # Update progress
                category = match.get("category", "unknown")
                keyword = match.get("keyword", "unknown")
                await progress_mgr.add_match(session_id, category, keyword)
                
                total_matches += 1
                categories_found[category] = categories_found.get(category, 0) + 1
            
            # Extract UPI handles if found
            upi_list = match.get("upi_handles", [])
            for upi in upi_list:
                with get_db_session() as db:
                    upi_handle = UPIHandle(
                        session_id=session_id,
                        handle=upi,
                        main_url=url,
                        detection_method="regex",  # or "ocr", "qr"
                    )
                    db.add(upi_handle)
                
                await progress_mgr.add_upi_handle(session_id, upi)
                upi_handles_found.append(upi)
            
        except Exception as e:
            logger.error(f"[ingest:page_error] url={url} error={e}")
            continue
    
    return {
        "pages_processed": pages_processed,
        "total_matches": total_matches,
        "categories_found": categories_found,
        "upi_count": len(upi_handles_found),
        "upi_samples": upi_handles_found[:10],
    }


async def analyze_page(url: str, session_id: str):
    """
    Placeholder: Replace with actual core_analyzer page processing.
    Should return list of matches: [{"keyword": "...", "snippet": "...", ...}]
    """
    # This is where you call your existing analyzer logic
    # For now, a stub:
    return []


# ============================================================================
# INTEGRATION STEPS FOR YOUR ACTUAL INGEST CONTROLLER
# ============================================================================
"""
1. Copy this file as ingest_controller_v2.py next to ingest_controller.py

2. Update your ingest_routes.py:
   from controllers.ingest_controller_v2 import handle_ingest

3. In your core_analyzer.py, update process_ingest_payload to accept callbacks:
   async def process_ingest_payload(payload, on_match=None, on_upi=None, ...):
       # For each match:
       if on_match:
           await on_match(category, keyword)
       if on_upi:
           await on_upi(handle)

4. Update your database models to use the provided database_models.py:
   - Replace your Hit model with KeywordMatch
   - Add CrawlSession model
   - Add UPIHandle model
   - etc.

5. Test:
   curl -X POST http://localhost:8000/ingest \\
     -H "Content-Type: application/json" \\
     -d '{
       "session_id": "test-1",
       "urls": ["https://example.com"],
       "total_urls": 1
     }'

6. Check progress:
   curl http://localhost:8000/progress/status/test-1

7. Connect frontend:
   <ProgressTracker sessionId="test-1" />

Done! Your ingest pipeline now has real-time progress tracking and full database persistence.
"""
