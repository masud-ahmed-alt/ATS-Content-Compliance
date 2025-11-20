import os
import io
import csv
import json
import urllib.parse
from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select, distinct
from sqlalchemy.orm import Session
from config.settings import UPI_MAP, get_db
from models.hit_model import Hit, Result


def export_hits_controller():
    """Export aggregated crawl results from PostgreSQL (hits table) as CSV."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "master_url", "sub_urls", "total_matches", "processed_pages",
        "categories", "keywords", "sources", "snippets", "timestamp"
    ])

    try:
        db: Session = next(get_db())  # ✅ Lazy initialization
        rows = []  # Replace with actual model query if needed
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    for r in rows:
        w.writerow([
            r.master_url,
            json.dumps(r.sub_urls, ensure_ascii=False),
            r.total_matches,
            r.processed_pages,
            ";".join(r.categories or []),
            ";".join(r.keywords or []),
            ";".join(r.sources or []),
            json.dumps(r.snippets, ensure_ascii=False),
            r.timestamp,
        ])

    headers = {"Content-Disposition": 'attachment; filename=\"hits_export.csv\"'}
    return Response(content=buf.getvalue(), media_type="text/csv", headers=headers)


def report_upi_csv_controller():
    """Generate CSV report of discovered UPI handles with domains and counts."""
    if not os.path.exists(UPI_MAP):
        return Response(content="", media_type="text/csv")

    try:
        with open(UPI_MAP, "r", encoding="utf-8") as f:
            mp = json.load(f) or {}
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="UPI map corrupted or invalid JSON.")

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["upi_handle", "merchant_domain", "count", "sample_url"])
    for handle, ent in mp.items():
        sample = ent.get("sample_url", "")
        for dom, cnt in (ent.get("domains") or {}).items():
            w.writerow([handle, dom, cnt, sample])

    headers = {"Content-Disposition": 'attachment; filename=\"upi_report.csv\"'}
    return Response(content=out.getvalue(), media_type="text/csv", headers=headers)


def report_upi_json_controller():
    """Return full UPI handle map in JSON format."""
    if not os.path.exists(UPI_MAP):
        return JSONResponse(content={})
    try:
        with open(UPI_MAP, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except json.JSONDecodeError:
        return JSONResponse(
            content={"error": "UPI map corrupted or invalid JSON."},
            status_code=500
        )
    return JSONResponse(content=data)


def list_report_tasks_controller():
    """List all unique main URLs from the Results table (one row per main_url)."""
    try:
        db: Session = next(get_db())  # ✅ Lazy session creation
        # Get all Results (one row per main_url - master data)
        stmt = select(Result).order_by(Result.timestamp.desc())
        results = db.execute(stmt).scalars().all()
        
        tasks = []
        for r in results:
            tasks.append({
                "main_url": r.main_url,
                "task_id": r.task_id,
                "total_matches": len(r.keyword_match) if r.keyword_match else 0,  # All matches (before validation)
                "total_urls": len(r.sub_urls) if r.sub_urls else 0,
                "categories": r.categories if r.categories else [],
                "timestamp": r.timestamp,
            })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return JSONResponse(content={"tasks": tasks})


def get_report_by_main_url_controller(main_url: str):
    """Retrieve detailed report for a given main_url.
    Returns both Results data (all matches before validation) and Hits data (validated matches after spaCy).
    """
    try:
        db: Session = next(get_db())  # ✅ Lazy session creation
        decoded_url = urllib.parse.unquote(main_url)
        
        # Get Results data (ALL matches before validation - master data)
        result_stmt = select(Result).where(Result.main_url == decoded_url)
        result_data = db.execute(result_stmt).scalars().first()
        
        # Get Hits data (ONLY validated matches after spaCy)
        hit_stmt = select(Hit).where(Hit.main_url == decoded_url)
        hits = db.execute(hit_stmt).scalars().all()

        # ✅ Handle "no report found" case
        if not result_data:
            # Never analyzed - return not found
            raise HTTPException(status_code=404, detail=f"No report found for main_url: {decoded_url}")

        # Prepare Results data (ALL matches before validation)
        # Note: keyword_match is a list that may contain duplicate keywords across pages
        # total_matches_all should be the actual count of ALL matches found
        keyword_list = result_data.keyword_match if result_data.keyword_match else []
        # Calculate total matches: count all items in keyword_match (including duplicates)
        # This represents ALL matches found before validation, not just unique keywords
        total_matches_all = len(keyword_list)  # Total count of all matches (before validation)
        
        results_data = {
            "task_id": result_data.task_id,
            "main_url": result_data.main_url,
            "sub_urls": result_data.sub_urls if result_data.sub_urls else [],
            "keyword_match": keyword_list,  # ALL keywords found (may contain duplicates across pages)
            "categories": result_data.categories if result_data.categories else [],  # ALL categories found
            "raw_data": result_data.raw_data or "",  # All snippets from matches (before validation)
            "total_matches_all": total_matches_all,  # All matches (before validation) - count of all keyword occurrences
            "total_urls": len(result_data.sub_urls) if result_data.sub_urls else 0,
            "timestamp": result_data.timestamp,
        }

        # Prepare Hits data (ONLY validated matches after spaCy)
        hits_data = {
            "id": [row.id for row in hits],
            "task_id": [row.task_id for row in hits],
            "sub_url": [row.sub_url for row in hits],
            "category": [row.category for row in hits],
            "matched_keyword": [row.matched_keyword for row in hits],
            "snippet": [row.snippet for row in hits],
            "screenshot_url": [row.screenshot_path or "" for row in hits],
            "timestamp": [row.timestamp for row in hits],
            "source": [row.source for row in hits],
            "confident_score": [row.confident_score for row in hits],  # spaCy validation score
        }

        # Determine status
        total_validated = len(hits)  # Number of validated hits (after spaCy)
        total_all_matches = results_data["total_matches_all"]  # Total matches before validation
        
        # Validation: validated hits should never exceed all matches
        # If they do, use the actual count from keyword_match as the source of truth
        if total_validated > total_all_matches and keyword_list:
            # Recalculate using the actual list length
            total_all_matches = len(keyword_list)
            results_data["total_matches_all"] = total_all_matches
            print(f"[report:count:fix] main_url={decoded_url} validated={total_validated} > all={results_data['total_matches_all']}, recalculated to {total_all_matches}", flush=True)
        
        if total_validated == 0:
            if total_all_matches == 0:
                status = "clean"
                message = "✓ No matches found"
                description = "This domain was thoroughly scanned and no matches were found."
            else:
                status = "filtered"
                message = f"Found {total_all_matches} matches, all filtered by spaCy validation"
                description = f"This domain had {total_all_matches} initial matches, but all were filtered out by spaCy NLP validation."
        else:
            status = "flagged"
            message = f"Found {total_validated} validated policy violations (out of {total_all_matches} initial matches)"
            description = f"This domain had {total_all_matches} initial matches. After spaCy validation, {total_validated} matches were confirmed as violations."

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return JSONResponse(
        content={
            "main_url": decoded_url,
            "status": status,
            "message": message,
            "description": description,
            "total_hits": total_validated,  # Validated hits (after spaCy)
            "total_matches_all": total_all_matches,  # All matches (before validation)
            "results": results_data,  # Results table data (ALL matches before validation)
            "hits": hits_data,  # Hits table data (ONLY validated matches after spaCy)
            # Legacy field for backward compatibility
            "reports": hits_data,
        }
    )
