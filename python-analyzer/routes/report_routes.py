import os
import io
import csv
import json
import urllib.parse
from fastapi import APIRouter, Response, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import select, distinct
from sqlalchemy.orm import Session
from config.settings import UPI_MAP, get_db
from models.hit_model import Hit

router = APIRouter(prefix="/report", tags=["reports"])

# -------------------- EXPORT AGGREGATED HITS (from Postgres) --------------------
@router.get("/export")
def export_hits():
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


# -------------------- UPI REPORT (CSV) --------------------
@router.get("/upi.csv")
def report_upi_csv():
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


# -------------------- UPI REPORT (JSON) --------------------
@router.get("/upi.json")
def report_upi_json():
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


# -------------------- LIST ALL UNIQUE MAIN URLS --------------------
@router.get("/tasks")
def list_report_tasks():
    """List all unique main URLs from the hits table (grouped)."""
    try:
        db: Session = next(get_db())  # ✅ Lazy session creation
        stmt = select(distinct(Hit.main_url))
        result = db.execute(stmt).scalars().all()
        tasks = [{"main_url": url} for url in result if url]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return JSONResponse(content={"tasks": tasks})


# -------------------- GET DETAILS FOR A SPECIFIC MAIN URL --------------------
@router.get("/tasks/{main_url:path}")
def get_report_by_main_url(main_url: str):
    """Retrieve detailed report for a given main_url."""
    try:
        db: Session = next(get_db())  # ✅ Lazy session creation
        decoded_url = urllib.parse.unquote(main_url)
        stmt = select(Hit).where(Hit.main_url == decoded_url)
        result = db.execute(stmt).scalars().all()

        if not result:
            raise HTTPException(status_code=404, detail=f"No report found for main_url: {decoded_url}")

        reports = {
            "id": [row.id for row in result],
            "task_id": [row.task_id for row in result],
            "sub_url": [row.sub_url for row in result],
            "category": [row.category for row in result],
            "matched_keyword": [row.matched_keyword for row in result],
            "snippet": [row.snippet for row in result],
            "screenshot_url": [row.screenshot_path or "" for row in result],
            "timestamp": [row.timestamp for row in result],
            "source": [row.source for row in result],
            "confident_score": [row.confident_score for row in result],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return JSONResponse(
        content={
            "main_url": decoded_url,
            "total_hits": len(result),
            "reports": reports,
        }
    )
