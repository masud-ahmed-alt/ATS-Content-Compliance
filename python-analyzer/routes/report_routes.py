import os
import io
import csv
import json
from fastapi import APIRouter, Response, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import select, distinct
from sqlalchemy.orm import Session
from config.settings import UPI_MAP, get_db
from models.hit_model import Hit

router = APIRouter(prefix="/report", tags=["reports"])

# -------------------- EXPORT AGGREGATED HITS (from Postgres) --------------------
@router.get("/export")
def export_hits(db=next(get_db())):
    """
    Export aggregated crawl results from PostgreSQL (hits table) as CSV.
    Columns mirror the aggregated schema you write in flush_master_hits().
    """
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "master_url",
        "sub_urls",
        "total_matches",
        "processed_pages",
        "categories",
        "keywords",
        "sources",
        "snippets",
        "timestamp",
    ])

    try:
        rows = db.execute(select(HitRecord)).scalars().all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    for r in rows:
        # r.sub_urls / r.snippets are JSON; categories/keywords/sources are arrays
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

    csv_bytes = buf.getvalue()
    headers = {"Content-Disposition": 'attachment; filename="hits_export.csv"'}
    return Response(content=csv_bytes, media_type="text/csv", headers=headers)

# -------------------- UPI REPORT (CSV) --------------------
@router.get("/upi.csv")
def report_upi_csv():
    """
    Generate CSV report of discovered UPI handles with domains and counts.
    Still sourced from UPI_MAP JSON (policy/state file).
    """
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

    headers = {"Content-Disposition": 'attachment; filename="upi_report.csv"'}
    return Response(content=out.getvalue(), media_type="text/csv", headers=headers)

# -------------------- UPI REPORT (JSON) --------------------
@router.get("/upi.json")
def report_upi_json():
    """
    Return full UPI handle map in JSON format.
    """
    if not os.path.exists(UPI_MAP):
        return JSONResponse(content={})
    try:
        with open(UPI_MAP, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except json.JSONDecodeError:
        return JSONResponse(content={"error": "UPI map corrupted or invalid JSON."}, status_code=500)
    return JSONResponse(content=data)


# All reports for frontend admin use only.
# No authentication is implemented here; ensure proper security in deployment.
@router.get("/tasks")
def list_report_tasks():
    """
    List all unique task IDs and their corresponding main URLs from the hits table.
    """
    db: Session = next(get_db())
    try:
        # Get unique (task_id, main_url) pairs
        stmt = select(distinct(Hit.task_id), Hit.main_url)
        result = db.execute(stmt).all()

        # Convert to list of dictionaries
        tasks = [{"task_id": row[0], "main_url": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return JSONResponse(content={"tasks": tasks})

@router.get("/tasks/{task_id}")
def get_task_report(task_id: str):
    """
    Retrieve detailed report for a specific task_id.
    Returns grouped field arrays including full MinIO screenshot URLs.
    """
    db: Session = next(get_db())
    try:
        stmt = select(Hit).where(Hit.task_id == task_id)
        result = db.execute(stmt).scalars().all()

        if not result:
            raise HTTPException(status_code=404, detail=f"No report found for task_id: {task_id}")

        # Use main_url from first record
        main_url = result[0].main_url if result else None

        # Group fields column-wise
        reports = {
            "id": [row.id for row in result],
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
            "task_id": task_id,
            "main_url": main_url,
            "total_hits": len(result),
            "reports": reports,
        }
    )
