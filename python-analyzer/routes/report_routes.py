from fastapi import APIRouter
from controllers.report_controller import (
    export_hits_controller,
    report_upi_csv_controller,
    report_upi_json_controller,
    list_report_tasks_controller,
    get_report_by_main_url_controller,
)

router = APIRouter(prefix="/report", tags=["reports"])

# -------------------- EXPORT AGGREGATED HITS (from Postgres) --------------------
@router.get("/export")
def export_hits():
    """Export aggregated crawl results from PostgreSQL (hits table) as CSV."""
    return export_hits_controller()


# -------------------- UPI REPORT (CSV) --------------------
@router.get("/upi.csv")
def report_upi_csv():
    """Generate CSV report of discovered UPI handles with domains and counts."""
    return report_upi_csv_controller()


# -------------------- UPI REPORT (JSON) --------------------
@router.get("/upi.json")
def report_upi_json():
    """Return full UPI handle map in JSON format."""
    return report_upi_json_controller()


# -------------------- LIST ALL UNIQUE MAIN URLS --------------------
@router.get("/tasks")
def list_report_tasks():
    """List all unique main URLs from the hits table (grouped)."""
    return list_report_tasks_controller()


# -------------------- GET DETAILS FOR A SPECIFIC MAIN URL --------------------
@router.get("/tasks/{main_url:path}")
def get_report_by_main_url(main_url: str):
    """Retrieve detailed report for a given main_url."""
    return get_report_by_main_url_controller(main_url)
