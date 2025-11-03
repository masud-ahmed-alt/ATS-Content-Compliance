ATS Starter Repository (v4)
===========================

Advanced Threat Surface (ATS) – a modular web content intelligence stack for detecting suspicious or illegal activity (e.g., piracy, payment abuse, or counterfeit listings) through large-scale crawling and analysis.


🚀 Overview
------------

The ATS Starter stack includes four main services:

Service | Port | Description
--------|------|-------------
Go Fetcher | 8080 | Bulk URL crawler and queue dispatcher (supports .xlsx / .csv uploads)
Python Analyzer | 8000 | Core analysis engine – regex, alias, semantic, OCR, QR, and UPI mapping
OpenSearch | 9200 | Full-text search and indexing backend for hit storage
OpenSearch Dashboards | 5601 | UI for browsing indexed matches and domain stats
Playwright Renderer | 9000 | Headless JS-rendering service for dynamic pages (auto-escalation ready)
MinIO (S3) | 7001 | Object storage backend for screenshots and artifacts


⚡ Quick Start
--------------

1. Run the stack:

   docker compose up --build

2. Once running:

   Interface | URL | Credentials
   -----------|-----|-------------
   Frontend UI | http://localhost:5173 | admin / 1234
   MinIO Console | http://localhost:7001 | admin / minioadmin
   OpenSearch Dashboards | http://localhost:5601 | default user


💡 Using the Frontend
---------------------

1. Open http://localhost:5173
2. Login with admin / 1234
3. Upload a .xlsx or .csv file containing URLs (one per line or cell)
4. Each URL becomes a queued crawl task in the Go Fetcher
5. The Analyzer automatically processes completed crawl batches
6. View results in OpenSearch Dashboards or export via API


🧰 Available Services
---------------------

🔹 Go Fetcher (:8080)
---------------------

Endpoints:
- POST /fetch — submit a list of URLs for crawling
- GET /tasks — list recent tasks
- GET /task/{id} — get task details (without HTML)

Features:
- Supports bulk upload (default: up to 1000 URLs per batch)
- Crawls each domain (same-host BFS, configurable depth)
- Streams results to analyzer once complete
- Avoids HTML storage in Redis for performance

Key Environment Variables:
  MAX_WORKERS=10
  PAGE_LIMIT=1000
  PER_TASK_FETCH_CONCURRENCY=5
  FETCH_TIMEOUT=15s
  ANALYZER_ENDPOINT=http://python-analyzer:8000/ingest


🔹 Python Analyzer (:8000)
--------------------------

Core Capabilities:
- Regex + alias + semantic keyword detection
- OCR and QR decoding (UPI normalization)
- JS-render fallback via Playwright
- Auto-escalation of JS-heavy domains
- OpenSearch + DB persistence of all hits
- Screenshot capture via Renderer service

Supported APIs:
  POST /ingest             # Receive batch payloads from Go Fetcher
  GET  /export             # Export all matches as CSV
  GET  /report/upi.csv     # Export UPI handle-domain mapping

Environment Tuning:
  MAX_IMGS=10
  MAX_IMG_BYTES=2097152
  FUZZ_THRESHOLD=0.8
  JS_ESCALATE_THRESHOLD=2
  USE_SEMANTIC=true


🔹 Renderer (:9000)
-------------------

GET /render?url=...
- Uses Playwright Chromium headless rendering
- Returns fully hydrated HTML or screenshots
- Auto-managed by analyzer (triggered for JS-heavy sites)


🔹 OpenSearch & Dashboards
--------------------------

OpenSearch:
- Stores indexed matches (illegal_hits index)

Dashboards:
- Import the predefined visualization set:
  dashboards/objects.ndjson
- Includes:
  - Index pattern
  - Hit viewer
  - Category filters
  - Domain summary tables


⚙️ Policy Management (Playwright Domains)
----------------------------------------

When certain domains repeatedly require JS rendering to detect content, they are auto-added to a Playwright allowlist stored at:

  /data/playwright_domains.txt

Manual Control via API:
Operation | Example
-----------|---------
List domains | curl http://localhost:8000/policy/playwright-domains
Add domain | curl -X POST -H "Content-Type: application/json" -d '{"domain":"example.com"}' http://localhost:8000/policy/playwright-domains
Remove domain | curl -X DELETE http://localhost:8000/policy/playwright-domains/example.com


🔁 Auto-Escalation Logic
------------------------

When a domain yields matches only after JS rendering and this happens ≥ JS_ESCALATE_THRESHOLD (default 2) times, the domain is permanently added to the Playwright list.


📤 Export & Reports
-------------------

Description | Command
-------------|---------
Export all hits | curl http://localhost:8000/export > hits_out.csv
Export UPI domain map | curl http://localhost:8000/report/upi.csv > upi_map.csv


🧪 (Optional) CLI Utilities
---------------------------

_Not yet implemented but planned for v5._

Command | Description
----------|-------------
cli/send_urls.py urls.txt | Push URLs directly to fetcher without frontend
cli/monitor_queue.py | Check queue length, task stats, etc.


🧩 Notes & Best Practices
--------------------------

- Extend keywords/keywords.yml with more regional patterns and brand aliases
- Tune analyzer’s fuzzy + semantic thresholds for better precision
- Keep MAX_WORKERS moderate to avoid overloading target sites
- All service data (stats, screenshots, mappings) persist under /data/


🧭 Summary Architecture
------------------------

[Frontend Uploads XLSX/CSV]
          ↓
     [Go Fetcher]
 (Redis queue + BFS crawler)
          ↓
   [Python Analyzer]
 (Regex, OCR, QR, UPI, OpenSearch)
          ↓
 [Playwright Renderer] ──▶ [JS-heavy domains]
          ↓
 [OpenSearch + Dashboards]
 (Visualize and query matches)
          ↓
 [MinIO Storage]
 (Screenshots, OCR assets)


🧱 License & Version
---------------------

Version: v4  
License: MIT  
Maintainer: ATS Core Team  
Next planned release: v5 (CLI tools + incremental export + metrics API)
