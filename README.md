
ATS Starter Repo (v4)
=====================

Services
- OpenSearch (9200) + Dashboards (5601)
- Python Analyzer (8000): regex + alias + fuzzy + OCR + QR + UPI mapping + Playwright policy
- Go Fetcher (8080): bulk file worker (default) + POST /fetch
- Playwright Renderer (9000): GET /render?url=...

Quick start
1) docker compose up --build
2) Frontend: http://localhost:5173/
   upload url.xslx or csv
   Credential: admin | 1234



   __________NOT IMPLEMENTED Currently__________
2) (Optional) Push URLs via CLI (from host): cd cli && ./send_urls.py urls.txt
3) Export hits CSV: curl http://localhost:8000/export > hits_out.csv
4) UPI mapping CSV: curl http://localhost:8000/report/upi.csv > upi_map.csv
5) Policy:
   - List:   curl http://localhost:8000/policy/playwright-domains
   - Add:    curl -X POST http://localhost:8000/policy/playwright-domains -H 'Content-Type: application/json' -d '{"domain":"example.com"}'
   - Remove: curl -X DELETE http://localhost:8000/policy/playwright-domains/example.com

Auto-escalation
- If a domain yields matches only after JS rendering >= JS_ESCALATE_THRESHOLD (default 2), it will be added to the Playwright-only list stored at /data/playwright_domains.txt.

OpenSearch Dashboards
- Import dashboards/objects.ndjson via Saved Objects to get an index pattern and a saved table.

Notes
- Tune env vars on analyzer: MAX_IMGS, MAX_IMG_BYTES, FUZZ_THRESHOLD, JS_ESCALATE_THRESHOLD.
- Extend keywords/keywords.yml with more regional brands/aliases and categories.
