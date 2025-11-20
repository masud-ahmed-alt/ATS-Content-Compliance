#!/usr/bin/env python3
"""
core_analyzer.py — Batch-aware, memory-safe analyzer (DB-only) with spaCy NLP Validation + JS-render fallback.

Features:
• PostgreSQL persistence only (Hit + Result) — no CSV, no OpenSearch
• Heavy JS pages via Playwright renderer (two endpoints: /render for HTML, /render-and-screenshot for screenshots)
• spaCy NLP validation (Named Entity Recognition, dependency parsing, context analysis)
• YAML-driven regex + alias + brand matching
• OCR + QR + UPI + crypto detection
• Screenshot queue with backpressure
• Batch-aware ingestion and final summary persistence
"""

from __future__ import annotations

# ========= Stdlib =========
import os, io, re, gc, time, json, asyncio, threading, requests, hashlib, base64, uuid
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from collections import defaultdict
from urllib.parse import urlparse, urlsplit, parse_qs
from concurrent.futures import ThreadPoolExecutor

# ========= Optional perf =========
try:
    import uvloop  # type: ignore
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("[uvloop:enabled]", flush=True)
except Exception:
    print("[uvloop:disabled]", flush=True)

# ========= External libs =========
from selectolax.parser import HTMLParser  # type: ignore
from PIL import Image
import pytesseract  # type: ignore
import regex as regx  # type: ignore
import yaml  # type: ignore

# QR (optional)
try:
    from pyzbar.pyzbar import decode as qr_decode  # type: ignore
    _HAS_PYZBAR = True
except Exception:
    _HAS_PYZBAR = False
    def qr_decode(_): return []

# OpenCV QR (optional)
try:
    import cv2, numpy as np  # type: ignore
    _HAS_CV2 = True
except Exception:
    _HAS_CV2 = False

# ========= Local project imports =========
from config.settings import (
    SessionLocal,
    MAX_IMGS as CFG_MAX_IMGS,
    MAX_IMG_BYTES as CFG_MAX_IMG_BYTES,
    opensearch_client,
    minio_client,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
)
from models.hit_model import Result, Hit
from libs.screenshot import capture_screenshot
from libs.renderer_integration import create_renderer_client
from libs.opensearch_indexer import OpenSearchIndexer
from libs.dlq import dlq, FailedHit, FailedScreenshot
from libs.metrics import increment_metric, get_all_metrics, export_metrics

# ========= Tunables / Env =========
# Dynamic resource allocation: Adapt to available CPU cores
_AVAILABLE_CPUS = os.cpu_count() or 4
MAX_CONCURRENT_PAGES   = int(os.environ.get("MAX_CONCURRENT_PAGES", str(min(_AVAILABLE_CPUS * 8, 50))))  # Dynamic: 8x CPU cores, max 50
CPU_WORKERS            = int(os.environ.get("CPU_WORKERS", str(min(_AVAILABLE_CPUS, 6))))  # Dynamic: up to 6, or CPU count
IO_WORKERS             = int(os.environ.get("IO_WORKERS", str(min(_AVAILABLE_CPUS * 4, 32))))  # Dynamic: 4x CPU cores, max 32
MAX_SCREENSHOT_WORKERS = int(os.environ.get("MAX_SCREENSHOT_WORKERS", str(min(_AVAILABLE_CPUS, 5))))  # Dynamic: up to CPU count, max 5

OCR_MIN_DIM            = int(os.environ.get("OCR_MIN_DIM", "200"))
IMG_HTTP_TIMEOUT_SEC   = float(os.environ.get("IMG_HTTP_TIMEOUT_SEC", "8"))

HIT_BATCH_SIZE         = int(os.environ.get("HIT_BATCH_SIZE", "200"))
PG_FLUSH_INTERVAL_SEC  = float(os.environ.get("PG_FLUSH_INTERVAL_SEC", "1.0"))

MAX_IMGS               = int(os.environ.get("MAX_IMGS", str(CFG_MAX_IMGS)))
MAX_IMG_BYTES          = int(os.environ.get("MAX_IMG_BYTES", str(CFG_MAX_IMG_BYTES)))

RENDERER_URL           = os.environ.get("RENDERER_URL", "http://localhost:9000")
PW_DOMAINS_FILE        = os.environ.get("PW_DOMAINS_FILE", "/data/playwright_domains.txt")

# ========= spaCy NLP Validation Control =========
# Set to True to enable spaCy NLP validation, False to skip NLP validation
ENABLE_SPACY_VALIDATION = os.environ.get("ENABLE_SPACY_VALIDATION", "false").lower() in ("1", "true", "yes")
USE_SPACY               = ENABLE_SPACY_VALIDATION and os.environ.get("USE_SPACY", "true").lower() in ("1", "true", "yes")
SPACY_MODEL_NAME        = os.environ.get("SPACY_MODEL_NAME", "en_core_web_sm")  # or "en_core_web_lg" for better accuracy
SPACY_THRESHOLD         = float(os.environ.get("SPACY_THRESHOLD", "0.60"))
AUTOLOAD_SPACY          = os.environ.get("AUTOLOAD_SPACY", "true").lower() in ("1", "true", "yes")

# ========= GC tuning =========
# Aggressive GC for memory efficiency: lower thresholds trigger GC more frequently
# This prevents memory buildup and allows dynamic memory allocation
gc.set_threshold(500, 8, 3)  # More aggressive: 500 (was 700), 8 (was 10), 3 (was 5)

# ========= HTTP session =========
# Dynamic connection pool: Scale based on available CPU cores
_DYNAMIC_POOL_CONNECTIONS = min(_AVAILABLE_CPUS * 8, 64)  # 8x CPU cores, max 64
_DYNAMIC_POOL_MAXSIZE = min(_AVAILABLE_CPUS * 16, 128)   # 16x CPU cores, max 128
_SESS = requests.Session()
_ADAPTER = requests.adapters.HTTPAdapter(
    pool_connections=_DYNAMIC_POOL_CONNECTIONS,
    pool_maxsize=_DYNAMIC_POOL_MAXSIZE,
    max_retries=2
)
_SESS.mount("http://", _ADAPTER)
_SESS.mount("https://", _ADAPTER)
_SESS.headers.update({"User-Agent": "ats-ocr/2.3"})

# ========= ThreadPools =========
CPU_POOL: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=CPU_WORKERS)
IO_POOL:  ThreadPoolExecutor = ThreadPoolExecutor(max_workers=IO_WORKERS)
DB_POOL:  ThreadPoolExecutor = ThreadPoolExecutor(max_workers=5)  # For blocking DB ops

# ========= Renderer Client =========
renderer_client = None
try:
    if RENDERER_URL:
        # Extract base URL (remove /render or /render-and-screenshot suffix)
        base_url = RENDERER_URL.rsplit("/render", 1)[0] if "/render" in RENDERER_URL else RENDERER_URL
        renderer_client = create_renderer_client(base_url, timeout=30)
        print(f"[renderer:initialized] {base_url}", flush=True)
except Exception as e:
    print(f"[renderer:error] Failed to initialize: {e}", flush=True)

# ========= OpenSearch Indexer =========
opensearch_indexer = None
try:
    if opensearch_client:
        opensearch_indexer = OpenSearchIndexer(opensearch_client)
        opensearch_indexer.create_indices_if_not_exist()
        print(f"[opensearch:initialized] Indexer ready", flush=True)
except Exception as e:
    print(f"[opensearch:error] Failed to initialize indexer: {e}", flush=True)

# ========= Globals / Queues =========
MAIN_LOOP: asyncio.AbstractEventLoop | None = None
# Dynamic queue size: Scale based on available CPU cores (more CPU = larger queues)
_DYNAMIC_QUEUE_SIZE = min(_AVAILABLE_CPUS * 500, 4000)  # 500x CPU cores, max 4000
hit_queue: asyncio.Queue[Hit]  = asyncio.Queue(maxsize=_DYNAMIC_QUEUE_SIZE)

@dataclass
class ScreenshotJob:
    sub_url: str
    keyword: str
    main_url: str
    task_id: str

_screenshot_queue: asyncio.Queue[ScreenshotJob] | None = None

# Matching & batch accumulators
_match_lock = threading.Lock()
_batch_lock = threading.Lock()
match_buffer: dict[str, dict] = defaultdict(lambda: {"sub_urls": set(), "matches": []})
_batch_accumulator: dict[str, dict] = defaultdict(lambda: {
    "total_pages": 0, "total_matches": 0, "categories": set(),
    "keywords": [], "snippets": [], "sub_urls": [], "last_batch": 0
})
# HTML storage: track URLs with hits and their HTML content
# Use OrderedDict for LRU behavior
from collections import OrderedDict
_html_storage: OrderedDict[str, str] = OrderedDict()  # url -> html content (LRU)
_html_saved: set[str] = set()  # URLs that have already had HTML saved to MinIO
# Dynamic HTML storage: Scale based on available CPU cores
_DYNAMIC_HTML_STORAGE = min(_AVAILABLE_CPUS * 125, 1000)  # 125x CPU cores, max 1000
_HTML_STORAGE_MAX_SIZE = int(os.environ.get("HTML_STORAGE_MAX_SIZE", str(_DYNAMIC_HTML_STORAGE)))

# ========= Screenshot workers =========
async def _screenshot_worker():
    assert _screenshot_queue is not None
    while True:
        job = await _screenshot_queue.get()
        try:
            await _process_screenshot_job(job)
        except Exception as e:
            print(f"[screenshot:error] {job.sub_url} -> {e}", flush=True)
        finally:
            _screenshot_queue.task_done()

async def _process_screenshot_job(job: ScreenshotJob):
    loop = asyncio.get_event_loop()
    max_retries = 3
    retry_delay = 2.0
    
    for attempt in range(max_retries):
        try:
            data = await loop.run_in_executor(IO_POOL, lambda: capture_screenshot(job.sub_url, job.keyword))
            if not data:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    increment_metric("screenshot_failures")
                    failed_screenshot = FailedScreenshot(
                        sub_url=job.sub_url,
                        keyword=job.keyword,
                        main_url=job.main_url,
                        task_id=job.task_id,
                        error="screenshot_capture_failed",
                        retry_count=attempt + 1
                    )
                    dlq.enqueue_screenshot(failed_screenshot)
                    return

            storage_url = await loop.run_in_executor(IO_POOL, lambda: _store_screenshot(job, data))
            if not storage_url:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    increment_metric("screenshot_failures")
                    increment_metric("minio_errors")
                    failed_screenshot = FailedScreenshot(
                        sub_url=job.sub_url,
                        keyword=job.keyword,
                        main_url=job.main_url,
                        task_id=job.task_id,
                        error="screenshot_storage_failed",
                        retry_count=attempt + 1
                    )
                    dlq.enqueue_screenshot(failed_screenshot)
                    return

            # Transaction: assign screenshot to hit (with retry)
            success = await loop.run_in_executor(DB_POOL, lambda: _assign_screenshot_to_hit(job, storage_url))
            if success:
                return  # Success
            else:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    increment_metric("screenshot_failures")
                    failed_screenshot = FailedScreenshot(
                        sub_url=job.sub_url,
                        keyword=job.keyword,
                        main_url=job.main_url,
                        task_id=job.task_id,
                        error="screenshot_db_assignment_failed",
                        retry_count=attempt + 1
                    )
                    dlq.enqueue_screenshot(failed_screenshot)
                    return
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            else:
                increment_metric("screenshot_failures")
                failed_screenshot = FailedScreenshot(
                    sub_url=job.sub_url,
                    keyword=job.keyword,
                    main_url=job.main_url,
                    task_id=job.task_id,
                    error=str(e),
                    retry_count=attempt + 1
                )
                dlq.enqueue_screenshot(failed_screenshot)
                print(f"[screenshot:error] {job.sub_url} -> {e} (all retries exhausted)", flush=True)
                return

# ========= DLQ Retry Worker =========
async def _dlq_retry_worker():
    """Background worker to retry failed hits and screenshots from DLQ."""
    while True:
        try:
            await asyncio.sleep(60)  # Check every minute
            
            # Retry failed hits
            failed_hit = dlq.dequeue_hit()
            if failed_hit and failed_hit.retry_count < 5:
                try:
                    hit = Hit(
                        task_id=failed_hit.task_id,
                        main_url=failed_hit.main_url,
                        sub_url=failed_hit.sub_url,
                        category=failed_hit.category,
                        matched_keyword=failed_hit.matched_keyword,
                        snippet=failed_hit.snippet,
                        screenshot_path=None,
                        timestamp=failed_hit.timestamp,
                        source=failed_hit.source,
                        confident_score=failed_hit.confident_score
                    )
                    try:
                        hit_queue.put_nowait(hit)
                        print(f"[dlq:retry:hit:success] {failed_hit.sub_url} - {failed_hit.matched_keyword}", flush=True)
                    except asyncio.QueueFull:
                        # Re-queue with incremented retry count
                        failed_hit.retry_count += 1
                        dlq.enqueue_hit(failed_hit)
                except Exception as e:
                    print(f"[dlq:retry:hit:error] {e}", flush=True)
                    failed_hit.retry_count += 1
                    if failed_hit.retry_count < 5:
                        dlq.enqueue_hit(failed_hit)
            
            # Retry failed screenshots
            failed_screenshot = dlq.dequeue_screenshot()
            if failed_screenshot and failed_screenshot.retry_count < 5:
                try:
                    job = ScreenshotJob(
                        sub_url=failed_screenshot.sub_url,
                        keyword=failed_screenshot.keyword,
                        main_url=failed_screenshot.main_url,
                        task_id=failed_screenshot.task_id,
                    )
                    try:
                        if _screenshot_queue:
                            _screenshot_queue.put_nowait(job)
                            print(f"[dlq:retry:screenshot:success] {failed_screenshot.sub_url} - {failed_screenshot.keyword}", flush=True)
                        else:
                            failed_screenshot.retry_count += 1
                            dlq.enqueue_screenshot(failed_screenshot)
                    except asyncio.QueueFull:
                        failed_screenshot.retry_count += 1
                        dlq.enqueue_screenshot(failed_screenshot)
                except Exception as e:
                    print(f"[dlq:retry:screenshot:error] {e}", flush=True)
                    failed_screenshot.retry_count += 1
                    if failed_screenshot.retry_count < 5:
                        dlq.enqueue_screenshot(failed_screenshot)
        except Exception as e:
            print(f"[dlq:retry:worker:error] {e}", flush=True)
            await asyncio.sleep(60)

def _store_screenshot(job: ScreenshotJob, payload: Dict[str, Any]) -> str | None:
    if not payload:
        return None

    minio_meta = payload.get("minio")
    if isinstance(minio_meta, dict) and minio_meta.get("ok") and minio_meta.get("url"):
        return minio_meta.get("url")

    b64 = payload.get("screenshot_b64")
    if not b64:
        return None

    try:
        blob = base64.b64decode(b64)
    except Exception as exc:
        print(f"[screenshot:decode:error] {job.sub_url} -> {exc}", flush=True)
        return None

    object_name = _build_screenshot_object_name(job)
    
    # Retry logic for MinIO operations (handles transient I/O errors)
    max_retries = 3
    retry_delay = 1.0
    for attempt in range(max_retries):
        try:
            buffer = io.BytesIO(blob)
            minio_client.put_object(
                MINIO_BUCKET,
                object_name,
                buffer,
                length=len(blob),
                content_type="image/png",
            )
            return _minio_public_url(object_name)
        except Exception as exc:
            error_msg = str(exc).lower()
            # Check if it's a transient error (I/O, connection, or storage resource issue)
            is_transient = any(keyword in error_msg for keyword in [
                "insufficient", "timeout", "connection", "network", 
                "temporary", "retry", "unable to write", "no online disks"
            ])
            
            if attempt < max_retries - 1 and is_transient:
                print(f"[screenshot:minio:retry] {job.sub_url} attempt {attempt + 1}/{max_retries}: {exc}", flush=True)
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                continue
            else:
                increment_metric("minio_errors")
                print(f"[screenshot:minio:error] {job.sub_url} -> {exc}", flush=True)
                return None
    
    return None

def _assign_screenshot_to_hit(job: ScreenshotJob, storage_url: str) -> bool:
    """Assign screenshot to hit. Returns True on success, False on failure."""
    db = SessionLocal()
    try:
        hit = (
            db.query(Hit)
            .filter(
                Hit.task_id == job.task_id,
                Hit.sub_url == job.sub_url,
                Hit.matched_keyword == job.keyword,
                Hit.screenshot_path.is_(None),
            )
            .order_by(Hit.id.desc())
            .first()
        )
        if not hit:
            print(f"[screenshot:db:warn] No hit found for {job.sub_url} - {job.keyword}", flush=True)
            return False
        hit.screenshot_path = storage_url
        db.commit()
        print(f"[screenshot:stored] {job.sub_url} -> {storage_url}", flush=True)
        return True
    except Exception as exc:
        db.rollback()
        print(f"[screenshot:db:error] {job.sub_url} -> {exc}", flush=True)
        increment_metric("db_timeouts")
        return False
    finally:
        db.close()

def _safe_slug(value: str) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    if not value:
        value = "match"
    return value[:80]

def _build_screenshot_object_name(job: ScreenshotJob) -> str:
    main_slug = _safe_slug(job.main_url)
    keyword_slug = _safe_slug(job.keyword)
    return f"screenshots/{main_slug}/{keyword_slug}-{uuid.uuid4().hex}.png"

def _build_html_object_name(url: str) -> str:
    """Build MinIO object name for HTML page"""
    url_slug = _safe_slug(url)
    return f"html-pages/{url_slug}-{uuid.uuid4().hex}.html.gz"

def _minio_public_url(object_name: str) -> str:
    endpoint = MINIO_ENDPOINT.split("://")[-1].rstrip("/")
    return f"{endpoint}/{MINIO_BUCKET}/{object_name}" if endpoint else f"{MINIO_BUCKET}/{object_name}"

async def _ensure_screenshot_workers():
    global _screenshot_queue
    if _screenshot_queue is not None:
        return
    # Dynamic screenshot queue: Scale based on available CPU cores
    _DYNAMIC_SCREENSHOT_QUEUE = min(_AVAILABLE_CPUS * 125, 1000)  # 125x CPU cores, max 1000
    _screenshot_queue = asyncio.Queue(maxsize=_DYNAMIC_SCREENSHOT_QUEUE)
    for _ in range(MAX_SCREENSHOT_WORKERS):
        asyncio.create_task(_screenshot_worker())
    # Start DLQ retry worker
    asyncio.create_task(_dlq_retry_worker())
    print(f"[screenshot:workers] started={MAX_SCREENSHOT_WORKERS}", flush=True)

# ========= PostgreSQL bulk flusher =========
async def pg_flusher():
    buf: List[Hit] = []
    last = time.time()
    while True:
        try:
            item = await hit_queue.get()
            buf.append(item)
            hit_queue.task_done()
            now = time.time()
            if len(buf) >= HIT_BATCH_SIZE or (now - last) >= PG_FLUSH_INTERVAL_SEC:
                await _flush_pg(buf); buf.clear(); last = now
        except Exception as e:
            print(f"[pg_flusher:error] {e}", flush=True)
            await asyncio.sleep(0.25)

async def _flush_pg(batch: List[Hit]):
    """
    Flush hits to database - run blocking DB ops in thread pool.
    This prevents the event loop from blocking during large bulk inserts.
    """
    if not batch:
        return
    
    loop = asyncio.get_event_loop()
    
    def _do_flush():
        """Blocking database bulk insert - runs in thread pool"""
        db = SessionLocal()
        try:
            db.bulk_save_objects(batch)
            db.commit()
            print(f"[pg:bulk] {len(batch)} hits", flush=True)
            return True
        except Exception as e:
            print(f"[pg:error] {e}", flush=True)
            db.rollback()
            raise
        finally:
            db.close()
    
    try:
        # Run in thread pool with 30-second timeout
        await asyncio.wait_for(
            loop.run_in_executor(DB_POOL, _do_flush),
            timeout=30.0
        )
    except asyncio.TimeoutError:
        increment_metric("db_timeouts")
        print(f"[pg:timeout] - flush exceeded 30 seconds for {len(batch)} hits", flush=True)
        # Add failed hits to DLQ
        for hit in batch:
            failed_hit = FailedHit(
                task_id=hit.task_id,
                main_url=hit.main_url,
                sub_url=hit.sub_url,
                category=hit.category,
                matched_keyword=hit.matched_keyword,
                snippet=hit.snippet,
                timestamp=hit.timestamp,
                source=hit.source,
                confident_score=hit.confident_score or 0,
                error="db_flush_timeout",
                retry_count=0
            )
            dlq.enqueue_hit(failed_hit)
    except Exception as e:
        increment_metric("db_timeouts")
        print(f"[pg:flush:exception] {e}", flush=True)
        # Add failed hits to DLQ
        for hit in batch:
            failed_hit = FailedHit(
                task_id=hit.task_id,
                main_url=hit.main_url,
                sub_url=hit.sub_url,
                category=hit.category,
                matched_keyword=hit.matched_keyword,
                snippet=hit.snippet,
                timestamp=hit.timestamp,
                source=hit.source,
                confident_score=hit.confident_score or 0,
                error=str(e),
                retry_count=0
            )
            dlq.enqueue_hit(failed_hit)

async def _ensure_bg_workers():
    global MAIN_LOOP
    if MAIN_LOOP is not None: return
    MAIN_LOOP = asyncio.get_running_loop()
    asyncio.create_task(pg_flusher())
    await _ensure_screenshot_workers()
    print("[background:workers] started", flush=True)

# ========= Helpers =========
_trans_table = str.maketrans({"\r": " ", "\n": " ", "\t": " "})
def _clean(s: str) -> str:
    return " ".join(s.translate(_trans_table).split())

def _absolute_img_src(page_url: str, src: str) -> str:
    if not src: return ""
    if src.startswith("//"): return "https:" + src
    if src.startswith("/") and "://" in page_url:
        proto, rest = page_url.split("://", 1)
        base = proto + "://" + rest.split("/", 1)[0]
        return base + src
    return src

# ========= Keyword config =========
_KW_PATH = os.environ.get("KEYWORDS_FILE", "/app/keywords/enhanced-keywords.yml")
try:
    with open(_KW_PATH, "r", encoding="utf-8") as f:
        _cfg = yaml.safe_load(f) or {}
except Exception as e:
    print(f"[keywords:error] cannot load {_KW_PATH}: {e}", flush=True)
    _cfg = {"keywords": []}

KW = _cfg.get("keywords", [])
COMPILED: List[Tuple[str, str, regx.Pattern]] = []
ALIASES:  List[Tuple[str, str, str]] = []
_seen = set()

for e in KW:
    term = (e.get("term") or "").strip()
    cat  = (e.get("category") or "uncat").strip()
    for pat in (e.get("patterns") or []) or []:
        try:
            COMPILED.append((term, cat, regx.compile(pat, regx.I)))
        except Exception as ex:
            print(f"[regex:skip] {term}: {ex}", flush=True)
    aliases = e.get("aliases") or []
    brands  = e.get("brands")  or []
    if isinstance(aliases, str): aliases = [aliases]
    if isinstance(brands,  str): brands  = [brands]
    for a in aliases + brands:
        a = (a or "").strip().lower()
        if a and len(a) >= 3 and (term, cat, a) not in _seen:
            ALIASES.append((term, cat, a)); _seen.add((term, cat, a))

# ========= Regexes & scoring =========
_UPI_CONTEXT_RE = regx.compile(
    r"\b[a-zA-Z0-9._-]{2,}@(upi|paytm|ybl|okicici|oksbi|okaxis|okhdfcbank|ibl|axl|idfcbank|apl|payu|pingpay|barodampay|boi|zomato)\b",
    flags=regx.I,
)
_BTC_RE = regx.compile(r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b")
_ETH_RE = regx.compile(r"\b0x[a-fA-F0-9]{40}\b")

_PAYMENT_TOKENS = (
    "buy","order","pay","scan","checkout","upi","gpay",
    "phonepe","paytm","payment","merchant","qr","amount","send","transfer"
)

def _context_score(text:str, idx:int) -> float:
    w = text[max(0,idx-80):idx+80].casefold()
    return min(sum(1 for t in _PAYMENT_TOKENS if t in w)/4, 1.0)

# ========= spaCy NLP validation =========
_SPACY_MODEL = None
_SPACY_CACHE: dict[str, float] = {}
_SPACY_CACHE_MAX = int(os.environ.get("SPACY_CACHE_MAX", "100000"))
_SPACY_LOADING = False
_SPACY_LOADED = False
_spacy_lock = threading.Lock()

def load_spacy_model(model_name: str = None, wait: bool = False, timeout: float = 300.0):
    """Load spaCy model for NLP validation."""
    global _SPACY_MODEL, _SPACY_LOADING, _SPACY_LOADED
    
    if not ENABLE_SPACY_VALIDATION:
        print("[spacy:disabled] ENABLE_SPACY_VALIDATION is False, skipping spaCy model loading", flush=True)
        return
    
    if not USE_SPACY:
        print("[spacy:disabled] USE_SPACY is False", flush=True)
        return
    
    if model_name is None:
        model_name = SPACY_MODEL_NAME
    
    # Prevent concurrent loads
    with _spacy_lock:
        if _SPACY_LOADED and _SPACY_MODEL is not None:
            print(f"[spacy:already_loaded] Model already loaded", flush=True)
            return
        
        if _SPACY_LOADING:
            if wait:
                print(f"[spacy:waiting] Waiting for ongoing load to complete...", flush=True)
                start_time = time.time()
                while _SPACY_LOADING and (time.time() - start_time) < timeout:
                    time.sleep(0.5)
                if _SPACY_LOADED and _SPACY_MODEL is not None:
                    print(f"[spacy:ready] Model loaded by another thread", flush=True)
                    return
            else:
                print(f"[spacy:loading] Model is being loaded by another thread", flush=True)
                return
        
        _SPACY_LOADING = True
    
    try:
        import spacy  # type: ignore
        
        print(f"[spacy:loading] {model_name}", flush=True)
        
        # Try to load model (spacy.load will raise error if model not installed)
        try:
            model = spacy.load(model_name)
            print(f"[spacy:loaded] Model loaded successfully: {model_name}", flush=True)
        except OSError:
            # Model not found - try to download it automatically
            print(f"[spacy:warn] Model '{model_name}' not found. Attempting to download...", flush=True)
            try:
                import subprocess
                result = subprocess.run(
                    ["python", "-m", "spacy", "download", model_name],
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout
                )
                if result.returncode == 0:
                    print(f"[spacy:download:success] Model '{model_name}' downloaded successfully", flush=True)
                    # Try loading again
                    model = spacy.load(model_name)
                    print(f"[spacy:loaded] Model loaded successfully after download: {model_name}", flush=True)
                else:
                    print(f"[spacy:download:error] Failed to download model: {result.stderr}", flush=True)
                    print(f"[spacy:error] Model '{model_name}' not found. Install with: python -m spacy download {model_name}", flush=True)
                    print(f"[spacy:error] Available models: python -m spacy info", flush=True)
                    with _spacy_lock:
                        _SPACY_MODEL = None
                        _SPACY_LOADING = False
                        _SPACY_LOADED = False
                    return
            except (subprocess.TimeoutExpired, Exception) as e:
                print(f"[spacy:download:error] Failed to download model: {e}", flush=True)
                print(f"[spacy:error] Model '{model_name}' not found. Install with: python -m spacy download {model_name}", flush=True)
                print(f"[spacy:error] Available models: python -m spacy info", flush=True)
                with _spacy_lock:
                    _SPACY_MODEL = None
                    _SPACY_LOADING = False
                    _SPACY_LOADED = False
                return
        
        with _spacy_lock:
            _SPACY_MODEL = model
            _SPACY_LOADED = True
            _SPACY_LOADING = False
        
        print(f"[spacy:ready] Model is ready for NLP validation", flush=True)
    
    except ImportError:
        print(f"[spacy:error] spaCy not installed. Install with: pip install spacy", flush=True)
        with _spacy_lock:
            _SPACY_MODEL = None
            _SPACY_LOADING = False
            _SPACY_LOADED = False
    except Exception as e:
        print(f"[spacy:error] Failed to load model: {e}", flush=True)
        import traceback
        print(f"[spacy:error:traceback] {traceback.format_exc()}", flush=True)
        with _spacy_lock:
            _SPACY_MODEL = None
            _SPACY_LOADING = False
            _SPACY_LOADED = False

def spacy_validate(keyword: str, snippet: str, category: str) -> float:
    """
    Return validation score [0..1] using spaCy NLP.
    Validates if keyword appears in snippet as relevant entity with proper context.
    If disabled or model absent, returns 1.0 (bypass validation).
    Results are cached to avoid repeated inference.
    """
    if not ENABLE_SPACY_VALIDATION:
        return 1.0  # Skip NLP validation if disabled
    
    if not USE_SPACY:
        return 1.0
    
    # Check if model is loaded, if not try to load it synchronously
    if _SPACY_MODEL is None:
        if not _SPACY_LOADING:
            print(f"[spacy:warn] Model not loaded, attempting synchronous load...", flush=True)
            load_spacy_model(SPACY_MODEL_NAME, wait=False)
        
        # If still not loaded, return 1.0 to bypass validation
        if _SPACY_MODEL is None:
            return 1.0
    
    # Prepare cache key
    key_src = f"{category}|{keyword}|{snippet[:400]}"
    key = hashlib.sha256(key_src.encode("utf-8")).hexdigest()
    
    v = _SPACY_CACHE.get(key)
    if v is not None:
        return v
    
    try:
        import spacy  # type: ignore
        
        doc = _SPACY_MODEL(snippet[:512])  # Process snippet
        keyword_lower = keyword.lower()
        
        # Map categories to expected entity types
        category_entities = {
            "payments": ["MONEY", "ORG", "PRODUCT", "PERSON"],
            "personal_info": ["PERSON", "EMAIL", "PHONE"],
            "financial": ["MONEY", "ORG", "DATE"],
            "crypto": ["MONEY", "ORG", "PRODUCT"],
        }
        
        expected_entities = category_entities.get(category, ["ORG", "PRODUCT"])
        
        # Check for keyword in text and entities
        score = 0.0
        
        # 1. Check if keyword appears as named entity
        for ent in doc.ents:
            if keyword_lower in ent.text.lower() and ent.label_ in expected_entities:
                # Found keyword as relevant entity type
                score = max(score, 0.9)
        
        # 2. Check if keyword appears in text with relevant context
        for token in doc:
            if keyword_lower in token.text.lower():
                # Check dependencies for payment-related relationships
                if category == "payments":
                    payment_verbs = ["pay", "send", "transfer", "scan", "receive", "payment"]
                    for child in token.children:
                        if child.lemma_.lower() in payment_verbs:
                            score = max(score, 0.85)
                    
                    # Check parent relationships
                    if token.head and token.head.lemma_.lower() in payment_verbs:
                        score = max(score, 0.85)
                
                # Check if in relevant phrase
                if token.head.pos_ == "VERB" and category == "payments":
                    score = max(score, 0.75)
        
        # 3. Check similarity to category keywords
        category_keywords = {
            "payments": ["pay", "payment", "transfer", "upi", "bank"],
            "personal_info": ["name", "email", "phone", "address", "contact"],
            "financial": ["amount", "money", "price", "cost", "fee"],
        }
        
        category_kws = category_keywords.get(category, [])
        snippet_lower = snippet.lower()
        
        # Count category keywords in snippet
        kw_count = sum(1 for kw in category_kws if kw in snippet_lower)
        if kw_count > 0 and keyword_lower in snippet_lower:
            score = max(score, 0.7 + (kw_count * 0.05))  # Boost score based on context
        
        # Clamp to [0.0, 1.0]
        score = max(0.0, min(1.0, score))
        
        # If no strong signal, return lower score
        if score == 0.0 and keyword_lower in snippet_lower:
            score = 0.5  # Keyword found but no strong context
        
        _SPACY_CACHE[key] = score
        
        # Keep cache bounded
        if len(_SPACY_CACHE) > _SPACY_CACHE_MAX:
            try:
                _SPACY_CACHE.clear()
            except Exception:
                pass
        
        return score
    
    except Exception as e:
        print(f"[spacy:error] validate failure: {e}", flush=True)
        return 0.0

# If AUTOLOAD_SPACY is enabled we start a background thread to load the model
if ENABLE_SPACY_VALIDATION and AUTOLOAD_SPACY and USE_SPACY:
    def _bg_load_spacy():
        try:
            print(f"[spacy:bg_load] Starting background load: {SPACY_MODEL_NAME}", flush=True)
            load_spacy_model(SPACY_MODEL_NAME, wait=False)
            if _SPACY_MODEL is not None:
                print(f"[spacy:bg_load:success] Model loaded successfully", flush=True)
            else:
                print(f"[spacy:bg_load:failed] Model failed to load", flush=True)
        except Exception as e:
            print(f"[spacy:bg_load:error] {e}", flush=True)
            import traceback
            print(f"[spacy:bg_load:traceback] {traceback.format_exc()}", flush=True)
    
    t_spacy = threading.Thread(target=_bg_load_spacy, daemon=True, name="spacy-autoload")
    t_spacy.start()
    print(f"[spacy] autoload thread started (model={SPACY_MODEL_NAME})", flush=True)

def get_spacy_status() -> Dict[str, Any]:
    """
    Get diagnostic information about spaCy model status.
    Useful for debugging why NLP validation might not be working.
    """
    with _spacy_lock:
        return {
            "enable_spacy_validation": ENABLE_SPACY_VALIDATION,
            "use_spacy": USE_SPACY,
            "model_loaded": _SPACY_MODEL is not None,
            "model_loading": _SPACY_LOADING,
            "model_loaded_flag": _SPACY_LOADED,
            "model_name": SPACY_MODEL_NAME,
            "spacy_threshold": SPACY_THRESHOLD,
            "cache_size": len(_SPACY_CACHE),
            "autoload_enabled": AUTOLOAD_SPACY,
        }

# ========= Text extraction =========
def extract_text(html: str) -> tuple[str, HTMLParser]:
    tree = HTMLParser(html)
    parts: List[str] = []
    for node in tree.css("body :not(script):not(style):not(nav):not(footer)"):
        try:
            t = node.text(separator=" ", strip=True)
            if t and len(t) > 3: parts.append(t)
        except Exception: continue
        if len(parts) >= 20000: break
    return " ".join(parts), tree

# ========= OCR + QR =========
def _ocr_image(img: Image.Image) -> str:
    try:
        g = img.convert("L")
        w, h = g.size
        if w < OCR_MIN_DIM or h < OCR_MIN_DIM:
            return ""
        if w < 300 or h < 300:
            g = g.resize((w*2, h*2))
        conf = "--psm 6 -c tessedit_char_whitelist=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@._-"
        return pytesseract.image_to_string(g, config=conf)
    except Exception:
        return ""

def _try_qr_opencv(img: Image.Image) -> List[str]:
    if not _HAS_CV2: return []
    try:
        cv_img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
        data, _, _ = cv2.QRCodeDetector().detectAndDecode(cv_img)
        return [data] if data else []
    except Exception:
        return []

def ocr_and_qr(url: str, tree: HTMLParser, task_id:str|None=None, master:str|None=None) -> List[Tuple[str, str, str]]:
    results: List[Tuple[str,str,str]] = []
    for i, img in enumerate(tree.css("img")):
        if i >= MAX_IMGS: break
        src = _absolute_img_src(url, img.attributes.get("src") or "")
        if not src: continue
        try:
            with _SESS.get(src, timeout=IMG_HTTP_TIMEOUT_SEC, stream=True) as r:
                r.raise_for_status()
                content = r.raw.read(MAX_IMG_BYTES, decode_content=True)
            with Image.open(io.BytesIO(content)) as im:
                im.load()
                items = []
                if _HAS_PYZBAR:
                    try: items.extend(qr_decode(im))
                    except Exception: pass
                if _HAS_CV2:
                    try:
                        for p in _try_qr_opencv(im):
                            if p: items.append(type("X", (), {"data": p.encode()}))
                    except Exception: pass
                for c in items:
                    payload = getattr(c, "data", b"").decode("utf-8", errors="ignore")
                    upi = normalize_upi_from_payload(payload)
                    if upi:
                        snip = f"QR->UPI:{upi}"
                        record_hit(url, "payments", "upi-qr", snip, "qr", master or url, 0.9, task_id)
                        results.append(("upi-qr","payments", snip))
                if not results:
                    txt = _ocr_image(im)
                    if txt:
                        results += match_text(url, _clean(txt), master=master or url, task_id=task_id)
            del content
        except Exception:
            continue
    return results

# ========= UPI normalize =========
def normalize_upi_from_payload(data: str):
    try:
        if data.startswith(("upi:", "upi://")):
            u = data if data.startswith("upi://") else "upi://" + data.split(":",1)[1]
            qs = parse_qs(urlsplit(u).query)
            pa = qs.get("pa",[None])[0]
            if pa: return pa.lower()
        m = re.search(r"\b[a-zA-Z0-9._-]{3,}@[a-zA-Z]{2,}\b", data)
        if m: return m.group(0).lower()
    except Exception:
        pass
    return None

# ========= HTML saving to MinIO =========
def _save_html_to_minio(url: str, html: str) -> str | None:
    """Save HTML page to MinIO (gzipped). Returns MinIO URL or None on error."""
    if not html or not url:
        return None
    
    try:
        import gzip
        # Compress HTML
        html_bytes = html.encode("utf-8")
        gz_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buffer, mode="wb") as gz_file:
            gz_file.write(html_bytes)
        gz_data = gz_buffer.getvalue()
        
        object_name = _build_html_object_name(url)
        
        # Retry logic for MinIO operations
        max_retries = 3
        retry_delay = 1.0
        for attempt in range(max_retries):
            try:
                buffer = io.BytesIO(gz_data)
                minio_client.put_object(
                    MINIO_BUCKET,
                    object_name,
                    buffer,
                    length=len(gz_data),
                    content_type="text/html",
                )
                storage_url = _minio_public_url(object_name)
                print(f"[html:saved] {url} -> {storage_url}", flush=True)
                return storage_url
            except Exception as exc:
                error_msg = str(exc).lower()
                is_transient = any(keyword in error_msg for keyword in [
                    "insufficient", "timeout", "connection", "network",
                    "temporary", "retry", "unable to write", "no online disks"
                ])
                
                if attempt < max_retries - 1 and is_transient:
                    print(f"[html:minio:retry] {url} attempt {attempt + 1}/{max_retries}: {exc}", flush=True)
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    print(f"[html:minio:error] {url} -> {exc}", flush=True)
                    return None
    except Exception as exc:
        print(f"[html:save:error] {url} -> {exc}", flush=True)
        return None
    
    return None

# ========= Hit recording (DB-only) =========
def record_hit(url:str, cat:str, k:str, snip:str, src:str,
               master:str|None=None, confidence:float=1.0,
               task_id:str|None=None):
    if not master: master = url
    snip = _clean(snip)
    ts = int(time.time())
    
    # Save HTML to MinIO on first hit for this URL
    with _match_lock:
        # Check if this is the first hit for this URL
        if url not in _html_saved and url in _html_storage:
            html_content = _html_storage.get(url)
            if html_content:
                # Save HTML in thread pool (non-blocking)
                IO_POOL.submit(_save_html_to_minio, url, html_content)
                _html_saved.add(url)
        
        bucket = match_buffer[master]
        for e in bucket["matches"]:
            if e["url"] == url and e["keyword"] == k:
                return
        bucket["sub_urls"].add(url)
        bucket["matches"].append({
            "url": url, "category": cat, "keyword": k, "snippet": snip,
            "timestamp": ts, "source": src, "confidence": confidence
        })
    
    # screenshot enqueue (confidence gate)
    if confidence >= 0.7 and _screenshot_queue is not None:
        job = ScreenshotJob(
            sub_url=url,
            keyword=k,
            main_url=master,
            task_id=task_id or "unknown",
        )
        try:
            _screenshot_queue.put_nowait(job)
            increment_metric("total_screenshots_processed")
        except asyncio.QueueFull:
            increment_metric("screenshots_dropped")
            increment_metric("queue_overflow_count")
            # Add to DLQ instead of silently dropping
            failed_screenshot = FailedScreenshot(
                sub_url=url,
                keyword=k,
                main_url=master,
                task_id=task_id or "unknown",
                error="screenshot_queue_full",
                retry_count=0
            )
            dlq.enqueue_screenshot(failed_screenshot)
    # DB enqueue
    hit = Hit(
        task_id=task_id or "unknown",
        main_url=master,
        sub_url=url,
        category=cat,
        matched_keyword=k,
        snippet=snip[:500],
        screenshot_path=None,
        timestamp=ts,
        source=src,
        confident_score=int(max(0.0, min(1.0, confidence))*100)
    )
    try:
        hit_queue.put_nowait(hit)
        increment_metric("total_hits_processed")
    except asyncio.QueueFull:
        increment_metric("hits_dropped")
        increment_metric("queue_overflow_count")
        print(f"[pg:queue_full] Hit dropped, adding to DLQ: {url} - {k}", flush=True)
        # Add to DLQ instead of silently dropping
        failed_hit = FailedHit(
            task_id=task_id or "unknown",
            main_url=master,
            sub_url=url,
            category=cat,
            matched_keyword=k,
            snippet=snip[:500],
            timestamp=ts,
            source=src,
            confident_score=int(max(0.0, min(1.0, confidence))*100),
            error="hit_queue_full",
            retry_count=0
        )
        dlq.enqueue_hit(failed_hit)

# ========= Matching (with spaCy validation) =========
def match_text(url:str, text:str, master:str|None=None, task_id:str|None=None) -> List[Tuple[str,str,str]]:
    """
    Find all matches in text and return ALL matches (before validation) for Results table.
    ALL matches are saved to Results table (before validation) - master data.
    Only validated matches (after spaCy) are saved to Hits table.
    """
    if not text.strip():
        return []
    text = _clean(text)
    low  = text.casefold()
    all_matches: List[Tuple[str,str,str]] = []  # All matches BEFORE validation (for Results table)
    
    # Regex patterns - collect ALL matches first (BEFORE validation)
    for term, cat, pat in COMPILED:
        for m in pat.finditer(text):
            idx  = m.start()
            snip = text[max(0,idx-100):idx+100]
            # Basic context check (keep for filtering obvious false positives)
            ctx  = _context_score(text, idx) if cat == "payments" else 1.0
            if cat == "payments" and ctx < 0.30:
                continue
            all_matches.append((term, cat, snip))

    # Aliases / brands - collect ALL matches first (BEFORE validation)
    for term, cat, alias in ALIASES:
        idx = low.find(alias)
        if idx != -1:
            snip = text[max(0,idx-100):idx+100]
            # Basic context check
            ctx  = _context_score(text, idx) if cat == "payments" else 1.0
            if cat == "payments" and ctx < 0.25:
                continue
            all_matches.append((term, cat, snip))

    # UPI handle in context - collect ALL matches first (BEFORE validation)
    for m in _UPI_CONTEXT_RE.finditer(text):
        idx  = m.start()
        snip = text[max(0,idx-80):idx+80]
        ctx  = _context_score(text, idx)
        if ctx >= 0.30:
            all_matches.append(("upi-handle", "payments", snip))

    # Crypto wallets - collect ALL matches first (BEFORE validation)
    for m in _BTC_RE.finditer(text):
        snip = text[max(0,m.start()-80):m.start()+80]
        all_matches.append(("bitcoin", "crypto", snip))

    for m in _ETH_RE.finditer(text):
        snip = text[max(0,m.start()-80):m.start()+80]
        all_matches.append(("ethereum", "crypto", snip))

    # Remove duplicates
    seen=set(); unique_matches=[]
    for t in all_matches:
        if t not in seen:
            seen.add(t); unique_matches.append(t)
    
    # Now validate with spaCy and save to Hits table (ONLY validated ones)
    for term, cat, snip in unique_matches:
        # Run spaCy validation
        spacy_score = spacy_validate(term, snip, cat)
        
        # Save to Hits table (ONLY validated matches, AFTER spaCy validation)
        if not ENABLE_SPACY_VALIDATION or not USE_SPACY or spacy_score >= SPACY_THRESHOLD:
            # Validated - save to Hits table
            # Determine source
            source = "regex"
            if any(term == t and cat == c for t, c, _ in ALIASES):
                source = "alias"
            elif term in ["upi-handle"]:
                source = "context"
            elif term in ["bitcoin", "ethereum"]:
                source = "regex"
            
            record_hit(url, cat, term, snip, source, master, spacy_score, task_id=task_id)
    
    # Return ALL matches (before validation) for Results table
    # Results table stores ALL matches as master data (one row per main_url)
    return unique_matches

# ========= JS-render fallback =========
def _domain_of(u: str) -> str:
    try: return urlparse(u).netloc.lower()
    except Exception: return ""

def load_pw_domains() -> set[str]:
    if not os.path.exists(PW_DOMAINS_FILE): return set()
    with open(PW_DOMAINS_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def add_pw_domain(dom: str):
    if not dom: return
    s = load_pw_domains()
    if dom in s: return
    os.makedirs(os.path.dirname(PW_DOMAINS_FILE), exist_ok=True)
    with open(PW_DOMAINS_FILE, "a", encoding="utf-8") as f:
        f.write(dom + "\n")
    print(f"[playwright:add] {dom}", flush=True)

async def _process_page_async(p:dict, main_url:str, task_id:str) -> List[Tuple[str,str,str]]:
    url  = p.get("final_url") or p.get("url")
    html = p.get("html") or p.get("html_content") or p.get("HTML") or p.get("htmlContent") or ""
    if not url:
        print(f"[process_page:warn] task={task_id} main={main_url}: Page missing URL, keys={list(p.keys())}", flush=True)
        return []
    if not html:
        # Debug: Check what fields are actually present
        html_fields = {k: v[:50] if isinstance(v, str) and len(v) > 50 else v for k, v in p.items() if 'html' in k.lower()}
        non_html_fields = {k: type(v).__name__ for k, v in p.items() if 'html' not in k.lower()}
        print(f"[process_page:warn] task={task_id} main={main_url} url={url}: Page missing HTML content", flush=True)
        print(f"[process_page:debug] Available fields: {list(p.keys())}, HTML fields: {html_fields}, Other fields: {non_html_fields}", flush=True)
        return []

    # Store HTML content for potential saving when hits are detected
    # Limit storage size to prevent memory issues
    with _match_lock:
        # LRU cache: move to end if exists, add to end if new
        if url in _html_storage:
            _html_storage.move_to_end(url)
        else:
            # Remove oldest if at capacity (LRU eviction)
            if len(_html_storage) >= _HTML_STORAGE_MAX_SIZE:
                oldest_url, _ = _html_storage.popitem(last=False)  # Remove oldest
                _html_saved.discard(oldest_url)
            _html_storage[url] = html

    domain = _domain_of(url)
    force_render = domain in load_pw_domains()

    async def process_html(content: str):
        text, tree = await asyncio.get_event_loop().run_in_executor(CPU_POOL, lambda: extract_text(content))
        results = await asyncio.get_event_loop().run_in_executor(CPU_POOL, lambda: match_text(url, text, master=main_url, task_id=task_id))
        if any(c == "payments" for _, c, _ in results):
            results += await asyncio.get_event_loop().run_in_executor(CPU_POOL, lambda: ocr_and_qr(url, tree, task_id=task_id, master=main_url))
        return results, text

    results, text = await process_html(html)

    # Detect heavy JS sites and escalate to Playwright renderer
    is_heavy_js = force_render or (not results and len(text) < 200)
    
    if renderer_client and is_heavy_js:
        try:
            print(f"[render:trigger] {url} -> heavy JS detected, requesting HTML render", flush=True)
            
            rendered_html = await asyncio.get_event_loop().run_in_executor(
                IO_POOL, lambda: renderer_client.render_html(url)
            )
            
            if rendered_html and "<html" in rendered_html.lower():
                rres, rtext = await process_html(rendered_html)
                
                with _match_lock:
                    _html_storage[url] = rendered_html
                
                if rres:
                    add_pw_domain(domain)
                    results.extend(rres)
                    print(f"[render:success] {url} -> {len(rres)} results from rendered HTML", flush=True)
                elif len(rtext) > len(text):
                    add_pw_domain(domain)
                    print(f"[render:content] {url} -> rendered HTML has more content ({len(rtext)} vs {len(text)} chars)", flush=True)
        except Exception as e:
            increment_metric("renderer_timeouts")
            print(f"[render:fail] {url} -> {e}", flush=True)
    return results

# ========= Graceful draining =========
async def _drain_queues():
    async def _drain(q: asyncio.Queue, label: str, timeout: float = 30.0):  # Increased from 10s to 30s
        start = time.time()
        while not q.empty():
            await asyncio.sleep(0.25)
            if time.time() - start > timeout:
                try: size = q.qsize()
                except Exception: size = -1
                print(f"[drain:timeout] {label} ({size} left)", flush=True)
                break

    if _screenshot_queue is not None:
        await _drain(_screenshot_queue, "screenshots", 60)
    await _drain(hit_queue, "pg", 30)
    await asyncio.sleep(0)

# ========= Persist summary =========
async def _persist_result(task_id: str, main_url: str, sub_urls: List[str],
                          kws: List[str], cats: List[str], total_matches: int = None,
                          snippets: List[str] = None):
    """
    Persist result - run blocking DB ops in thread pool to avoid blocking event loop.
    This ensures other routes remain responsive during large ingests.
    
    Args:
        task_id: Task identifier
        main_url: Main URL being analyzed
        sub_urls: List of sub URLs scanned
        kws: List of keywords found (may contain duplicates across pages)
        cats: List of categories found
        total_matches: Total count of ALL matches (before validation). If None, uses len(kws).
        snippets: List of all snippets found (for raw_data field)
    """
    loop = asyncio.get_event_loop()
    
    # Use provided total_matches or fallback to len(kws)
    total_count = total_matches if total_matches is not None else len(kws)
    
    # Prepare raw_data: Join all snippets with separator
    raw_data_content = ""
    if snippets and len(snippets) > 0:
        # Join snippets with a clear separator for easy parsing
        raw_data_content = "\n---SNIPPET---\n".join(snippets)
        # Limit total size to prevent database issues (max 1MB)
        if len(raw_data_content) > 1000000:
            raw_data_content = raw_data_content[:1000000] + "\n... (truncated)"
    
    def _do_persist():
        """Blocking database operation - runs in thread pool.
        Saves ALL matches (before spaCy validation) to Results table.
        Results table: one row per main_url (master data with all matches).
        """
        db = SessionLocal()
        try:
            # Use merge to ensure one row per main_url
            # This saves ALL matches (before spaCy validation) as master data
            result = Result(
                task_id=task_id,
                main_url=main_url,
                sub_urls=sub_urls,
                keyword_match=kws,  # ALL keywords found (before validation) - may contain duplicates
                categories=cats,     # ALL categories found (before validation)
                raw_data=raw_data_content,  # All snippets from matches (before validation)
                cleaned_data="",  # Not required for now
                timestamp=int(time.time())
            )
            # Merge ensures one row per main_url (updates if exists, inserts if new)
            db.merge(result)
            db.commit()
            print(f"[db:result:ok] {task_id} main_url={main_url} ({len(sub_urls)} urls, {total_count} total matches BEFORE validation, {len(kws)} keywords in list)", flush=True)
            return True
        except Exception as e:
            print(f"[db:result:error] {task_id} -> {e}", flush=True)
            db.rollback()
            raise
        finally:
            db.close()
    
    try:
        # Run in thread pool with 30-second timeout
        await asyncio.wait_for(
            loop.run_in_executor(DB_POOL, _do_persist),
            timeout=30.0
        )
    except asyncio.TimeoutError:
        increment_metric("db_timeouts")
        print(f"[db:timeout] {task_id} - database persist exceeded 30 seconds", flush=True)
    except Exception as e:
        increment_metric("db_timeouts")
        print(f"[db:persist:exception] {task_id} -> {e}", flush=True)

# ========= Ingest entry =========
async def process_ingest_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call this from your /ingest route.
    Accepts both batch (task_id, main_url, batch_num, pages[], is_complete)
    and legacy single-payload (no batch fields).
    """
    try:
        await _ensure_bg_workers()
        # Ensure spaCy model is loaded if enabled
        if ENABLE_SPACY_VALIDATION and USE_SPACY and _SPACY_MODEL is None and not _SPACY_LOADING:
            load_spacy_model(SPACY_MODEL_NAME, wait=False)
    except Exception as e:
        print(f"[ingest:bg_workers:error] Failed to ensure background workers: {e}", flush=True)
        # Don't fail the entire ingest if workers fail - they might already be running

    task_id = (
        payload.get("task_id")
        or payload.get("id")
        or payload.get("request_id")
        or payload.get("session_id")
        or payload.get("requestId")
        or "unknown"
    )
    main_url = payload.get("main_url") or "unknown"
    batch_num = payload.get("batch_num", 1)
    is_complete = payload.get("is_complete", False)
    pages = payload.get("pages") or payload.get("Pages") or []
    total_pages = payload.get("total_pages") or len(pages)

    is_legacy = "batch_num" not in payload and "is_complete" not in payload
    if is_legacy:
        is_complete = True
        batch_num = 1

    print(f"[ingest] task={task_id} batch={batch_num} pages={total_pages} complete={is_complete} main={main_url}", flush=True)
    
    # Debug: Check if pages array is valid
    if not pages or len(pages) == 0:
        print(f"[ingest:warn] task={task_id} main={main_url}: Empty pages array! Payload had {len(payload.get('pages', []) or payload.get('Pages', []))} pages", flush=True)
        return {
            "task_id": task_id,
            "main_url": main_url,
            "error": "empty pages array",
            "status": "failed"
        }
    
    print(f"[ingest:debug] task={task_id} main={main_url}: Processing {len(pages)} pages, first page URL: {pages[0].get('url') or pages[0].get('final_url') or 'N/A'}", flush=True)

    with _match_lock:
        if batch_num == 1:
            match_buffer.pop(main_url, None)
        match_buffer[main_url]["task_id"] = task_id

    sem = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
    all_results: List[Tuple[str, str, str]] = []
    sub_urls: List[str] = []
    processed_count = 0
    error_count = 0

    async def worker(page):
        nonlocal processed_count, error_count
        async with sem:
            try:
                res = await _process_page_async(page, main_url, task_id)
                all_results.extend(res)
                u = page.get("final_url") or page.get("url")
                if u: sub_urls.append(u)
                processed_count += 1
                if processed_count % 100 == 0:
                    print(f"[ingest:progress] task={task_id} main={main_url}: Processed {processed_count}/{total_pages} pages, found {len(all_results)} matches so far", flush=True)
            except Exception as e:
                error_count += 1
                print(f"[ingest:worker:error] task={task_id} page={page.get('url') or 'N/A'}: {e}", flush=True)
                import traceback
                print(f"[ingest:worker:traceback] {traceback.format_exc()}", flush=True)

    CHUNK = MAX_CONCURRENT_PAGES * 2
    print(f"[ingest:debug] task={task_id} main={main_url}: Starting to process {total_pages} pages in chunks of {CHUNK}", flush=True)
    for i in range(0, total_pages, CHUNK):
        chunk = pages[i:i+CHUNK]
        print(f"[ingest:debug] task={task_id} main={main_url}: Processing chunk {i//CHUNK + 1}, pages {i} to {min(i+CHUNK, total_pages)}", flush=True)
        await asyncio.gather(*[worker(p) for p in chunk], return_exceptions=True)
        # ✅ Aggressive GC after every batch to prevent memory buildup and enable dynamic allocation
        await asyncio.sleep(0)
        gc.collect()
        
        # Memory-aware: If processing large batches, trigger more aggressive cleanup
        if len(chunk) > 20:
            # Force full GC cycle for large batches
            gc.collect(2)  # Full collection

    print(f"[ingest:debug] task={task_id} main={main_url}: Finished processing pages. Processed={processed_count}, Errors={error_count}, Results={len(all_results)}, SubURLs={len(sub_urls)}", flush=True)
    
    cats = sorted({c for _, c, _ in all_results})
    kws = [k for k, _, _ in all_results]
    snippets = [s for _, _, s in all_results]  # Collect all snippets for raw_data

    with _batch_lock:
        acc = _batch_accumulator[task_id]
        acc["total_pages"]   += len(sub_urls)
        acc["total_matches"] += len(kws)
        acc["categories"].update(cats)
        acc["keywords"].extend(kws)
        acc["snippets"].extend(snippets)  # Accumulate snippets
        acc["sub_urls"].extend(sub_urls)
        acc["last_batch"]     = batch_num

    print(f"[ingest:debug] task={task_id} main={main_url}: Accumulated stats - pages={acc['total_pages']}, matches={acc['total_matches']}, categories={len(acc['categories'])}, snippets={len(acc.get('snippets', []))}, is_complete={is_complete}", flush=True)

    # Let queues drain a bit
    await _drain_queues()

    if is_complete:
        with _batch_lock:
            acc = _batch_accumulator[task_id]
            final_cats = sorted(acc["categories"])
            final_kws  = acc["keywords"]
            final_snippets = acc.get("snippets", [])  # Get all accumulated snippets
            final_urls = acc["sub_urls"]
            total_all_matches = acc["total_matches"]  # Total count of ALL matches (before validation)

        await _persist_result(task_id, main_url, final_urls, final_kws, final_cats, total_all_matches, final_snippets)

        # Index to OpenSearch for dashboards
        try:
            if opensearch_indexer:
                result_data = {
                    "session_id": task_id,
                    "main_url": main_url,
                    "total_pages": len(final_urls),
                    "total_matches": len(final_kws),
                    "categories": final_cats,
                    "keywords": final_kws,
                    "status": "completed"
                }
                opensearch_indexer.index_session_result(result_data)
                print(f"[opensearch:indexed] {task_id} results", flush=True)
        except Exception as e:
            print(f"[opensearch:error] Failed to index results: {e}", flush=True)

        # Cleanup
        with _batch_lock:
            _batch_accumulator.pop(task_id, None)
        with _match_lock:
            match_buffer.pop(main_url, None)
            # Clean up HTML storage for URLs in this batch
            # URLs with hits are already saved to MinIO, so we can remove them from memory
            for url in final_urls:
                _html_storage.pop(url, None)
                _html_saved.discard(url)  # Remove from saved set as well

        # Memory-aware cleanup: Aggressive GC for large batches
        if len(final_urls) > 500:
            gc.collect(2)  # Full collection cycle
        elif len(final_urls) > 100:
            gc.collect(1)  # Generation 1 collection

        print(f"[ingest:complete] {task_id} batches={batch_num} pages={len(final_urls)} matches={len(final_kws)}", flush=True)
        return {
            "task_id": task_id,
            "main_url": main_url,
            "total_pages": len(final_urls),
            "total_matches": len(final_kws),
            "total_batches": batch_num,
            "categories": final_cats,
            "spacy_validation_enabled": bool(ENABLE_SPACY_VALIDATION and USE_SPACY and _SPACY_MODEL is not None),
            "status": "completed",
        }

    # Incremental update - cleanup HTML for processed URLs in this batch
    # (even if batch is incomplete, we can free memory for processed URLs)
    with _match_lock:
        for url in sub_urls:
            # Only remove if URL has been saved or if we need space
            if url in _html_saved or len(_html_storage) > _HTML_STORAGE_MAX_SIZE * 0.8:
                _html_storage.pop(url, None)
                if url in _html_saved:
                    _html_saved.discard(url)

    with _batch_lock:
        acc = _batch_accumulator[task_id]
        return {
            "task_id": task_id,
            "main_url": main_url,
            "batch_num": batch_num,
            "batch_pages": len(sub_urls),
            "batch_matches": len(kws),
            "cumulative_pages": acc["total_pages"],
            "cumulative_matches": acc["total_matches"],
            "spacy_validation_enabled": bool(ENABLE_SPACY_VALIDATION and USE_SPACY and _SPACY_MODEL is not None),
            "status": "processing",
        }
