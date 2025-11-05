#!/usr/bin/env python3
"""
core_analyzer_async.py  –  scalable analyzer (>1000 URLs)

• Async + thread/IO pool hybrid (regex/semantic/OCR offloaded)
• Guarded pyzbar/libzbar import
• Memory-safe (drops HTML once parsed, bounded queues)
• Streaming/batched OpenSearch + PostgreSQL writes (async flushers)
• Per-match recording (Hit) with resilient backpressure (lossless)
• Async screenshot queue with worker pool (bounded)
• Optional semantic similarity (cached)
• uvloop (optional) for faster asyncio
"""

from __future__ import annotations

# ========= Imports =========
import os, re, io, time, json, asyncio, requests, gc, threading
from typing import Dict, Any, List, Tuple
from collections import defaultdict
from urllib.parse import urlparse, urlsplit, parse_qs
from concurrent.futures import ThreadPoolExecutor

# uvloop (optional)
try:
    import uvloop  # type: ignore
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("[uvloop:enabled]", flush=True)
except Exception:
    print("[uvloop:disabled]", flush=True)

from selectolax.parser import HTMLParser
from PIL import Image

# --- QR: pyzbar guarded ---
try:
    from pyzbar.pyzbar import decode as qr_decode  # type: ignore
    _HAS_PYZBAR = True
except Exception:
    _HAS_PYZBAR = False
    def qr_decode(_):  # fallback noop
        return []

import pytesseract
import regex as regx

from opensearchpy import OpenSearch, RequestsHttpConnection  # type: ignore
from opensearchpy.helpers import bulk  # type: ignore

# --- Optional OpenCV QR ---
try:
    import cv2, numpy as np  # type: ignore
    _HAS_CV2 = True
except Exception:
    _HAS_CV2 = False

# ========= Config / env =========
DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)

from config.settings import (
    MAX_IMGS, MAX_IMG_BYTES, FUZZ_THRESHOLD,
    OPENSEARCH_HOST, PW_DOMAINS as CFG_PW_DOMAINS, SessionLocal
)
from models.hit_model import Result, Hit  # Hit used for per-match DB writes
from libs.screenshot import capture_screenshot

# ========= Tunables =========
MAX_CONCURRENT_PAGES   = int(os.environ.get("MAX_CONCURRENT_PAGES", "50"))
CPU_WORKERS            = int(os.environ.get("CPU_WORKERS", str(os.cpu_count() or 8)))
IO_WORKERS             = int(os.environ.get("IO_WORKERS", "32"))
MAX_SCREENSHOT_WORKERS = int(os.environ.get("MAX_SCREENSHOT_WORKERS", "5"))
ES_BATCH_SIZE          = int(os.environ.get("ES_BATCH_SIZE", "250"))
ES_FLUSH_INTERVAL_SEC  = float(os.environ.get("ES_FLUSH_INTERVAL_SEC", "1.0"))
OCR_MIN_DIM            = int(os.environ.get("OCR_MIN_DIM", "200"))
IMG_HTTP_TIMEOUT_SEC   = float(os.environ.get("IMG_HTTP_TIMEOUT_SEC", "8"))
HIT_BATCH_SIZE         = int(os.environ.get("HIT_BATCH_SIZE", "200"))
PG_FLUSH_INTERVAL_SEC  = float(os.environ.get("PG_FLUSH_INTERVAL_SEC", "1.0"))
ES_QUEUE_MAXSIZE       = int(os.environ.get("ES_QUEUE_MAXSIZE", "4000"))
PG_QUEUE_MAXSIZE       = int(os.environ.get("PG_QUEUE_MAXSIZE", "4000"))
SS_QUEUE_MAXSIZE       = int(os.environ.get("SS_QUEUE_MAXSIZE", "1000"))

# ========= HTTP session =========
_SESS = requests.Session()
_ADAPTER = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=128, max_retries=2)
_SESS.mount("http://", _ADAPTER)
_SESS.mount("https://", _ADAPTER)
_SESS.headers.update({"User-Agent": "ats-ocr/2.2"})

# ========= Executors & GC tuning =========
CPU_POOL: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=CPU_WORKERS)
IO_POOL:  ThreadPoolExecutor = ThreadPoolExecutor(max_workers=IO_WORKERS)

# Tuned GC: reduce full GC frequency, keep young-gen quick
gc.set_threshold(700, 10, 5)

# ========= Semantic model (optional) =========
USE_SEMANTIC = os.environ.get("USE_SEMANTIC", "true").lower() in ("1", "true", "yes")
_SEMANTIC_MODEL = None
_SEMANTIC_THRESHOLD = float(os.environ.get("SEMANTIC_THRESHOLD", "0.70"))
_SEM_CACHE: dict[str, float] = {}

def load_semantic_model(path: str):
    global _SEMANTIC_MODEL
    if not USE_SEMANTIC:
        print("[semantic:disabled]", flush=True); return
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
        _SEMANTIC_MODEL = SentenceTransformer(path)
        print(f"[semantic:loaded] {path}", flush=True)
    except Exception as e:
        print(f"[semantic:error] {e}", flush=True)

def semantic_validate(keyword: str, snippet: str, category: str) -> float:
    if not USE_SEMANTIC or not _SEMANTIC_MODEL:
        return 1.0
    key = f"{category}|{keyword}|{snippet[:200]}"
    v = _SEM_CACHE.get(key)
    if v is not None:
        return v
    try:
        from sentence_transformers import util  # type: ignore
        q = f"{category}: {keyword}"
        emb_q = _SEMANTIC_MODEL.encode(q, convert_to_tensor=True, normalize_embeddings=True)
        emb_s = _SEMANTIC_MODEL.encode(snippet, convert_to_tensor=True, normalize_embeddings=True)
        sim = float(util.cos_sim(emb_q, emb_s))
        _SEM_CACHE[key] = sim
        if len(_SEM_CACHE) > 100_000:
            _SEM_CACHE.clear()
        return sim
    except Exception:
        return 0.0

# ========= Fast cleaners & regex =========
_trans_table = str.maketrans({"\r": " ", "\n": " ", "\t": " "})
def _clean(s: str) -> str:
    s = s.translate(_trans_table)
    return " ".join(s.split())

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

# ========= Renderer integration =========
RENDERER_HTML = os.environ.get("RENDERER_URL", "http://playwright-renderer:9000/render-html")

def domain_of(url: str) -> str:
    try: return urlparse(url).netloc.lower()
    except Exception: return ""

def fetch_rendered_html(url: str) -> str:
    try:
        r = _SESS.get(RENDERER_HTML, params={"url": url}, timeout=25)
        r.raise_for_status()
        if "application/json" in (r.headers.get("content-type") or ""):
            return (r.json() or {}).get("html", "")
        return r.text
    except Exception as e:
        print(f"[render:fail] {url} -> {e}", flush=True)
        return ""

# ========= Text extraction =========
def extract_text(html: str) -> tuple[str, HTMLParser]:
    tree = HTMLParser(html)
    parts: List[str] = []
    for node in tree.css("body :not(script):not(style):not(nav):not(footer)"):
        try:
            t = node.text(separator=" ", strip=True)
            if t and len(t) > 3:
                parts.append(t)
        except Exception:
            continue
        if len(parts) >= 20000:
            break
    return " ".join(parts), tree

# ========= UPI normalization =========
import re as _re
def normalize_upi_from_payload(data: str):
    try:
        if data.startswith(("upi:", "upi://")):
            u = data if data.startswith("upi://") else "upi://" + data.split(":",1)[1]
            qs = parse_qs(urlsplit(u).query)
            pa = qs.get("pa",[None])[0]
            if pa: return pa.lower()
        m = _re.search(r"\b[a-zA-Z0-9._-]{3,}@[a-zA-Z]{2,}\b", data)
        if m: return m.group(0).lower()
    except Exception:
        pass
    return None

# ========= OCR + QR helpers =========
def _absolute_img_src(page_url: str, src: str) -> str:
    if not src: return ""
    if src.startswith("//"): return "https:" + src
    if src.startswith("/") and "://" in page_url:
        proto, rest = page_url.split("://", 1)
        base = proto + "://" + rest.split("/", 1)[0]
        return base + src
    return src

def _iter_img_urls(url: str, tree: HTMLParser):
    for i, img in enumerate(tree.css("img")):
        if i >= MAX_IMGS: break
        src = _absolute_img_src(url, img.attributes.get("src") or "")
        if src: yield src

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

# ========= OpenSearch robust init =========
def _es_host_from_cfg() -> str:
    env_url = os.environ.get("OPENSEARCH_URL")
    if env_url:
        return env_url
    try:
        host = OPENSEARCH_HOST
        if isinstance(host, str):
            return host if host.startswith("http") else "http://" + host
    except Exception:
        pass
    return "http://opensearch:9200"

ES_URL = _es_host_from_cfg()
es = OpenSearch(
    hosts=[ES_URL],
    timeout=30,
    max_retries=3,
    retry_on_timeout=True,
    connection_class=RequestsHttpConnection,
)

# ========= Async queues & background flushers (lossless, minimal logs) =========
MAIN_LOOP: asyncio.AbstractEventLoop | None = None  # captured once

hit_queue: asyncio.Queue[Hit]  = asyncio.Queue(maxsize=PG_QUEUE_MAXSIZE)
es_queue:  asyncio.Queue[dict] = asyncio.Queue(maxsize=ES_QUEUE_MAXSIZE)

async def pg_flusher():
    buf: List[Hit] = []
    last = time.time()
    while True:
        try:
            hit = await hit_queue.get()
            buf.append(hit)
            hit_queue.task_done()
            now = time.time()
            if len(buf) >= HIT_BATCH_SIZE or (now - last) >= PG_FLUSH_INTERVAL_SEC:
                await _flush_pg(buf)
                buf.clear()
                last = now
        except Exception as e:
            print(f"[pg_flusher:error] {e}", flush=True)
            await asyncio.sleep(0.3)

async def es_flusher():
    buf: List[dict] = []
    last = time.time()
    while True:
        try:
            doc = await es_queue.get()
            buf.append(doc)
            es_queue.task_done()
            now = time.time()
            if len(buf) >= ES_BATCH_SIZE or (now - last) >= ES_FLUSH_INTERVAL_SEC:
                await _flush_es(buf)
                buf.clear()
                last = now
        except Exception as e:
            print(f"[es_flusher:error] {e}", flush=True)
            await asyncio.sleep(0.3)

async def _flush_pg(batch: List[Hit]):
    if not batch:
        return
    try:
        db = SessionLocal()
        db.bulk_save_objects(batch)
        db.commit()
        print(f"[pg:bulk] {len(batch)} hits", flush=True)
    except Exception as e:
        print(f"[pg:error] bulk insert -> {e}", flush=True)
        try: db.rollback()
        except Exception: pass
    finally:
        try: db.close()
        except Exception: pass

async def _flush_es(batch: List[dict]):
    if not batch:
        return
    try:
        success, _ = await asyncio.get_event_loop().run_in_executor(
            IO_POOL, lambda: bulk(es, batch)
        )
        if success > 0:
            print(f"[es:bulk] indexed {success} docs", flush=True)
    except Exception as e:
        print(f"[es:error] bulk -> {e}", flush=True)

# ========= Screenshot queue (async) =========
_screenshot_queue: asyncio.Queue[Tuple[str, str]] | None = None
_screenshot_workers_started = False

async def _screenshot_worker():
    while True:
        url, keyword = await _screenshot_queue.get()  # type: ignore
        try:
            await asyncio.get_event_loop().run_in_executor(
                IO_POOL, lambda: capture_screenshot(url, keyword)
            )
        except Exception as e:
            print(f"[screenshot:err] {url}->{e}", flush=True)
        finally:
            _screenshot_queue.task_done()  # type: ignore

async def _ensure_screenshot_workers():
    global _screenshot_queue, _screenshot_workers_started
    if _screenshot_workers_started:
        return
    _screenshot_queue = asyncio.Queue(maxsize=SS_QUEUE_MAXSIZE)
    for _ in range(MAX_SCREENSHOT_WORKERS):
        asyncio.create_task(_screenshot_worker())
    _screenshot_workers_started = True
    print(f"[screenshot:workers] started={MAX_SCREENSHOT_WORKERS}", flush=True)

# ========= In-memory match bucket (lightweight dedupe) =========
match_buffer: dict[str, dict] = defaultdict(lambda: {"sub_urls": set(), "matches": []})
_match_lock = threading.Lock()

# ========= Keyword config (robust load) =========
import yaml
_KW_PATH = os.environ.get("KEYWORDS_FILE","/app/keywords/keywords2.yml")
try:
    with open(_KW_PATH,"r",encoding="utf-8") as f:
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

# ========= Lossless enqueue helper (works from threads or main loop) =========
def _lossless_put(q: asyncio.Queue, item):
    """
    Try fast path put_nowait(); if full, block via MAIN_LOOP until space is available.
    Safe to call from threadpool threads.
    """
    global MAIN_LOOP
    try:
        q.put_nowait(item)
        return
    except asyncio.QueueFull:
        pass
    loop = MAIN_LOOP
    if loop is None:
        # Should not happen once background workers started; fallback to busy-wait
        while True:
            try:
                q.put_nowait(item)
                return
            except asyncio.QueueFull:
                time.sleep(0.01)
    # Block until enqueued (lossless, applies backpressure)
    fut = asyncio.run_coroutine_threadsafe(q.put(item), loop)
    fut.result()

# ========= Core record logic (ES + PG + screenshot) =========
def record_hit(url:str, cat:str, k:str, snip:str, src:str,
               master:str|None=None, confidence:float=1.0,
               task_id:str|None=None):
    """Record a single match (lossless enqueue; minimal logs)."""
    if not master: master = url
    snip = _clean(snip)
    ts = int(time.time())

    # Deduplicate per master/url/keyword (light)
    with _match_lock:
        bucket = match_buffer[master]
        for e in bucket["matches"]:
            if e["url"] == url and e["keyword"] == k:
                return
        bucket["sub_urls"].add(url)
        bucket["matches"].append({
            "url": url, "category": cat, "keyword": k, "snippet": snip,
            "timestamp": ts, "source": src, "confidence": confidence
        })

    # Screenshot enqueue (lossless backpressure if busy)
    if confidence >= 0.7 and _screenshot_queue is not None:
        _lossless_put(_screenshot_queue, (url, k))

    # ES enqueue (lossless)
    _lossless_put(es_queue, {
        "_index": "illegal_hits",
        "_source": {
            "url": url, "category": cat, "keyword": k, "snippet": snip,
            "ts": ts, "source": src, "master_url": master, "confidence": confidence
        }
    })
    score = 0
    if confidence and isinstance(confidence, (float, int)):
        score = max(0, min(int(confidence * 100), 100))

    # PG enqueue (lossless)
    hit = Hit(
        task_id=task_id or "unknown",
        main_url=master,
        sub_url=url,
        category=cat,
        matched_keyword=k,
        snippet=snip,
        screenshot_path=None,
        timestamp=ts,
        source=src,
        confident_score=score
    )
    _lossless_put(hit_queue, hit)

# ========= Matching text =========
def _context_score(text:str, idx:int) -> float:
    w = text[max(0,idx-80):idx+80].casefold()
    return min(sum(1 for t in _PAYMENT_TOKENS if t in w)/4, 1.0)

def match_text(url:str, text:str, master:str|None=None, task_id:str|None=None) -> List[Tuple[str,str,str]]:
    if not text.strip():
        return []
    text = _clean(text)
    low  = text.casefold()
    results: List[Tuple[str,str,str]] = []

    for term, cat, pat in COMPILED:
        for m in pat.finditer(text):
            idx  = m.start()
            snip = text[max(0,idx-100):idx+100]
            sem  = semantic_validate(term, snip, cat)
            if USE_SEMANTIC and sem < _SEMANTIC_THRESHOLD:
                continue
            ctx  = _context_score(text, idx) if cat == "payments" else 1.0
            if cat == "payments" and ctx < 0.3:
                continue
            # Pass raw semantic similarity (used for confident_score)
            record_hit(url, cat, term, snip, "regex", master, sem, task_id=task_id)

            results.append((term, cat, snip))

    for term, cat, alias in ALIASES:
        idx = low.find(alias)
        if idx != -1:
            snip = text[max(0,idx-100):idx+100]
            sem  = semantic_validate(term, snip, cat)
            if USE_SEMANTIC and sem < _SEMANTIC_THRESHOLD:
                continue
            ctx  = _context_score(text, idx) if cat == "payments" else 1.0
            if cat == "payments" and ctx < 0.25:
                continue
            record_hit(url, cat, term, snip, "alias", master, sem, task_id=task_id)
            results.append((term, cat, snip))

    for m in _UPI_CONTEXT_RE.finditer(text):
        idx  = m.start()
        snip = text[max(0,idx-80):idx+80]
        ctx  = _context_score(text, idx)
        if ctx >= 0.3:
            record_hit(url, "payments", "upi-handle", snip, "context", master, 0.85 * ctx, task_id=task_id)
            results.append(("upi-handle", "payments", snip))

    for m in _BTC_RE.finditer(text):
        snip = text[max(0,m.start()-80):m.start()+80]
        record_hit(url, "crypto", "bitcoin", snip, "regex", master, 0.95, task_id=task_id)
        results.append(("bitcoin", "crypto", snip))

    for m in _ETH_RE.finditer(text):
        snip = text[max(0,m.start()-80):m.start()+80]
        record_hit(url, "crypto", "ethereum", snip, "regex", master, 0.95, task_id=task_id)
        results.append(("ethereum", "crypto", snip))

    # Unique compacted
    seen=set(); out=[]
    for t in results:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

# ========= OCR + QR =========
def ocr_and_qr(url: str, tree: HTMLParser, task_id:str|None=None, master:str|None=None) -> List[Tuple[str, str, str]]:
    results: List[Tuple[str,str,str]] = []
    for src in _iter_img_urls(url, tree):
        try:
            with _SESS.get(src, timeout=IMG_HTTP_TIMEOUT_SEC, stream=True) as r:
                r.raise_for_status()
                content = r.raw.read(MAX_IMG_BYTES, decode_content=True)
            with Image.open(io.BytesIO(content)) as img:
                img.load()
                items = []
                if _HAS_PYZBAR:
                    try:
                        items.extend(qr_decode(img))
                    except Exception:
                        pass
                if _HAS_CV2:
                    try:
                        for p in _try_qr_opencv(img):
                            if p:
                                items.append(type("X", (), {"data": p.encode()}))
                    except Exception:
                        pass
                for c in items:
                    payload = getattr(c, "data", b"").decode("utf-8", errors="ignore")
                    upi = normalize_upi_from_payload(payload)
                    if upi:
                        snip = f"QR->UPI:{upi}"
                        record_hit(url, "payments", "upi-qr", snip, "qr", master, 0.9, task_id=task_id)
                        results.append(("upi-qr","payments", snip))

                if not results:
                    txt = _ocr_image(img)
                    if txt:
                        results += match_text(url, _clean(txt), master=master or url, task_id=task_id)
            del content
        except Exception:
            continue
    return results

# ========= Page worker (async) =========
async def _process_page_async(p:dict, main_url:str, task_id:str) -> List[Tuple[str,str,str]]:
    url  = p.get("final_url") or p.get("url")
    html = p.get("html") or ""
    if not url or not html:
        return []
    try:
        text, tree = await asyncio.get_event_loop().run_in_executor(
            CPU_POOL, lambda: extract_text(html)
        )
        del html
        text_clean = _clean(text)

        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            CPU_POOL, lambda: match_text(url, text_clean, master=main_url, task_id=task_id)
        )

        if any(c == "payments" for _, c, _ in results):
            extra = await loop.run_in_executor(
                CPU_POOL, lambda: ocr_and_qr(url, tree, task_id=task_id, master=main_url)
            )
            results += extra
        return results
    except Exception as e:
        print(f"[page:error]{url}->{e}", flush=True)
        return []

# ========= Startup guards for background workers =========
_bg_workers_started = False
async def _ensure_bg_workers():
    global _bg_workers_started, MAIN_LOOP
    if _bg_workers_started:
        return
    MAIN_LOOP = asyncio.get_running_loop()
    # Start ES/PG flushers (add a bit of concurrency for ES)
    asyncio.create_task(pg_flusher())
    for _ in range(2):
        asyncio.create_task(es_flusher())
    # Start screenshot workers
    await _ensure_screenshot_workers()
    _bg_workers_started = True
    print("[background:workers] started", flush=True)

# ========= Entry point =========
async def process_ingest_payload(payload:Dict[str,Any]) -> Dict[str,Any]:
    await _ensure_bg_workers()

    task_id  = payload.get("id") or payload.get("task_id") or "unknown"
    main_url = payload.get("main_url") or "unknown"
    pages    = payload.get("pages") or payload.get("Pages") or []

    # Minimal, useful logs only
    print(f"[ingest:start]{task_id}|pages={len(pages)}|main={main_url}", flush=True)

    with _match_lock:
        match_buffer.pop(main_url, None)
        match_buffer[main_url]["task_id"] = task_id

    sem = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
    all_results: List[Tuple[str,str,str]] = []
    sub_urls: List[str] = []

    async def _worker(p):
        async with sem:
            r = await _process_page_async(p, main_url, task_id)
            all_results.extend(r)
            u = p.get("final_url") or p.get("url")
            if u:
                sub_urls.append(u)

    await asyncio.gather(*[_worker(p) for p in pages])

    cats = sorted({c for _, c, _ in all_results})
    kws  = [k for k, _, _ in all_results]

    # Drain screenshot queue
    if _screenshot_queue is not None:
        await _screenshot_queue.join()

    # Drain ES/PG queues, allow time-based flush of small remainders
    await es_queue.join()
    await hit_queue.join()
    await asyncio.sleep(max(ES_FLUSH_INTERVAL_SEC, PG_FLUSH_INTERVAL_SEC))

    # Persist Result row
    try:
        db = SessionLocal()
        db.bulk_save_objects([
            Result(task_id=task_id, main_url=main_url, sub_urls=sub_urls,
                   keyword_match=kws, word_count_raw_data=0,
                   word_count_cleaned_data=0, categories=cats,
                   raw_data="", cleaned_data="", timestamp=int(time.time()))
        ])
        db.commit()
        print(f"[result:saved]{task_id}|pages={len(sub_urls)}|matches={len(kws)}", flush=True)
    except Exception as e:
        print(f"[db:error]{task_id}->{e}", flush=True)
        try: db.rollback()
        except Exception: pass
    finally:
        try: db.close()
        except Exception: pass

    if len(sub_urls) >= 200:
        gc.collect()

    with _match_lock:
        match_buffer.pop(main_url, None)

    return {
        "task_id": task_id,
        "main_url": main_url,
        "total_pages": len(sub_urls),
        "total_matches": len(kws),
        "categories": cats,
        "semantic_enabled": bool(USE_SEMANTIC and _SEMANTIC_MODEL is not None)
    }
