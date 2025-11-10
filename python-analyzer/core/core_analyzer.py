#!/usr/bin/env python3
"""
core_analyzer.py — Batch-aware, memory-safe analyzer (DB-only) with Semantic Validation + JS-render fallback.

Features:
• PostgreSQL persistence only (Hit + Result) — no CSV, no OpenSearch
• Heavy JS pages via Playwright renderer (RENDERER_URL=/render-and-screenshot)
• Semantic validation (SentenceTransformer cosine similarity threshold)
• YAML-driven regex + alias + brand matching
• OCR + QR + UPI + crypto detection
• Screenshot queue with backpressure
• Batch-aware ingestion and final summary persistence
"""

from __future__ import annotations

# ========= Stdlib =========
import os, io, re, gc, time, json, asyncio, threading, requests
from typing import Dict, Any, List, Tuple
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
)
from models.hit_model import Result, Hit
from libs.screenshot import capture_screenshot

# ========= Tunables / Env =========
MAX_CONCURRENT_PAGES   = int(os.environ.get("MAX_CONCURRENT_PAGES", "50"))
CPU_WORKERS            = int(os.environ.get("CPU_WORKERS", str(os.cpu_count() or 8)))
IO_WORKERS             = int(os.environ.get("IO_WORKERS", "32"))
MAX_SCREENSHOT_WORKERS = int(os.environ.get("MAX_SCREENSHOT_WORKERS", "5"))

OCR_MIN_DIM            = int(os.environ.get("OCR_MIN_DIM", "200"))
IMG_HTTP_TIMEOUT_SEC   = float(os.environ.get("IMG_HTTP_TIMEOUT_SEC", "8"))

HIT_BATCH_SIZE         = int(os.environ.get("HIT_BATCH_SIZE", "200"))
PG_FLUSH_INTERVAL_SEC  = float(os.environ.get("PG_FLUSH_INTERVAL_SEC", "1.0"))

MAX_IMGS               = int(os.environ.get("MAX_IMGS", str(CFG_MAX_IMGS)))
MAX_IMG_BYTES          = int(os.environ.get("MAX_IMG_BYTES", str(CFG_MAX_IMG_BYTES)))

RENDERER_URL           = os.environ.get("RENDERER_URL", "http://localhost:9000/render-and-screenshot")
PW_DOMAINS_FILE        = os.environ.get("PW_DOMAINS_FILE", "/data/playwright_domains.txt")

# Semantic validation
USE_SEMANTIC           = os.environ.get("USE_SEMANTIC", "true").lower() in ("1", "true", "yes")
SEMANTIC_THRESHOLD     = float(os.environ.get("SEMANTIC_THRESHOLD", "0.70"))
SEMANTIC_MODEL_PATH    = os.environ.get("SEMANTIC_MODEL_PATH", "sentence-transformers/all-MiniLM-L6-v2")
AUTOLOAD_SEMANTIC      = os.environ.get("AUTOLOAD_SEMANTIC", "true").lower() in ("1", "true", "yes")

# ========= GC tuning =========
gc.set_threshold(700, 10, 5)

# ========= HTTP session =========
_SESS = requests.Session()
_ADAPTER = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=128, max_retries=2)
_SESS.mount("http://", _ADAPTER)
_SESS.mount("https://", _ADAPTER)
_SESS.headers.update({"User-Agent": "ats-ocr/2.3"})

# ========= ThreadPools =========
CPU_POOL: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=CPU_WORKERS)
IO_POOL:  ThreadPoolExecutor = ThreadPoolExecutor(max_workers=IO_WORKERS)

# ========= Globals / Queues =========
MAIN_LOOP: asyncio.AbstractEventLoop | None = None
hit_queue: asyncio.Queue[Hit]  = asyncio.Queue(maxsize=4000)
_screenshot_queue: asyncio.Queue[Tuple[str, str]] | None = None

# Matching & batch accumulators
_match_lock = threading.Lock()
_batch_lock = threading.Lock()
match_buffer: dict[str, dict] = defaultdict(lambda: {"sub_urls": set(), "matches": []})
_batch_accumulator: dict[str, dict] = defaultdict(lambda: {
    "total_pages": 0, "total_matches": 0, "categories": set(),
    "keywords": [], "sub_urls": [], "last_batch": 0
})

# ========= Screenshot workers =========
async def _screenshot_worker():
    assert _screenshot_queue is not None
    while True:
        url, keyword = await _screenshot_queue.get()
        try:
            await asyncio.get_event_loop().run_in_executor(IO_POOL, lambda: capture_screenshot(url, keyword))
        except Exception as e:
            print(f"[screenshot:error] {url} -> {e}", flush=True)
        finally:
            _screenshot_queue.task_done()

async def _ensure_screenshot_workers():
    global _screenshot_queue
    if _screenshot_queue is not None:
        return
    _screenshot_queue = asyncio.Queue(maxsize=1000)
    for _ in range(MAX_SCREENSHOT_WORKERS):
        asyncio.create_task(_screenshot_worker())
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
    if not batch: return
    db = SessionLocal()
    try:
        db.bulk_save_objects(batch)
        db.commit()
        print(f"[pg:bulk] {len(batch)} hits", flush=True)
    except Exception as e:
        print(f"[pg:error] {e}", flush=True)
        db.rollback()
    finally:
        db.close()

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
_KW_PATH = os.environ.get("KEYWORDS_FILE", "/app/keywords/keywords2.yml")
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

# ========= Semantic validation =========
_SEMANTIC_MODEL = None
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
    if not USE_SEMANTIC or _SEMANTIC_MODEL is None:
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

# Autoload (optional)
if AUTOLOAD_SEMANTIC and USE_SEMANTIC:
    load_semantic_model(SEMANTIC_MODEL_PATH)

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

# ========= Hit recording (DB-only) =========
def record_hit(url:str, cat:str, k:str, snip:str, src:str,
               master:str|None=None, confidence:float=1.0,
               task_id:str|None=None):
    if not master: master = url
    snip = _clean(snip)
    ts = int(time.time())
    # light in-memory dedupe for (url,keyword) within a master bucket
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
    # screenshot enqueue (confidence gate)
    if confidence >= 0.7:
        if _screenshot_queue is not None:
            try: _screenshot_queue.put_nowait((url, k))
            except asyncio.QueueFull: pass
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
    except asyncio.QueueFull:
        print("[pg:queue_full] dropping hit", flush=True)

# ========= Matching (with semantic) =========
def match_text(url:str, text:str, master:str|None=None, task_id:str|None=None) -> List[Tuple[str,str,str]]:
    if not text.strip():
        return []
    text = _clean(text)
    low  = text.casefold()
    results: List[Tuple[str,str,str]] = []

    # Regex patterns
    for term, cat, pat in COMPILED:
        for m in pat.finditer(text):
            idx  = m.start()
            snip = text[max(0,idx-100):idx+100]
            sem  = semantic_validate(term, snip, cat)
            if USE_SEMANTIC and sem < SEMANTIC_THRESHOLD:
                continue
            ctx  = _context_score(text, idx) if cat == "payments" else 1.0
            if cat == "payments" and ctx < 0.30:
                continue
            record_hit(url, cat, term, snip, "regex", master, sem, task_id=task_id)
            results.append((term, cat, snip))

    # Aliases / brands
    for term, cat, alias in ALIASES:
        idx = low.find(alias)
        if idx != -1:
            snip = text[max(0,idx-100):idx+100]
            sem  = semantic_validate(term, snip, cat)
            if USE_SEMANTIC and sem < SEMANTIC_THRESHOLD:
                continue
            ctx  = _context_score(text, idx) if cat == "payments" else 1.0
            if cat == "payments" and ctx < 0.25:
                continue
            record_hit(url, cat, term, snip, "alias", master, sem, task_id=task_id)
            results.append((term, cat, snip))

    # UPI handle in context
    for m in _UPI_CONTEXT_RE.finditer(text):
        idx  = m.start()
        snip = text[max(0,idx-80):idx+80]
        ctx  = _context_score(text, idx)
        if ctx >= 0.30:
            record_hit(url, "payments", "upi-handle", snip, "context", master, 0.85 * ctx, task_id=task_id)
            results.append(("upi-handle", "payments", snip))

    # Crypto wallets
    for m in _BTC_RE.finditer(text):
        snip = text[max(0,m.start()-80):m.start()+80]
        record_hit(url, "crypto", "bitcoin", snip, "regex", master, 0.95, task_id=task_id)
        results.append(("bitcoin", "crypto", snip))

    for m in _ETH_RE.finditer(text):
        snip = text[max(0,m.start()-80):m.start()+80]
        record_hit(url, "crypto", "ethereum", snip, "regex", master, 0.95, task_id=task_id)
        results.append(("ethereum", "crypto", snip))

    # unique compacted
    seen=set(); out=[]
    for t in results:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

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
    html = p.get("html") or ""
    if not url or not html:
        return []

    domain = _domain_of(url)
    force_render = domain in load_pw_domains()

    async def process_html(content: str):
        text, tree = await asyncio.get_event_loop().run_in_executor(CPU_POOL, lambda: extract_text(content))
        results = await asyncio.get_event_loop().run_in_executor(CPU_POOL, lambda: match_text(url, text, master=main_url, task_id=task_id))
        if any(c == "payments" for _, c, _ in results):
            results += await asyncio.get_event_loop().run_in_executor(CPU_POOL, lambda: ocr_and_qr(url, tree, task_id=task_id, master=main_url))
        return results, text

    # First pass (raw HTML)
    results, text = await process_html(html)

    # Escalate to Playwright render if domain flagged OR low-content & no hits
    if RENDERER_URL and (force_render or (not results and len(text) < 200)):
        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                IO_POOL, lambda: _SESS.get(RENDERER_URL, params={"url": url}, timeout=25)
            )
            # Your endpoint returns full HTML; ensure it looks like a document
            if resp.status_code == 200 and "<html" in (resp.text or "").lower():
                rres, rtext = await process_html(resp.text)
                if rres:
                    add_pw_domain(domain)
                    results.extend(rres)
        except Exception as e:
            print(f"[render:fail] {url} -> {e}", flush=True)

    return results

# ========= Graceful draining =========
async def _drain_queues():
    async def _drain(q: asyncio.Queue, label: str, timeout: float = 10.0):
        start = time.time()
        while not q.empty():
            await asyncio.sleep(0.25)
            if time.time() - start > timeout:
                try: size = q.qsize()
                except Exception: size = -1
                print(f"[drain:timeout] {label} ({size} left)", flush=True)
                break

    if _screenshot_queue is not None:
        await _drain(_screenshot_queue, "screenshots", 15)
    await _drain(hit_queue, "pg", 15)
    await asyncio.sleep(0)

# ========= Persist summary =========
async def _persist_result(task_id: str, main_url: str, sub_urls: List[str],
                          kws: List[str], cats: List[str]):
    db = SessionLocal()
    try:
        db.merge(Result(
            task_id=task_id,
            main_url=main_url,
            sub_urls=sub_urls,
            keyword_match=kws,
            categories=cats,
            raw_data="",
            cleaned_data="",
            timestamp=int(time.time())
        ))
        db.commit()
        print(f"[db:result:ok] {task_id} ({len(sub_urls)} urls, {len(kws)} hits)", flush=True)
    except Exception as e:
        print(f"[db:result:error] {task_id} -> {e}", flush=True)
        db.rollback()
    finally:
        db.close()

# ========= Ingest entry =========
async def process_ingest_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call this from your /ingest route.
    Accepts both batch (task_id, main_url, batch_num, pages[], is_complete)
    and legacy single-payload (no batch fields).
    """
    await _ensure_bg_workers()

    task_id = payload.get("id") or payload.get("task_id") or "unknown"
    main_url = payload.get("main_url") or "unknown"
    batch_num = payload.get("batch_num", 1)
    is_complete = payload.get("is_complete", False)
    pages = payload.get("pages") or payload.get("Pages") or []
    total_pages = len(pages)

    is_legacy = "batch_num" not in payload and "is_complete" not in payload
    if is_legacy:
        is_complete = True
        batch_num = 1

    print(f"[ingest] task={task_id} batch={batch_num} pages={total_pages} complete={is_complete} main={main_url}", flush=True)

    with _match_lock:
        if batch_num == 1:
            match_buffer.pop(main_url, None)
        match_buffer[main_url]["task_id"] = task_id

    sem = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
    all_results: List[Tuple[str, str, str]] = []
    sub_urls: List[str] = []

    async def worker(page):
        async with sem:
            try:
                res = await _process_page_async(page, main_url, task_id)
                all_results.extend(res)
                u = page.get("final_url") or page.get("url")
                if u: sub_urls.append(u)
            except Exception as e:
                print(f"[ingest:worker:error] {e}", flush=True)

    # Stream in chunks to limit memory
    CHUNK = MAX_CONCURRENT_PAGES * 2
    for i in range(0, total_pages, CHUNK):
        chunk = pages[i:i+CHUNK]
        await asyncio.gather(*[worker(p) for p in chunk], return_exceptions=True)
        if i % (CHUNK*5) == 0:
            await asyncio.sleep(0); gc.collect()

    cats = sorted({c for _, c, _ in all_results})
    kws = [k for k, _, _ in all_results]

    with _batch_lock:
        acc = _batch_accumulator[task_id]
        acc["total_pages"]   += len(sub_urls)
        acc["total_matches"] += len(kws)
        acc["categories"].update(cats)
        acc["keywords"].extend(kws)
        acc["sub_urls"].extend(sub_urls)
        acc["last_batch"]     = batch_num

    # Let queues drain a bit
    await _drain_queues()

    if is_complete:
        with _batch_lock:
            acc = _batch_accumulator[task_id]
            final_cats = sorted(acc["categories"])
            final_kws  = acc["keywords"]
            final_urls = acc["sub_urls"]

        await _persist_result(task_id, main_url, final_urls, final_kws, final_cats)

        # Cleanup
        with _batch_lock:
            _batch_accumulator.pop(task_id, None)
        with _match_lock:
            match_buffer.pop(main_url, None)

        if len(final_urls) > 500:
            gc.collect()

        print(f"[ingest:complete] {task_id} batches={batch_num} pages={len(final_urls)} matches={len(final_kws)}", flush=True)
        return {
            "task_id": task_id,
            "main_url": main_url,
            "total_pages": len(final_urls),
            "total_matches": len(final_kws),
            "total_batches": batch_num,
            "categories": final_cats,
            "semantic_enabled": bool(USE_SEMANTIC and _SEMANTIC_MODEL is not None),
            "status": "completed",
        }

    # Incremental update
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
            "semantic_enabled": bool(USE_SEMANTIC and _SEMANTIC_MODEL is not None),
            "status": "processing",
        }
