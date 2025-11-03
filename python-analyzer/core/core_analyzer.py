#!/usr/bin/env python3
"""
core_analyzer.py (batch analyzer with optional semantic validation
+ Playwright fallback + domain stats + UPI aggregation)

- Optional SentenceTransformer semantics (USE_SEMANTIC=true/false)
- Renders JS-heavy pages via Playwright renderer when needed
- Auto-escalates domains to always-render after repeated JS wins
- OCR + QR (UPI normalization)
- Records hits in DB + OpenSearch (dedupe + single screenshot per URL)
- Aggregates UPI handle -> domains map under /data/upi_map.json
- Persists domain stats under /data/domain_stats.json
- Persists PW domain allowlist under /data/playwright_domains.txt

Entry point:
    async def process_ingest_payload(payload: Dict[str, Any]) -> Dict[str, Any]

Payload formats supported (from Go fetcher):
- AnalyzerBatch with "pages": [{ "url", "final_url", "html", ... }]
- Legacy: task.Pages-like array with "url" and "html"
"""

from __future__ import annotations
import os, re, io, time, json, yaml, requests
from typing import Dict, Any, Iterable, List, Tuple
from collections import defaultdict
from urllib.parse import urlparse, urlsplit, parse_qs

from selectolax.parser import HTMLParser
from PIL import Image
from pyzbar.pyzbar import decode as qr_decode
import pytesseract
import regex as regx
from opensearchpy import OpenSearch, RequestsHttpConnection

# ---------------------------
# Optional OpenCV QR fallback
# ---------------------------
try:
    import cv2, numpy as np
    _HAS_CV2 = True
except Exception:
    _HAS_CV2 = False

# ---------------------------
# Config / env
# ---------------------------
DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)

UPI_MAP_PATH = os.path.join(DATA_DIR, "upi_map.json")
DOMAIN_STATS_PATH = os.path.join(DATA_DIR, "domain_stats.json")
PW_DOMAINS_FILE = os.path.join(DATA_DIR, "playwright_domains.txt")
for p, init in [(UPI_MAP_PATH, {}), (DOMAIN_STATS_PATH, {})]:
    if not os.path.exists(p):
        with open(p, "w", encoding="utf-8") as f:
            json.dump(init, f)
if not os.path.exists(PW_DOMAINS_FILE):
    with open(PW_DOMAINS_FILE, "w", encoding="utf-8") as f:
        f.write("")

from config.settings import (
    MAX_IMGS, MAX_IMG_BYTES, FUZZ_THRESHOLD,
    OPENSEARCH_HOST, PW_DOMAINS as CFG_PW_DOMAINS, SessionLocal
)
from models.hit_model import Result, Hit
from libs.screenshot import capture_screenshot

# ---------------------------
# HTTP session (pooled)
# ---------------------------
_SESS = requests.Session()
_ADAPTER = requests.adapters.HTTPAdapter(pool_connections=12, pool_maxsize=24, max_retries=2)
_SESS.mount("http://", _ADAPTER)
_SESS.mount("https://", _ADAPTER)
_SESS.headers.update({"User-Agent": "ats-ocr/1.0"})

_screenshot_cache: set[str] = set()

# ---------------------------
# Semantic model (optional)
# ---------------------------
USE_SEMANTIC = os.environ.get("USE_SEMANTIC", "true").lower() in ("1", "true", "yes")
_SEMANTIC_MODEL = None
_SEMANTIC_THRESHOLD = float(os.environ.get("SEMANTIC_THRESHOLD", "0.75"))
_SEM_CACHE: dict[str, float] = {}

def load_semantic_model(model_path: str):
    """Load fine-tuned SentenceTransformer model once at startup."""
    global _SEMANTIC_MODEL
    if not USE_SEMANTIC:
        print("[semantic:model:disabled]", flush=True)
        return
    try:
        from sentence_transformers import SentenceTransformer
        _SEMANTIC_MODEL = SentenceTransformer(model_path)
        print(f"[semantic:model:loaded] {model_path}", flush=True)
    except Exception as e:
        print(f"[semantic:model:error] {e}", flush=True)
        _SEMANTIC_MODEL = None

def semantic_validate(keyword: str, snippet: str, category: str) -> float:
    """Return semantic similarity (0..1) or 1.0 if disabled/unavailable."""
    if not USE_SEMANTIC or not _SEMANTIC_MODEL:
        return 1.0
    key = f"{category}|{keyword}|{snippet[:200]}"
    if key in _SEM_CACHE:
        return _SEM_CACHE[key]
    try:
        from sentence_transformers import util
        query = f"{category}: {keyword}"
        emb_q = _SEMANTIC_MODEL.encode(query, convert_to_tensor=True, normalize_embeddings=True)
        emb_s = _SEMANTIC_MODEL.encode(snippet, convert_to_tensor=True, normalize_embeddings=True)
        sim = float(util.cos_sim(emb_q, emb_s))
        _SEM_CACHE[key] = sim
        return sim
    except Exception:
        return 0.0

# ---------------------------
# Cleaning & regex
# ---------------------------
_CLEAN_NEWLINES = re.compile(r"[\t\r\n]+")
_CLEAN_SPACES = re.compile(r"\s{2,}")
_CLEAN_NONASCII = re.compile(r"[^\x20-\x7E]+")

def _clean(s: str) -> str:
    s = _CLEAN_NEWLINES.sub(" ", s)
    s = _CLEAN_SPACES.sub(" ", s)
    s = _CLEAN_NONASCII.sub(" ", s)
    return s.strip()

_UPI_CONTEXT_RE = regx.compile(
    r"\b[a-zA-Z0-9._-]{2,}@(upi|paytm|ybl|okicici|oksbi|okaxis|okhdfcbank|ibl|axl|idfcbank|apl|payu|pingpay|barodampay|boi|zomato)\b",
    flags=regx.I
)
_BTC_RE = regx.compile(r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b")
_ETH_RE = regx.compile(r"\b0x[a-fA-F0-9]{40}\b")

_PAYMENT_TOKENS = (
    "buy","order","pay","scan","checkout","upi","gpay",
    "phonepe","paytm","payment","merchant","qr","amount","send","transfer"
)

# ---------------------------
# Renderer integration
# ---------------------------
RENDERER_HTML = os.environ.get("RENDERER_URL", "http://localhost:9000/render-html")

def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def _load_pw_domains_file() -> set[str]:
    try:
        with open(PW_DOMAINS_FILE, "r", encoding="utf-8") as f:
            return {ln.strip().lower() for ln in f if ln.strip()}
    except Exception:
        return set()

def _save_pw_domains_file(domains: set[str]) -> None:
    try:
        with open(PW_DOMAINS_FILE, "w", encoding="utf-8") as f:
            for d in sorted(domains):
                f.write(d + "\n")
    except Exception:
        pass

def _is_pw_domain(url: str) -> bool:
    dom = domain_of(url)
    if not dom:
        return False
    # union of config list + persisted file
    cfg = {d.lower() for d in (CFG_PW_DOMAINS or [])}
    file_set = _load_pw_domains_file()
    return any(d in dom for d in cfg.union(file_set))

def _get_domain_stats() -> dict:
    try:
        with open(DOMAIN_STATS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_domain_stats(stats: dict) -> None:
    try:
        with open(DOMAIN_STATS_PATH, "w", encoding="utf-8") as f:
            json.dump(stats, f)
    except Exception:
        pass

def incr_domain_stat(dom: str, key: str):
    stats = _get_domain_stats()
    entry = stats.get(dom, {"seen": 0, "js_success": 0})
    entry[key] = int(entry.get(key, 0)) + 1
    stats[dom] = entry
    _save_domain_stats(stats)

def get_domain_stat(dom: str) -> dict:
    stats = _get_domain_stats()
    return stats.get(dom, {"seen": 0, "js_success": 0})

def escalate_pw_domain(dom: str):
    s = _load_pw_domains_file()
    if dom not in s:
        s.add(dom)
        _save_pw_domains_file(s)
        print(f"[policy:pw:add] {dom}", flush=True)

def fetch_rendered_html(url: str) -> str:
    if not RENDERER_HTML:
        return ""
    try:
        r = _SESS.get(RENDERER_HTML, params={"url": url}, timeout=25)
        r.raise_for_status()
        if "application/json" in (r.headers.get("content-type") or ""):
            return (r.json() or {}).get("html", "")
        return r.text
    except Exception as e:
        print(f"[rendered:fail] {url} -> {e}", flush=True)
        return ""

# ---------------------------
# Text extraction
# ---------------------------
def extract_text(html: str) -> tuple[str, HTMLParser]:
    tree = HTMLParser(html)
    # conservative: skip script/style/nav/footer
    parts = []
    for node in tree.css("body :not(script):not(style):not(nav):not(footer)"):
        try:
            t = node.text(separator=" ", strip=True)
            if t and len(t) > 3:
                parts.append(t)
        except Exception:
            continue
    return " ".join(parts), tree

# ---------------------------
# UPI helpers
# ---------------------------
_UPI_REGEX_GENERIC = re.compile(r"\b[a-zA-Z0-9._-]{3,}@[a-zA-Z]{2,}\b")

def normalize_upi_from_payload(data: str):
    try:
        if data.startswith("upi:") or data.startswith("upi://"):
            u = data if data.startswith("upi://") else "upi://" + data.split(":", 1)[1]
            pr = urlsplit(u)
            qs = parse_qs(pr.query)
            pa = qs.get("pa", [None])[0]
            if pa:
                return pa.lower()
        m = _UPI_REGEX_GENERIC.search(data)
        if m:
            return m.group(0).lower()
    except Exception:
        pass
    return None

def _read_upi_map() -> dict:
    try:
        with open(UPI_MAP_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_upi_map(mp: dict) -> None:
    try:
        with open(UPI_MAP_PATH, "w", encoding="utf-8") as f:
            json.dump(mp, f)
    except Exception:
        pass

def upi_map_update(url: str, handle: str):
    if not handle:
        return
    dom = domain_of(url)
    if not dom:
        return
    mp = _read_upi_map()
    ent = mp.get(handle, {"domains": {}, "sample_url": url})
    ent["domains"][dom] = ent["domains"].get(dom, 0) + 1
    if not ent.get("sample_url"):
        ent["sample_url"] = url
    mp[handle] = ent
    _save_upi_map(mp)

# ---------------------------
# OCR + QR
# ---------------------------
def _absolute_img_src(page_url: str, src: str) -> str:
    if not src:
        return ""
    if src.startswith("//"):
        return "https:" + src
    if src.startswith("/") and "://" in page_url:
        proto, rest = page_url.split("://", 1)
        base = proto + "://" + rest.split("/", 1)[0]
        return base + src
    return src

def _iter_img_urls(url: str, tree: HTMLParser) -> Iterable[str]:
    count = 0
    for img in tree.css("img"):
        src = _absolute_img_src(url, img.attributes.get("src") or "")
        if src:
            yield src
            count += 1
            if count >= MAX_IMGS:
                break

def _ocr_image(img: Image.Image) -> str:
    try:
        g = img.convert("L")
        w, h = g.size
        if w < 300 or h < 300:
            g = g.resize((w * 2, h * 2))
        conf = "--psm 6 -c tessedit_char_whitelist=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@._-"
        return pytesseract.image_to_string(g, config=conf)
    except Exception:
        return ""

def _try_qr_opencv(img: Image.Image) -> List[str]:
    if not _HAS_CV2:
        return []
    try:
        cv_img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
        d = cv2.QRCodeDetector()
        data, _, _ = d.detectAndDecode(cv_img)
        return [data] if data else []
    except Exception:
        return []

def ocr_and_qr(url: str, tree: HTMLParser) -> List[Tuple[str, str, str]]:
    results: List[Tuple[str, str, str]] = []
    for src in _iter_img_urls(url, tree):
        try:
            with _SESS.get(src, timeout=10, stream=True) as r:
                r.raise_for_status()
                content = r.raw.read(MAX_IMG_BYTES, decode_content=True)
            img = Image.open(io.BytesIO(content))
            # QR
            codes = qr_decode(img)
            if not codes:
                for p in _try_qr_opencv(img):
                    if p:
                        upi = normalize_upi_from_payload(p)
                        if upi:
                            snip = f"QR->UPI:{upi}"
                            record_hit(url, "payments", "upi-qr", snip, "qr")
                            results.append(("upi-qr", "payments", snip))
                            upi_map_update(url, upi)
                            continue
            for c in codes:
                payload = c.data.decode("utf-8", errors="ignore")
                upi = normalize_upi_from_payload(payload)
                if upi:
                    snip = f"QR->UPI:{upi}"
                    record_hit(url, "payments", "upi-qr", snip, "qr")
                    results.append(("upi-qr", "payments", snip))
                    upi_map_update(url, upi)
            # OCR
            if not results:
                txt = _ocr_image(img)
                if txt:
                    results += match_text(url, _clean(txt))
        except Exception:
            continue
    return results

# ---------------------------
# Keyword config
# ---------------------------
es = OpenSearch(
    OPENSEARCH_HOST,
    timeout=30, max_retries=3, retry_on_timeout=True,
    connection_class=RequestsHttpConnection,
)

with open(os.environ.get("KEYWORDS_FILE", "/app/keywords/keywords.yml"), "r", encoding="utf-8") as f:
    _cfg = yaml.safe_load(f) or {}
KW = _cfg.get("keywords", [])

COMPILED: List[Tuple[str, str, regx.Pattern]] = []
ALIASES: List[Tuple[str, str, str]] = []
_seen_alias = set()

for e in KW:
    term = (e.get("term") or "").strip()
    cat = (e.get("category") or "uncat").strip()
    for pat in (e.get("patterns") or []) or []:
        try:
            COMPILED.append((term, cat, regx.compile(pat, regx.I)))
        except Exception:
            pass
    for a in (e.get("aliases") or []) + (e.get("brands") or []):
        if a:
            al = a.strip().casefold()
            if len(al) >= 3 and (term, cat, al) not in _seen_alias:
                ALIASES.append((term, cat, al))
                _seen_alias.add((term, cat, al))

match_buffer: dict[str, dict] = defaultdict(lambda: {"sub_urls": set(), "matches": []})

# ---------------------------
# Helpers
# ---------------------------
def _context_score(text: str, idx: int) -> float:
    w = text[max(0, idx - 80): idx + 80].casefold()
    weight = sum(1 for t in _PAYMENT_TOKENS if t in w)
    return min(weight / 4, 1.0)

def clear_screenshot_cache():
    global _screenshot_cache
    _screenshot_cache.clear()
    print("[screenshot:cache:cleared]", flush=True)

# ---------------------------
# Record hit (dedupe + 1 screenshot per URL)
# ---------------------------
def record_hit(url: str, cat: str, k: str, snip: str, src: str,
               master: str | None = None, confidence: float = 1.0) -> None:
    if not master:
        master = url
    ts = int(time.time())
    snip = _clean(snip)
    bucket = match_buffer[master]

    # Deduplicate (url, keyword)
    for e in bucket["matches"]:
        if e["url"] == url and e["keyword"] == k:
            return

    # Single screenshot per URL
    ss_url = ""
    if url not in _screenshot_cache:
        try:
            ss_result = capture_screenshot(url, k)
            if isinstance(ss_result, dict):
                ss_url = ss_result.get("screenshot") or ss_result.get("url") or ""
            else:
                ss_url = getattr(ss_result, "screenshot", "")
            _screenshot_cache.add(url)
            print(f"[screenshot:captured] {url}", flush=True)
        except Exception as e:
            print(f"[screenshot:error] {url} -> {e}", flush=True)
    else:
        print(f"[screenshot:cached] {url}", flush=True)

    # Buffer
    bucket["sub_urls"].add(url)
    bucket["matches"].append({
        "url": url, "category": cat, "keyword": k, "snippet": snip,
        "timestamp": ts, "source": src, "confidence": float(confidence)
    })

    # DB write (best-effort)
    try:
        db = SessionLocal()
        hit = Hit(
            task_id=bucket.get("task_id", "unknown"),
            main_url=master, sub_url=url, category=cat,
            matched_keyword=k, snippet=snip, timestamp=ts,
            screenshot_path=ss_url, source=src
        )
        db.add(hit); db.commit()
    except Exception as e:
        print(f"[db:hit:error] {url} -> {e}", flush=True)
        if 'db' in locals(): db.rollback()
    finally:
        if 'db' in locals(): db.close()

    # OpenSearch (best-effort)
    try:
        es.index(index="illegal_hits", document={
            "url": url, "category": cat, "keyword": k, "snippet": snip,
            "ts": ts, "source": src, "master_url": master,
            "confidence": float(confidence)
        })
    except Exception:
        pass

# ---------------------------
# Matching + semantic verification
# ---------------------------
def match_text(url: str, text: str, master: str | None = None) -> List[Tuple[str, str, str]]:
    if not text.strip():
        return []
    text = _clean(text)
    low = text.casefold()
    results: List[Tuple[str, str, str]] = []

    # Regex patterns
    for term, cat, pat in COMPILED:
        for m in pat.finditer(text):
            idx = m.start()
            snip = text[max(0, idx - 100): idx + 100]
            sem_conf = semantic_validate(term, snip, cat)
            if USE_SEMANTIC and sem_conf < _SEMANTIC_THRESHOLD:
                continue
            ctx_pay = _context_score(text, idx) if cat == "payments" else 1.0
            if cat == "payments" and ctx_pay < 0.3:
                continue
            record_hit(url, cat, term, snip, "regex", master, confidence=sem_conf * ctx_pay)
            results.append((term, cat, snip))

    # Alias exact contains
    for term, cat, alias_cf in ALIASES:
        idx = low.find(alias_cf)
        if idx != -1:
            snip = text[max(0, idx - 100): idx + 100]
            sem_conf = semantic_validate(term, snip, cat)
            if USE_SEMANTIC and sem_conf < _SEMANTIC_THRESHOLD:
                continue
            ctx_pay = _context_score(text, idx) if cat == "payments" else 1.0
            if cat == "payments" and ctx_pay < 0.25:
                continue
            record_hit(url, cat, term, snip, "alias", master, confidence=sem_conf * ctx_pay)
            results.append((term, cat, snip))

    # UPI handles
    for m in _UPI_CONTEXT_RE.finditer(text):
        idx = m.start()
        ctx = _context_score(text, idx)
        if ctx >= 0.3:
            snip = text[max(0, idx - 80): idx + 80]
            record_hit(url, "payments", "upi-handle", snip, "context", master, confidence=0.85 * ctx)
            results.append(("upi-handle", "payments", snip))
            # Update UPI map if we can extract handle
            upi = normalize_upi_from_payload(snip)
            if upi:
                upi_map_update(url, upi)

    # Bitcoin & Ethereum
    for m in _BTC_RE.finditer(text):
        snip = text[max(0, m.start() - 80): m.start() + 80]
        record_hit(url, "crypto", "bitcoin", snip, "regex", master, confidence=0.95)
        results.append(("bitcoin", "crypto", snip))

    for m in _ETH_RE.finditer(text):
        snip = text[max(0, m.start() - 80): m.start() + 80]
        record_hit(url, "crypto", "ethereum", snip, "regex", master, confidence=0.95)
        results.append(("ethereum", "crypto", snip))

    # Deduplicate (preserve order)
    seen = set(); unique = []
    for t in results:
        if t not in seen:
            seen.add(t); unique.append(t)
    return unique

# ---------------------------
# JS heaviness heuristic (quick)
# ---------------------------
JS_HEAVY_TEXT_THRESHOLD = int(os.environ.get("JS_HEAVY_TEXT_THRESHOLD", "400"))
JS_ESCALATE_THRESHOLD = int(os.environ.get("JS_ESCALATE_THRESHOLD", "2"))

JS_HEAVY_MARKERS = (
    "__NEXT_DATA__", "id=\"__next\"", "data-reactroot", "ng-version",
    "vite", "webpackJsonp", "window.__APOLLO_STATE__", "nuxt",
    "id=\"root\"", "id=\"app\"", "astro-island", "svelte"
)

def _is_js_light_and_sparse(html: str, tree: HTMLParser) -> bool:
    try:
        body_texts = []
        for node in tree.css("body :not(script):not(style):not(nav):not(footer)"):
            try:
                t = node.text(separator=" ", strip=True)
                if t and len(t) > 3:
                    body_texts.append(t)
            except Exception:
                continue
        visible_chars = sum(len(x) for x in body_texts)
        low_html = html.lower()
        marker_hits = sum(1 for m in JS_HEAVY_MARKERS if m.lower() in low_html)
        # sparse text but framework markers present -> likely JS-heavy
        return visible_chars < JS_HEAVY_TEXT_THRESHOLD and marker_hits >= 1
    except Exception:
        return False

# ---------------------------
# Entry point
# ---------------------------
async def process_ingest_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accepts a batch payload from the Go fetcher. We expect:
      {
        "id": "...", "main_url": "...",
        "pages": [ { "url": "...", "final_url": "...", "html": "...", ... }, ... ]
      }
    """
    clear_screenshot_cache()

    task_id = payload.get("id") or payload.get("task_id") or "unknown-task"
    main_url = payload.get("main_url") or "master:unknown"
    pages = payload.get("pages", [])
    if not pages:
        # legacy: some callers send {"pages":[{"url","HTML"}]} already uppercased
        pages = payload.get("Pages", [])

    match_buffer[main_url]["task_id"] = task_id
    sub_urls: List[str] = []
    all_matches: List[Tuple[str, str, str]] = []
    print(f"[batch:start] task={task_id} | pages={len(pages)}", flush=True)

    for p in pages:
        # normalize page fields
        url = p.get("final_url") or p.get("url")
        html = p.get("html") or p.get("HTML") or ""
        if not url or not html:
            continue
        sub_urls.append(url)
        dom = domain_of(url)
        incr_domain_stat(dom, "seen")

        try:
            # If domain is in PW list, render first
            force_pw = _is_pw_domain(url)
            if force_pw:
                rendered = fetch_rendered_html(url)
                if rendered:
                    html = rendered

            # Parse & extract
            text, tree = extract_text(html)
            text_clean = _clean(text)
            results = match_text(url, text_clean, master=main_url)
            results += ocr_and_qr(url, tree)

            # If no results and page looks JS-light/sparse, try PW once
            if not results and not force_pw and RENDERER_HTML:
                if _is_js_light_and_sparse(html, tree):
                    rendered = fetch_rendered_html(url)
                    if rendered:
                        rtext, rtree = extract_text(rendered)
                        rtext_clean = _clean(rtext)
                        rres = match_text(url, rtext_clean, master=main_url)
                        rres += ocr_and_qr(url, rtree)
                        if rres:
                            # mark JS success; possibly escalate domain
                            incr_domain_stat(dom, "js_success")
                            st = get_domain_stat(dom)
                            if int(st.get("js_success", 0)) >= JS_ESCALATE_THRESHOLD:
                                escalate_pw_domain(dom)
                            results.extend(rres)

            all_matches.extend(results)

        except Exception as e:
            print(f"[batch:error] {url} -> {e}", flush=True)

    # Summaries
    cats = sorted({c for (_, c, _) in all_matches})
    kws = [k for k, _, _ in all_matches]

    # Persist Result (DB)
    db = SessionLocal()
    try:
        record = Result(
            task_id=task_id,
            main_url=main_url,
            sub_urls=sub_urls,
            keyword_match=kws,
            word_count_raw_data=0,       # we don't keep full raw body here
            word_count_cleaned_data=0,   # reserved
            categories=cats,
            raw_data="", cleaned_data="",
            timestamp=int(time.time()),
        )
        db.add(record)
        db.commit()
        print(f"[result:saved] task_id={task_id} | matches={len(kws)}", flush=True)
    except Exception as e:
        db.rollback()
        print(f"[result:error] {task_id} -> {e}", flush=True)
    finally:
        db.close()

    return {
        "task_id": task_id,
        "main_url": main_url,
        "total_pages": len(sub_urls),
        "total_matches": len(kws),
        "categories": cats,
        "semantic_enabled": bool(USE_SEMANTIC and _SEMANTIC_MODEL is not None),
    }
