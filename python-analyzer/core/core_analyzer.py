#!/usr/bin/env python3
"""
core_analyzer_async.py  –  scalable analyzer (>1000 URLs)

• Parallel HTML analysis with asyncio.Semaphore
• Thread-pool OCR / QR decoding
• Memory-safe (drops HTML once parsed)
• Batched DB + OpenSearch writes
• Optional semantic similarity
"""

from __future__ import annotations
import os, re, io, time, json, asyncio, requests
from typing import Dict, Any, List, Tuple
from collections import defaultdict
from urllib.parse import urlparse, urlsplit, parse_qs
from concurrent.futures import ThreadPoolExecutor

from selectolax.parser import HTMLParser
from PIL import Image
from pyzbar.pyzbar import decode as qr_decode
import pytesseract
import regex as regx
from opensearchpy import OpenSearch, RequestsHttpConnection

# --- Optional OpenCV QR ---
try:
    import cv2, numpy as np
    _HAS_CV2 = True
except Exception:
    _HAS_CV2 = False

# --- Config / env ---
DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)

from config.settings import (
    MAX_IMGS, MAX_IMG_BYTES, FUZZ_THRESHOLD,
    OPENSEARCH_HOST, PW_DOMAINS as CFG_PW_DOMAINS, SessionLocal
)
from models.hit_model import Result, Hit
from libs.screenshot import capture_screenshot

# --- HTTP session ---
_SESS = requests.Session()
_ADAPTER = requests.adapters.HTTPAdapter(pool_connections=24, pool_maxsize=48, max_retries=2)
_SESS.mount("http://", _ADAPTER)
_SESS.mount("https://", _ADAPTER)
_SESS.headers.update({"User-Agent": "ats-ocr/2.0"})

# --- Async control ---
MAX_CONCURRENT_PAGES = int(os.environ.get("MAX_CONCURRENT_PAGES", "20"))
EXECUTOR = ThreadPoolExecutor(max_workers=6)

# --- Semantic model ---
USE_SEMANTIC = os.environ.get("USE_SEMANTIC", "true").lower() in ("1", "true", "yes")
_SEMANTIC_MODEL = None
_SEMANTIC_THRESHOLD = float(os.environ.get("SEMANTIC_THRESHOLD", "0.75"))
_SEM_CACHE: dict[str, float] = {}

def load_semantic_model(path: str):
    global _SEMANTIC_MODEL
    if not USE_SEMANTIC:
        print("[semantic:disabled]", flush=True); return
    try:
        from sentence_transformers import SentenceTransformer
        _SEMANTIC_MODEL = SentenceTransformer(path)
        print(f"[semantic:loaded] {path}", flush=True)
    except Exception as e:
        print(f"[semantic:error] {e}", flush=True)

def semantic_validate(keyword: str, snippet: str, category: str) -> float:
    if not USE_SEMANTIC or not _SEMANTIC_MODEL:
        return 1.0
    key = f"{category}|{keyword}|{snippet[:200]}"
    if key in _SEM_CACHE: return _SEM_CACHE[key]
    try:
        from sentence_transformers import util
        q = f"{category}: {keyword}"
        emb_q = _SEMANTIC_MODEL.encode(q, convert_to_tensor=True, normalize_embeddings=True)
        emb_s = _SEMANTIC_MODEL.encode(snippet, convert_to_tensor=True, normalize_embeddings=True)
        sim = float(util.cos_sim(emb_q, emb_s))
        _SEM_CACHE[key] = sim
        return sim
    except Exception:
        return 0.0

# --- Regex / cleaning ---
_CLEAN_NEWLINES = re.compile(r"[\t\r\n]+")
_CLEAN_SPACES = re.compile(r"\s{2,}")
_CLEAN_NONASCII = re.compile(r"[^\x20-\x7E]+")
def _clean(s: str) -> str:
    return _CLEAN_NONASCII.sub(" ", _CLEAN_SPACES.sub(" ", _CLEAN_NEWLINES.sub(" ", s))).strip()

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

# --- Renderer integration ---
RENDERER_HTML = os.environ.get("RENDERER_URL", "http://localhost:9000/render-html")

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

# --- Text extraction ---
def extract_text(html: str) -> tuple[str, HTMLParser]:
    tree = HTMLParser(html)
    parts = []
    for node in tree.css("body :not(script):not(style):not(nav):not(footer)"):
        try:
            t = node.text(separator=" ", strip=True)
            if t and len(t) > 3: parts.append(t)
        except Exception: continue
    return " ".join(parts), tree

# --- UPI normalization ---
def normalize_upi_from_payload(data: str):
    try:
        if data.startswith(("upi:", "upi://")):
            u = data if data.startswith("upi://") else "upi://" + data.split(":",1)[1]
            qs = parse_qs(urlsplit(u).query)
            pa = qs.get("pa",[None])[0]
            if pa: return pa.lower()
        m = re.search(r"\b[a-zA-Z0-9._-]{3,}@[a-zA-Z]{2,}\b", data)
        if m: return m.group(0).lower()
    except Exception: pass
    return None

# --- OCR + QR helpers ---
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
        if w < 300 or h < 300: g = g.resize((w*2, h*2))
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

def ocr_and_qr(url: str, tree: HTMLParser) -> List[Tuple[str, str, str]]:
    results = []
    for src in _iter_img_urls(url, tree):
        try:
            with _SESS.get(src, timeout=10, stream=True) as r:
                r.raise_for_status()
                content = r.raw.read(MAX_IMG_BYTES, decode_content=True)
            img = Image.open(io.BytesIO(content))
            # QR
            for c in qr_decode(img) + [type("X",(),{"data":p.encode()}) for p in _try_qr_opencv(img)]:
                payload = getattr(c,"data",b"").decode("utf-8",errors="ignore")
                upi = normalize_upi_from_payload(payload)
                if upi:
                    snip=f"QR->UPI:{upi}"
                    record_hit(url,"payments","upi-qr",snip,"qr")
                    results.append(("upi-qr","payments",snip))
            # OCR
            if not results:
                txt = _ocr_image(img)
                if txt:
                    results += match_text(url,_clean(txt))
        except Exception:
            continue
    return results

# --- Keyword config ---
import yaml
with open(os.environ.get("KEYWORDS_FILE","/app/keywords/keywords.yml"),"r",encoding="utf-8") as f:
    _cfg = yaml.safe_load(f) or {}
KW = _cfg.get("keywords", [])
COMPILED=[];ALIASES=[];_seen=set()
for e in KW:
    term=(e.get("term") or "").strip(); cat=(e.get("category") or "uncat").strip()
    for pat in (e.get("patterns") or []) or []:
        try: COMPILED.append((term,cat,regx.compile(pat,regx.I)))
        except Exception: pass
    for a in (e.get("aliases") or [])+(e.get("brands") or []):
        if a and len(a)>=3 and (term,cat,a.lower()) not in _seen:
            ALIASES.append((term,cat,a.lower())); _seen.add((term,cat,a.lower()))

# --- Storage & buffers ---
match_buffer: dict[str, dict] = defaultdict(lambda: {"sub_urls": set(),"matches":[]})
es = OpenSearch(OPENSEARCH_HOST,timeout=30,max_retries=3,retry_on_timeout=True,
                connection_class=RequestsHttpConnection)

# --- Core record logic ---
def record_hit(url:str,cat:str,k:str,snip:str,src:str,master:str|None=None,confidence:float=1.0):
    if not master: master=url
    snip=_clean(snip)
    bucket=match_buffer[master]
    for e in bucket["matches"]:
        if e["url"]==url and e["keyword"]==k: return
    ss_url=""
    try:
        ss_result=capture_screenshot(url,k)
        ss_url=ss_result.get("screenshot") if isinstance(ss_result,dict) else getattr(ss_result,"screenshot","")
    except Exception as e:
        print(f"[screenshot:err] {url}->{e}")
    bucket["sub_urls"].add(url)
    bucket["matches"].append({
        "url":url,"category":cat,"keyword":k,"snippet":snip,
        "timestamp":int(time.time()),"source":src,"confidence":confidence
    })
    try:
        es.index(index="illegal_hits",document={
            "url":url,"category":cat,"keyword":k,"snippet":snip,
            "ts":int(time.time()),"source":src,"master_url":master,"confidence":confidence
        })
    except Exception: pass

# --- Matching text ---
def _context_score(text:str,idx:int)->float:
    w=text[max(0,idx-80):idx+80].casefold()
    return min(sum(1 for t in _PAYMENT_TOKENS if t in w)/4,1.0)

def match_text(url:str,text:str,master:str|None=None)->List[Tuple[str,str,str]]:
    if not text.strip(): return []
    text=_clean(text); low=text.casefold(); results=[]
    for term,cat,pat in COMPILED:
        for m in pat.finditer(text):
            idx=m.start(); snip=text[max(0,idx-100):idx+100]
            sem=semantic_validate(term,snip,cat)
            if USE_SEMANTIC and sem<_SEMANTIC_THRESHOLD: continue
            ctx=_context_score(text,idx) if cat=="payments" else 1.0
            if cat=="payments" and ctx<0.3: continue
            record_hit(url,cat,term,snip,"regex",master,sem*ctx)
            results.append((term,cat,snip))
    for term,cat,alias in ALIASES:
        idx=low.find(alias)
        if idx!=-1:
            snip=text[max(0,idx-100):idx+100]
            sem=semantic_validate(term,snip,cat)
            if USE_SEMANTIC and sem<_SEMANTIC_THRESHOLD: continue
            ctx=_context_score(text,idx) if cat=="payments" else 1.0
            if cat=="payments" and ctx<0.25: continue
            record_hit(url,cat,term,snip,"alias",master,sem*ctx)
            results.append((term,cat,snip))
    for m in _UPI_CONTEXT_RE.finditer(text):
        snip=text[max(0,m.start()-80):m.start()+80]
        ctx=_context_score(text,m.start())
        if ctx>=0.3:
            record_hit(url,"payments","upi-handle",snip,"context",master,0.85*ctx)
            results.append(("upi-handle","payments",snip))
    for m in _BTC_RE.finditer(text):
        snip=text[max(0,m.start()-80):m.start()+80]
        record_hit(url,"crypto","bitcoin",snip,"regex",master,0.95)
        results.append(("bitcoin","crypto",snip))
    for m in _ETH_RE.finditer(text):
        snip=text[max(0,m.start()-80):m.start()+80]
        record_hit(url,"crypto","ethereum",snip,"regex",master,0.95)
        results.append(("ethereum","crypto",snip))
    seen=set(); out=[]
    for t in results:
        if t not in seen: seen.add(t); out.append(t)
    return out

# --- Page worker (async) ---
async def _process_page_async(p:dict,main_url:str,task_id:str)->list[tuple[str,str,str]]:
    url=p.get("final_url") or p.get("url"); html=p.get("html") or ""
    if not url or not html: return []
    dom=domain_of(url)
    try:
        text,tree=extract_text(html); del html
        text_clean=_clean(text)
        results=match_text(url,text_clean,master=main_url)
        if any(c=="payments" for _,c,_ in results):
            extra=await asyncio.get_event_loop().run_in_executor(EXECUTOR,lambda: ocr_and_qr(url,tree))
            results+=extra
        return results
    except Exception as e:
        print(f"[page:error]{url}->{e}",flush=True)
        return []

# --- Entry point ---
async def process_ingest_payload(payload:Dict[str,Any])->Dict[str,Any]:
    task_id=payload.get("id") or payload.get("task_id") or "unknown"
    main_url=payload.get("main_url") or "unknown"
    pages=payload.get("pages") or payload.get("Pages") or []
    print(f"[batch:start]{task_id}|pages={len(pages)}",flush=True)
    match_buffer.pop(main_url,None)
    match_buffer[main_url]["task_id"]=task_id
    sem=asyncio.Semaphore(MAX_CONCURRENT_PAGES)
    all_results=[]; sub_urls=[]
    async def _worker(p):
        async with sem:
            r=await _process_page_async(p,main_url,task_id)
            all_results.extend(r)
            if u:=p.get("final_url") or p.get("url"): sub_urls.append(u)
    await asyncio.gather(*[_worker(p) for p in pages])
    cats=sorted({c for _,c,_ in all_results})
    kws=[k for k,_,_ in all_results]
    try:
        db=SessionLocal()
        db.bulk_save_objects([
            Result(task_id=task_id,main_url=main_url,sub_urls=sub_urls,
                   keyword_match=kws,word_count_raw_data=0,
                   word_count_cleaned_data=0,categories=cats,
                   raw_data="",cleaned_data="",timestamp=int(time.time()))
        ])
        db.commit()
        print(f"[result:saved]{task_id}|pages={len(sub_urls)}|matches={len(kws)}",flush=True)
    except Exception as e:
        print(f"[db:error]{task_id}->{e}",flush=True)
        if 'db' in locals(): db.rollback()
    finally:
        if 'db' in locals(): db.close()
    match_buffer.pop(main_url,None)
    return {"task_id":task_id,"main_url":main_url,"total_pages":len(sub_urls),
            "total_matches":len(kws),"categories":cats,
            "semantic_enabled":bool(USE_SEMANTIC and _SEMANTIC_MODEL is not None)}
