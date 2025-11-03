import os
import re
import yaml

PROJECT_ROOT = "/app"
KEYWORDS_FILE = os.environ.get("KEYWORDS_FILE", os.path.join(PROJECT_ROOT, "keywords", "keywords.yml"))

def load_keywords(path=KEYWORDS_FILE):
    """Load and compile keyword regex patterns."""
    if not os.path.exists(path):
        print(f"[WARN] Keywords file missing: {path}", flush=True)
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        print(f"[ERROR] Failed reading {path}: {e}", flush=True)
        return []

    patterns = []
    for entry in cfg.get("keywords", []):
        term = entry.get("term", "").strip()
        category = entry.get("category", "uncategorized")
        for pat in entry.get("patterns", []) or []:
            try:
                re.compile(pat)
                patterns.append({"pattern": pat, "term": term, "category": category})
            except re.error as e:
                print(f"[WARN] Invalid regex skipped: {pat} ({e})", flush=True)

    print(f"[INIT] Loaded {len(patterns)} patterns from {path}", flush=True)
    return patterns
