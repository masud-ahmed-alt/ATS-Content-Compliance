#!/usr/bin/env python3
import requests, time, os, sys, json, datetime
import gc


# ------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------
FETCHER = os.environ.get("FETCHER_URL", "http://localhost:8080/fetch")
ANALYZER_EXPORT = os.environ.get("ANALYZER_EXPORT", "http://localhost:8000/export")
RETRY_COUNT = int(os.environ.get("EXPORT_RETRIES", "5"))
RETRY_DELAY = int(os.environ.get("EXPORT_DELAY", "15"))  # seconds
EXPORT_TIMEOUT = int(os.environ.get("EXPORT_TIMEOUT", "120"))  # seconds

# ------------------------------------------------------------------
# SEND URL FUNCTION
# ------------------------------------------------------------------
def send_url(url: str):
    try:
        print(f"[send] Sending URL to fetcher: {url}", flush=True)
        r = requests.post(FETCHER, json={"url": url}, timeout=10)
        if r.status_code in (200, 202):
            print(f"  [ok] {url} accepted by fetcher", flush=True)
            return True
        else:
            print(f"  [error] Fetcher returned {r.status_code}: {r.text[:100]}", flush=True)
            return False
    except Exception as e:
        print(f"  [send:error] {url} -> {e}", flush=True)
        return False

# ------------------------------------------------------------------
# EXPORT ANALYZER RESULTS
# ------------------------------------------------------------------
def export_results():
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            print(f"[export] Attempt {attempt}/{RETRY_COUNT} — fetching {ANALYZER_EXPORT}", flush=True)
            r = requests.get(ANALYZER_EXPORT, timeout=EXPORT_TIMEOUT)
            if r.status_code == 200 and len(r.content) > 0:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                out_file = f"hits_out_{timestamp}.csv"
                with open(out_file, "wb") as f:
                    f.write(r.content)
                print(f"[done] Analyzer results saved to {out_file}", flush=True)
                return True
            else:
                print(f"[warn] Empty or non-200 response ({r.status_code})", flush=True)
        except requests.exceptions.Timeout:
            print(f"[warn] Analyzer export timed out (attempt {attempt})", flush=True)
        except Exception as e:
            print(f"[error] Export fetch failed on attempt {attempt}: {e}", flush=True)

        time.sleep(RETRY_DELAY)

    print("[error] Analyzer export failed after all retries", flush=True)
    return False

# ------------------------------------------------------------------
# CLEANUP FUNCTION
# ------------------------------------------------------------------
def cleanup_memory():
    """Forcefully clear variables and release memory before exit."""
    print("[cleanup] Releasing memory and closing resources...", flush=True)

    # First, run garbage collection on existing objects
    gc.collect()

    # Then delete global variables safely
    globals_to_keep = {"__name__", "__file__", "__package__", "cleanup_memory", "gc", "sys"}
    for name in list(globals()):
        if name not in globals_to_keep:
            del globals()[name]

    print("[cleanup] Memory cleared successfully", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()


# ------------------------------------------------------------------
# MAIN WORKFLOW
# ------------------------------------------------------------------
def main():
    fname = sys.argv[1] if len(sys.argv) > 1 else "urls.txt"

    if not os.path.exists(fname):
        print(f"[error] URL list file not found: {fname}")
        sys.exit(1)

    with open(fname) as f:
        urls = [l.strip() for l in f if l.strip()]

    print(f"[info] Loaded {len(urls)} URLs from {fname}", flush=True)

    for u in urls:
        ok = send_url(u)
        print(f"[info] sent {u} -> {ok}", flush=True)
        time.sleep(1)

    print("[wait] Allowing analyzer to finish processing...", flush=True)
    time.sleep(10)

    if export_results():
        cleanup_memory()  # clear memory only if export was successful

    print("[exit] Job complete.", flush=True)


if __name__ == "__main__":
    main()
