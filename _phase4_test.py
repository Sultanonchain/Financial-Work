"""Phase 4 backend tests — validation only.

The /api/compare success path internally invokes the full /api/analyze
pipeline (yfinance fetch, DCF, etc.), which is too slow + network-dependent
to run as a unit test.  We exercise the input-validation surface only;
the live success path is verified on the Vercel preview against real
tickers before merge.
"""
import os
import sys

for k in list(os.environ):
    if k.startswith(("KV_", "REDIS_", "UPSTASH_")):
        os.environ.pop(k, None)
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret-ignore")

import importlib
import app as app_module  # noqa: E402
importlib.reload(app_module)
app = app_module.app
client = app.test_client()

FAILED = []
def check(label, ok, detail=""):
    tag = "PASS" if ok else "FAIL"
    print(f"  [{tag}] {label}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILED.append(label)
def section(t):
    print(f"\n── {t} " + "─" * max(0, 60 - len(t)))


section("Ticker-count validation (frontend would gate UI; backend re-checks)")
# Empty.
r = client.get("/api/compare?tickers=")
check("empty → 400", r.status_code == 400)

# Single ticker.
r = client.get("/api/compare?tickers=AAPL")
check("1 ticker → 400", r.status_code == 400)
check("error mentions 2-or-3", "2 or 3" in (r.get_json().get("error") or ""),
      r.get_json())

# Too many tickers.
r = client.get("/api/compare?tickers=AAPL,MSFT,GOOG,META")
check("4 tickers → 400", r.status_code == 400)

# Two identical tickers dedupe to 1 → still rejected.
r = client.get("/api/compare?tickers=AAPL,AAPL")
check("AAPL,AAPL (dedupes to 1) → 400", r.status_code == 400)

# 3 identical tickers dedupe to 1 → still rejected.
r = client.get("/api/compare?tickers=TSLA,tsla,TsLa")
check("case-insensitive dedupe to 1 → 400", r.status_code == 400)

# Whitespace tickers get filtered before count check.
r = client.get("/api/compare?tickers=   ,   ,   ")
check("only-whitespace → 400", r.status_code == 400)

# ── Smoke: 2-ticker and 3-ticker paths reach analyze (not validated to
# return 200 — analyze itself hits yfinance and may fail in this offline
# test env — but they should NOT return 400 from validation). ─────────
section("Smoke (route reaches analyze, doesn't 400 on validation)")
r = client.get("/api/compare?tickers=AAPL,MSFT")
check("2 tickers → not 400 (reaches analyze)", r.status_code != 400,
      f"status={r.status_code}, body={r.get_data(as_text=True)[:200]}")
r = client.get("/api/compare?tickers=AAPL,MSFT,GOOG")
check("3 tickers → not 400 (reaches analyze)", r.status_code != 400,
      f"status={r.status_code}")

# Order-preserving dedup: AAPL,MSFT,AAPL → dedupes to [AAPL,MSFT] which
# is still 2, so reaches analyze (not 400).
r = client.get("/api/compare?tickers=AAPL,MSFT,AAPL")
check("dup-after-dedup keeps count valid", r.status_code != 400)

print()
if FAILED:
    print(f"❌ {len(FAILED)} check(s) failed:")
    for f in FAILED:
        print(f"   - {f}")
    sys.exit(1)
print("✅ All checks passed.")
