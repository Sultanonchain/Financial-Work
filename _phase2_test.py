"""End-to-end tests for the Phase 2 watchlist backend.

Runs without OAuth or KV — Flask test_client + a fake user dropped into
the session.  Local-dev path mirrors to a temp /tmp file we point at.
"""
import importlib
import os
import sys
import tempfile

# Force the no-KV path.
for k in list(os.environ):
    if k.startswith(("KV_", "REDIS_", "UPSTASH_")):
        os.environ.pop(k, None)
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret-ignore")

import app as app_module  # noqa: E402
importlib.reload(app_module)
app = app_module.app

tmpdir = tempfile.mkdtemp(prefix="valus_phase2_")
app_module.WATCHLIST_FILE = os.path.join(tmpdir, ".valus_watchlists.json")
app_module._WATCHLISTS_MEM.clear()


def _as_user(client, sub="wl-tester", email="wl@example.com"):
    with client.session_transaction() as s:
        s["user"] = {"sub": sub, "email": email, "name": "Watchlist Tester", "picture": ""}


FAILED = []


def check(label, ok, detail=""):
    tag = "PASS" if ok else "FAIL"
    print(f"  [{tag}] {label}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILED.append(label)


def section(t):
    print(f"\n── {t} " + "─" * max(0, 60 - len(t)))


# ── 1. Empty by default ──────────────────────────────────────────────
section("Empty default")
client = app.test_client()
_as_user(client)
r = client.get("/api/watchlist")
check("GET → 200", r.status_code == 200)
body = r.get_json()
check("empty items", body["items"] == [])
check("count = 0", body["count"] == 0)
check("max = 200", body["max"] == 200)

# ── 2. Add items ─────────────────────────────────────────────────────
section("Add items (POST replaces list)")
r = client.post("/api/watchlist", json={"items": [
    {"ticker": "AAPL", "name": "Apple",  "sector": "Technology", "addedAt": 1700000000},
    {"ticker": "MSFT", "name": "Microsoft"},
    {"ticker": "GOOG"},
]})
check("POST → 200", r.status_code == 200)
check("count = 3", r.get_json()["count"] == 3)

r = client.get("/api/watchlist")
items = {it["ticker"]: it for it in r.get_json()["items"]}
check("AAPL present", "AAPL" in items)
check("MSFT present", "MSFT" in items)
check("GOOG present", "GOOG" in items)
check("AAPL name carried through", items["AAPL"]["name"] == "Apple")
check("MSFT name carried through", items["MSFT"]["name"] == "Microsoft")
check("GOOG name defaulted to empty", items["GOOG"]["name"] == "")
check("AAPL addedAt preserved", items["AAPL"]["addedAt"] == 1700000000)

# ── 3. Dedupe by ticker on POST ──────────────────────────────────────
section("Dedupe + casing + trim on POST")
r = client.post("/api/watchlist", json={"items": [
    {"ticker": "tsla", "name": "Tesla"},
    {"ticker": "TSLA", "name": "Tesla duplicate"},  # dedup target
    {"ticker": "   ", "name": "blank"},              # rejected
    {"ticker": None, "name": "none"},                # rejected
    {"name": "no ticker"},                            # rejected
]})
body = r.get_json()
check("POST → 200", r.status_code == 200)
check("only 1 cleaned item", body["count"] == 1, f"got {body}")
r = client.get("/api/watchlist")
items = r.get_json()["items"]
check("ticker uppercased", items[0]["ticker"] == "TSLA")
check("first entry wins on dup", items[0]["name"] == "Tesla")

# ── 4. Validation ────────────────────────────────────────────────────
section("Validation")
r = client.post("/api/watchlist", json={"items": "not a list"})
check("items must be a list → 400", r.status_code == 400)
r = client.post("/api/watchlist", json={})
check("missing items → 400", r.status_code == 400)

# ── 5. Cap at 200 ────────────────────────────────────────────────────
section("Hard cap at 200")
big = [{"ticker": f"X{i:04d}"} for i in range(250)]
r = client.post("/api/watchlist", json={"items": big})
check("oversize POST still 200", r.status_code == 200)
check("server clipped to 200", r.get_json()["count"] == 200)

# ── 6. Field length clamps ───────────────────────────────────────────
section("Field length clamps")
long_name = "x" * 500
r = client.post("/api/watchlist", json={"items": [
    {"ticker": "ABCDEFGHIJKLMN", "name": long_name, "sector": "y" * 200},
]})
check("clamps applied → 200", r.status_code == 200)
items = client.get("/api/watchlist").get_json()["items"]
check("ticker clamped to 12 chars", len(items[0]["ticker"]) <= 12)
check("name clamped to 120 chars", len(items[0]["name"]) <= 120)
check("sector clamped to 60 chars", len(items[0]["sector"]) <= 60)

# ── 7. DELETE one ticker ─────────────────────────────────────────────
section("DELETE single ticker")
# Reset to a known state.
client.post("/api/watchlist", json={"items": [
    {"ticker": "AAPL"}, {"ticker": "MSFT"}, {"ticker": "GOOG"},
]})

r = client.delete("/api/watchlist?ticker=MSFT")
check("DELETE → 200", r.status_code == 200)
check("count = 2", r.get_json()["count"] == 2)

# Tickers remaining.
items = {it["ticker"] for it in client.get("/api/watchlist").get_json()["items"]}
check("MSFT removed", "MSFT" not in items)
check("AAPL still present", "AAPL" in items)
check("GOOG still present", "GOOG" in items)

# Casing tolerated.
client.post("/api/watchlist", json={"items": [{"ticker": "NVDA"}]})
r = client.delete("/api/watchlist?ticker=nvda")
check("DELETE lowercase param works", r.status_code == 200)

# Missing ticker query.
r = client.delete("/api/watchlist")
check("missing ?ticker → 400", r.status_code == 400)

# Removing ghost ticker.
r = client.delete("/api/watchlist?ticker=GHOST")
check("ghost ticker → 404", r.status_code == 404)

# ── 8. Per-user isolation ────────────────────────────────────────────
section("Per-user isolation")
c1 = app.test_client(); _as_user(c1, sub="user-a")
c2 = app.test_client(); _as_user(c2, sub="user-b")
c1.post("/api/watchlist", json={"items": [{"ticker": "AMZN"}]})
c2.post("/api/watchlist", json={"items": [{"ticker": "META"}]})
a_items = {it["ticker"] for it in c1.get("/api/watchlist").get_json()["items"]}
b_items = {it["ticker"] for it in c2.get("/api/watchlist").get_json()["items"]}
check("user-a sees only AMZN", a_items == {"AMZN"})
check("user-b sees only META", b_items == {"META"})

# ── 9. Auth gate ─────────────────────────────────────────────────────
section("Auth gate")
anon = app.test_client()
check("anon GET → 401", anon.get("/api/watchlist").status_code == 401)
check("anon POST → 401", anon.post("/api/watchlist", json={"items": []}).status_code == 401)
check("anon DELETE → 401", anon.delete("/api/watchlist?ticker=AAPL").status_code == 401)

# ── Summary ──────────────────────────────────────────────────────────
print()
if FAILED:
    print(f"❌ {len(FAILED)} check(s) failed:")
    for f in FAILED: print(f"   - {f}")
    sys.exit(1)
print("✅ All checks passed.")
