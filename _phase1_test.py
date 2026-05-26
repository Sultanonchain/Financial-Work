"""End-to-end test for the multi-portfolio backend.  Runs without OAuth
or KV — uses Flask's test_client + monkey-patched session.

Run from the repo root:  python3 _phase1_test.py
"""
import json
import os
import sys

# Force the no-KV local-dev path so we don't need Upstash to test.
for k in list(os.environ):
    if k.startswith("KV_") or k.startswith("REDIS_") or k.startswith("UPSTASH_"):
        os.environ.pop(k, None)
# Avoid clobbering any real local mirror.
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret-please-ignore")

import importlib
import app as app_module  # noqa: E402
importlib.reload(app_module)
app = app_module.app

# Point the local mirror at a temp file so we don't touch the real one.
import tempfile
tmpdir = tempfile.mkdtemp(prefix="valus_phase1_")
app_module.PORTFOLIO_FILE = os.path.join(tmpdir, ".valus_portfolios.json")
app_module._PORTFOLIOS_MEM.clear()


def _as_user(client, sub="test-user-1", email="tester@example.com"):
    """Drop a fake user into the Flask session so require_user() succeeds."""
    with client.session_transaction() as s:
        s["user"] = {"sub": sub, "email": email, "name": "Test User", "picture": ""}


FAILED = []


def check(label, ok, detail=""):
    tag = "PASS" if ok else "FAIL"
    print(f"  [{tag}] {label}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILED.append(label)


def section(title):
    print(f"\n── {title} " + "─" * (60 - len(title)))


# ── 1. First-ever read seeds a default portfolio ─────────────────────
section("First read seeds default portfolio")
client = app.test_client()
_as_user(client)
r = client.get("/api/portfolios")
check("GET /api/portfolios → 200", r.status_code == 200, r.data)
data = r.get_json()
check("one portfolio exists", len(data["portfolios"]) == 1)
check("default name = 'My Portfolio'",
      data["portfolios"][0]["name"] == "My Portfolio")
check("default_pid points at it",
      data["default_pid"] == data["portfolios"][0]["id"])
check("free cap = 3", data["cap"] == 3)
default_pid = data["default_pid"]

# ── 2. Legacy /api/portfolio still works (no pid) ─────────────────────
section("Legacy /api/portfolio remains functional")
r = client.get("/api/portfolio")
check("GET /api/portfolio → 200", r.status_code == 200)
check("returns empty items list", r.get_json()["items"] == [])

r = client.post("/api/portfolio",
                json={"items": [{"ticker": "AAPL", "name": "Apple",
                                 "price": 250, "iv": 300, "mos": 16}]})
check("POST /api/portfolio → 200", r.status_code == 200)
check("count = 1", r.get_json()["count"] == 1)

r = client.get("/api/portfolio")
check("GET /api/portfolio now has the ticker",
      r.get_json()["items"][0]["ticker"] == "AAPL")

# Confirm the same ticker shows in /api/portfolios count.
r = client.get("/api/portfolios")
check("/api/portfolios count = 1 for default", r.get_json()["portfolios"][0]["count"] == 1)

# ── 3. Create up to cap ────────────────────────────────────────────────
section("Create portfolios (free cap = 3)")
r = client.post("/api/portfolios", json={"name": "Speculative"})
check("create #2 → 201", r.status_code == 201)
spec_pid = r.get_json()["id"]

# Duplicate-name guard fires BEFORE we hit the cap (still room for one more).
r = client.post("/api/portfolios", json={"name": "  speculative  "})  # case + ws
check("duplicate name → 409", r.status_code == 409)

# Empty + overlong name validation (also pre-cap).
r = client.post("/api/portfolios", json={"name": "   "})
check("empty name → 400", r.status_code == 400)
r = client.post("/api/portfolios", json={"name": "x" * 60})
check("overlong name → 400", r.status_code == 400)

r = client.post("/api/portfolios", json={"name": "Dividend Picks"})
check("create #3 → 201", r.status_code == 201)
div_pid = r.get_json()["id"]

# Now at the cap.
r = client.post("/api/portfolios", json={"name": "Should Fail"})
check("create #4 → 402 (cap hit)", r.status_code == 402,
      f"got {r.status_code}: {r.data}")
body = r.get_json()
check("error code = portfolio_cap_exceeded",
      body.get("error") == "portfolio_cap_exceeded")
check("cap reported = 3", body.get("cap") == 3)
check("upgrade_url present", bool(body.get("upgrade_url")))

# ── 4. Rename ─────────────────────────────────────────────────────────
section("Rename portfolio")
r = client.patch(f"/api/portfolios/{spec_pid}", json={"name": "Speculative Bets"})
check("rename → 200", r.status_code == 200)
check("new name returned", r.get_json()["name"] == "Speculative Bets")

# Rename to a name that already exists on another portfolio.
r = client.patch(f"/api/portfolios/{spec_pid}", json={"name": "Dividend Picks"})
check("rename to duplicate → 409", r.status_code == 409)

# Rename non-existent.
r = client.patch("/api/portfolios/000000000000", json={"name": "Ghost"})
check("rename ghost id → 404", r.status_code == 404)

# Invalid id format.
r = client.patch("/api/portfolios/not-hex", json={"name": "x"})
check("invalid id → 400", r.status_code == 400)

# ── 5. Per-portfolio items via ?pid= ──────────────────────────────────
section("Items scoped to one portfolio (?pid=)")
r = client.post(f"/api/portfolio?pid={spec_pid}",
                json={"items": [{"ticker": "TSLA", "name": "Tesla"},
                                {"ticker": "NVDA", "name": "Nvidia"}]})
check("POST items into spec_pid → 200", r.status_code == 200)
check("count = 2", r.get_json()["count"] == 2)

r = client.get(f"/api/portfolio?pid={spec_pid}")
tickers = sorted(it["ticker"] for it in r.get_json()["items"])
check("speculative has TSLA + NVDA", tickers == ["NVDA", "TSLA"])

# Default portfolio must still show its own AAPL.
r = client.get("/api/portfolio")
default_tickers = sorted(it["ticker"] for it in r.get_json()["items"])
check("default still has only AAPL", default_tickers == ["AAPL"])

# List counts reflect per-portfolio items.
r = client.get("/api/portfolios")
by_id = {p["id"]: p for p in r.get_json()["portfolios"]}
check("default count = 1", by_id[default_pid]["count"] == 1)
check("spec count = 2", by_id[spec_pid]["count"] == 2)
check("dividend count = 0", by_id[div_pid]["count"] == 0)

# ── 6. Change default ─────────────────────────────────────────────────
section("Change default portfolio")
r = client.post("/api/portfolios/default", json={"pid": spec_pid})
check("set default → 200", r.status_code == 200)
r = client.get("/api/portfolios")
check("default_pid now = spec_pid", r.get_json()["default_pid"] == spec_pid)
# /api/portfolio without ?pid= should now return spec's items.
r = client.get("/api/portfolio")
default_tickers = sorted(it["ticker"] for it in r.get_json()["items"])
check("/api/portfolio (no pid) follows new default",
      default_tickers == ["NVDA", "TSLA"])

# ── 7. Delete a portfolio ─────────────────────────────────────────────
section("Delete portfolios")
r = client.delete(f"/api/portfolios/{div_pid}")
check("delete dividend → 200", r.status_code == 200)
r = client.get("/api/portfolios")
ids = {p["id"] for p in r.get_json()["portfolios"]}
check("dividend gone", div_pid not in ids)

# Delete the current default — default should reassign automatically.
r = client.delete(f"/api/portfolios/{spec_pid}")
check("delete current default → 200", r.status_code == 200)
new_default = r.get_json()["default_pid"]
check("default reassigned to remaining portfolio",
      new_default == default_pid)

# Try deleting the very last one.
r = client.delete(f"/api/portfolios/{default_pid}")
check("delete last portfolio → 409", r.status_code == 409)
check("error code = cannot_delete_last",
      r.get_json().get("error") == "cannot_delete_last")

# ── 8. v2 → v3 migration is one-shot ──────────────────────────────────
section("v2 → v3 migration (legacy single-portfolio shape)")
# Drop a fresh user with a v2 shape directly into the local mirror, then
# read it via the public API and confirm it migrated.
LEGACY_USER = "legacy-user-99"
app_module._PORTFOLIOS_MEM[LEGACY_USER] = {
    "items": [{"ticker": "MSFT", "name": "Microsoft"},
              {"ticker": "GOOG", "name": "Alphabet"}],
    "updated_at": 1700000000.0,
}
c2 = app.test_client()
_as_user(c2, sub=LEGACY_USER, email="legacy@example.com")
r = c2.get("/api/portfolios")
check("legacy user GET /api/portfolios → 200", r.status_code == 200)
body = r.get_json()
check("legacy: one portfolio after migration",
      len(body["portfolios"]) == 1)
check("legacy: name = My Portfolio",
      body["portfolios"][0]["name"] == "My Portfolio")
check("legacy: count = 2 (items carried forward)",
      body["portfolios"][0]["count"] == 2)
# The in-memory record should now be v3 shape.
rec = app_module._PORTFOLIOS_MEM[LEGACY_USER]
check("legacy: in-memory now v3", rec.get("schema") == 3)

# ── 9. Auth gate ──────────────────────────────────────────────────────
section("Auth gate")
anon = app.test_client()
r = anon.get("/api/portfolios")
check("anon GET → 401", r.status_code == 401)
r = anon.post("/api/portfolios", json={"name": "x"})
check("anon POST → 401", r.status_code == 401)

# ── Summary ───────────────────────────────────────────────────────────
print()
if FAILED:
    print(f"❌ {len(FAILED)} check(s) failed:")
    for f in FAILED:
        print(f"   - {f}")
    sys.exit(1)
print("✅ All checks passed.")
