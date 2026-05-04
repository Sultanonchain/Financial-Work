from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from flask_cors import CORS
# Load .env for local dev (Vercel/Render inject env vars natively, so this is a no-op there)
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except Exception:
    pass
import yfinance as yf
import numpy as np
import pandas as pd
import requests
import traceback
import time
import re
import os
import json as _json_top
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
from urllib.parse import urlencode, urlparse

# Optional .env loading for local dev
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = Flask(__name__)
# Cookie session signing.
#   - In production (Vercel) we REFUSE to start without SECRET_KEY. Each
#     lambda instance generates its own per-process random key otherwise,
#     which means cookies signed by instance A can't be verified by
#     instance B. OAuth state lives in the session, so the round-trip
#     to Google fails for any user that happens to land on a different
#     instance for the callback — exactly the "some people can't sign
#     in" symptom.
#   - In local dev we use a deterministic fallback so sessions survive
#     a `python3 app.py` restart.
_secret = os.environ.get("SECRET_KEY")
if not _secret:
    if os.environ.get("VERCEL"):
        raise RuntimeError(
            "SECRET_KEY environment variable is required in production. "
            "Without it Flask sessions/OAuth state break across lambda "
            "instances, causing intermittent sign-in failures. Set a "
            "long random string in the Vercel project env."
        )
    _secret = "valus-dev-only-fixed-key-do-not-use-in-prod"
app.secret_key = _secret
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=bool(os.environ.get("VERCEL")),  # secure cookies on prod only
)
CORS(app, supports_credentials=True)

# ── Vercel KV (Redis) adapter — durable storage when KV_URL is set ────
# Falls back silently to a process-level dict + /tmp file when env is
# missing (local dev with no KV provisioned still works exactly like
# before).
_kv = None
try:
    if os.environ.get("KV_URL"):
        import redis as _redis
        _kv = _redis.from_url(os.environ["KV_URL"], decode_responses=True,
                              socket_connect_timeout=2, socket_timeout=2)
        # Probe once on startup so a misconfigured KV fails loud at boot
        # rather than at first user action.
        _kv.ping()
except Exception as _e:
    print(f"[valus] KV unavailable, falling back to /tmp + in-memory: {_e}")
    _kv = None

def kv_get(key):
    if _kv:
        try: return _kv.get(key)
        except Exception: pass
    return None

def kv_set(key, value, ttl=None):
    if _kv:
        try:
            if ttl: _kv.setex(key, ttl, value)
            else:   _kv.set(key, value)
            return True
        except Exception: pass
    return False

# ── Rate limiting — uses Redis when available, falls back to memory ────
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

def _rate_limit_key():
    """Per-user counting when signed in, per-IP for anonymous."""
    try:
        u = session.get("user") or {}
        email = u.get("email")
        if email:
            return f"user:{email}"
    except Exception:
        pass
    return f"ip:{get_remote_address()}"

def _is_signed_in():
    try:
        return bool((session.get("user") or {}).get("email"))
    except Exception:
        return False

# Dynamic limits — signed-in users get 3-5x more than anonymous
def limit_analyze():
    return "40 per minute; 300 per day" if _is_signed_in() else "10 per minute; 50 per day"
def limit_discover():
    return "30 per minute; 500 per day" if _is_signed_in() else "10 per minute; 100 per day"
def limit_medium():
    return "60 per minute; 1000 per day" if _is_signed_in() else "20 per minute; 200 per day"
def limit_light():
    return "120 per minute; 2000 per day" if _is_signed_in() else "30 per minute; 300 per day"

_limiter_storage = os.environ.get("KV_URL") if _kv else None
limiter = Limiter(
    key_func=_rate_limit_key,
    app=app,
    storage_uri=_limiter_storage or "memory://",
    default_limits=[],
    headers_enabled=True,
)

# ── Google OAuth (optional — gracefully disabled when env vars absent) ─
_oauth = None
_GOOGLE_CONFIGURED = bool(
    os.environ.get("GOOGLE_CLIENT_ID") and
    os.environ.get("GOOGLE_CLIENT_SECRET")
)
if _GOOGLE_CONFIGURED:
    try:
        from authlib.integrations.flask_client import OAuth
        _oauth = OAuth(app)
        _oauth.register(
            name="google",
            client_id=os.environ["GOOGLE_CLIENT_ID"],
            client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"},
        )
    except Exception as _e:
        print(f"[valus] OAuth setup failed: {_e}")
        _oauth = None
        _GOOGLE_CONFIGURED = False

RISK_FREE_RATE   = 0.043   # 10-yr US Treasury proxy (Apr 2025 ~4.3%)
EQUITY_RISK_PREM = 0.060   # FIN 415 template MRP (6.0% — matches academic standard)

# FX rate cache (in-memory, lives for the process lifetime — good enough for a session)
_fx_cache: dict = {}

# ── Catalyst / Discovery Layer cache ──────────────────────────────────────────
# Refreshed once per hour so news and filings stay current without hammering APIs.
_catalyst_cache: dict = {}
CATALYST_CACHE_TTL = 3600   # seconds

# Keyword lists for positive catalyst and risk classification
_CATALYST_STRONG = [
    "billion contract", "billion deal", "$1b ", "$2b ", "$3b ", "$5b ", "$10b ",
    "fda approv", "fda grants", "breakthrough therapy designation", "accelerated approval",
    "government contract", "defense contract", "wins contract", "awarded contract",
    "major contract award", "exclusive agreement", "landmark deal",
]
_CATALYST_MODERATE = [
    "product launch", "new product", "new platform", "new model",
    "earnings beat", "raised guidance", "record revenue", "record quarter",
    "exceeded expectations", "exceeded estimates", "strategic partnership",
    "expanded partnership", "new agreement", "acquisition complete",
    "fda clearance", "positive phase", "clinical trial success",
]
_RISK_KEYWORDS = [
    "lawsuit", "class action", "sec investigation", "doj investigation",
    "antitrust investigation", "securities fraud", "data breach",
    "consent decree", "regulatory fine", "recall", "safety warning",
    "subpoena", "criminal charge", "fraud allegation", "investigation opened",
    "sec charges", "department of justice", "preliminary injunction",
]

# ── Transformative catalysts ────────────────────────────────────────────────
# These are events that signal a *durable* growth shift, not a one-time IV
# bump.  When one fires, the model lifts Stage-1 growth in addition to the
# usual IV multiplier — a "first commercial flight" reflects 2-3 years of
# accelerating revenue, not just a sentiment pop.
#
# Coverage by industry (curated to the events that have actually moved
# stocks 20%+ over the past 24 months):
_CATALYST_TRANSFORMATIVE = [
    # eVTOL / Urban Air Mobility — JOBY, ACHR, EVTL
    "first commercial flight", "first commercial ride", "first paid passenger",
    "first paid flight", "first paid ride", "part 135 certification",
    "type certificate", "air taxi launch", "urban air mobility launch",
    "passenger flights begin", "commercial operations begin",
    "first commercial passenger", "first passenger flight",
    "commercial passenger flight", "begins air taxi", "passenger air taxi",
    "commercial service launch", "begins commercial service",
    "completes first passenger", "completes first commercial",
    # Semiconductors — MU, INTC, TSMC, ARM
    "tape-out", "tape out", "first silicon", "yield ramp",
    "hbm4 sampling", "hbm3e sampling", "cxl ga", "ga release",
    "high-volume production", "production ramp",
    # Autos / EV / Robotics — TSLA, GM, F, RIVN
    "deliveries record", "delivery record", "production milestone",
    "factory online", "factory commissioned", "robotaxi launch",
    "robotaxi expansion", "fsd unsupervised", "optimus production",
    # Energy — CEG, VST, NEE, OXY
    "first power", "grid connection", "facility commissioned",
    "ppa signed", "hyperscaler ppa", "datacenter ppa",
    "reactor restart", "haleu delivery", "small modular reactor",
    # Monopoly / sole-supplier signals
    "only us producer", "sole us producer", "sole supplier",
    "exclusive supplier", "no us competitor", "only domestic supplier",
    "trusted foundry designation",
    # Biotech / Pharma — LLY, VRTX, REGN, MRNA
    "bla filing", "bla accepted", "bla approval", "phase 3 success",
    "phase 3 met primary", "fda priority review", "breakthrough therapy",
    "label expansion", "first patient dosed",
    # Defense — LMT, RTX, NOC, GD, HII
    "sole-source contract", "indefinite delivery indefinite quantity",
    "idiq award", "production go-ahead", "milestone c approval",
    # AI infra / Cloud — NVDA, MSFT, GOOGL, AMZN
    "blackwell shipping", "blackwell ga", "rubin shipping",
    "sovereign ai contract", "gigawatt deal",
]

# ── Policy / sovereign-capital signal ───────────────────────────────────────
# These keywords detect *policy-driven* tailwinds and headwinds that pure
# DCF can't see: CHIPS Act grants, DPA Title III orders, DOE loan guarantees,
# tariff carve-outs, export-control wins.  Flagged separately from the
# generic momentum/risk scanner so the UI can show "Policy Tailwind"
# distinct from "Earnings Beat".
_POLICY_TAILWIND_KEYWORDS = [
    "chips act", "chips grant", "chips funding", "chips program office",
    "doe loan", "doe loan guarantee", "loan guarantee", "loan programs office",
    "dpa title iii", "defense production act", "title iii award",
    "pentagon contract", "defense department contract", "dod contract",
    "national security investment", "strategic stockpile", "tax credit",
    "production tax credit", "investment tax credit", "ira credit",
    "buy american", "section 232", "tariff exemption", "export control win",
    "fedramp authorization", "fedramp high",
    "ppa signed", "power purchase agreement",
    "haleu", "sole source contract", "indefinite delivery",
    "trusted foundry", "trusted supplier",
]

# Headwinds specific to policy / geopolitical risk (different bucket from
# generic litigation risk — these speak to capital flow reversals).
_POLICY_HEADWIND_KEYWORDS = [
    "export control" ,    # generic — context-disambiguated below if it co-occurs with "win"
    "outbound investment restriction", "cfius blocked",
    "tariff retaliation", "china ban", "china restriction",
    "delisting risk", "denied entity list", "entity list addition",
    "sanctions added", "ofac sanctioned",
]

# ── Industry-specific DCF guardrails ──────────────────────────────────────────
# These prevent Gordon Growth Model blow-up for mature/capital-intensive sectors.
# max_tg:      maximum terminal growth rate (capped at long-run GDP)
# min_wacc:    WACC floor regardless of capital structure
# wacc_spread: minimum (WACC − terminal growth) to avoid near-zero denominators
# max_s1:      cap on Stage 1 growth — prevents using cyclical recovery as perpetual rate
INDUSTRY_PARAMS = {
    # Airlines: low long-run growth, high capital intensity, often junk-rated
    "airlines":        {"max_tg": 0.020, "min_wacc": 0.085, "wacc_spread": 0.025, "max_s1": 0.12},
    # Utilities: regulated, GDP-matched growth ceiling
    "utilities":       {"max_tg": 0.022, "min_wacc": 0.075, "wacc_spread": 0.020, "max_s1": 0.08},
    # Telecom: mature, high CapEx, GDP-pace growth
    "telecom":         {"max_tg": 0.022, "min_wacc": 0.075, "wacc_spread": 0.020, "max_s1": 0.10},
    # Energy (traditional): finite resources → terminal growth capped at 1.5%
    "energy":          {"max_tg": 0.015, "min_wacc": 0.075, "wacc_spread": 0.022, "max_s1": 0.15},
    # Materials / Mining: finite resources → terminal growth capped at 1.5%
    "materials":       {"max_tg": 0.015, "min_wacc": 0.075, "wacc_spread": 0.022, "max_s1": 0.12},
    # Autos: cyclical, capital-intensive
    "auto":            {"max_tg": 0.025, "min_wacc": 0.075, "wacc_spread": 0.020, "max_s1": 0.15},
    # Structural Transformer: traditional-sector company reinventing itself via AI/Robotics capex
    # (e.g. Tesla — classified as Auto but investing at tech-company intensity)
    # Higher TG ceiling, 8% floor, lifted Stage 1 cap for platform-scale growth
    "structural_transformer": {"max_tg": 0.030, "min_wacc": 0.080, "wacc_spread": 0.018, "max_s1": 0.35},
    # Payment Networks / Asset-Light Credit: zero-marginal-cost scaling, global duopoly moats.
    # 3.5% TG ceiling: meaningful premium above 2.5% default (reflects cashless tailwinds)
    # without triggering the 42× Gordon Growth explosion of a 5%-TG / 7.5%-WACC combination.
    # 7.5% WACC floor: very low capital risk vs. traditional financial services.
    "payment_network": {"max_tg": 0.035, "min_wacc": 0.075, "wacc_spread": 0.020, "max_s1": 0.20},
    # Default (tech, consumer, healthcare, etc.) — global sanity floor
    "default":         {"max_tg": 0.025, "min_wacc": 0.075, "wacc_spread": 0.020, "max_s1": 0.25},
}

# Industry-standard multiples for fallback valuation when DCF is unavailable
# Sourced from Damodaran sector averages (updated annually)
# (forward_pe_multiple, ev_ebitda_multiple) — None means that metric isn't reliable for sector
SECTOR_MULT_PARAMS = {
    "technology":         (25.0, 23.0),  # moat ×1.20 → ~30x fpe / ~27.6x ev/ebitda
    "consumer_cyclical":  (18.0, 12.0),
    "consumer_defensive": (22.0, 14.0),
    "healthcare":         (20.0, 14.0),
    "industrials":        (18.0, 12.0),
    "financial":          (13.0, None),   # P/B preferred for banks; use PE as proxy
    "energy":             (14.0,  8.0),
    "materials":          (15.0,  9.0),
    "utilities":          (17.0, 11.0),
    "telecom":            (15.0,  7.0),
    "airlines":           (10.0,  6.0),
    "auto":               (12.0,  7.0),
    "real_estate":        (None, 18.0),   # FFO/AFFO preferred; EV/EBITDA as proxy
    "default":            (17.0, 12.0),
}

# Minimum EV/EBITDA multiple used as the Bear Case institutional floor.
# Prevents a stress scenario from assigning impossible valuations to healthy businesses.
INDUSTRY_BEAR_EV_FLOORS = {
    "technology":         10.0,
    "consumer_cyclical":   6.0,
    "consumer_defensive":  8.0,
    "healthcare":          8.0,
    "industrials":         6.0,
    "financial":           5.0,
    "energy":              4.0,
    "materials":           4.0,
    "utilities":           7.0,
    "telecom":             5.0,
    "airlines":            4.0,
    "auto":                4.0,
    "real_estate":         9.0,
    "default":             6.0,
}

# Sector/industry default betas when yfinance returns None
INDUSTRY_BETA_DEFAULTS = {
    "airlines":          1.75,
    "aerospace":         1.30,
    "auto":              1.30,
    "energy":            1.35,
    "materials":         1.20,
    "financial":         1.15,
    "utilities":         0.65,
    "technology":        1.25,
    "healthcare":        0.90,
    "consumer_cyclical": 1.20,
    "consumer_defensive":0.70,
    "industrials":       1.10,
    "real_estate":       1.00,
    "communication":     0.90,
    # Asset-light network businesses: low capital risk, recurring fee revenue
    "payment_network":   0.85,
    # Biotech: high binary risk on pipeline events, but diversified portfolio beta
    "biotech":           1.45,
    # Semiconductors: cyclical demand but large-cap leaders trend toward tech beta
    "semiconductors":    1.35,
}

# ── Discovery Layer helpers ────────────────────────────────────────────────────

def _fetch_edgar_8k(ticker: str) -> list:
    """
    Fetch the 5 most recent 8-K filing summaries from SEC EDGAR Atom feed.
    Returns list of {date, title, summary} dicts. Silently returns [] on any error.
    """
    try:
        url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?"
            f"action=getcompany&CIK={ticker}&type=8-K"
            f"&dateb=&owner=include&count=5&output=atom"
        )
        resp = requests.get(
            url, timeout=6,
            headers={"User-Agent": "VALUS Research tool@valus.ai Accept-Encoding: gzip"}
        )
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.text)
        ns = {"a": "http://www.w3.org/2005/Atom"}
        entries = []
        for entry in root.findall("a:entry", ns)[:5]:
            title   = (entry.findtext("a:title",   default="", namespaces=ns) or "")
            updated = (entry.findtext("a:updated",  default="", namespaces=ns) or "")[:10]
            summary = (entry.findtext("a:summary",  default="", namespaces=ns) or "")
            # Strip HTML tags from EDGAR summary
            summary_clean = re.sub(r"<[^>]+>", " ", summary).strip()[:300]
            entries.append({"date": updated, "title": title, "summary": summary_clean})
        return entries
    except Exception:
        return []


# ── Google News RSS — third news source ──────────────────────────────────────
# yfinance.news has been broken since Yahoo deprecated their news format
# months ago (returns empty lists for most tickers).  EDGAR 8-K Atom feed
# returns generic "8-K - Current report" titles with summaries too brief
# to keyword-match.  Google News RSS is the cheap, reliable third source:
# free, no API key, returns recent articles with rich titles and
# descriptions that actually contain the keywords our scorer looks for.
def _fetch_google_news(ticker: str, company_name: str = "") -> list:
    """
    Fetch up to 10 recent news headlines from Google News RSS.
    Returns list of {title, summary, ts} dicts (ts is unix epoch seconds).
    Silently returns [] on any error so the catalyst scanner degrades gracefully.
    """
    try:
        # Build a query that combines ticker + company name when available;
        # ticker-only queries return too many false positives (e.g. "MU" =
        # University of Missouri news).
        q_parts = [ticker]
        if company_name:
            # Strip common corporate suffixes that just add noise
            cname = re.sub(r"\b(Inc|Corp|Corporation|Co|Ltd|Plc|Limited|N\.V\.|S\.A\.)\.?$",
                           "", company_name).strip()
            if cname and cname.lower() != ticker.lower():
                q_parts.append(f'"{cname}"')
        q = " stock OR ".join(q_parts) + " stock"
        url = (
            "https://news.google.com/rss/search"
            f"?q={requests.utils.quote(q)}"
            "&hl=en-US&gl=US&ceid=US:en"
        )
        resp = requests.get(
            url, timeout=6,
            headers={"User-Agent": "Mozilla/5.0 (compatible; VALUS Research tool)"}
        )
        if resp.status_code != 200:
            return []
        # Google News RSS is standard RSS 2.0
        root = ET.fromstring(resp.text)
        items = []
        # Channel > item path
        for item in root.findall(".//item")[:10]:
            title  = (item.findtext("title")       or "").strip()
            desc   = (item.findtext("description") or "").strip()
            pubdate = (item.findtext("pubDate")    or "").strip()
            # Strip HTML from description (often contains nested anchor tags)
            desc_clean = re.sub(r"<[^>]+>", " ", desc).strip()[:400]
            # Parse pubDate — RFC 822 format like "Tue, 28 Apr 2026 14:32:00 GMT"
            ts = 0
            try:
                from email.utils import parsedate_to_datetime
                ts = int(parsedate_to_datetime(pubdate).timestamp())
            except Exception:
                ts = 0
            if title:
                items.append({"title": title, "summary": desc_clean, "ts": ts})
        return items
    except Exception:
        return []


# ── Heuristic headline scorer ───────────────────────────────────────────────
# Replaces the old binary "is_strong / is_mod / is_risk" matching with a
# numeric score in [-1.0, +1.0].  Uses sector context, co-occurrence, and
# negation detection so the model "decides on its own" instead of just
# pattern-matching.  Outputs feed momentum_premium, growth_catalyst_lift_pp,
# and wacc_risk_add as smooth functions of the aggregated score.
_NEGATION_TOKENS = (
    "no longer", "not approved", "denied", "rejected", "fails to",
    "failed to", "missed", "misses", "delayed", "postponed",
    "halted", "withdrawn", "suspended", "cancelled", "canceled",
)

# ── Optional Claude API headline interpreter ────────────────────────────────
# When ANTHROPIC_API_KEY is set, ambiguous headlines (heuristic score in the
# narrow band [-0.2, +0.2]) are sent to Claude Haiku for structured JSON
# interpretation.  Adds ~1-2s + a few cents/day; falls back to heuristic
# gracefully if the call fails or the key is missing.
_CLAUDE_INTERP_CACHE = {}        # {(ticker, title_hash): (ts, payload)}
_CLAUDE_INTERP_CACHE_TTL = 3600  # 1 hour — same as the catalyst cache

def _claude_interpret_headline(ticker, sector, title, summary):
    """
    Returns dict { score: float, durability: str, confidence: str, reason: str }
    or None if disabled / failed.  Cheap-cached so repeated tickers don't
    double-bill.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    cache_key = (ticker, hash((title or "")[:200]))
    cached = _CLAUDE_INTERP_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < _CLAUDE_INTERP_CACHE_TTL:
        return cached[1]
    # Redis lookup — cuts cost across Vercel instances
    redis_key = f"valus:claude:{ticker}:{hash((title or '')[:200])}"
    if _kv:
        try:
            raw = _kv.get(redis_key)
            if raw:
                parsed = _json_top.loads(raw)
                _CLAUDE_INTERP_CACHE[cache_key] = (time.time(), parsed)
                return parsed
        except Exception:
            pass

    try:
        prompt = (
            "You are a buyside equity analyst trained in Peter Lynch's method "
            "from One Up On Wall Street.  Read this headline through Lynch's lens.\n\n"
            "LYNCH'S SIX CATEGORIES (classify the company first):\n"
            "  slowGrower  — large, mature, low single-digit growth, bought for dividend.\n"
            "  stalwart    — multibillion-dollar, 10–12% earnings growth, recession-resilient (KO, PG).\n"
            "                Realistic upside: 30–50% over 1–2 years; not a tenbagger.\n"
            "  fastGrower  — small/aggressive, 20–25%+ growth, room to expand.  Tenbagger territory.\n"
            "                Risk: outgrowing the industry or running out of runway.\n"
            "  cyclical    — earnings rise/fall with macro (autos, airlines, steel, chemicals).\n"
            "                Timing is everything; can lose 80% in a downturn.\n"
            "  turnaround  — battered/near-bankruptcy companies that may rebound (Chrysler-style).\n"
            "  assetPlay   — hidden assets (real estate, cash, brand) not reflected in price.\n\n"
            "LYNCH'S CHECKLIST (weight headlines by what matters per category):\n"
            "  • P/E vs growth (PEG): low PEG = bullish, high = bearish.\n"
            "  • Insider buying / company buybacks = strong positive.\n"
            "  • Low institutional ownership = positive (room to be discovered).\n"
            "  • Strong balance sheet (low debt/equity, net cash) = resilience.\n"
            "  • Consistent earnings > sporadic earnings.\n"
            "  • For stalwarts: dividend safety, payout ratio.\n"
            "  • For fast growers: replicable concept, room to expand, earnings durability.\n"
            "  • For cyclicals: where in the cycle (top = bearish even on good news).\n"
            "  • For turnarounds: cash to survive, debt structure, plan credibility.\n\n"
            "LYNCH'S RED FLAGS (treat as bearish even if news sounds positive):\n"
            "  • Hot stock in a hot industry with no earnings.\n"
            "  • 'The next [Microsoft/Tesla]' framing.\n"
            "  • Diworsification — acquisitions outside core competence.\n"
            "  • Whisper stocks, hyped 'sure things,' takeover speculation as the thesis.\n"
            "  • Heavy debt taken on to fund growth or acquisitions.\n\n"
            f"Ticker: {ticker}\nSector: {sector or '?'}\n"
            f"Headline: {title}\n"
            f"Summary: {(summary or '')[:400]}\n\n"
            "Return ONLY a single-line JSON object with keys:\n"
            "  category: one of \"slowGrower\", \"stalwart\", \"fastGrower\", \"cyclical\", \"turnaround\", \"assetPlay\"\n"
            "  score: -1.0 to +1.0 (Lynch-weighted: a 'strong earnings beat' is huge for a stalwart, modest for a fast grower)\n"
            "  durability: one of \"oneTime\", \"stage1\", \"terminal\"\n"
            "  confidence: one of \"low\", \"med\", \"high\"\n"
            "  reason: under 60 characters, Lynch-flavored (e.g. 'PEG <1, insider buying' or 'cyclical near peak')\n"
            "If the headline is press-release filler, hot-stock hype, or unrelated, return score near 0 and confidence=low."
        )
        body = {
            "model":      "claude-haiku-4-5-20251001",
            "max_tokens": 150,
            "messages":   [{"role": "user", "content": prompt}],
        }
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json=body, timeout=8,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        # Pull the text content out of the messages response shape
        txt = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                txt += block.get("text", "")
        # Find the JSON object in the response (Claude sometimes wraps it)
        import json as _json_loads
        m = re.search(r"\{[^{}]*\}", txt)
        if not m:
            return None
        parsed = _json_loads.loads(m.group(0))
        # Normalize + clamp
        _VALID_CATEGORIES = {"slowGrower", "stalwart", "fastGrower",
                             "cyclical", "turnaround", "assetPlay"}
        cat = str(parsed.get("category", ""))
        result = {
            "category":   cat if cat in _VALID_CATEGORIES else None,
            "score":      max(-1.0, min(1.0, float(parsed.get("score", 0.0)))),
            "durability": str(parsed.get("durability", "oneTime")),
            "confidence": str(parsed.get("confidence", "low")),
            "reason":     str(parsed.get("reason", ""))[:60],
        }
        _CLAUDE_INTERP_CACHE[cache_key] = (time.time(), result)
        if _kv:
            try:
                _kv.setex(redis_key, _CLAUDE_INTERP_CACHE_TTL, _json_top.dumps(result))
            except Exception:
                pass
        return result
    except Exception:
        return None


def _score_headline(title: str, summary: str, ticker: str, sector: str, industry: str):
    """
    Returns dict { score, durability, is_transformative, matched }.
    score:        [-1.0, +1.0] aggregate signal
    durability:   "oneTime" | "stage1" | "terminal"
    matched:      list of which keyword bucket(s) hit
    """
    text = ((title or "") + " " + (summary or "")).lower()
    s_lower = (sector or "").lower()
    i_lower = (industry or "").lower()

    is_transformative = any(kw in text for kw in _CATALYST_TRANSFORMATIVE)
    is_strong         = any(kw in text for kw in _CATALYST_STRONG)
    is_moderate       = any(kw in text for kw in _CATALYST_MODERATE)
    is_risk           = any(kw in text for kw in _RISK_KEYWORDS)
    is_policy_tw      = any(kw in text for kw in _POLICY_TAILWIND_KEYWORDS)
    is_policy_hw      = any(kw in text for kw in _POLICY_HEADWIND_KEYWORDS)

    # Base score by strongest match
    score = 0.0
    matched = []
    if is_transformative: score = max(score, 0.6); matched.append("transformative")
    if is_strong:         score = max(score, 0.4); matched.append("strong")
    if is_moderate:       score = max(score, 0.2); matched.append("moderate")
    if is_policy_tw:      score = max(score, 0.35); matched.append("policy_tw")
    if is_risk:           score = min(score - 0.4, score); matched.append("risk")
    if is_policy_hw:      score = min(score - 0.3, score); matched.append("policy_hw")

    # Sector context modulation: a "first commercial ride" headline on JOBY
    # (eVTOL) is high-signal; the same headline on AAPL is generic noise.
    sector_match_bonus = 1.0
    if is_transformative:
        # eVTOL keywords on industrial/aerospace sectors get amplified
        if any(kw in text for kw in ["air taxi", "evtol", "part 135", "passenger flight"]):
            sector_match_bonus = 1.5 if ("aerospace" in i_lower or "air" in i_lower) else 0.6
        # Semi keywords on tech sectors
        elif any(kw in text for kw in ["tape-out", "hbm", "first silicon", "yield ramp"]):
            sector_match_bonus = 1.5 if ("semiconductor" in i_lower or "electronic" in i_lower) else 0.5
        # Biotech keywords on healthcare sectors
        elif any(kw in text for kw in ["bla filing", "phase 3", "fda priority", "breakthrough therapy"]):
            sector_match_bonus = 1.5 if ("biotech" in i_lower or "drug" in i_lower or "pharma" in i_lower) else 0.6
        # Energy keywords on energy sectors
        elif any(kw in text for kw in ["ppa signed", "datacenter ppa", "reactor restart", "haleu"]):
            sector_match_bonus = 1.5 if ("energy" in s_lower or "utilities" in s_lower) else 0.7

    # Co-occurrence bonuses
    if "billion" in text and ("contract" in text or "deal" in text):
        sector_match_bonus *= 1.2
    if is_risk and any(kw in text for kw in ["doj", "department of justice", "antitrust"]):
        score -= 0.15  # criminal/antitrust is heavier than civil

    score *= sector_match_bonus

    # Negation: flip sign if a negation token appears alongside a positive match
    if any(neg in text for neg in _NEGATION_TOKENS):
        if score > 0:
            score = -abs(score) * 0.6
            matched.append("negated")

    # Clamp to [-1, +1]
    score = max(-1.0, min(1.0, score))

    # Durability: transformatives drive Stage-1 growth (2-3 yr lift), strong
    # catalysts drive a one-time IV multiplier, policy tailwinds can drive
    # terminal (longer-duration) shifts.
    if is_transformative and score > 0:
        durability = "stage1"
    elif is_policy_tw and score > 0.3:
        durability = "terminal"
    else:
        durability = "oneTime"

    # ── Optional Claude API tie-breaker for ambiguous scores ────────────
    # When the heuristic lands in the ambiguous band [-0.2, +0.2] AND no
    # keyword bucket fired, ask Claude to interpret the headline directly.
    # This is the "model decides on its own" path — turns generic headlines
    # into structured signal instead of dropping them on the floor.
    claude_used = False
    claude_reason = None
    claude_overrode = False
    claude_category = None
    # Path 1 — ambiguous band: heuristic gave nothing useful, let Claude decide.
    if -0.2 <= score <= 0.2 and not matched:
        ai = _claude_interpret_headline(ticker, sector, title, summary)
        if ai is not None:
            score = ai["score"]
            if ai.get("durability") in ("stage1", "terminal", "oneTime"):
                durability = ai["durability"]
            if score > 0.4 and durability == "stage1":
                is_transformative = True
            claude_used = True
            claude_reason = ai.get("reason")
            claude_category = ai.get("category")
            matched.append("claude")
    # Path 2 — high-stakes second opinion: heuristic is confidently strong on a
    # durable tag; ask Claude to sanity-check.  Only override if Claude returns
    # high-confidence opposite sign — catches false positives like "Tesla recalls
    # 2 vehicles" being scored as a major risk.
    elif abs(score) >= 0.5 and durability in ("stage1", "terminal"):
        ai = _claude_interpret_headline(ticker, sector, title, summary)
        if ai is not None:
            claude_category = ai.get("category")
            if ai.get("confidence") == "high":
                ai_score = ai["score"]
                # Opposite sign with meaningful magnitude → override
                if (score > 0 and ai_score < -0.3) or (score < 0 and ai_score > 0.3):
                    score = ai_score
                    if ai.get("durability") in ("stage1", "terminal", "oneTime"):
                        durability = ai["durability"]
                    is_transformative = (score > 0.4 and durability == "stage1")
                    claude_overrode = True
                    claude_reason = ai.get("reason")
                    matched.append("claude_override")
                claude_used = True

    return {
        "score":             round(score, 3),
        "durability":        durability if not (is_transformative and score > 0) else "stage1",
        "is_transformative": is_transformative,
        "matched":           matched,
        "claude_used":       claude_used,
        "claude_overrode":   claude_overrode,
        "claude_reason":     claude_reason,
        "claude_category":   claude_category,
    }


def get_catalyst_insights(ticker: str, info: dict, stock) -> dict:
    """
    Discovery Layer: scan recent news (yfinance) + SEC 8-K filings for catalysts and risks.

    Returns:
        insights          – up to 3 bullet strings for the Live Analyst Notes box
        momentum_premium  – 0.05–0.10 multiplier if strong positive catalyst found (else 0)
        wacc_risk_add     – 0.01 if a material risk keyword found (else 0)
        has_positive_catalyst / has_material_risk – booleans for UI badges
        catalyst_labels / risk_labels – short strings describing what triggered the flag
    """
    now = time.time()
    if ticker in _catalyst_cache:
        cached = _catalyst_cache[ticker]
        if now - cached["ts"] < CATALYST_CACHE_TTL:
            return cached["data"]

    insights          = []
    momentum_premium  = 0.0
    wacc_risk_add     = 0.0
    has_positive      = False
    has_risk          = False
    catalyst_labels   = []
    risk_labels       = []
    # Policy-signal accumulators — distinct from generic momentum/risk so
    # the UI can present "Policy Tailwind" as its own chip.
    policy_tailwind_labels = []
    policy_headwind_labels = []
    # New-style outputs from the heuristic scorer.  Per-headline scores feed
    # into a Stage-1 growth lift (transformative durability) on top of the
    # existing IV multiplier.  Age decay weights newer headlines heavier.
    news_interpretation     = []
    growth_catalyst_lift_pp = 0.0
    transformative_labels   = []
    sector_for_score   = (info or {}).get("sector", "")
    industry_for_score = (info or {}).get("industry", "")
    seven_days_ago    = now - 7 * 86400

    # ── 1. yfinance news + Google News RSS (combined) ────────────────────────
    # yfinance.news is mostly empty since Yahoo deprecated their format;
    # Google News RSS is the reliable backstop with rich titles + summaries.
    # Both feed the same scanner loop below; we dedupe on lowercase title.
    try:
        yf_news = getattr(stock, "news", None) or []
        # Normalize yfinance shape → {title, summary, ts}
        yf_normalized = []
        for n in yf_news:
            ts = n.get("providerPublishTime", 0) or 0
            if ts > seven_days_ago:
                yf_normalized.append({
                    "title":   n.get("title",   "") or "",
                    "summary": n.get("summary", "") or "",
                    "ts":      ts,
                })
        # Pull Google News, then merge.  Company-name-aware query reduces
        # false positives (ticker MU vs University of Missouri news, etc).
        gnews = _fetch_google_news(ticker, (info or {}).get("longName") or (info or {}).get("shortName") or "")
        gnews_recent = [n for n in gnews if n.get("ts", 0) > seven_days_ago or n.get("ts", 0) == 0]
        # Dedupe on the first 60 chars of the lowercase title
        seen = set()
        merged = []
        for n in (yf_normalized + gnews_recent):
            key = (n.get("title") or "").lower()[:60]
            if key and key not in seen:
                seen.add(key)
                merged.append(n)
        recent = sorted(merged, key=lambda x: x.get("ts", 0), reverse=True)
        for item in recent[:12]:
            raw_title   = item.get("title",   "") or ""
            raw_snippet = item.get("summary", "") or ""
            text = (raw_title + " " + raw_snippet).lower()

            is_strong = any(kw in text for kw in _CATALYST_STRONG)
            is_mod    = any(kw in text for kw in _CATALYST_MODERATE)
            is_risk   = any(kw in text for kw in _RISK_KEYWORDS)
            # Policy detection — disambiguate "export control" so a *win*
            # against a competitor reads as tailwind, not headwind.
            is_policy_tw = any(kw in text for kw in _POLICY_TAILWIND_KEYWORDS)
            is_policy_hw = any(kw in text for kw in _POLICY_HEADWIND_KEYWORDS) and not (
                "export control" in text and any(w in text for w in ["win", "tailwind", "exemption", "carve-out", "carve out"])
            )

            if is_strong:
                has_positive = True
                momentum_premium = max(momentum_premium, 0.08)
                if len(catalyst_labels) < 2:
                    catalyst_labels.append(raw_title[:70])
            elif is_mod and not has_positive:
                has_positive = True
                momentum_premium = max(momentum_premium, 0.05)
                if len(catalyst_labels) < 2:
                    catalyst_labels.append(raw_title[:70])

            if is_risk:
                has_risk = True
                wacc_risk_add = 0.01
                if len(risk_labels) < 2:
                    risk_labels.append(raw_title[:70])

            if is_policy_tw and len(policy_tailwind_labels) < 3:
                policy_tailwind_labels.append(raw_title[:80])
            if is_policy_hw and len(policy_headwind_labels) < 3:
                policy_headwind_labels.append(raw_title[:80])

            # ── Heuristic scorer + age-weighted growth lift ─────────────
            # Each headline produces a smooth [-1, +1] score.  Age decay
            # weights a 1-day-old headline at full strength, a 6-day-old
            # at ~14%.  Transformative catalysts (durability=stage1) feed
            # growth_catalyst_lift_pp, capped at +3pp.
            pub_ts = item.get("ts", 0) or item.get("providerPublishTime", 0)
            age_days = max(0.0, (now - pub_ts) / 86400.0) if pub_ts else 7.0
            age_weight = max(0.0, 1.0 - age_days / 7.0)
            interp = _score_headline(raw_title, raw_snippet, ticker,
                                     sector_for_score, industry_for_score)
            news_interpretation.append({
                "title":     raw_title[:120],
                "score":     interp["score"],
                "weighted":  round(interp["score"] * age_weight, 3),
                "durability": interp["durability"],
                "age_days":  round(age_days, 1),
                "matched":   interp["matched"],
                "claude_used":   interp.get("claude_used", False),
                "claude_reason": interp.get("claude_reason"),
            })
            if interp["is_transformative"] and interp["score"] > 0:
                lift = 3.0 * (interp["score"]) * age_weight   # max +3pp
                if lift > growth_catalyst_lift_pp:
                    growth_catalyst_lift_pp = lift
                    if len(transformative_labels) < 3:
                        transformative_labels.append(raw_title[:80])

            if len(insights) < 3:
                date_str = datetime.fromtimestamp(pub_ts).strftime("%b %d") if pub_ts else ""
                headline = raw_title[:100]
                insights.append(f"{date_str}: {headline}" if date_str else headline)
    except Exception:
        pass

    # ── 2. SEC EDGAR 8-K filings ─────────────────────────────────────────────
    try:
        filings = _fetch_edgar_8k(ticker)
        for f in filings[:3]:
            text = (f["title"] + " " + f["summary"]).lower()

            is_strong = any(kw in text for kw in _CATALYST_STRONG)
            is_mod    = any(kw in text for kw in _CATALYST_MODERATE)
            is_risk   = any(kw in text for kw in _RISK_KEYWORDS)
            is_policy_tw = any(kw in text for kw in _POLICY_TAILWIND_KEYWORDS)
            is_policy_hw = any(kw in text for kw in _POLICY_HEADWIND_KEYWORDS) and not (
                "export control" in text and any(w in text for w in ["win", "tailwind", "exemption", "carve-out", "carve out"])
            )

            if is_strong and not has_positive:
                has_positive = True
                momentum_premium = max(momentum_premium, 0.08)
                catalyst_labels.append(f"SEC 8-K: {f['title'][:60]}")
            elif is_mod and not has_positive:
                has_positive = True
                momentum_premium = max(momentum_premium, 0.05)
                catalyst_labels.append(f"SEC 8-K: {f['title'][:60]}")
            if is_risk and not has_risk:
                has_risk = True
                wacc_risk_add = 0.01
                risk_labels.append(f"SEC 8-K: {f['title'][:60]}")
            if is_policy_tw and len(policy_tailwind_labels) < 3:
                policy_tailwind_labels.append(f"SEC 8-K: {f['title'][:70]}")
            if is_policy_hw and len(policy_headwind_labels) < 3:
                policy_headwind_labels.append(f"SEC 8-K: {f['title'][:70]}")

            # SEC 8-K headlines also feed the heuristic scorer.  We don't
            # have a publish timestamp here, so age_weight defaults to 1.0
            # (8-Ks are by definition recent and material).
            interp_8k = _score_headline(f["title"], f["summary"], ticker,
                                        sector_for_score, industry_for_score)
            news_interpretation.append({
                "title":      f"SEC 8-K: {f['title'][:100]}",
                "score":      interp_8k["score"],
                "weighted":   interp_8k["score"],
                "durability": interp_8k["durability"],
                "age_days":   None,
                "matched":    interp_8k["matched"],
                "claude_used":   interp_8k.get("claude_used", False),
                "claude_reason": interp_8k.get("claude_reason"),
            })
            if interp_8k["is_transformative"] and interp_8k["score"] > 0:
                lift = 3.0 * interp_8k["score"]
                if lift > growth_catalyst_lift_pp:
                    growth_catalyst_lift_pp = lift
                    if len(transformative_labels) < 3:
                        transformative_labels.append(f"SEC 8-K: {f['title'][:70]}")

            # Include filing as insight bullet if room remains
            if len(insights) < 3 and f["date"] and f["title"]:
                insights.append(f"SEC 8-K ({f['date']}): {f['title'][:90]}")
    except Exception:
        pass

    # ── 3. Fallback: analyst consensus summary ────────────────────────────────
    if not insights:
        n_analysts = info.get("numberOfAnalystOpinions")
        target     = safe(info.get("targetMeanPrice"))
        rating     = info.get("recommendationKey", "").replace("_", " ").title()
        if n_analysts and target:
            insights.append(
                f"Analyst consensus: ${target:.2f} target · {rating or 'N/A'} "
                f"({n_analysts} analysts)"
            )

    # Cap the growth lift at +3pp.  Stage-1 ceiling guard later in the
    # pipeline (effective_max_s1) provides the second line of defense.
    growth_catalyst_lift_pp = max(0.0, min(growth_catalyst_lift_pp, 3.0))
    # Sort interpretations by absolute weighted score so the top signal
    # surfaces first in the UI.
    news_interpretation.sort(key=lambda x: -abs(x.get("weighted", 0.0)))

    result = {
        "insights":              insights[:3],
        "momentum_premium":      round(momentum_premium, 3),
        "wacc_risk_add":         round(wacc_risk_add, 3),
        "has_positive_catalyst": has_positive,
        "has_material_risk":     has_risk,
        "catalyst_labels":       catalyst_labels[:2],
        "risk_labels":           risk_labels[:2],
        # Policy signals — separate from generic catalyst/risk so the UI
        # can show them as their own chips with sovereign-capital framing.
        "policy_tailwind":         bool(policy_tailwind_labels),
        "policy_tailwind_labels":  policy_tailwind_labels[:3],
        "policy_headwind":         bool(policy_headwind_labels),
        "policy_headwind_labels":  policy_headwind_labels[:3],
        # New: model-decides-on-its-own outputs.  The IV pipeline reads
        # growth_catalyst_lift_pp at the Stage-1 assignment; the UI
        # surfaces news_interpretation as per-headline score chips.
        "growth_catalyst_lift_pp": round(growth_catalyst_lift_pp, 2),
        "transformative_labels":   transformative_labels,
        "news_interpretation":     news_interpretation[:8],
    }
    _catalyst_cache[ticker] = {"ts": now, "data": result}
    return result


def _analyst_divergence_note(iv, analyst_target, price, rev_growth_pct, forward_pe):
    """
    Cross-reference model IV vs analyst consensus.
    Returns a dcf_notes dict when divergence > 15% of current price, else None.
    """
    if not iv or not analyst_target or not price or price <= 0:
        return None
    at  = float(analyst_target)
    div = (at - iv) / price * 100   # positive = analyst above model

    if abs(div) < 15:
        return None   # numbers are close enough; no note needed

    if div > 15:
        # Consensus is materially above model
        reasons = []
        if rev_growth_pct and rev_growth_pct > 20:
            reasons.append(
                f"market pricing in sustained {rev_growth_pct:.0f}% revenue growth "
                "not yet reflected in modelled FCF base"
            )
        if forward_pe and forward_pe > 40:
            reasons.append(
                f"premium multiple ({forward_pe:.0f}x fwd P/E) embeds growth optionality "
                "not captured by FCF discounting"
            )
        if not reasons:
            reasons.append(
                "market assigns a higher multiple to expected earnings power "
                "than the DCF growth-rate path implies"
            )
        text = (
            f"Analyst consensus (${at:.2f}) is {abs(div):.0f}% above VALUS model IV (${iv:.2f}) — "
            + "; ".join(reasons) + "."
        )
        return {"type": "info", "text": text}
    else:
        # Model is materially above consensus
        text = (
            f"VALUS model IV (${iv:.2f}) exceeds analyst consensus (${at:.2f}) "
            f"by {abs(div):.0f}% — DCF is highly sensitive to growth-rate assumptions. "
            "Verify Stage 1 growth and WACC inputs against updated guidance."
        )
        return {"type": "info", "text": text}


def _detect_structural_transformer(info: dict, sector: str, industry: str,
                                    cashflow, rev_ttm=None):
    """
    Detect companies undergoing structural transformation via massive AI/Robotics CapEx.

    These are companies listed in 'traditional' sectors (auto, industrial, consumer)
    that are investing at technology-company intensity.  The canonical example is Tesla:
    classified as Consumer Cyclical / Auto Manufacturers but deploying capital into
    AI inference, robotics (Optimus), and autonomous-vehicle infrastructure at a rate
    that makes standard auto-sector DCF parameters badly wrong.

    Criteria (ALL must be met):
      1. Sector is auto / industrial / consumer (otherwise already priced as tech)
      2. Industry is NOT retail / e-commerce / logistics (their CapEx is for
         fulfilment centres & data-centre capacity, not AI moonshots)
      3. Annual CapEx / TTM Revenue > 8%  (high capex intensity signal)

    Returns:
      (is_transformer: bool, capex_abs: float, capex_to_rev: float, addback_rate: float)
      capex_abs     – absolute CapEx in reporting currency (positive)
      capex_to_rev  – CapEx as fraction of revenue (e.g. 0.087 for Tesla)
      addback_rate  – fraction of CapEx treated as growth investment (0.50 or 0.20)
    """
    s   = (sector   or "").lower()
    ind = (industry or "").lower()

    # Must be in a traditionally-priced sector to qualify
    is_traditional = (
        any(x in ind for x in ["auto", "automobile", "vehicle", "motor"]) or
        "industrial" in s or
        ("consumer" in s and "tech" not in s)
    )
    if not is_traditional:
        return False, 0.0, 0.0, 0.50

    # For internet / broadline retail: CapEx funds fulfilment centres and commercial
    # cloud, NOT proprietary AI moonshots → exclude from full Structural Transformer.
    # HOWEVER, for mega-scale platform retailers (AMZN, Alibaba) a partial 20%
    # logistics-investment normalization is still warranted.  We return the CapEx
    # data with addback_rate=0.20 and is_transformer=False so downstream code can
    # apply just the FCF normalisation without the full overlay.
    _RETAIL_PARTIAL = [
        "internet retail", "broadline retail", "e-commerce", "catalog retail",
    ]
    _RETAIL_FULL_EXCL = [
        "specialty retail", "home improvement retail", "discount stores",
        "department stores", "grocery stores", "food distribution",
        "wholesale", "wholesale distributors", "general merchandise",
    ]
    is_retail_partial = any(x in ind for x in _RETAIL_PARTIAL)
    is_retail_full_excl = any(x in ind for x in _RETAIL_FULL_EXCL)
    if is_retail_full_excl:
        return False, 0.0, 0.0, 0.50   # no addback at all
    if is_retail_partial:
        # Compute CapEx, then return partial normalization (no full transformer)
        capex_abs = 0.0
        try:
            if cashflow is not None and not cashflow.empty:
                cpx_key = next(
                    (k for k in cashflow.index if "Capital Expenditure" in str(k)), None
                )
                if cpx_key:
                    raw = safe(cashflow.loc[cpx_key, cashflow.columns[0]])
                    if raw is not None:
                        capex_abs = abs(float(raw))
        except Exception:
            pass
        if capex_abs == 0.0:
            _ce = safe(info.get("capitalExpenditures"))
            if _ce:
                capex_abs = abs(float(_ce)) * 4
        if capex_abs > 0 and rev_ttm and rev_ttm > 0:
            capex_to_rev = capex_abs / rev_ttm
            # Only normalise if CapEx intensity is meaningfully high (>= 6%)
            if capex_to_rev >= 0.06:
                return False, capex_abs, capex_to_rev, 0.20
        return False, 0.0, 0.0, 0.50

    # Extract annual CapEx from the cashflow statement
    capex_abs = 0.0
    try:
        if cashflow is not None and not cashflow.empty:
            cpx_key = next(
                (k for k in cashflow.index if "Capital Expenditure" in str(k)), None
            )
            if cpx_key:
                raw = safe(cashflow.loc[cpx_key, cashflow.columns[0]])
                if raw is not None:
                    capex_abs = abs(float(raw))
    except Exception:
        pass

    # Fallback: yfinance capitalExpenditures field (quarterly × 4 annualised)
    if capex_abs == 0.0:
        _ce = safe(info.get("capitalExpenditures"))
        if _ce:
            capex_abs = abs(float(_ce)) * 4  # quarterly → annual

    if capex_abs == 0.0 or not rev_ttm or rev_ttm <= 0:
        return False, 0.0, 0.0, 0.50

    capex_to_rev = capex_abs / rev_ttm
    if capex_to_rev < 0.08:      # < 8% of revenue → standard company, not a transformer
        return False, 0.0, capex_to_rev, 0.50

    # Addback rate: auto/industrial companies (TSLA, etc.) have highly purposeful
    # growth CapEx → treat 50% as investment.  Consumer non-auto companies that
    # somehow pass the above filters are more mixed → conservative 20% addback.
    is_auto_or_industrial = (
        any(x in ind for x in ["auto", "automobile", "vehicle", "motor"]) or
        "industrial" in s
    )
    addback_rate = 0.50 if is_auto_or_industrial else 0.20

    return True, capex_abs, capex_to_rev, addback_rate


def _classify_industry(sector: str, industry: str) -> str:
    """Map yfinance sector/industry strings to one of our INDUSTRY_PARAMS keys."""
    s   = (sector   or "").lower()
    ind = (industry or "").lower()
    if any(x in ind for x in ["airline", "air freight", "airport services"]):
        return "airlines"
    if "utilities" in s or "electric util" in ind or "gas util" in ind:
        return "utilities"
    if any(x in ind for x in ["telecom", "wireless", "integrated telecom"]):
        return "telecom"
    if "energy" in s and "renewable" not in ind:
        return "energy"
    if "materials" in s or "mining" in ind or "metals" in ind:
        return "materials"
    if any(x in ind for x in ["auto", "automobile"]):
        return "auto"
    if any(x in ind for x in ["credit service", "payment network", "payment processing"]):
        return "payment_network"
    return "default"

def _default_beta(sector: str, industry: str) -> float:
    """Return a reasonable beta when yfinance provides None."""
    s   = (sector   or "").lower()
    ind = (industry or "").lower()
    if "airline" in ind:                    return INDUSTRY_BETA_DEFAULTS["airlines"]
    if "utilities" in s:                    return INDUSTRY_BETA_DEFAULTS["utilities"]
    if "energy" in s:                       return INDUSTRY_BETA_DEFAULTS["energy"]
    if "materials" in s or "mining" in ind: return INDUSTRY_BETA_DEFAULTS["materials"]
    if any(x in ind for x in ["auto","automobile"]): return INDUSTRY_BETA_DEFAULTS["auto"]
    # Payment networks before broad 'financial' catch-all
    if any(x in ind for x in ["credit service","payment network","payment processing"]):
        return INDUSTRY_BETA_DEFAULTS["payment_network"]
    if "financial" in s or "bank" in ind:   return INDUSTRY_BETA_DEFAULTS["financial"]
    # Biotech / semiconductors before broad technology / health
    if "biotech" in ind or "biopharmaceutic" in ind: return INDUSTRY_BETA_DEFAULTS["biotech"]
    if "semiconductor" in ind:              return INDUSTRY_BETA_DEFAULTS["semiconductors"]
    if "technology" in s or "software" in ind: return INDUSTRY_BETA_DEFAULTS["technology"]
    if "health" in s:                       return INDUSTRY_BETA_DEFAULTS["healthcare"]
    if "consumer" in s and "cycl" in s:     return INDUSTRY_BETA_DEFAULTS["consumer_cyclical"]
    if "consumer" in s:                     return INDUSTRY_BETA_DEFAULTS["consumer_defensive"]
    if "industrial" in s:                   return INDUSTRY_BETA_DEFAULTS["industrials"]
    if "real estate" in s:                  return INDUSTRY_BETA_DEFAULTS["real_estate"]
    if "communic" in s:                     return INDUSTRY_BETA_DEFAULTS["communication"]
    return 1.0

def _sector_to_mult_key(sector: str) -> str:
    """Map a yfinance sector string to a SECTOR_MULT_PARAMS key."""
    s = (sector or "").lower()
    if "tech" in s:                         return "technology"
    if "consumer" in s and "cycl" in s:     return "consumer_cyclical"
    if "consumer" in s:                     return "consumer_defensive"
    if "health" in s:                       return "healthcare"
    if "industrial" in s:                   return "industrials"
    if "financial" in s or "bank" in s:     return "financial"
    if "real estate" in s:                  return "real_estate"
    if "energy" in s:                       return "energy"
    if "material" in s:                     return "materials"
    if "utilit" in s:                       return "utilities"
    if "communic" in s:                     return "telecom"
    return "default"


def calc_multiples_val(info, sector, industry, fx_rate, ebitda_ttm=None, moat_premium=1.0,
                       override_ev_ebitda=None):
    """
    Fallback valuation when DCF is unavailable (negative equity or negative FCF).
    Method 1: Forward EPS × sector median Forward P/E
    Method 2: (EBITDA × sector EV/EBITDA − Net Debt) / Shares
    moat_premium:       multiplier applied to both sector multiples for High-Moat companies.
    override_ev_ebitda: if set, forces EV/EBITDA to this value (used for Structural Transformers
                        to apply an AI-platform 35× multiple rather than auto 7×).
    Returns (value, method_label) or (None, None).
    """
    ind_class = _classify_industry(sector, industry)
    class_to_mult = {
        "airlines": "airlines", "utilities": "utilities", "telecom": "telecom",
        "energy": "energy", "materials": "materials", "auto": "auto",
        "default": _sector_to_mult_key(sector),
    }
    mk = class_to_mult.get(ind_class, "default")
    base_fpe, base_ev = SECTOR_MULT_PARAMS.get(mk, SECTOR_MULT_PARAMS["default"])

    # Structural Transformer override: replace sector EV/EBITDA with AI-platform multiple
    if override_ev_ebitda is not None:
        base_ev = override_ev_ebitda

    # Apply moat expansion — 20% above sector median for backbone companies
    fpe_mult      = round(base_fpe * moat_premium, 1) if base_fpe else None
    ev_ebitda_mult = round(base_ev  * moat_premium, 1) if base_ev  else None
    premium_tag   = f" +{round((moat_premium-1)*100):.0f}% moat" if moat_premium > 1.0 else ""

    shares = safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0) or 0
    if shares <= 0:
        return None, None

    # Method 1: Forward EPS × sector Forward P/E multiple
    fwd_eps = safe(info.get("forwardEps"))
    if fwd_eps and fwd_eps > 0 and fpe_mult:
        return round(fwd_eps * fpe_mult, 2), f"Forward P/E ({fpe_mult:.0f}x sector median{premium_tag})"

    # Method 2: EV/EBITDA → derive equity value per share
    ebitda = ebitda_ttm or safe(info.get("ebitda"))
    if ebitda and ebitda > 0 and ev_ebitda_mult:
        ebitda_usd = ebitda * fx_rate
        ev_implied = ebitda_usd * ev_ebitda_mult
        cash     = (safe(info.get("totalCash"), 0) or 0) * fx_rate
        debt     = (safe(info.get("totalDebt"), 0) or 0) * fx_rate
        net_debt = debt - cash
        eq_val   = ev_implied - net_debt
        if eq_val > 0:
            return round(eq_val / shares, 2), f"EV/EBITDA ({ev_ebitda_mult:.0f}x sector median{premium_tag})"

    return None, None


# ── Sanity envelope clamp (used by every IV-producing path) ────────────────
# Promotes the previous one-sided post-DCF clamp into a true bounded envelope:
# floor at 5% of price (or $0.01), ceiling at 6× price OR 2.5× analyst target,
# whichever is higher.  Catches both the "19,000% MOS" upper-tail nonsense and
# the "IV = 0.01 on a healthy stock" lower-tail nonsense — a single function
# every IV-generating path goes through, eliminating bypass routes.
def _clamp_iv(iv, price, analyst_target=None):
    """
    Returns clamped IV, or None if inputs are unusable.
    Caller should rescale scenarios in lockstep using the returned ratio.
    """
    if iv is None or price is None or price <= 0:
        return iv
    floor = max(price * 0.05, 0.01)
    ceil_base = price * 6.0
    if analyst_target and analyst_target > 0:
        ceil_base = max(ceil_base, analyst_target * 2.5)
    return max(min(iv, ceil_base), floor)


# ── Emergency IV Cascade ────────────────────────────────────────────────────
# Last-resort valuation when every primary path (DCF, multiples, sultan-split)
# has failed.  The cascade tries four progressively-weaker but always-defensible
# methods; only returns None if nothing usable exists, in which case the
# response will fall back to price itself + a "data unavailable" note.
#
# Methods (highest preference first):
#   1. Analyst Target — when analyst consensus exists, use it.  Defensible
#      because it represents real coverage opinion even if our model can't run.
#   2. Cash-Only Distress Proxy — (cash - debt) / shares.  For deeply broken
#      companies this is a liquidation-style anchor.
#   3. Distressed P/B — 0.5× book value per share.  For banks and negative-
#      equity names where book is the most stable available signal.
#   4. P/Revenue — 1.0× sector-median EV/Revenue × TTM revenue / shares.
#      Last resort for REITs / ADRs / BDCs where FCF and earnings both fail.
def _emergency_iv(info, fx_rate, sector, industry, analyst_target_price=None):
    """
    Returns (iv, label, confidence) — confidence always "low".
    iv is clamped only at the call site via _clamp_iv (we don't know price here).
    Returns (None, None, None) if absolutely no signal can be produced.
    """
    shares = safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0) or 0

    # 1) Analyst target — most defensible last-resort
    if analyst_target_price and analyst_target_price > 0:
        return float(analyst_target_price), "Analyst Target (last-resort)", "low"

    # 2) Cash-only distress proxy — useful when book is impaired
    cash = (safe(info.get("totalCash"), 0) or 0) * fx_rate
    debt = (safe(info.get("totalDebt"), 0) or 0) * fx_rate
    if shares > 0:
        cash_only = (cash - debt) / shares
        if cash_only > 0.01:
            return round(cash_only, 2), "Cash-Only (distress proxy)", "low"

    # 3) Distressed P/B — for banks / neg-equity names
    book_per_share = safe(info.get("bookValue"))
    if book_per_share and book_per_share > 0:
        # 0.4× for banks (more conservative), 0.5× for everything else
        s_lower = (sector or "").lower()
        i_lower = (industry or "").lower()
        is_bank = ("bank" in i_lower or "financial" in s_lower or "insurance" in i_lower)
        mult = 0.4 if is_bank else 0.5
        return round(book_per_share * mult, 2), "Distressed P/B", "low"

    # 4) Price/Revenue floor — sector median EV/Revenue × TTM revenue / shares
    revenue_ttm = safe(info.get("totalRevenue"))
    if revenue_ttm and revenue_ttm > 0 and shares > 0:
        rev_usd = revenue_ttm * fx_rate
        # Crude sector EV/Revenue median table.  Conservative — these are
        # floors, not targets.  Tech ~3x, Energy ~1x, Banks N/A.
        s_lower = (sector or "").lower()
        if "technology" in s_lower or "communication" in s_lower:
            ev_rev = 3.0
        elif "consumer" in s_lower or "retail" in s_lower:
            ev_rev = 1.5
        elif "healthcare" in s_lower:
            ev_rev = 2.5
        elif "energy" in s_lower or "utilities" in s_lower:
            ev_rev = 1.0
        elif "financial" in s_lower or "real estate" in s_lower:
            ev_rev = 1.5
        else:
            ev_rev = 1.5
        ev_implied = rev_usd * ev_rev
        net_debt = debt - cash
        eq_val   = ev_implied - net_debt
        if eq_val > 0:
            return round(eq_val / shares, 2), f"P/Revenue (sector ~{ev_rev:.1f}x median)", "low"

    return None, None, None


def _bear_floor_iv(info, sector, industry, fx_rate, ebitda_ttm=None):
    """
    Minimum share price for the bear scenario, derived from the sector's EV/EBITDA floor.
    Prevents stress assumptions from producing impossible valuations for healthy companies.
    Returns 0.0 when EBITDA is unavailable or the floor equity value is negative.
    """
    ind_class = _classify_industry(sector, industry)
    key = ind_class if ind_class != "default" else _sector_to_mult_key(sector)
    floor_mult = INDUSTRY_BEAR_EV_FLOORS.get(key, INDUSTRY_BEAR_EV_FLOORS["default"])

    ebitda = ebitda_ttm or safe(info.get("ebitda"))
    shares = safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0) or 0
    if not ebitda or ebitda <= 0 or shares <= 0:
        return 0.0

    ev_floor = ebitda * fx_rate * floor_mult
    cash     = (safe(info.get("totalCash"), 0) or 0) * fx_rate
    debt     = (safe(info.get("totalDebt"), 0) or 0) * fx_rate
    net_debt = debt - cash
    eq_floor = ev_floor - net_debt
    return max(eq_floor / shares, 0.0)


def _calc_dcf_confidence(info, sector, industry, fcf_series, dcf_available, valuation_method):
    """
    DCF Reliability Score — rates how trustworthy the model output is for this security.

    Factors penalised:
      • FCF inconsistency (negative years in recent history)
      • Thin FCF margin (< 5%) — small assumption changes swing IV wildly
      • High financial leverage (D/E > 150%) — debt amplifies model error
      • Cyclical / commodity sectors with unpredictable cash flows
      • Shrinking revenue base

    Returns (level, label, strengths, weaknesses):
      level: "high" | "moderate" | "low" | "not_applicable"
    """
    s   = (sector   or "").lower()
    ind = (industry or "").lower()

    # ── Not applicable ───────────────────────────────────────────────────────
    if valuation_method in ("banking", "biotech"):
        reason = ("Banks and financial companies are valued via P/B + P/E — "
                  "DCF is structurally inappropriate (interest is operating cost)."
                  if valuation_method == "banking"
                  else "Biotech uses EV/Revenue + pipeline analysis — "
                       "FCF-based DCF doesn't capture pipeline optionality.")
        return "not_applicable", "Specialist Method", [], [reason]

    if not dcf_available:
        return "not_applicable", "DCF N/A", [],  [
            "No positive free cash flow — DCF intrinsic value cannot be computed. "
            "Multiples-based valuation is used instead."
        ]

    score     = 100
    strengths = []
    warnings  = []

    # ── 1. FCF consistency ────────────────────────────────────────────────────
    if fcf_series:
        n_pos = sum(1 for f in fcf_series if f > 0)
        n_tot = len(fcf_series)
        n_neg = n_tot - n_pos
        ratio = n_pos / n_tot
        if ratio < 0.50:
            score -= 35
            warnings.append(
                f"FCF negative in {n_neg} of last {n_tot} years — "
                "model is extrapolating from an unstable earnings base"
            )
        elif ratio < 0.80:
            score -= 15
            warnings.append(
                f"FCF inconsistent ({n_neg} negative year{'s' if n_neg > 1 else ''} "
                f"in last {n_tot}) — projections carry elevated uncertainty"
            )
        else:
            strengths.append(f"FCF positive in {n_pos}/{n_tot} years — consistent cash generation")

    # ── 2. FCF margin (thin = high IV sensitivity) ────────────────────────────
    rev_v = safe(info.get("totalRevenue"))
    fcf_v = safe(info.get("freeCashflow"))
    if rev_v and fcf_v and rev_v > 0:
        fcf_pct = fcf_v / rev_v * 100
        if fcf_pct < 3:
            score -= 25
            warnings.append(
                f"Thin FCF margin ({fcf_pct:.1f}%) — a 1pp change in cost assumptions "
                "can move IV by 20%+ ; treat output as a range, not a point estimate"
            )
        elif fcf_pct < 8:
            score -= 10
            warnings.append(
                f"Moderate FCF margin ({fcf_pct:.1f}%) — cost structure adds model sensitivity"
            )
        else:
            strengths.append(f"Strong FCF margin ({fcf_pct:.1f}%)")

    # ── 3. Leverage (D/E amplifies error) ─────────────────────────────────────
    de = safe(info.get("debtToEquity"))
    if de is not None:
        if de > 250:
            score -= 20
            warnings.append(
                f"High leverage ({de/100:.1f}× D/E) — large interest obligations "
                "make FCF projections fragile; EV/EBITDA is often more reliable here"
            )
        elif de > 130:
            score -= 8
            warnings.append(f"Above-average leverage ({de/100:.1f}× D/E) — monitor debt service")
        else:
            strengths.append(f"Conservative balance sheet ({de/100:.1f}× D/E)")

    # ── 4. Cyclicality penalty ─────────────────────────────────────────────────
    if any(x in ind for x in ["airline", "air freight", "airport"]):
        score -= 22
        warnings.append(
            "Airlines: boom/bust FCF cycle makes any 10-year projection speculative; "
            "EV/EBITDA through-the-cycle is the standard institutional approach"
        )
    elif any(x in ind for x in ["auto", "automobile", "vehicle"]):
        score -= 14
        warnings.append(
            "Autos: cyclical demand + high CapEx intensity — "
            "DCF terminal value is highly sensitive to assumed peak-cycle margins"
        )
    elif any(x in s + ind for x in ["oil", "gas", "coal", "metal", "mining"]):
        score -= 12
        warnings.append(
            "Commodities: FCF is correlated with spot prices — "
            "terminal value embeds commodity-price risk not visible in the model"
        )

    # ── 5. Shrinking revenue ───────────────────────────────────────────────────
    rev_g = safe(info.get("revenueGrowth"))
    if rev_g is not None and rev_g < -0.05:
        score -= 15
        warnings.append(
            f"Revenue declining {rev_g*100:.1f}% YoY — "
            "growth assumptions may be overstated relative to recent trajectory"
        )

    # ── Determine level ────────────────────────────────────────────────────────
    if score >= 75:
        return "high",     "High Confidence",     strengths, warnings
    elif score >= 48:
        return "moderate", "Moderate Confidence", strengths, warnings
    else:
        return "low",      "Low Confidence",      strengths, warnings


def _detect_moat(net_margin, revenue_growth, earnings_growth, fcf_margin, roe,
                  rev_ttm_bn=None):
    """
    Four-path moat classifier.  All percentage inputs are already in % form (e.g. 20 = 20%).

    Path A — High-Growth Backbone:      Net margin > 20%  AND  Revenue growth > 15%
    Path B — Mature Cash Machine:       Net margin > 25%  AND  Earnings growth > 15%
    Path C — Capital-Light Compounder:  Net margin > 20%  AND  FCF margin > 20%  AND  ROE > 25%
    Path D — Platform Scale Economy:    Revenue > $200B   AND  Revenue growth > 8%
                                        AND  FCF margin > 2%
              (catches mega-cap platform cos like AMZN whose blended margins are low
               because they reinvest aggressively — AWS/Advertising drive hidden economics)

    Returns (is_high_moat: bool, path_label: str | None, reasons: list[str])
    """
    nm    = net_margin      or 0
    rg    = revenue_growth  or 0
    eg    = earnings_growth or 0
    fm    = fcf_margin      or 0
    roe_v = roe             or 0
    rev_b = rev_ttm_bn      or 0   # billions

    if nm > 20 and rg > 15:
        return True, "High-Growth Backbone", [
            f"Net margin {nm:.1f}%", f"Revenue growth {rg:.1f}%"]
    if nm > 25 and eg > 15:
        return True, "Mature Cash Machine", [
            f"Net margin {nm:.1f}%", f"Earnings growth {eg:.1f}%"]
    if nm > 20 and fm > 20 and roe_v > 25:
        return True, "Capital-Light Compounder", [
            f"Net margin {nm:.1f}%", f"FCF margin {fm:.1f}%", f"ROE {roe_v:.1f}%"]
    # Path D: Platform Scale Economy — massive revenue with profitable FCF and solid growth.
    # Low blended margins don't tell the full story for platform companies that run
    # high-margin digital segments (cloud, advertising) alongside capital-intensive logistics.
    if rev_b > 200 and rg > 8 and fm > 2:
        return True, "Platform Scale Economy", [
            f"${rev_b:.0f}B revenue scale", f"Revenue growth {rg:.1f}%",
            f"FCF margin {fm:.1f}%"]
    return False, None, []


def _get_valuation_method(sector: str, industry: str) -> str:
    """
    Route each company to its most appropriate primary valuation methodology.
    Returns: 'dcf' | 'dcf_energy' | 'biotech' | 'banking'
    """
    s   = (sector   or "").lower()
    ind = (industry or "").lower()

    # Biotech / early-stage pharma — negative FCF, pipeline drives value
    if "biotech" in ind or "biotechnology" in ind:
        return "biotech"
    if "biopharmaceutical" in ind or ("pharmaceutical" in ind and "specialty" in ind):
        return "biotech"

    # Asset-light payment networks — network-effect moats; DCF is correct here.
    # Must be checked BEFORE the broad "financial" catch-all below, because yfinance
    # puts Visa/Mastercard/AXP in "Financial Services / Credit Services" or
    # "Financial Services / Payment Networks" — sectors that would otherwise fall into banking.
    _PAYMENT_INDUSTRIES = ["credit service", "payment network", "payment processing"]
    if any(x in ind for x in _PAYMENT_INDUSTRIES):
        return "dcf"

    # High-growth fintech / brokerage / capital markets — these companies earn from
    # transaction fees, AUM growth, and product expansion, NOT net interest margin.
    # P/B banking model systematically undervalues them; route to DCF instead.
    # Examples: Robinhood (HOOD), SoFi (SOFI), Coinbase (COIN), Affirm (AFRM),
    #           Upstart (UPST), Nu Holdings (NU), Block (SQ).
    _FINTECH_GROWTH_INDUSTRIES = [
        "capital markets", "securities brokerage", "investment brokerage",
        "consumer lending", "online lending", "fintech",
        "financial exchanges", "crypto exchange",
        "asset management",  # growth-stage asset managers
    ]
    if any(x in ind for x in _FINTECH_GROWTH_INDUSTRIES):
        return "dcf"

    # Banking / Financial services — interest is operating cost; DCF structurally wrong
    if any(x in s for x in ["financial", "bank"]):
        return "banking"
    if any(x in ind for x in ["bank", "insurance",
                               "investment bank", "thrift", "mortgage", "diversified financials"]):
        return "banking"

    # Energy / Mining / Commodities — finite reserves cap terminal growth at 1.5%
    if ("energy" in s and "renewable" not in ind) or "mining" in ind or "metals" in ind:
        return "dcf_energy"

    return "dcf"


def calc_banking_val(info, fx_rate):
    """
    P/B + P/E blend for banks and financial companies.
    Applies a 20% efficiency premium to both multiples when ROE > 15%.
    Returns (value_per_share, method_label) or (None, None).
    """
    bvps    = safe(info.get("bookValue"))       # book value per share
    fwd_eps = safe(info.get("forwardEps"))
    roe     = (safe(info.get("returnOnEquity")) or 0) * 100   # convert to %

    pb_mult = 1.30
    pe_mult = 11.0
    roe_tag = ""
    if roe > 15:
        pb_mult  = round(pb_mult * 1.20, 3)
        pe_mult  = round(pe_mult * 1.20, 1)
        roe_tag  = f" + ROE {roe:.1f}% efficiency premium"

    val_pb = round(bvps    * pb_mult, 2) if bvps    and bvps    > 0 else None
    val_pe = round(fwd_eps * pe_mult, 2) if fwd_eps and fwd_eps > 0 else None

    if val_pb and val_pe:
        val    = round((val_pb + val_pe) / 2, 2)
        method = f"P/B ({pb_mult:.2f}x) + P/E ({pe_mult:.1f}x) blend{roe_tag}"
    elif val_pb:
        val, method = val_pb, f"P/B ({pb_mult:.2f}x){roe_tag}"
    elif val_pe:
        val, method = val_pe, f"P/E ({pe_mult:.1f}x){roe_tag}"
    else:
        return None, None

    return max(val, 0.0), method


def calc_biotech_val(info, fx_rate, rev_ttm=None, analyst_target=None):
    """
    EV/Revenue-based valuation for biotech / early-stage pharma.
    Multiple range: 10x–15x (12x median). R&D > 30% of revenue adds +10% pipeline premium.
    Blends 50/50 with analyst consensus price target when available.
    Returns (value_per_share, method_label) or (None, None).
    """
    shares = safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0) or 0
    if shares <= 0:
        return None, None

    rev = rev_ttm or safe(info.get("totalRevenue"))
    if not rev or rev <= 0:
        # Pre-revenue: analyst target is the only anchor
        if analyst_target and analyst_target > 0:
            return round(float(analyst_target), 2), "Analyst Target (pre-revenue biotech)"
        return None, None

    ev_rev_mult = 12.0
    ev_implied  = rev * fx_rate * ev_rev_mult
    cash     = (safe(info.get("totalCash"), 0) or 0) * fx_rate
    debt     = (safe(info.get("totalDebt"), 0) or 0) * fx_rate
    net_debt = debt - cash
    eq_val   = ev_implied - net_debt
    if eq_val <= 0:
        if analyst_target and analyst_target > 0:
            return round(float(analyst_target), 2), "Analyst Target (negative equity biotech)"
        return None, None

    val_ev_rev = eq_val / shares

    # Pipeline premium: 10% uplift when R&D > 30% of revenue (active clinical spend)
    rnd = safe(info.get("researchDevelopment")) or 0
    has_pipeline = (rev > 0 and rnd / rev > 0.30)
    if has_pipeline:
        val_ev_rev *= 1.10
    pp_tag = " + Pipeline Premium" if has_pipeline else ""

    # 50/50 blend with analyst consensus when available
    if analyst_target and analyst_target > 0:
        val_final = 0.50 * val_ev_rev + 0.50 * float(analyst_target)
        method = f"EV/Revenue ({ev_rev_mult:.0f}x){pp_tag} 50% + Analyst Target 50%"
    else:
        val_final = val_ev_rev
        method = f"EV/Revenue ({ev_rev_mult:.0f}x){pp_tag}"

    return round(max(val_final, 0), 2), method


def _analyst_alignment_check(iv, analyst_target, price):
    """
    Sultan Split: unconditionally blend 90% VALUS model IV + 10% analyst
    consensus target whenever an analyst target is available.

    Rationale: The model captures long-run intrinsic value; the analyst
    consensus only acts as a gentle anchor.  The 90/10 split gives the
    fundamental model primacy while still cross-referencing the Street view.

    Returns (adjusted_iv, was_adjusted: bool, pre_blend_iv).
    """
    if not iv or not analyst_target or not price or price <= 0:
        return iv, False, iv
    at = float(analyst_target)
    if at <= 0:
        return iv, False, iv
    # Unconditional Sultan Split: 90% model + 10% analyst (always fires)
    blended = round(max(0.90 * iv + 0.10 * at, 0.0), 2)
    return blended, True, round(iv, 2)


def get_quarterly_balance_data(stock, info, fx_rate):
    """
    Pull the most recent quarterly balance sheet for cash, debt, and shares.
    Falls back to info-dict values when quarterly data is unavailable.
    Returns a dict with keys: total_cash, total_debt, shares (all in trading currency).
    """
    result = {
        "total_cash": (safe(info.get("totalCash"), 0) or 0) * fx_rate,
        "total_debt": (safe(info.get("totalDebt"), 0) or 0) * fx_rate,
        "shares":     safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0) or 0,
    }
    try:
        qbs = stock.quarterly_balance_sheet
        if qbs is None or qbs.empty:
            return result
        col = qbs.columns[0]  # Most recent quarter
        for k in ["Cash Cash Equivalents And Short Term Investments", "Cash And Cash Equivalents"]:
            if k in qbs.index:
                v = safe(qbs.loc[k, col])
                if v is not None and v >= 0:
                    result["total_cash"] = v * fx_rate
                    break
        for k in ["Total Debt", "Long Term Debt And Capital Lease Obligation", "Long Term Debt"]:
            if k in qbs.index:
                v = safe(qbs.loc[k, col])
                if v is not None and v >= 0:
                    result["total_debt"] = v * fx_rate
                    break
        if "Share Issued" in qbs.index:
            v = safe(qbs.loc["Share Issued", col])
            if v and v > 0:
                result["shares"] = v
    except Exception:
        pass
    return result


def get_fx_rate(from_ccy: str, to_ccy: str) -> float:
    """
    Return the exchange rate: 1 from_ccy = X to_ccy.
    E.g. get_fx_rate('EUR','USD') ≈ 1.09
    Falls back to 1.0 if unavailable.
    """
    if from_ccy == to_ccy:
        return 1.0
    key = f"{from_ccy}{to_ccy}"
    if key in _fx_cache:
        return _fx_cache[key]
    try:
        ticker_sym = f"{from_ccy}{to_ccy}=X"
        fi = yf.Ticker(ticker_sym).fast_info
        rate = float(fi.get("lastPrice") or fi.get("regularMarketPrice") or 1.0)
        if rate and not np.isnan(rate) and 0.0001 < rate < 100000:
            _fx_cache[key] = rate
            return rate
    except Exception:
        pass
    # Try inverse
    try:
        ticker_sym2 = f"{to_ccy}{from_ccy}=X"
        fi2 = yf.Ticker(ticker_sym2).fast_info
        inv = float(fi2.get("lastPrice") or fi2.get("regularMarketPrice") or 1.0)
        if inv and not np.isnan(inv) and inv > 0:
            rate = 1.0 / inv
            _fx_cache[key] = rate
            return rate
    except Exception:
        pass
    return 1.0

PERIOD_MAP = {
    "1d":  ("1d",  "5m"),
    "5d":  ("5d",  "30m"),
    "1m":  ("1mo", "1d"),
    "3m":  ("3mo", "1d"),
    "6m":  ("6mo", "1d"),
    "ytd": ("ytd", "1d"),
    "1y":  ("1y",  "1d"),
    "5y":  ("5y",  "1wk"),
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def safe(val, default=None):
    try:
        f = float(val)
        return None if (np.isnan(f) or np.isinf(f)) else f
    except Exception:
        return default


def _qual_score_pct(pct, good, great):
    """Score a percentage value into one of {weak, ok, strong, elite}.
    `good` is the threshold for ok→strong, `great` is strong→elite."""
    if pct is None:
        return None
    if pct >= great: return "elite"
    if pct >= good:  return "strong"
    if pct >= 0:     return "ok"
    return "weak"


def _qual_score_low(val, good, great):
    """Score a metric where lower is better (e.g. debt/equity)."""
    if val is None:
        return None
    if val <= great: return "elite"
    if val <= good:  return "strong"
    if val <= good * 2: return "ok"
    return "weak"


def _build_quality_metrics(info, base_fcf, revenue_ttm):
    """
    Snapshot of profitability, returns, and leverage from the live info
    dict + already-computed FCF & revenue. No additional API calls.

    Each metric reports value (% or ratio), label, and a qualitative tier
    so the UI can colour-code.
    """
    if not info:
        return None
    roe = safe(info.get("returnOnEquity"))
    roa = safe(info.get("returnOnAssets"))
    op_margin = safe(info.get("operatingMargins"))
    profit_margin = safe(info.get("profitMargins"))
    de  = safe(info.get("debtToEquity"))    # yfinance returns this as a percent (e.g. 145 for 1.45x)
    fcf_margin = None
    if base_fcf and revenue_ttm and revenue_ttm > 0:
        fcf_margin = float(base_fcf) / float(revenue_ttm)

    de_ratio = (de / 100.0) if de is not None else None
    metrics = []

    if roe is not None:
        metrics.append({
            "key": "roe", "label": "Return on Equity",
            "value_pct": round(roe * 100, 1),
            "tier": _qual_score_pct(roe * 100, good=10, great=20),
        })
    if roa is not None:
        metrics.append({
            "key": "roa", "label": "Return on Assets",
            "value_pct": round(roa * 100, 1),
            "tier": _qual_score_pct(roa * 100, good=5, great=10),
        })
    if op_margin is not None:
        metrics.append({
            "key": "op_margin", "label": "Operating Margin",
            "value_pct": round(op_margin * 100, 1),
            "tier": _qual_score_pct(op_margin * 100, good=10, great=20),
        })
    if profit_margin is not None:
        metrics.append({
            "key": "profit_margin", "label": "Net Margin",
            "value_pct": round(profit_margin * 100, 1),
            "tier": _qual_score_pct(profit_margin * 100, good=8, great=15),
        })
    if fcf_margin is not None:
        metrics.append({
            "key": "fcf_margin", "label": "FCF Margin",
            "value_pct": round(fcf_margin * 100, 1),
            "tier": _qual_score_pct(fcf_margin * 100, good=8, great=15),
        })
    if de_ratio is not None:
        metrics.append({
            "key": "debt_equity", "label": "Debt / Equity",
            "value_ratio": round(de_ratio, 2),
            "tier": _qual_score_low(de_ratio, good=1.0, great=0.5),
        })
    return metrics if metrics else None


def clean(v):
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    if isinstance(v, list):
        return [clean(i) for i in v]
    if isinstance(v, dict):
        return {k: clean(x) for k, x in v.items()}
    return v


# ── Tax rate ───────────────────────────────────────────────────────────────────

def calc_tax_rate(info, income_stmt):
    """Derive effective tax rate from income statement if not available in info."""
    t = safe(info.get("effectiveTaxRate"))
    if t is not None and 0.05 < t < 0.50:
        return t
    try:
        if income_stmt is not None and not income_stmt.empty:
            for tax_k in ["Tax Provision"]:
                for ebt_k in ["Pretax Income"]:
                    if tax_k in income_stmt.index and ebt_k in income_stmt.index:
                        tp = safe(income_stmt.loc[tax_k].iloc[0])
                        eb = safe(income_stmt.loc[ebt_k].iloc[0])
                        if tp is not None and eb and eb > 0:
                            rate = abs(tp) / eb
                            if 0.02 < rate < 0.50:
                                return rate
    except Exception:
        pass
    return 0.21


# ── WACC ───────────────────────────────────────────────────────────────────────

def calc_wacc(info, income_stmt, tax_rate=0.21, fx_rate=1.0):
    # Use reported beta; fall back to industry default when yfinance returns None
    beta_raw = safe(info.get("beta"))
    if beta_raw is None or np.isnan(beta_raw):
        beta_raw = _default_beta(info.get("sector",""), info.get("industry",""))
    beta = float(min(max(beta_raw, 0.3), 3.0))
    coe  = RISK_FREE_RATE + beta * EQUITY_RISK_PREM

    # Cost of debt: interest expense / total debt (both in reporting ccy, ratio is neutral)
    cod = 0.05
    try:
        iexp = None
        if income_stmt is not None and not income_stmt.empty:
            for lbl in ["Interest Expense Non Operating", "Interest Expense"]:
                if lbl in income_stmt.index:
                    v = safe(income_stmt.loc[lbl].iloc[0])
                    if v is not None:
                        iexp = abs(v); break
        debt_rep = safe(info.get("totalDebt"), 0) or 0
        if iexp and debt_rep > 0:
            c = iexp / debt_rep
            if 0.01 < c < 0.25:
                cod = c
    except Exception:
        pass

    mcap = safe(info.get("marketCap"), 0) or 0
    debt = (safe(info.get("totalDebt"), 0) or 0) * fx_rate
    tc   = mcap + debt

    if tc > 0:
        we_raw = mcap / tc
        wd_raw = debt / tc
        # Cap debt weight at 75%: prevents distressed-leverage from pulling WACC
        # below the point where the Gordon Growth Model becomes meaningless.
        wd = min(wd_raw, 0.75)
        we = 1.0 - wd
    else:
        we, wd = 0.85, 0.15

    wacc = we * coe + wd * cod * (1 - tax_rate)
    if not wacc or np.isnan(wacc):
        wacc = coe
    return {
        "wacc": min(max(wacc, 0.05), 0.20),
        "coe": coe, "cod": cod,
        "tax": tax_rate, "beta": beta,
        "we": we, "wd": wd,
    }


# ── Expectation Gap Engine ─────────────────────────────────────────────────────
# Answers "where is the market wrong?" by reverse-engineering the growth rate
# the current stock price is implicitly pricing in, then comparing it to the
# model's forecast.  The gap between those two numbers is the signal.

def solve_implied_growth(price, base_fcf, s2_ratio, tg, wacc, yrs,
                          info, fx_rate, net_debt_override=None,
                          low=0.0, high=1.5, tol=5e-5):
    """
    Reverse DCF: binary-search for the Stage-1 growth rate `g` such that
    run_dcf_single(g, ...) produces an intrinsic value ≈ current price.
    Returns (implied_g, converged: bool).
    """
    if not price or not base_fcf or base_fcf <= 0:
        return None, False
    try:
        for _ in range(80):
            mid = (low + high) / 2
            s2  = max(mid * s2_ratio, tg + 0.005, 0.02)
            iv, *_ = run_dcf_single(
                base_fcf, mid, s2, tg, wacc, yrs, info, fx_rate,
                net_debt_override=net_debt_override)
            if iv is None:
                return None, False
            diff = iv - price
            if abs(diff) < tol * price:
                return mid, True
            if diff < 0:
                low = mid
            else:
                high = mid
        return (low + high) / 2, False
    except Exception:
        return None, False


# ── Mag 7 Tag ───────────────────────────────────────────────────────────────
MAG_7_TICKERS = {"AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA"}


def _is_mag7(ticker):
    return (ticker or "").upper() in MAG_7_TICKERS


# ── Strategic Asset Classifier ──────────────────────────────────────────────
# Recognizes companies where US capital allocation is policy-driven, not
# purely market-driven.  Pure DCF systematically undervalues these names
# because the discount rate doesn't reflect government backstops, CHIPS Act
# subsidies, DPA Title III orders, or sovereign supply-chain mandates.
#
# Each entry carries a *per-ticker* reason string so the model isn't just
# "you're on a list" — it's a transparent thesis the UI surfaces.
#
# Tier deltas (applied downstream to WACC, sector ceiling, IV floor):
#   wacc_delta:    subtracted from WACC (lower = higher IV)
#   ceiling_lift:  added to sector growth ceiling
#   iv_floor_mult: minimum IV expressed as a multiple of price (e.g., 0.85
#                  means strategic assets won't be tagged "overvalued by 50%"
#                  when there's a clear policy thesis the model misses).
#   narrative:     one-sentence framing for the verdict layer
#
# Curation principle: only tickers where the *reason for inclusion* is a
# specific, citable government program or sovereign-supply role.  No
# generic "national champion" hand-waving.

STRATEGIC_ASSETS = {
    # ── Tier 1: Semiconductor Sovereignty ─────────────────────────────────
    "MU":   ("semi_sovereignty", "Sole US-headquartered DRAM and HBM producer · CHIPS Act $6.1B grant (Idaho/NY fabs) · HBM is the bottleneck for AI inference."),
    "INTC": ("semi_sovereignty", "CHIPS Act $8.5B grant + $11B in loans · only US leading-edge logic foundry · DoD ‘trusted foundry’ pivot under DPA Title III."),
    "TXN":  ("semi_sovereignty", "CHIPS Act $1.6B grant · analog/embedded silicon ubiquitous in US defense and auto."),
    "ADI":  ("semi_sovereignty", "Defense + auto analog leader; supply-chain critical with strict export-control moat."),
    "MCHP": ("semi_sovereignty", "Defense MCUs in satellites and missiles; ITAR/export-control protection."),
    "ON":   ("semi_sovereignty", "Power semis for EVs and grid · NY fab supported by DPA framework."),
    "AMAT": ("semi_sovereignty", "Semi capital equipment monopoly stack; export controls to China are a tailwind, not a risk."),
    "KLAC": ("semi_sovereignty", "Process-control monopoly in semi metrology; export-control protected."),
    "LRCX": ("semi_sovereignty", "Etch/deposition stack; same export-control moat as AMAT/KLAC."),

    # ── Tier 2: Defense Primes ────────────────────────────────────────────
    "LMT":  ("defense_prime", "F-35, missile defense, hypersonics — ~70% revenue from US gov; multi-decade contract backlog."),
    "RTX":  ("defense_prime", "Patriot, NASAMS, hypersonics; record post-Ukraine restocking demand."),
    "NOC":  ("defense_prime", "Sentinel ICBM, B-21 — sole-source on US strategic deterrent platforms."),
    "GD":   ("defense_prime", "Virginia/Columbia-class subs and combat systems; sovereign-supplier lock."),
    "LHX":  ("defense_prime", "Tactical comms and EW — Pentagon ‘irreplaceable supplier’ tier."),
    "HII":  ("defense_prime", "Sole US builder of nuclear aircraft carriers and Virginia-class submarines."),

    # ── Tier 3: Energy Sovereignty ────────────────────────────────────────
    "CEG":  ("energy_sovereignty", "Largest US nuclear fleet · Microsoft 20-year PPA on Three Mile Island restart · AI datacenter power gold."),
    "VST":  ("energy_sovereignty", "Comanche Peak nuclear + Texas gas; signed AI hyperscaler PPAs at premium pricing."),
    "NEE":  ("energy_sovereignty", "Largest US wind/solar/nuclear utility · IRA tax-credit dominant beneficiary."),
    "OXY":  ("energy_sovereignty", "Permian #1 + Direct Air Capture with DOE-backed Stratos plant; Buffett-aligned strategic stake."),

    # ── Tier 4: Critical Materials & Nuclear ──────────────────────────────
    "MP":   ("critical_material", "Mountain Pass — only operating US rare-earth mine and processor · DPA Title III funded."),
    "LEU":  ("critical_material", "Sole US uranium enrichment producer · DOE HALEU contracts for next-gen reactors."),
    "BWXT": ("critical_material", "Sole supplier of US Navy nuclear reactors and components."),

    # ── Tier 5: Urban Air Mobility (Pre-revenue / emerging franchises) ────
    # Smaller WACC delta + larger ceiling lift reflects the speculative
    # nature — these are franchise bets, not mature cash flows.  IV floor
    # is lower (0.75) because pure DCF on pre-revenue eVTOL is meaningless;
    # the model leans on analyst targets + transformative catalysts.
    "JOBY": ("urban_air_mobility", "First eVTOL operator with FAA Part 135 air-carrier certificate · Toyota and Delta backing · NYC commercial launch · airworthiness criteria finalized."),
    "ACHR": ("urban_air_mobility", "Stellantis manufacturing partnership · United Airlines order book · UAE deployment underway · Type Cert progress on schedule."),
    "RKLB": ("urban_air_mobility", "Sole-source US small-launch alternative to SpaceX · DoD STP-S29 mission · Neutron rocket development · expanding government revenue mix."),
}

# Tier-level effects.  Strategic premium is meaningful but bounded — these
# numbers are deliberately conservative so the layer never single-handedly
# flips a verdict; it just keeps the model from systematically penalizing
# strategic assets for the wrong reasons.
_STRATEGIC_TIER_DELTAS = {
    # tier:               (wacc_delta, ceiling_lift, iv_floor_mult, label)
    "semi_sovereignty":   (-0.010, 0.05, 0.85, "Semiconductor Sovereignty"),
    "defense_prime":      (-0.005, 0.02, 0.90, "Defense Prime"),
    "energy_sovereignty": (-0.0075, 0.03, 0.88, "Energy Sovereignty"),
    "critical_material":  (-0.010, 0.05, 0.85, "Critical Materials"),
    # UAM is speculative — smaller WACC delta but higher ceiling lift
    # (these are pre-revenue franchises where growth is the whole thesis).
    # IV floor is lower because pure DCF on pre-revenue eVTOL is meaningless.
    "urban_air_mobility": (-0.0050, 0.08, 0.75, "Urban Air Mobility"),
}


def _strategic_classifier(ticker):
    """
    Returns a strategic-asset profile dict (or None for non-strategic names).

    The classifier is intentionally curation-driven.  Algorithmic detection
    of "national security relevance" from sector codes alone produces too
    many false positives (every regional bank shows up under 'Financial
    Services'); a transparent curated list with per-ticker reasons is more
    defensible and easier to audit.
    """
    if not ticker:
        return None
    entry = STRATEGIC_ASSETS.get(ticker.upper())
    if entry is None:
        return None
    tier, reason = entry
    deltas = _STRATEGIC_TIER_DELTAS.get(tier)
    if deltas is None:
        return None
    wacc_delta, ceiling_lift, iv_floor_mult, tier_label = deltas
    return {
        "is_strategic":     True,
        "strategic_tier":   tier,
        "strategic_label":  tier_label,
        "strategic_reason": reason,
        "wacc_delta":       wacc_delta,
        "ceiling_lift":     ceiling_lift,
        "iv_floor_mult":    iv_floor_mult,
        "narrative": (
            f"VALUS recognizes {ticker.upper()} as a strategic US asset — "
            f"{tier_label}.  Pure DCF systematically undervalues these names "
            "because the discount rate ignores government backstops and "
            "policy-driven capital flows."
        ),
    }


# ── "Priced For" Verdict ────────────────────────────────────────────────────

def _priced_for_verdict(implied_g, sector_ceiling, price, iv, margin_of_safety=None):
    """
    Tier the stock by margin of safety (IV vs price) — primary driver — with
    a sector-ceiling override that bumps anything beyond ceiling × 1.2 into
    the Miracle tier regardless of MOS.

    This matches what the top card shows: positive MOS → undervalued tiers,
    negative MOS → overvalued tiers.  Eliminates the inconsistency where
    an OVERVALUED stock could be tagged "Priced for Discount".

    Returns dict: {tier, label, color, narrative}
    """
    if iv is None or price is None or price <= 0:
        return None

    # Compute MOS if not provided
    if margin_of_safety is None:
        margin_of_safety = (iv - price) / price * 100

    mos = margin_of_safety   # +ve = undervalued; -ve = overvalued

    # Sector ceiling override — only fires when DCF ALSO says overvalued.
    # When MOS is positive (model says undervalued) but implied growth happens
    # to exceed the ceiling, we trust MOS — the implied-growth math can be
    # noisy on highly-leveraged or low-FCF stocks (e.g. Ford).
    if (mos < -10 and implied_g is not None and sector_ceiling
            and implied_g > sector_ceiling * 1.20):
        return {"tier": "miracle", "label": "Priced for Miracle", "color": "red",
                "narrative": (f"Market implies {implied_g*100:.1f}% growth — exceeds sector "
                              f"ceiling × 1.2; speculative.")}

    # Hard speculative override — independent of MOS sign.  When implied
    # growth blows past ceiling × 1.5, the model is fragile regardless of
    # which direction MOS leans; surface it so investors don't see a
    # "Priced for Discount" tag on a moonshot.
    if (implied_g is not None and sector_ceiling
            and implied_g > sector_ceiling * 1.50):
        return {"tier": "miracle", "label": "Priced for Miracle", "color": "red",
                "narrative": (f"Market implies {implied_g*100:.1f}% growth — far above sector "
                              f"ceiling; treat output as low-confidence.")}

    if mos >= 40:
        return {"tier": "deep_discount", "label": "Priced for Deep Discount", "color": "green",
                "narrative": f"Trading {mos:.0f}% below VALUS fair value — market overly pessimistic."}
    if mos >= 15:
        return {"tier": "discount", "label": "Priced for Discount", "color": "green",
                "narrative": f"Trading {mos:.0f}% below VALUS fair value — undervalued."}
    if mos >= -10:
        return {"tier": "fair_value", "label": "Priced for Fair Value", "color": "blue",
                "narrative": "VALUS and market are aligned — fair value zone."}
    if mos >= -25:
        return {"tier": "growth", "label": "Priced for Growth", "color": "amber",
                "narrative": (f"Market paying a growth premium — VALUS sees stock as "
                              f"overvalued by {abs(mos):.0f}%.")}
    if mos >= -50:
        return {"tier": "excellence", "label": "Priced for Excellence", "color": "amber",
                "narrative": (f"Market expecting flawless execution — VALUS sees stock as "
                              f"overvalued by {abs(mos):.0f}%.")}
    return {"tier": "miracle", "label": "Priced for Miracle", "color": "red",
            "narrative": (f"Market pricing in extraordinary outcomes — VALUS sees stock as "
                          f"overvalued by {abs(mos):.0f}%.")}


def _sector_growth_ceiling(sector, industry, is_structural_transformer=False, moat_detected=False):
    """Same ladder as _reality_reconciliation, exposed for the verdict layer."""
    s_lower   = (sector   or "").lower()
    ind_lower = (industry or "").lower()
    if is_structural_transformer:
        return 0.50, "Structural Transformer"
    if moat_detected and ("technology" in s_lower or "communication" in s_lower):
        return 0.45, "Moat-Tech"
    if "technology" in s_lower or "communication" in s_lower:
        return 0.40, "Tech / Comms"
    if any(x in ind_lower for x in [
            "capital markets", "securities brokerage", "consumer lending",
            "online lending", "fintech", "financial exchanges"]):
        return 0.40, "Growth Fintech"
    if "healthcare" in s_lower or "biotech" in ind_lower:
        return 0.35, "Healthcare"
    if "financial" in s_lower or "bank" in ind_lower or "insurance" in ind_lower:
        return 0.20, "Banking / Insurance"
    if "energy" in s_lower or "materials" in s_lower or "mining" in ind_lower:
        return 0.15, "Energy / Materials"
    return 0.25, "Default"


# ── Debt + Momentum Classifier ──────────────────────────────────────────────

def _debt_momentum_classifier(info, balance_sheet, fcf_series, price_history):
    """
    Classify leveraged stocks into:
      - "Deleveraging Story":   debt falling, FCF positive, interest cov > 2× — apply uplift
      - "Speculative Distress": debt growing OR FCF negative, momentum without fundamentals
      - "Recovery Watch":       leverage high, mixed signals
      - "Healthy Leverage":     debt low or improving with stable cash flow
      - "Stable":               low leverage, no special handling needed

    Returns dict: {classification, label, color, narrative, premium_pct, flags}
    premium_pct is an IV-uplift factor: e.g. 0.10 = +10% IV (only for Deleveraging).
    """
    total_debt = safe(info.get("totalDebt"), 0) or 0
    total_cash = safe(info.get("totalCash"), 0) or 0
    ebitda     = safe(info.get("ebitda"), 0) or 0
    ebit       = safe(info.get("ebitda"), 0) or 0   # proxy when EBIT not available
    interest   = safe(info.get("interestExpense")) or 0

    # Debt-to-EBITDA (leverage)
    debt_to_ebitda = (total_debt / ebitda) if ebitda > 0 else None
    net_debt_to_ebitda = ((total_debt - total_cash) / ebitda) if ebitda > 0 else None

    # Interest coverage (how easily can EBIT pay interest)
    interest_cov = (ebit / abs(interest)) if interest and abs(interest) > 0 else None

    # FCF trajectory (last 2 years)
    fcf_positive_recent = bool(fcf_series and fcf_series[0] > 0)
    fcf_improving       = bool(fcf_series and len(fcf_series) >= 2 and fcf_series[0] > fcf_series[1])

    # Debt trend (use balance_sheet quarterly comparison if available)
    debt_trend = None    # negative = deleveraging
    try:
        if balance_sheet is not None and not balance_sheet.empty:
            for label in ["Total Debt", "Long Term Debt"]:
                if label in balance_sheet.index:
                    series = balance_sheet.loc[label].dropna()
                    if len(series) >= 2:
                        recent, prior = float(series.iloc[0]), float(series.iloc[1])
                        if prior > 0:
                            debt_trend = (recent - prior) / prior
                        break
    except Exception:
        pass

    # 1-year price momentum
    price_1yr_pct = None
    try:
        if price_history and len(price_history) >= 30:
            recent = price_history[-1].get("close") if isinstance(price_history[-1], dict) else None
            old    = price_history[0].get("close")  if isinstance(price_history[0], dict)  else None
            if recent and old and old > 0:
                price_1yr_pct = (recent - old) / old
    except Exception:
        pass

    flags = []
    is_leveraged = (debt_to_ebitda is not None and debt_to_ebitda > 3.0)
    has_momentum = (price_1yr_pct is not None and price_1yr_pct > 0.20)

    # Stable: low leverage — no classification needed
    if not is_leveraged and (debt_to_ebitda is None or debt_to_ebitda < 2.0):
        return {
            "classification": "stable",
            "label": "Stable Capital Structure",
            "color": "neutral",
            "narrative": "Low-leverage profile — no debt-momentum signal.",
            "premium_pct": 0.0,
            "flags": [],
            "debt_to_ebitda": round(debt_to_ebitda, 2) if debt_to_ebitda is not None else None,
            "interest_coverage": round(interest_cov, 2) if interest_cov is not None else None,
            "debt_trend_pct": round(debt_trend * 100, 1) if debt_trend is not None else None,
        }

    # Healthy Leverage: leveraged but cash flow strong
    if is_leveraged and fcf_positive_recent and (interest_cov is None or interest_cov > 4.0):
        return {
            "classification": "healthy_leverage",
            "label": "Healthy Leverage",
            "color": "blue",
            "narrative": (f"Debt/EBITDA {debt_to_ebitda:.1f}× — high but covered by strong cash flow "
                          f"(coverage {interest_cov:.1f}×)." if interest_cov else
                          f"Debt/EBITDA {debt_to_ebitda:.1f}× — supported by positive FCF."),
            "premium_pct": 0.0,
            "flags": [],
            "debt_to_ebitda": round(debt_to_ebitda, 2),
            "interest_coverage": round(interest_cov, 2) if interest_cov is not None else None,
            "debt_trend_pct": round(debt_trend * 100, 1) if debt_trend is not None else None,
        }

    # Deleveraging Story: high leverage + falling debt + improving FCF + momentum
    if (is_leveraged and has_momentum
            and debt_trend is not None and debt_trend < -0.05
            and fcf_positive_recent
            and (interest_cov is None or interest_cov > 2.0)):
        # Deleveraging velocity → premium between 5% and 15%
        velocity = min(abs(debt_trend), 0.30)              # cap at 30% YoY paydown
        premium_pct = round(0.05 + (velocity / 0.30) * 0.10, 3)   # 5–15% IV uplift
        flags.append(f"Debt down {abs(debt_trend)*100:.1f}% YoY — equity rerating opportunity")
        if interest_cov: flags.append(f"Interest coverage improving: {interest_cov:.1f}×")
        return {
            "classification": "deleveraging",
            "label": "Deleveraging Story",
            "color": "green",
            "narrative": (
                f"Debt/EBITDA {debt_to_ebitda:.1f}× and falling — paying down debt while "
                f"FCF is positive.  Equity becomes safer (and more valuable) as leverage "
                f"normalises.  Applying +{premium_pct*100:.1f}% rerating premium."
            ),
            "premium_pct": premium_pct,
            "flags": flags,
            "debt_to_ebitda": round(debt_to_ebitda, 2),
            "interest_coverage": round(interest_cov, 2) if interest_cov is not None else None,
            "debt_trend_pct": round(debt_trend * 100, 1),
        }

    # Speculative Distress: high leverage + price momentum + bad fundamentals
    if (is_leveraged and has_momentum and (
            (debt_trend is not None and debt_trend > 0.05) or
            (not fcf_positive_recent) or
            (interest_cov is not None and interest_cov < 1.5))):
        if not fcf_positive_recent:    flags.append("Free cash flow negative")
        if debt_trend and debt_trend > 0.05: flags.append(f"Debt growing {debt_trend*100:.1f}% YoY")
        if interest_cov and interest_cov < 1.5: flags.append(f"Interest coverage critically low: {interest_cov:.1f}×")
        flags.append("Price momentum decoupled from fundamentals — high reversal risk")
        return {
            "classification": "speculative_distress",
            "label": "Speculative Distress",
            "color": "red",
            "narrative": (
                f"Debt/EBITDA {debt_to_ebitda:.1f}× combined with weak fundamentals "
                f"and price momentum.  Rally is not supported by improving cash flow "
                f"or deleveraging — treat as momentum-driven, not fundamental."
            ),
            "premium_pct": 0.0,
            "flags": flags,
            "debt_to_ebitda": round(debt_to_ebitda, 2),
            "interest_coverage": round(interest_cov, 2) if interest_cov is not None else None,
            "debt_trend_pct": round(debt_trend * 100, 1) if debt_trend is not None else None,
        }

    # Recovery Watch: leveraged, mixed signals, no clear classification
    if is_leveraged:
        return {
            "classification": "recovery_watch",
            "label": "Recovery Watch",
            "color": "amber",
            "narrative": (
                f"Debt/EBITDA {debt_to_ebitda:.1f}× — leveraged but signals mixed.  "
                f"Watch for FCF stability and continued debt paydown."
            ),
            "premium_pct": 0.0,
            "flags": flags,
            "debt_to_ebitda": round(debt_to_ebitda, 2),
            "interest_coverage": round(interest_cov, 2) if interest_cov is not None else None,
            "debt_trend_pct": round(debt_trend * 100, 1) if debt_trend is not None else None,
        }

    # Default: not flagged
    return {
        "classification": "stable",
        "label": "Stable Capital Structure",
        "color": "neutral",
        "narrative": "Capital structure does not require special handling.",
        "premium_pct": 0.0,
        "flags": [],
        "debt_to_ebitda": round(debt_to_ebitda, 2) if debt_to_ebitda is not None else None,
        "interest_coverage": round(interest_cov, 2) if interest_cov is not None else None,
        "debt_trend_pct": round(debt_trend * 100, 1) if debt_trend is not None else None,
    }


# ── Verdict Summary (clean numbered "why" explanation) ─────────────────────

def _build_verdict_summary(ticker, priced_for, implied_growth_pct, model_growth_pct,
                            sector_ceiling_pct, sector_ceiling_label, price, iv,
                            margin_of_safety, analyst_target,
                            debt_momentum, is_cash_rich, cash_pct_of_mcap,
                            is_mag7, is_structural_transformer):
    """
    Distil the full analysis into a clean numbered explanation users can
    actually read.  Replaces the dense flag list with: a one-line headline,
    three numbered reasons, and a one-line verdict.

    Returns dict: {headline, reasons, verdict, tier, color}
    """
    if not priced_for or not price or price <= 0 or iv is None:
        return None

    tier  = priced_for.get("tier")
    label = priced_for.get("label")
    color = priced_for.get("color", "blue")
    reasons = []

    # Reason 1 — frame the price-vs-fundamentals story.  The CANONICAL signal
    # is margin_of_safety (price vs IV); implied growth is an interesting
    # secondary signal but can be noisy on leveraged or capex-heavy stocks
    # where the reverse-DCF is non-monotonic.  When the two disagree, we
    # trust MOS for the headline framing.
    mos = margin_of_safety
    has_growth = (implied_growth_pct is not None and model_growth_pct is not None
                  and abs(model_growth_pct) > 0.5)
    if has_growth and mos is not None:
        gap_pp = implied_growth_pct - model_growth_pct
        if mos > 5:
            # MODEL SAYS UNDERVALUED — market is being conservative.
            # Two separate facts: (1) growth-rate gap, (2) price gap.  We
            # state them as distinct sentences so the reader doesn't try
            # to derive 2 from 1 with arithmetic that doesn't add up.
            if implied_growth_pct < -1:
                ig_txt = f"contracting {abs(implied_growth_pct):.1f}%/yr"
            elif implied_growth_pct < 2:
                ig_txt = "near-zero growth"
            elif implied_growth_pct < 5:
                ig_txt = f"low single-digit growth (~{implied_growth_pct:.1f}%/yr)"
            else:
                ig_txt = f"~{implied_growth_pct:.1f}%/yr"
            reasons.append(
                f"The market is pricing in revenue growth of {ig_txt} for the "
                f"next decade; VALUS forecasts {model_growth_pct:.1f}%/yr "
                f"(~{abs(gap_pp):.0f}pp higher).  With that growth assumption, "
                f"VALUS fair value of ${iv:.2f} sits {abs(mos):.0f}% above today's "
                f"${price:.2f} — undervalued."
            )
        elif mos < -5:
            # MODEL SAYS OVERVALUED — market paying for more growth than VALUS
            # expects, or optionality the model can't price.
            if implied_growth_pct > model_growth_pct + 1:
                reasons.append(
                    f"The market is pricing in ~{implied_growth_pct:.1f}%/yr "
                    f"revenue growth for a decade; VALUS forecasts "
                    f"{model_growth_pct:.1f}%/yr (~{abs(gap_pp):.0f}pp gap).  With "
                    f"VALUS's lower growth assumption, fair value of ${iv:.2f} sits "
                    f"{abs(mos):.0f}% below today's ${price:.2f} — overvalued."
                )
            else:
                # Same/lower implied growth but still overvalued — capex, leverage,
                # or shareholder-dilution math is doing the work.  Use MOS framing.
                reasons.append(
                    f"VALUS fair value of ${iv:.2f} sits {abs(mos):.0f}% below "
                    f"today's price of ${price:.2f} — capital structure, capex "
                    f"intensity, or share dilution explain more of the gap than growth."
                )
        else:
            # Roughly fair value
            reasons.append(
                f"At ${price:.2f}, market and VALUS are broadly aligned — both expect "
                f"~{model_growth_pct:.1f}% annual growth and the price reflects that."
            )
    elif iv and price and mos is not None:
        # No reliable growth comparison — fall back to MOS-driven copy
        if mos > 5:
            reasons.append(
                f"VALUS fair value of ${iv:.2f} sits {mos:.0f}% above today's "
                f"price of ${price:.2f} — model sees the stock as underpriced."
            )
        elif mos < -5:
            reasons.append(
                f"VALUS fair value of ${iv:.2f} sits {abs(mos):.0f}% below today's "
                f"price of ${price:.2f} — model sees the stock as overpriced."
            )
        else:
            reasons.append(
                f"VALUS fair value of ${iv:.2f} is broadly in line with today's "
                f"price of ${price:.2f}."
            )

    # Reason 2 — sector credibility check
    if sector_ceiling_pct and implied_growth_pct is not None:
        ratio = implied_growth_pct / sector_ceiling_pct
        if ratio > 1.20:
            reasons.append(
                f"That growth rate exceeds the {sector_ceiling_label} sector ceiling of "
                f"{sector_ceiling_pct:.0f}% by {(ratio-1)*100:.0f}% — only ~1% of public "
                f"companies have sustained this for a decade."
            )
        elif ratio > 1.00:
            reasons.append(
                f"That growth rate sits at the {sector_ceiling_label} sector ceiling "
                f"({sector_ceiling_pct:.0f}%) — execution must be flawless."
            )
        elif ratio > 0.80:
            reasons.append(
                f"That growth rate is in the upper band for the {sector_ceiling_label} "
                f"sector (ceiling {sector_ceiling_pct:.0f}%) — credible but premium."
            )
        elif ratio > 0.40:
            reasons.append(
                f"That growth rate is well within the {sector_ceiling_label} sector "
                f"ceiling of {sector_ceiling_pct:.0f}% — comfortable assumption."
            )
        else:
            reasons.append(
                f"That growth rate is significantly below the {sector_ceiling_label} "
                f"sector ceiling of {sector_ceiling_pct:.0f}% — the market is being conservative."
            )

    # Reason 3 — capital structure / cash position / analyst alignment
    # Priority order: cash-rich (most differentiated) → debt classification →
    # structural transformer → analyst alignment → Mag 7
    third_reason = None

    if is_cash_rich and cash_pct_of_mcap and cash_pct_of_mcap >= 15:
        third_reason = (
            f"Cash-loaded balance sheet — net cash is {cash_pct_of_mcap:.0f}% of market cap, "
            f"providing strategic optionality (buybacks, M&A, R&D) and recession resilience "
            f"not fully captured by DCF."
        )
    elif analyst_target and price:
        at_gap = (analyst_target - price) / price * 100
        if abs(at_gap) > 5:
            direction = "above" if at_gap > 0 else "below"
            third_reason = (
                f"Sell-side analysts target ${analyst_target:.2f} ({abs(at_gap):.0f}% "
                f"{direction} current price) — they {'see further upside' if at_gap > 0 else 'see downside ahead'}."
            )

    if third_reason is None and debt_momentum:
        dm_class = debt_momentum.get("classification")
        if dm_class == "deleveraging":
            third_reason = (
                f"Debt is being paid down ({debt_momentum.get('debt_trend_pct', 0):.0f}% YoY) "
                f"while FCF stays positive — equity rerating opportunity (already priced into IV)."
            )
        elif dm_class == "speculative_distress":
            third_reason = (
                f"Capital structure is stressed (Debt/EBITDA "
                f"{debt_momentum.get('debt_to_ebitda', 0):.1f}×) and price momentum is "
                f"not supported by improving fundamentals — high reversal risk."
            )
        elif dm_class == "healthy_leverage":
            third_reason = (
                f"Despite Debt/EBITDA {debt_momentum.get('debt_to_ebitda', 0):.1f}×, "
                f"interest coverage is strong — leverage is not a downside risk."
            )

    if third_reason is None and is_structural_transformer:
        third_reason = (
            f"Structural Transformer status: market is pricing platform "
            f"optionality (AI/robotics/autonomy) on top of base business — "
            f"requires belief in long-tail upside scenarios."
        )

    if third_reason is None and is_mag7:
        third_reason = (
            f"As a Mag 7 member, this name moves with the broader AI productivity "
            f"thesis — concentrated exposure in your portfolio amplifies that risk."
        )

    if third_reason:
        reasons.append(third_reason)

    # Verdict line — always matches the sign of margin_of_safety so it's
    # consistent with the OVERVALUED/UNDERVALUED tag at the top of the page.
    mos = margin_of_safety or 0
    if tier == "deep_discount":
        verdict = (f"VALUS sees this stock as undervalued by {abs(mos):.0f}% — market is "
                   f"overly pessimistic; meaningful upside if fundamentals hold.")
    elif tier == "discount":
        verdict = (f"VALUS sees this stock as undervalued by {abs(mos):.0f}% — trading "
                   f"below fundamental value.")
    elif tier == "fair_value":
        verdict = "VALUS and market are aligned — fair value zone, no clear edge."
    elif tier == "growth":
        verdict = (f"VALUS sees this stock as overvalued by {abs(mos):.0f}% — market is "
                   f"paying a growth premium.")
    elif tier == "excellence":
        verdict = (f"VALUS sees this stock as overvalued by {abs(mos):.0f}% — market "
                   f"expects flawless execution; limited margin of error if growth slows.")
    elif tier == "miracle":
        verdict = (f"VALUS sees this stock as overvalued by {abs(mos):.0f}% — market is "
                   f"paying for outcomes very few companies achieve.")
    else:
        verdict = label or "Verdict pending."

    return {
        "tier":     tier,
        "label":    label,
        "color":    color,
        "headline": f"{ticker} is {label}",
        "reasons":  reasons,
        "verdict":  verdict,
    }


# ── Cash-Rich Premium ──────────────────────────────────────────────────────

def _cash_rich_premium(info, fx_rate, market_cap, base_fcf=None,
                        sector=None, industry=None):
    """
    Cash-rich detection — three independent signals, take the maximum.

    Signal A — NET CASH (existing): rewards companies with positive
        net cash position; scaled to market cap.
    Signal B — ABSOLUTE CASH HOARD (new): rewards mega-caps with $50B+
        cash piles regardless of debt.  Captures AAPL/MSFT/GOOGL strategic
        optionality that simple net-cash misses.
    Signal C — FCF-COVERED DEBT (new): if FCF can clear all debt in
        less than 2 years, the debt is "strategic" rather than distress.
        We treat gross cash as effective net cash for premium calc.

    Final premium = max(A, B, C), capped at +10%.

    EXCLUSIONS: Financials (banks/insurance/asset-managers/broker-dealers).
    Their balance-sheet "cash" includes deposits, float, and AUM-related
    balances that are NOT strategic optionality.

    Returns (premium_pct, is_cash_rich, cash_pct_of_mcap, narrative).
    """
    # Exclude depository banks, asset managers, brokerages, and capital
    # markets — their balance-sheet cash includes deposits / float / AUM,
    # not strategic optionality.  We DO include conglomerate holding
    # companies (Berkshire-class) where the cash is genuine optionality.
    s_lower = (sector or "").lower()
    i_lower = (industry or "").lower()
    _exclude_industries = (
        "bank",                # any bank (regional, money center, etc.)
        "thrift",
        "credit services",
        "savings",
        "asset management",
        "capital markets",
        "investment brokerage",
        "securities brokerage",
        "mortgage finance",
    )
    if any(x in i_lower for x in _exclude_industries):
        return 0.0, False, None, None

    total_cash = (safe(info.get("totalCash"), 0) or 0) * fx_rate
    total_debt = (safe(info.get("totalDebt"), 0) or 0) * fx_rate
    net_cash   = total_cash - total_debt
    fcf        = float(base_fcf) if base_fcf else 0.0

    if not market_cap or market_cap <= 0:
        return 0.0, False, None, None

    cash_b_abs = total_cash / 1e9   # gross cash in $B

    # ── Signal A — Net cash as % of market cap ───────────────────────────
    pct_net = (net_cash / market_cap * 100) if net_cash > 0 else 0.0
    if   pct_net >= 35: prem_a = 0.10
    elif pct_net >= 20: prem_a = 0.06
    elif pct_net >= 10: prem_a = 0.03
    else:               prem_a = 0.0

    # ── Signal B — Absolute cash hoard (catches AAPL/MSFT/GOOGL/BRK) ─────
    if   cash_b_abs >= 200: prem_b = 0.06
    elif cash_b_abs >= 100: prem_b = 0.04
    elif cash_b_abs >=  50: prem_b = 0.02
    else:                   prem_b = 0.0

    # ── Signal C — FCF coverage of debt (strategic vs distressed leverage) ─
    prem_c = 0.0
    years_to_payoff = None
    if fcf > 0 and total_debt > 0:
        years_to_payoff = total_debt / fcf
        if years_to_payoff < 2.0:
            # Treat gross cash as effective net cash since debt is non-threat
            pct_c = (total_cash / market_cap * 100) if market_cap > 0 else 0
            if   pct_c >= 35: prem_c = 0.07
            elif pct_c >= 20: prem_c = 0.05
            elif pct_c >= 10: prem_c = 0.03
            else:             prem_c = 0.0

    premium = max(prem_a, prem_b, prem_c)
    if premium <= 0:
        return 0.0, False, None, None

    is_rich = True
    pct = max(pct_net, (cash_b_abs / (market_cap / 1e9) * 100) if market_cap > 0 else 0)

    # Narrative explains which signal fired
    parts = []
    if prem_a == premium and prem_a > 0:
        parts.append(f"net cash {pct_net:.0f}% of market cap")
    if prem_b == premium and prem_b > 0:
        parts.append(f"${cash_b_abs:.0f}B absolute cash hoard")
    if prem_c == premium and prem_c > 0 and years_to_payoff is not None:
        parts.append(f"FCF clears all debt in {years_to_payoff:.1f} years")
    why = " · ".join(parts) if parts else f"cash position is {pct:.0f}% of market cap"

    narrative = (
        f"Cash-loaded balance sheet — {why}.  Strategic optionality "
        f"(buybacks, M&A, R&D) and recession resilience justify a "
        f"+{int(premium*100)}% IV premium not captured by raw FCF."
    )
    return premium, is_rich, round(pct, 1), narrative


# ── Scenario Coherence Enforcer ─────────────────────────────────────────────

def _enforce_scenario_coherence(scenarios, base_iv, price):
    """
    Hard-enforce: Bear < Base < Bull with sensible spreads.

    After Reality Reconciliation, scenarios scaled proportionally can produce
    non-monotonic ordering (Bull < Base) or extreme spreads (Bear at $30 when
    Base is $228).  This enforces:

      - Bear ≤ Base × 0.85       (max 15% downside from base)
      - Bear ≥ Base × 0.65       (min 35% downside from base)  ← prevents absurd lows
      - Bull ≥ Base × 1.10       (min 10% upside from base)
      - Bull ≤ Base × 1.50       (max 50% upside from base)
      - Strict: Bear < Base < Bull, always.

    If the model's natural numbers fall in the band, keep them.  If they don't,
    clamp into the band.  Bear is allowed to sit above current price now —
    bear means "downside vs fair value", not "downside vs market price".

    Mutates scenarios in place.
    """
    if not scenarios or base_iv is None or base_iv <= 0:
        return scenarios

    base = float(base_iv)
    bear_floor   = round(base * 0.65, 2)
    bear_ceiling = round(base * 0.85, 2)
    bull_floor   = round(base * 1.10, 2)
    bull_ceiling = round(base * 1.50, 2)

    # Set base
    scenarios.setdefault("base", {})
    scenarios["base"]["value"]  = round(base, 2)
    scenarios["base"]["upside"] = round((base - price) / price * 100, 1) if price else None

    # Bear: clamp into [bear_floor, bear_ceiling]
    bear_slot = scenarios.get("bear") or {}
    bear_v    = bear_slot.get("value")
    if bear_v is None or bear_v >= base:
        bear_v = bear_ceiling
    else:
        bear_v = max(min(bear_v, bear_ceiling), bear_floor)
    bear_slot["value"]  = round(bear_v, 2)
    bear_slot["upside"] = round((bear_v - price) / price * 100, 1) if price else None
    scenarios["bear"] = bear_slot

    # Bull: clamp into [bull_floor, bull_ceiling]
    bull_slot = scenarios.get("bull") or {}
    bull_v    = bull_slot.get("value")
    if bull_v is None or bull_v <= base:
        bull_v = bull_floor
    else:
        bull_v = max(min(bull_v, bull_ceiling), bull_floor)
    bull_slot["value"]  = round(bull_v, 2)
    bull_slot["upside"] = round((bull_v - price) / price * 100, 1) if price else None
    scenarios["bull"] = bull_slot

    # Recompute weighted using the same weights already in scenarios
    wb = (scenarios.get("base") or {}).get("weight", 60) / 100
    wu = (scenarios.get("bull") or {}).get("weight", 20) / 100
    wd = (scenarios.get("bear") or {}).get("weight", 20) / 100
    weighted = round(wb * base + wu * bull_v + wd * bear_v, 2)
    scenarios["weighted"]        = weighted
    scenarios["weighted_upside"] = round((weighted - price) / price * 100, 1) if price else None
    scenarios["coherence_enforced"] = True
    return scenarios


def _reality_reconciliation(iv, price, analyst_target, implied_g, sector, industry,
                             is_structural_transformer=False, moat_detected=False):
    """
    Reality Reconciliation Layer — when the model is materially off from market
    price AND the sell-side consensus aligns with market (not the model), the
    model is likely missing something the simple DCF cannot capture: growth
    optionality, contract pipelines, network effects, AI / robotics platform
    value, brand premiums, etc.

    Decision tree:
      1. Significant gap?  |IV − price| / price > 0.25  (else: skip, model is fine)
      2. Analyst alignment? analyst target on the price side, not the model side
      3. Credibility check: implied growth from reverse-DCF must be ≤ sector
         ceiling × 1.2.  If yes, blend.  If no, the gap is speculative — leave
         model alone but flag it.

    Blend formula (conservative — model still dominates):
        new_IV = 0.55 × model_IV + 0.25 × analyst_target + 0.20 × current_price

    Sector ceilings (max sustainable Stage-1 growth):
        Structural transformer:        50%
        High-growth tech / fintech:    40%
        Communications / SaaS:         35%
        Healthcare / Biotech:          35%
        Default:                       25%
        Banking / Insurance:           20%
        Energy / Materials:            15%

    Returns (reconciled_iv, was_reconciled, pre_iv, reason_string).
    """
    if iv is None or not price or price <= 0:
        return iv, False, iv, None

    pre_iv = round(float(iv), 2)
    gap_pct = (iv - price) / price

    # Skip when model and market are reasonably close
    if abs(gap_pct) <= 0.25:
        return iv, False, pre_iv, None

    # Skip when no analyst target available — can't validate the gap
    if not analyst_target or float(analyst_target) <= 0:
        return iv, False, pre_iv, None
    at = float(analyst_target)

    # Direction: is the model below price (undervaluing) or above (overvaluing)?
    model_below = iv < price
    # Analyst alignment: analyst must side with PRICE, not with model.
    # (Model below price → analyst should be near or above price.)
    # (Model above price → analyst should be near or below price.)
    if model_below:
        analyst_aligned_with_market = at >= price * 0.92
    else:
        analyst_aligned_with_market = at <= price * 1.08

    if not analyst_aligned_with_market:
        # Analysts agree with our model — the divergence is genuine, don't blend
        return iv, False, pre_iv, "Analyst consensus agrees with VALUS model — divergence trusted"

    # Sector growth ceiling
    s_lower   = (sector   or "").lower()
    ind_lower = (industry or "").lower()

    if is_structural_transformer:
        ceiling = 0.50
        ceiling_label = "Structural Transformer (50%)"
    elif moat_detected and ("technology" in s_lower or "communication" in s_lower):
        ceiling = 0.45
        ceiling_label = "Moat-Tech (45%)"
    elif "technology" in s_lower or "communication" in s_lower:
        ceiling = 0.40
        ceiling_label = "Tech / Comms (40%)"
    elif any(x in ind_lower for x in [
            "capital markets", "securities brokerage", "consumer lending",
            "online lending", "fintech", "financial exchanges"]):
        ceiling = 0.40
        ceiling_label = "Growth Fintech (40%)"
    elif "healthcare" in s_lower or "biotech" in ind_lower:
        ceiling = 0.35
        ceiling_label = "Healthcare (35%)"
    elif "financial" in s_lower or "bank" in ind_lower or "insurance" in ind_lower:
        ceiling = 0.20
        ceiling_label = "Banking / Insurance (20%)"
    elif "energy" in s_lower or "materials" in s_lower or "mining" in ind_lower:
        ceiling = 0.15
        ceiling_label = "Energy / Materials (15%)"
    else:
        ceiling = 0.25
        ceiling_label = "Default (25%)"

    # Credibility check: only reconcile if implied growth is within ceiling × 1.2
    if implied_g is not None:
        if implied_g > ceiling * 1.2:
            # Market is pricing in growth that exceeds the credible sector ceiling.
            # Don't blend — flag the speculation but trust the model.
            return (iv, False, pre_iv,
                    f"Market implies {implied_g*100:.1f}% growth — exceeds {ceiling_label} "
                    f"ceiling × 1.2; speculative gap left unblended")
        if implied_g < -0.05:
            # Market pricing in contraction below -5% — distress signal, trust model
            return (iv, False, pre_iv,
                    f"Market implies {implied_g*100:.1f}% (contraction) — distress signal, "
                    f"model trusted")

    # ── Reconcile: blend model 55%, analyst 25%, market 20% ──────────────────
    new_iv = round(0.55 * iv + 0.25 * at + 0.20 * price, 2)
    direction = "upward" if new_iv > iv else "downward"
    pct_change = abs(new_iv - iv) / max(iv, 0.01) * 100
    reason = (
        f"Reality Reconciliation ({direction}, {pct_change:.1f}%): model gap "
        f"{abs(gap_pct)*100:.0f}% with credible implied growth "
        f"{(implied_g or 0)*100:.1f}% ≤ {ceiling_label} ceiling. "
        f"Blend: 55% model · 25% analyst · 20% market."
    )
    return new_iv, True, pre_iv, reason


def build_expectation_gap(implied_g, model_g, price, model_iv_pre_sultan,
                           analyst_growth, sector, yrs=10):
    """
    Given implied and model growth rates, produce the full Expectation Gap
    payload: narrative sentences, disagreement score (0–10), and flags.

    implied_g           — rate market is pricing in (from reverse DCF)
    model_g             — VALUS Stage-1 forecast (s1)
    price               — current market price
    model_iv_pre_sultan — VALUS IV before Sultan Split (consensus_anchor_pre_iv)
    analyst_growth      — sell-side consensus growth (or None)
    sector              — for industry benchmark comparisons
    """
    if implied_g is None or model_g is None or model_g <= 0:
        return None

    gap_pp   = round((implied_g - model_g) * 100, 1)      # percentage points
    gap_pct  = round((implied_g - model_g) / model_g * 100, 1)  # % of model

    ig_pct   = round(implied_g * 100, 1)
    mg_pct   = round(model_g   * 100, 1)

    # Market disagreement vs model IV
    model_disagrees_pct = None
    if model_iv_pre_sultan and price and price > 0:
        model_disagrees_pct = round((model_iv_pre_sultan - price) / price * 100, 1)

    # ── Expectation Gap Score (0–10) ────────────────────────────────────────
    # 0 = market and model fully agree | 10 = massive disagreement
    raw_score = min(abs(gap_pp) / 3.0, 10.0)   # every 3pp of gap = 1 point
    score = round(raw_score, 1)

    # ── Narrative sentences ─────────────────────────────────────────────────
    direction = "above" if implied_g > model_g else "below"
    market_view = "overvalued" if model_iv_pre_sultan and model_iv_pre_sultan < price else "undervalued"

    primary = (
        f"To justify today's price, revenue must grow ~{ig_pct}% for {yrs} years. "
        f"Our model forecasts {mg_pct}% — a {abs(gap_pp)}pp expectation gap."
    )

    if gap_pp > 0:
        verdict = (
            f"The market is pricing in {abs(gap_pp)}pp more growth than VALUS forecasts. "
            f"Our model sees this stock as {market_view} by {abs(model_disagrees_pct or 0):.0f}%."
        )
    else:
        verdict = (
            f"The market demands {abs(gap_pp)}pp less growth than VALUS forecasts — "
            f"our model sees significant upside the market has not priced in."
        )

    # ── Unrealistic assumption flags ────────────────────────────────────────
    flags = []

    if implied_g > 0.50:
        flags.append(f"Market implies {ig_pct}% sustained growth — fewer than 1% of public companies have achieved this over 10 years.")

    elif implied_g > 0.35:
        flags.append(f"Market implies {ig_pct}% growth — historically achieved only by hyper-scalers in early expansion phases.")

    elif implied_g > 0.25:
        flags.append(f"Market implies {ig_pct}% growth — above the top-decile long-run growth rate for S&P 500 companies.")

    if analyst_growth and implied_g > analyst_growth * 2.0:
        cons_pct = round(analyst_growth * 100, 1)
        flags.append(f"Market prices in {ig_pct}% growth vs sell-side consensus of {cons_pct}% — a 2× premium above analyst expectations.")

    if implied_g < 0:
        flags.append("Market is pricing in negative growth — the stock is discounting a contraction scenario.")

    if abs(gap_pp) < 5 and score < 2:
        flags.append("Market and model are broadly aligned — no major expectation mismatch detected.")

    return {
        "implied_growth_pct":    ig_pct,
        "model_growth_pct":      mg_pct,
        "gap_pp":                gap_pp,
        "gap_pct":               gap_pct,
        "score":                 score,               # 0–10
        "primary_narrative":     primary,
        "verdict_narrative":     verdict,
        "flags":                 flags,
        "market_disagrees_pct":  model_disagrees_pct,
        "market_view":           market_view,
    }


# ── FIN 415 FCFE Model ─────────────────────────────────────────────────────────
# Implements the exact bottom-up FCFE formula from the FIN 415 DCF Equity Analyst
# Template (Spring 2026). Discount rate = Cost of Equity (Ke), not WACC.
# No net-debt bridge needed — FCFE is already an equity cash flow.

def run_fin415_fcfe(
    revenue_base, cogs_base, fixed_costs_base, da_base, amort_base,
    ppe_base, ltd_base, interest_rate, tax_rate, shares,
    rev_growths,      # list[float] length=yrs — one rate per year
    cogs_growth,      # float — applied uniformly (COGS/rev margin management)
    fc_growth,        # float — operating leverage: fixed costs grow slower than rev
    da_growth,        # float — D&A tracks capex intensity
    amort_growth,     # float — amortization (usually slow/flat)
    ppe_growth,       # float — net PPE asset base growth
    ltd_growth,       # float — LTD growth (negative = debt paydown)
    ke,               # float — cost of equity (CAPM discount rate)
    tgr,              # float — terminal growth rate
    yrs=10,
):
    """
    FIN 415 bottom-up FCFE DCF — exact cell-by-cell logic from the Excel NPV tab.

    FCFE[y] = EBIT − NetCapex + D&A + Amort − Interest×(1−Tax) + ΔDebt
    TV      = FCFE[10] × (1+TGR) / (Ke − TGR) / (1+Ke)^10
    NPV     = Σ PV(FCFE) + TV
    Price   = NPV / Shares

    Returns (fair_price, fcfe_rows, pv_terminal, npv, op_margins_by_year).
    """
    if ke <= tgr:
        tgr = ke - 0.005

    rev   = revenue_base
    cogs  = cogs_base
    fc    = fixed_costs_base
    da    = da_base
    amort = amort_base
    ppe   = ppe_base
    ltd   = ltd_base

    fcfe_rows = []
    total_pv  = 0.0

    for y in range(1, yrs + 1):
        g_rev = rev_growths[y - 1]

        # Income statement projections
        rev   = rev   * (1 + g_rev)
        cogs  = cogs  * (1 + cogs_growth)
        fc    = fc    * (1 + fc_growth)
        da    = da    * (1 + da_growth)
        amort = amort * (1 + amort_growth)
        ebit  = rev - cogs - fc - da

        # Debt / interest
        interest   = ltd * interest_rate          # interest on beginning-of-year LTD
        taxes      = max((ebit - interest) * tax_rate, 0.0)

        # CapEx (net fixed asset change)
        beg_ppe   = ppe
        ppe       = ppe * (1 + ppe_growth)
        net_capex = (ppe - beg_ppe) + da          # mirrors Excel: EndPPE − BegPPE + DA

        # Debt change
        beg_ltd    = ltd
        ltd        = ltd * (1 + ltd_growth)
        delta_debt = ltd - beg_ltd                # positive = new borrowing

        # FCFE — FIN 415 NPV tab formula (row 25)
        fcfe = (ebit
                - net_capex
                + da
                + amort
                - interest * (1 - tax_rate)
                + delta_debt)

        pv        = fcfe / ((1 + ke) ** y)
        total_pv += pv

        fcfe_rows.append({
            "year":      y,
            "revenue":   round(rev, 0),
            "ebit":      round(ebit, 0),
            "fcfe":      round(fcfe, 0),
            "pv":        round(pv, 0),
            "op_margin": round(ebit / rev * 100, 2) if rev > 0 else 0.0,
        })

    # Terminal value — Gordon Growth on Year-N FCFE, discounted back N years
    tv_fcfe      = fcfe_rows[-1]["fcfe"] * (1 + tgr)
    terminal_val = tv_fcfe / (ke - tgr)
    pv_terminal  = terminal_val / ((1 + ke) ** yrs)

    npv        = total_pv + pv_terminal
    fair_price = npv / shares if shares > 0 else None
    op_margins = [r["op_margin"] for r in fcfe_rows]

    return fair_price, fcfe_rows, pv_terminal, npv, op_margins


def _extract_fin415_inputs(info, income_stmt, cashflow, balance_sheet, fx_rate):
    """
    Pull base-year (TTM / most-recent annual) line items needed by run_fin415_fcfe().
    Applies fallback heuristics when individual items are unavailable.
    Returns a dict, or None if critical base data is missing.
    """
    def _get(stmt, *labels):
        """Read first matching label from a DataFrame index; returns abs value × fx."""
        if stmt is None or stmt.empty:
            return None
        for lbl in labels:
            if lbl in stmt.index:
                v = safe(stmt.loc[lbl].iloc[0])
                if v is not None:
                    return abs(float(v)) * fx_rate
        return None

    # ── Revenue and COGS ──────────────────────────────────────────────────────
    revenue = (safe(info.get("totalRevenue"), 0) or 0) * fx_rate
    cogs    = (safe(info.get("costOfRevenue"), 0) or 0) * fx_rate
    if not revenue or revenue <= 0:
        return None

    # ── Fixed (operating) costs = total opex minus COGS ─────────────────────
    total_opex  = _get(income_stmt, "Total Expenses", "Operating Expense",
                       "Total Operating Expenses") or 0
    fixed_costs = max(total_opex - cogs, revenue * 0.10)   # floor 10% of revenue

    # ── D&A and amortisation ──────────────────────────────────────────────────
    da    = (_get(cashflow, "Depreciation And Amortization", "Depreciation",
                  "Depreciation Depletion And Amortization")
             or revenue * 0.04)
    amort = (_get(cashflow, "Amortization Of Intangibles", "Amortization")
             or 0.0)

    # ── Net PPE ───────────────────────────────────────────────────────────────
    ppe = (_get(balance_sheet, "Net PPE", "Net Property Plant And Equipment",
                "Property Plant And Equipment Net",
                "Properties")
           or revenue * 0.25)

    # ── Long-term debt ────────────────────────────────────────────────────────
    ltd = (safe(info.get("longTermDebt") or info.get("totalDebt"), 0) or 0) * fx_rate

    # ── Interest rate on LTD ─────────────────────────────────────────────────
    iexp          = (_get(income_stmt, "Interest Expense Non Operating",
                          "Interest Expense") or 0.0)
    interest_rate = (iexp / ltd) if ltd > 0 else 0.05
    interest_rate = min(max(interest_rate, 0.01), 0.15)

    # ── Effective tax rate ────────────────────────────────────────────────────
    tax_raw  = safe(info.get("effectiveTaxRate"))
    tax_rate = float(min(max(tax_raw, 0.10), 0.35)) if tax_raw else 0.21

    # ── Shares outstanding ────────────────────────────────────────────────────
    shares = safe(info.get("sharesOutstanding")
                  or info.get("impliedSharesOutstanding"), 0) or 0
    if shares <= 0:
        return None

    return {
        "revenue":      revenue,
        "cogs":         cogs,
        "fixed_costs":  fixed_costs,
        "da":           da,
        "amort":        amort,
        "ppe":          ppe,
        "ltd":          ltd,
        "interest_rate": interest_rate,
        "tax_rate":     tax_rate,
        "shares":       shares,
    }


# ── Bank-appropriate DCF (Net Income → FCFE) ──────────────────────────────────

def run_banking_fcfe(info, fx_rate, ke, tg, yrs=10, growth=None):
    """
    Bank-appropriate Dividend-Adjusted FCFE.

    Banks have no meaningful CapEx or working-capital cycle — their economics
    are: Net Income → retained earnings (regulatory capital) → dividends to
    equity.  For an equity-DCF, FCFE collapses to Net Income (the cash
    available to equity holders, since regulatory reinvestment is a
    book-equity entry, not an actual cash drain).

    Compounds NI over a linear taper from g₁ to TG over `yrs` years, then
    Gordon-Growth terminal value, all discounted at Cost of Equity Ke.

    Returns (fair_price, npv) or (None, None) on missing data.
    """
    if ke is None or tg is None or ke <= tg:
        if ke is not None and tg is not None:
            tg = ke - 0.005
        else:
            return None, None

    ni = safe(info.get("netIncomeToCommon")) or safe(info.get("netIncome"))
    shares = safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"))
    if not (ni and ni > 0 and shares and shares > 0):
        return None, None

    g1 = growth if growth is not None else (
        safe(info.get("earningsGrowth")) or
        safe(info.get("revenueGrowth")) or 0.06
    )
    g1 = min(max(float(g1), 0.02), 0.20)   # banking earnings growth: 2–20%

    ni_curr  = float(ni) * fx_rate
    pv_total = 0.0
    last_fcfe = ni_curr

    for y in range(1, yrs + 1):
        # Linear taper from g₁ → tg
        gy = g1 + (tg - g1) * (y - 1) / max(yrs - 1, 1)
        ni_curr  = ni_curr * (1 + gy)
        last_fcfe = ni_curr
        pv_total += ni_curr / ((1 + ke) ** y)

    # Gordon Growth terminal value on year-N FCFE
    tv     = last_fcfe * (1 + tg) / (ke - tg)
    pv_tv  = tv / ((1 + ke) ** yrs)
    npv    = pv_total + pv_tv
    fair_price = npv / float(shares)
    return round(max(fair_price, 0.0), 2), npv


# ── Single DCF run ─────────────────────────────────────────────────────────────

def run_dcf_single(base_fcf, s1, s2, tg, wacc, yrs, info, fx_rate,
                   net_debt_override=None, stage1_years=None):
    """
    Run one DCF scenario. base_fcf must already be in trading currency.
    net_debt_override: pass pre-computed net debt (e.g. from quarterly balance sheet)
    to bypass the info-dict lookup — important for mega-caps where cash is material.
    stage1_years: how many years to use s1 growth (default: yrs // 2).
      Pass yrs to keep Stage 1 growth for the full projection horizon (Backbone moat).
    Returns (intrinsic_value, projected_rows, enterprise_value, equity_value, pv_terminal).
    """
    if wacc <= tg:
        tg = wacc - 0.01
    s1_cutoff = stage1_years if stage1_years is not None else (yrs // 2)
    fcf_run = base_fcf
    projected = []
    for y in range(1, yrs + 1):
        g = s1 if y <= s1_cutoff else s2
        fcf_run = fcf_run * (1 + g)
        pv = fcf_run / ((1 + wacc) ** y)
        projected.append({"year": y, "fcf": fcf_run, "pv": pv, "growth": g})

    tv_fcf = projected[-1]["fcf"] * (1 + tg)
    terminal_val = tv_fcf / (wacc - tg)
    pv_terminal  = terminal_val / ((1 + wacc) ** yrs)
    total_pv_fcf = sum(p["pv"] for p in projected)
    ev           = total_pv_fcf + pv_terminal

    if net_debt_override is not None:
        net_debt = net_debt_override
    else:
        cash     = (safe(info.get("totalCash"), 0) or 0) * fx_rate
        debt     = (safe(info.get("totalDebt"), 0) or 0) * fx_rate
        net_debt = debt - cash
    eq_val = ev - net_debt

    shares = safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0) or 0
    iv     = (eq_val / shares) if shares > 0 else None
    return iv, projected, ev, eq_val, pv_terminal


# ── Free Cash Flow ─────────────────────────────────────────────────────────────

def get_fcf_series(cashflow):
    """Annual FCF series from cashflow statement, newest-first."""
    if cashflow is None or cashflow.empty:
        return []
    try:
        if "Free Cash Flow" in cashflow.index:
            vals = cashflow.loc["Free Cash Flow"].values
        else:
            ok  = next((k for k in cashflow.index if "Operating Cash Flow" in k or
                       ("Continuing Operating" in k and "Cash" in k)), None)
            ck  = next((k for k in cashflow.index
                        if "Capital Expenditure" in k or ("Purchase" in k and "PPE" in k)), None)
            if not ok:
                return []
            ocf  = cashflow.loc[ok].values
            capx = cashflow.loc[ck].values if ck else np.zeros(len(ocf))
            vals = ocf + capx
        return [float(v) for v in vals if v is not None and not np.isnan(float(v))]
    except Exception:
        return []


def get_income_stmt_ttm(stock):
    """
    Compute key income statement metrics from quarterly data (TTM).
    For foreign companies, quarterly TTM is more current than annual filings.
    Returns dict with revenue, cogs, gross_profit, operating_income, net_income (all TTM).
    """
    result = {}
    try:
        q = stock.quarterly_income_stmt
        if q is None or q.empty or len(q.columns) < 4:
            return result
        cols = q.columns[:4]
        for key in ["Total Revenue", "Cost Of Revenue", "Gross Profit",
                    "Operating Income", "Net Income", "EBITDA"]:
            if key in q.index:
                vals = [safe(q.loc[key, c]) for c in cols]
                if all(v is not None for v in vals):
                    result[key] = sum(vals)
    except Exception:
        pass
    return result


def get_base_fcf(info, stock):
    """
    Best available FCF to use as DCF base:
    1. Yahoo Finance TTM FCF from info (most up-to-date)
    2. TTM computed from last 4 quarters
    3. Most recent annual FCF
    """
    # Yahoo Finance computes its own TTM FCF
    ttm = safe(info.get("freeCashflow"))
    if ttm and ttm > 0:
        return ttm, "ttm_yahoo"

    # Compute TTM from quarterly cashflow
    try:
        q = stock.quarterly_cashflow
        if q is not None and not q.empty and len(q.columns) >= 4:
            cols = q.columns[:4]
            fcf_key = "Free Cash Flow" if "Free Cash Flow" in q.index else None
            ocf_key = next((k for k in q.index if "Operating Cash Flow" in k or
                           "Continuing Operating" in k), None) if not fcf_key else None
            cpx_key = next((k for k in q.index if "Capital Expenditure" in k or
                           ("Purchase" in k and "PPE" in k)), None)
            if fcf_key:
                vals = [safe(q.loc[fcf_key, c]) for c in cols]
                if all(v is not None for v in vals):
                    ttm_q = sum(vals)
                    if ttm_q > 0:
                        return ttm_q, "ttm_quarterly"
            elif ocf_key:
                ocf_sum  = sum(safe(q.loc[ocf_key, c], 0) for c in cols)
                capx_sum = sum(safe(q.loc[cpx_key, c], 0) for c in cols) if cpx_key else 0
                ttm_q = ocf_sum + capx_sum
                if ttm_q > 0:
                    return ttm_q, "ttm_quarterly"
    except Exception:
        pass

    # Fall back to most recent annual
    series = get_fcf_series(stock.cashflow)
    if series and series[0] > 0:
        return series[0], "annual"

    return None, None


# ── Growth rate estimation ─────────────────────────────────────────────────────

# ── Blended Growth Calculator ─────────────────────────────────────────────────
# Combines historical revenue/earnings/FCF, next-2y analyst estimates, and
# Finviz 5-year EPS estimate into Stage-1 / Stage-2 / terminal growth rates.
# Falls back to industry-level proxy when data is sparse or volatile.
# Terminal growth is soft-capped at 6% (industry permitting), hard-capped at
# 8%, and never allowed to exceed Stage-2 minus 0.5pp.

_FINVIZ_GROWTH_CACHE = {}        # {ticker: (timestamp, growth_or_none)}
_FINVIZ_GROWTH_CACHE_TTL = 6 * 3600   # 6h — Finviz updates daily at most

# Long-history income statement cache. Sources data from SEC EDGAR's
# companyfacts API — 25+ years of annual revenue/net income, free, no key.
# Two-tier cache: ticker→CIK map (refreshed weekly), per-ticker facts (24h).
_LONG_INCOME_CACHE = {}          # {ticker: (timestamp, {"revenue":[...], "net_income":[...]})}
_LONG_INCOME_CACHE_TTL = 24 * 3600
_SEC_TICKER_MAP = None           # {ticker: cik_int}
_SEC_TICKER_MAP_TS = 0
_SEC_TICKER_MAP_TTL = 7 * 86400  # weekly

# SEC requires a User-Agent identifying the requester (name + contact).
# Override with SEC_USER_AGENT env var in prod.
_SEC_UA = os.environ.get(
    "SEC_USER_AGENT",
    "VALUS Research contact@valusfinancial.com",
)


def _sec_get_ticker_cik_map():
    """One-time fetch of SEC's ticker→CIK index (~10k tickers, ~1MB JSON)."""
    global _SEC_TICKER_MAP, _SEC_TICKER_MAP_TS
    if _SEC_TICKER_MAP and (time.time() - _SEC_TICKER_MAP_TS) < _SEC_TICKER_MAP_TTL:
        return _SEC_TICKER_MAP
    try:
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": _SEC_UA},
            timeout=10,
        )
        if r.status_code == 200:
            j = r.json() or {}
            _SEC_TICKER_MAP = {
                v["ticker"].upper(): int(v["cik_str"])
                for v in j.values() if v.get("ticker") and v.get("cik_str")
            }
            _SEC_TICKER_MAP_TS = time.time()
    except Exception:
        pass
    return _SEC_TICKER_MAP or {}


# us-gaap concept variants companies use for revenue and net income.
# Different filers use different tags depending on industry / era; we merge
# results across all of them by fiscal year.
_SEC_REVENUE_CONCEPTS = (
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
)
_SEC_NETINCOME_CONCEPTS = (
    "NetIncomeLoss",
    "ProfitLoss",
)


def _get_long_income_history(ticker):
    """
    25-year annual revenue & net-income history via SEC EDGAR companyfacts.
    Returns {"revenue":[most-recent-first floats], "net_income":[...]} or
    None when the ticker isn't a US filer or the API call fails. Last 10
    years are returned.
    """
    if not ticker:
        return None
    t = ticker.upper().strip()
    cached = _LONG_INCOME_CACHE.get(t)
    if cached and (time.time() - cached[0]) < _LONG_INCOME_CACHE_TTL:
        return cached[1]
    result = None
    try:
        cik_map = _sec_get_ticker_cik_map()
        cik = cik_map.get(t)
        if cik:
            cik_padded = f"{cik:010d}"
            r = requests.get(
                f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json",
                headers={"User-Agent": _SEC_UA},
                timeout=10,
            )
            if r.status_code == 200:
                gaap = (r.json() or {}).get("facts", {}).get("us-gaap", {})

                def _collect(concepts):
                    """Merge annual (10-K, FP=FY) values across concept variants."""
                    by_year = {}
                    for c in concepts:
                        usd = gaap.get(c, {}).get("units", {}).get("USD", [])
                        for x in usd:
                            if x.get("form") == "10-K" and x.get("fp") == "FY":
                                year = x.get("end", "")[:4]
                                val  = x.get("val")
                                if year and val is not None:
                                    # Several filings can refer to the same FY
                                    # (e.g. amended). Keep the largest absolute
                                    # value — usually the original full-year.
                                    prev = by_year.get(year)
                                    if prev is None or abs(val) > abs(prev):
                                        by_year[year] = val
                    return by_year

                rev_by_year = _collect(_SEC_REVENUE_CONCEPTS)
                ni_by_year  = _collect(_SEC_NETINCOME_CONCEPTS)

                # Sort newest-first, take last 10 years.
                rev = [rev_by_year[y] for y in sorted(rev_by_year, reverse=True)][:10]
                ni  = [ni_by_year[y]  for y in sorted(ni_by_year,  reverse=True)][:10]

                if rev or ni:
                    result = {"revenue": rev, "net_income": ni}
    except Exception:
        result = None
    _LONG_INCOME_CACHE[t] = (time.time(), result)
    return result


# ── Historical Valuation (Phase A) ────────────────────────────────────────────
# Point-in-time DCF replay: at each annual cutoff over the last 5 years, fetch
# the financials that were *publicly known on that date* via EDGAR companyfacts,
# then run the existing DCF engine with deterministic growth assumptions
# derived solely from data available at that point in history.  Combined with a
# monthly close-price line, this powers the Valuation History chart that
# competitors (alphaspread.com, simplywall.st) ship as a paid feature.
#
# Caching strategy: per-ticker companyfacts JSON cached 24h; computed history
# points cached 12h (refreshes when a new 10-K filing lands).
_SEC_COMPANYFACTS_CACHE = {}      # {ticker: (timestamp, facts_dict)}
_SEC_COMPANYFACTS_CACHE_TTL = 24 * 3600
_VALUATION_HISTORY_CACHE = {}     # {ticker: (timestamp, payload)}
_VALUATION_HISTORY_CACHE_TTL = 12 * 3600


def _sec_companyfacts(ticker):
    """Raw EDGAR companyfacts us-gaap dict for a ticker, cached 24h. None on failure."""
    if not ticker:
        return None
    t = ticker.upper().strip()
    cached = _SEC_COMPANYFACTS_CACHE.get(t)
    if cached and (time.time() - cached[0]) < _SEC_COMPANYFACTS_CACHE_TTL:
        return cached[1]
    facts = None
    try:
        cik_map = _sec_get_ticker_cik_map()
        cik = cik_map.get(t)
        if cik:
            r = requests.get(
                f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json",
                headers={"User-Agent": _SEC_UA},
                timeout=10,
            )
            if r.status_code == 200:
                facts = (r.json() or {}).get("facts", {}).get("us-gaap", {})
    except Exception:
        facts = None
    _SEC_COMPANYFACTS_CACHE[t] = (time.time(), facts)
    return facts


def _sec_dei_facts(ticker):
    """DEI namespace (entity-level data, e.g. shares outstanding)."""
    if not ticker:
        return None
    t = ticker.upper().strip()
    try:
        cik_map = _sec_get_ticker_cik_map()
        cik = cik_map.get(t)
        if not cik:
            return None
        r = requests.get(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json",
            headers={"User-Agent": _SEC_UA},
            timeout=10,
        )
        if r.status_code == 200:
            return (r.json() or {}).get("facts", {}).get("dei", {})
    except Exception:
        pass
    return None


def _pit_latest_value(facts, concepts, asof_iso, units=("USD",), forms=None):
    """
    Most-recent value for any of `concepts`, restricted to records where
    `filed <= asof_iso` and form ∈ forms (if provided). Picks the record with
    the latest `end` date (the most recent reporting period known by asof).
    Returns float or None.
    """
    if not facts:
        return None
    best_end = None
    best_val = None
    for c in concepts:
        for unit_key in units:
            recs = (facts.get(c, {}).get("units", {}).get(unit_key, []))
            for x in recs:
                filed = x.get("filed", "")
                if not filed or filed > asof_iso:
                    continue
                if forms and x.get("form") not in forms:
                    continue
                end = x.get("end")
                val = x.get("val")
                if not end or val is None:
                    continue
                if best_end is None or end > best_end:
                    best_end = end
                    best_val = val
    return best_val


def _pit_annual_series(facts, concepts, asof_iso):
    """
    Annual (FY) values keyed by fiscal year, restricted to filings made by
    asof_iso. Used to compute trailing CAGRs at a given point in history.
    Returns {YYYY: value} dict.
    """
    if not facts:
        return {}
    by_year = {}
    for c in concepts:
        for x in facts.get(c, {}).get("units", {}).get("USD", []):
            if x.get("form") != "10-K" or x.get("fp") != "FY":
                continue
            if x.get("filed", "") > asof_iso:
                continue
            year = (x.get("end") or "")[:4]
            val  = x.get("val")
            if year and val is not None:
                prev = by_year.get(year)
                if prev is None or abs(val) > abs(prev):
                    by_year[year] = val
    return by_year


_FCF_OCF_CONCEPTS = (
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
)
_FCF_CAPEX_CONCEPTS = (
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets",
)
_DEBT_CONCEPTS = (
    "LongTermDebt",
    "LongTermDebtNoncurrent",
)
_SHORT_DEBT_CONCEPTS = (
    "LongTermDebtCurrent",
    "DebtCurrent",
)
_CASH_CONCEPTS = (
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
)
_SHARES_CONCEPTS_DEI = (
    "EntityCommonStockSharesOutstanding",
)
_SHARES_CONCEPTS_GAAP = (
    "CommonStockSharesOutstanding",
    "WeightedAverageNumberOfDilutedSharesOutstanding",
)


def _pit_shares(facts_gaap, facts_dei, asof_iso):
    """Most recent shares-outstanding value as-of asof_iso (DEI preferred, GAAP fallback)."""
    if facts_dei:
        v = _pit_latest_value(facts_dei, _SHARES_CONCEPTS_DEI, asof_iso, units=("shares",))
        if v:
            return v
    if facts_gaap:
        v = _pit_latest_value(facts_gaap, _SHARES_CONCEPTS_GAAP, asof_iso, units=("shares",))
        if v:
            return v
    return None


def _trailing_cagr_at(annual_dict, asof_year, lookback=3):
    """CAGR of last `lookback` available FYs ≤ asof_year. None if too few points."""
    years = sorted([int(y) for y in annual_dict if int(y) <= asof_year], reverse=True)
    vals  = [annual_dict[str(y)] for y in years[:lookback + 1]
             if annual_dict.get(str(y)) and annual_dict[str(y)] > 0]
    if len(vals) < 3:
        return None
    n = len(vals) - 1
    try:
        return (vals[0] / vals[-1]) ** (1 / n) - 1
    except Exception:
        return None


def _get_monthly_price_history(ticker, years=5):
    """Monthly close prices for the last `years` years. Returns [{date, price}] list."""
    try:
        h = yf.Ticker(ticker).history(period=f"{years}y", interval="1mo", auto_adjust=False)
        if h is None or h.empty:
            return []
        out = []
        for idx, row in h.iterrows():
            close = row.get("Close")
            if close is None or pd.isna(close):
                continue
            out.append({
                "date":  idx.date().isoformat(),
                "price": round(float(close), 2),
            })
        return out
    except Exception:
        return []


def get_valuation_history(ticker, info, fx_rate=1.0, sector=None, industry=None):
    """
    Build a 5-year valuation history for the chart: at each annual cutoff,
    replay DCF using only data publicly known at that date. Combined with
    monthly close prices.

    Uses deterministic growth assumptions (point-in-time trailing CAGR with
    sector guardrails) — no Claude calls, no analyst estimates of the future.
    Cached 12h per ticker.

    Returns dict:
      {
        "ticker": str,
        "currency": "USD",
        "iv_points":   [{date, iv, fcf, growth_used, cagr_at_date}],
        "price_points":[{date, price}],
        "method": "edgar_pit_dcf_v1",
      }
    or None when EDGAR data isn't available (non-US filer, etc.).
    """
    if not ticker:
        return None
    t = ticker.upper().strip()
    cached = _VALUATION_HISTORY_CACHE.get(t)
    if cached and (time.time() - cached[0]) < _VALUATION_HISTORY_CACHE_TTL:
        return cached[1]

    facts_gaap = _sec_companyfacts(t)
    if not facts_gaap:
        _VALUATION_HISTORY_CACHE[t] = (time.time(), None)
        return None
    facts_dei  = _sec_dei_facts(t)

    # Resolve sector params for guardrails (default if unknown).
    ind_key = _classify_industry(sector or "", industry or "")
    ind_params = INDUSTRY_PARAMS.get(ind_key, INDUSTRY_PARAMS["default"])

    # Annual revenue series for point-in-time CAGR.
    rev_annual_full = _pit_annual_series(facts_gaap, _SEC_REVENUE_CONCEPTS, "9999-12-31")

    today = date.today()
    iv_points = []

    for years_ago in range(5, -1, -1):
        asof = date(today.year - years_ago, today.month, 1) - timedelta(days=1)
        asof_iso = asof.isoformat()

        # FCF (annual): take latest 10-K OCF and CapEx as of asof.
        ocf = _pit_latest_value(facts_gaap, _FCF_OCF_CONCEPTS, asof_iso, forms=("10-K",))
        if ocf is None or ocf <= 0:
            continue
        capex = _pit_latest_value(facts_gaap, _FCF_CAPEX_CONCEPTS, asof_iso, forms=("10-K",)) or 0
        # CapEx is reported as a positive payment; subtract.
        fcf = ocf - abs(capex)
        if fcf <= 0:
            continue

        # Shares outstanding as-of asof.
        shares = _pit_shares(facts_gaap, facts_dei, asof_iso)
        if not shares or shares <= 0:
            continue

        # Net debt as-of asof (both LTD and short-term where available).
        ltd  = _pit_latest_value(facts_gaap, _DEBT_CONCEPTS, asof_iso) or 0
        std  = _pit_latest_value(facts_gaap, _SHORT_DEBT_CONCEPTS, asof_iso) or 0
        cash = _pit_latest_value(facts_gaap, _CASH_CONCEPTS, asof_iso) or 0
        net_debt = (ltd + std) - cash

        # Growth: point-in-time 3yr revenue CAGR, capped by sector guardrails.
        # Falls back to industry mid-cycle proxy when history is too short.
        cagr = _trailing_cagr_at(
            {y: v for y, v in rev_annual_full.items() if int(y) <= asof.year},
            asof_year=asof.year,
            lookback=3,
        )
        if cagr is None:
            s1 = ind_params["max_s1"] * 0.65
        else:
            s1 = max(0.0, min(cagr, ind_params["max_s1"]))
        s2 = max(min(s1 * 0.55, ind_params["max_s1"] * 0.55), 0.02)
        tg = min(ind_params["max_tg"], max(s2 - 0.005, 0.005))
        wacc = max(ind_params["min_wacc"] + 0.005, tg + ind_params["wacc_spread"])

        # Replay DCF with these point-in-time inputs.
        try:
            iv, _proj, _ev, _eq, _pvt = run_dcf_single(
                base_fcf=fcf,
                s1=s1, s2=s2, tg=tg, wacc=wacc,
                yrs=10,
                info={"sharesOutstanding": shares},
                fx_rate=1.0,                 # USD-denominated EDGAR data
                net_debt_override=net_debt,
            )
        except Exception:
            iv = None

        if iv and iv > 0:
            iv_points.append({
                "date":         asof_iso,
                "iv":           round(float(iv), 2),
                "fcf":          round(float(fcf), 0),
                "growth_used":  round(float(s1) * 100, 2),
                "cagr_at_date": round(float(cagr) * 100, 2) if cagr is not None else None,
            })

    if not iv_points:
        _VALUATION_HISTORY_CACHE[t] = (time.time(), None)
        return None

    price_points = _get_monthly_price_history(t, years=5)

    payload = {
        "ticker":       t,
        "currency":     "USD",
        "iv_points":    iv_points,
        "price_points": price_points,
        "method":       "edgar_pit_dcf_v1",
    }
    _VALUATION_HISTORY_CACHE[t] = (time.time(), payload)
    return payload


def _get_revenue_history(income_stmt):
    """Annual revenue series, most recent first. Up to ~10 years if available."""
    if income_stmt is None or getattr(income_stmt, "empty", True):
        return []
    for key in ("Total Revenue", "TotalRevenue"):
        if key in income_stmt.index:
            row = income_stmt.loc[key].dropna()
            return [float(v) for v in row.values if v is not None and not pd.isna(v)]
    return []


def _get_earnings_history(income_stmt):
    """Annual net income series, most recent first."""
    if income_stmt is None or getattr(income_stmt, "empty", True):
        return []
    for key in ("Net Income", "NetIncome", "Net Income Common Stockholders"):
        if key in income_stmt.index:
            row = income_stmt.loc[key].dropna()
            return [float(v) for v in row.values if v is not None and not pd.isna(v)]
    return []


def _series_cagr(values):
    """CAGR from a series ordered most-recent-first. Needs ≥3 positive points."""
    pos = [v for v in values if v and v > 0]
    if len(pos) < 3:
        return None
    n = len(pos) - 1
    try:
        return (pos[0] / pos[-1]) ** (1 / n) - 1
    except Exception:
        return None


def _is_volatile(values, cv_threshold=1.0):
    """Coefficient of variation on year-over-year growth > threshold → volatile."""
    pos = [v for v in values if v and v > 0]
    if len(pos) < 4:
        return True
    growths = [(pos[i] - pos[i + 1]) / pos[i + 1] for i in range(len(pos) - 1)]
    mean = sum(growths) / len(growths)
    if abs(mean) < 1e-6:
        return False
    var = sum((g - mean) ** 2 for g in growths) / len(growths)
    return (var ** 0.5) / abs(mean) > cv_threshold


def _get_finviz_5y_growth(ticker):
    """
    Scrape Finviz 'EPS next 5Y' estimate. Returns float (e.g. 0.18) or None.
    Cached for 6h; failures cached as None to avoid repeated 5s timeouts.
    """
    if not ticker:
        return None
    t = ticker.upper().strip()
    cached = _FINVIZ_GROWTH_CACHE.get(t)
    if cached and (time.time() - cached[0]) < _FINVIZ_GROWTH_CACHE_TTL:
        return cached[1]
    growth = None
    try:
        r = requests.get(
            f"https://finviz.com/quote.ashx?t={t}",
            headers={"User-Agent": "Mozilla/5.0 (compatible; VALUS/1.0)"},
            timeout=5,
        )
        if r.status_code == 200:
            # Finviz snapshot table: "EPS next 5Y" cell followed by <b>NN.NN%</b>
            m = re.search(
                r"EPS next 5Y[^<]*</td>\s*<td[^>]*>\s*<b>\s*(-?[\d.]+)\s*%",
                r.text,
            )
            if m:
                v = float(m.group(1)) / 100
                if -0.5 < v < 1.0:        # sanity: -50% .. +100%
                    growth = v
    except Exception:
        growth = None
    _FINVIZ_GROWTH_CACHE[t] = (time.time(), growth)
    return growth


def compute_blended_growth(stock, info, fcf_series, income_stmt,
                           ind_params, ticker):
    """
    Blend historical, analyst, and Finviz signals into Stage-1, Stage-2, and
    terminal growth rates. Falls back to industry proxy when data is sparse
    or volatile.

    Returns dict:
      {
        "s1": float, "s2": float, "tg": float,
        "source": str, "confidence": "high"|"medium"|"low",
        "fellback_to_industry": bool,
        "components": { ... raw signals ... },
      }
    """
    rev = _get_revenue_history(income_stmt)
    eps = _get_earnings_history(income_stmt)

    # Long-history extension: yfinance free tier returns ~4 years of annual
    # data; stockanalysis.com serves ~10y free with no API-key throttle.
    # Prefer it when it gives a strictly longer series — falling back
    # silently to yfinance when the scrape fails or returns nothing.
    av = _get_long_income_history(ticker)
    if av:
        if av.get("revenue") and len(av["revenue"]) > len(rev):
            rev = av["revenue"]
        if av.get("net_income") and len(av["net_income"]) > len(eps):
            eps = av["net_income"]

    rev_cagr = _series_cagr(rev)
    fcf_cagr = _series_cagr(fcf_series or [])
    eps_cagr = _series_cagr(eps)

    hist_signals = [c for c in (rev_cagr, fcf_cagr, eps_cagr) if c is not None]
    hist_growth = sum(hist_signals) / len(hist_signals) if hist_signals else None

    # Next-2y analyst earnings estimate (average +1y and +2y when both exist)
    analyst_growth = None
    try:
        ee = stock.earnings_estimate
        if ee is not None and not ee.empty:
            vals = []
            for k in ("+1y", "+2y"):
                if k in ee.index:
                    v = safe(ee.loc[k, "growth"])
                    if v is not None and -0.5 < v < 1.0:
                        vals.append(float(v))
            if vals:
                analyst_growth = sum(vals) / len(vals)
    except Exception:
        pass

    finviz_growth = _get_finviz_5y_growth(ticker)

    components = {
        "hist_revenue_cagr": rev_cagr,
        "hist_fcf_cagr":     fcf_cagr,
        "hist_eps_cagr":     eps_cagr,
        "analyst_2y_avg":    analyst_growth,
        "finviz_5y_eps":     finviz_growth,
        "revenue_years":     len(rev),
        "earnings_years":    len(eps),
        "fcf_years":         len(fcf_series or []),
        "long_history_used": bool(av),
    }

    # Industry proxy: 70% of the sector's max_s1 ceiling represents a
    # mid-cycle "average mature company in this industry" growth rate.
    industry_proxy = ind_params["max_s1"] * 0.70

    # Sparseness/volatility detection — if both rev and fcf history are
    # too short or whipsawing, and we have no forward signals, fall back.
    rev_ok = len(rev) >= 3 and not _is_volatile(rev)
    fcf_ok = len(fcf_series or []) >= 3 and not _is_volatile(fcf_series or [])
    forward_ok = (analyst_growth is not None) or (finviz_growth is not None)
    must_fall_back = (not rev_ok) and (not fcf_ok) and (not forward_ok)

    if must_fall_back:
        s1 = industry_proxy
        source = "Industry proxy (sparse / volatile data)"
        confidence = "low"
        fellback = True
    else:
        # Stage 1 weighted blend: analyst > Finviz > historical.
        # Re-normalises across whichever signals are available.
        weights, values = [], []
        if analyst_growth is not None:
            weights.append(0.50); values.append(analyst_growth)
        if finviz_growth is not None:
            weights.append(0.30); values.append(finviz_growth)
        if hist_growth is not None:
            weights.append(0.20); values.append(hist_growth)
        if not values:
            s1 = industry_proxy
            source = "Industry proxy"
            confidence = "low"
            fellback = True
        else:
            s1 = sum(v * w for v, w in zip(values, weights)) / sum(weights)
            source = "Blended (analyst · Finviz · historical)"
            confidence = "high" if len(values) == 3 else "medium" if len(values) == 2 else "low"
            fellback = False

    # Cap Stage 1 at industry ceiling, floor at 0.
    s1 = max(min(s1, ind_params["max_s1"]), 0.0)

    # Stage 2 (years 6–10): mean-reversion. Weighted toward historical CAGR
    # and a mature-industry rate, with a small Finviz contribution if present.
    s2_pairs = []
    if hist_growth is not None:
        s2_pairs.append((hist_growth, 0.55))
    if finviz_growth is not None:
        s2_pairs.append((finviz_growth * 0.7, 0.25))   # discount: years 6–10 is past Finviz horizon
    s2_pairs.append((industry_proxy * 0.5, 0.20))      # mature-rate anchor
    s2 = sum(v * w for v, w in s2_pairs) / sum(w for _, w in s2_pairs)
    # Cap Stage 2 at 65% of industry max_s1 (matches existing convention),
    # floor at 2%, never above Stage 1.
    s2 = max(min(s2, ind_params["max_s1"] * 0.65, s1), 0.02)

    # Terminal growth: soft-cap at min(industry max_tg, 6%); hard-cap at 8%;
    # always strictly below Stage 2.
    tg_cap_soft = min(ind_params["max_tg"], 0.06)
    tg_cap_hard = 0.08
    tg = min(tg_cap_soft, tg_cap_hard)
    tg = min(tg, max(s2 - 0.005, 0.005))

    return {
        "s1": s1, "s2": s2, "tg": tg,
        "source": source,
        "confidence": confidence,
        "fellback_to_industry": fellback,
        "components": components,
    }


# ── Comparables-Based Profitability Model ────────────────────────────────────
# For early-stage / volatile-margin companies (e.g. Cava, Toast, Reddit) the
# DCF base FCF is misleading because the company is choosing growth over
# profit. This model picks 2–3 mature peers (e.g. Cava → Chipotle), pulls
# their operating margins, assigns a probability the target reaches those
# margins, and produces a probability-weighted base FCF.
#
# Curated peer map. Auto-discovery from sector+industry is unreliable
# (yfinance peer lists are noisy and often include unrelated names), so
# we curate the obvious analogues and gracefully no-op for everything else.
COMPARABLES_MAP = {
    # Restaurants — fast-casual / QSR / coffee
    "CAVA":  ["CMG", "SHAK", "QSR"],
    "SG":    ["CMG", "SHAK"],
    "WING":  ["CMG", "DPZ"],
    "DNUT":  ["SBUX", "DPZ"],
    "FWRG":  ["SHAK", "WING"],          # First Watch
    "BROS":  ["SBUX", "MCD"],           # Dutch Bros
    # Software / consumer fintech / payments
    "TOST":  ["SQ", "FI"],
    "AFRM":  ["PYPL", "V"],
    "HOOD":  ["SCHW", "IBKR"],
    "SOFI":  ["SCHW", "JPM"],
    "UPST":  ["DFS", "COF"],
    "MARA":  ["RIOT"],
    # Consumer / social / streaming-adjacent
    "RDDT":  ["META", "PINS", "SNAP"],
    "PINS":  ["META", "SNAP"],
    "SNAP":  ["META", "PINS"],
    "RBLX":  ["EA", "TTWO"],
    "DUOL":  ["MTCH", "RBLX"],
    "BMBL":  ["MTCH"],
    "MTCH":  ["META"],
    "SPOT":  ["NFLX"],
    "NFLX":  ["DIS"],
    # Mobility / delivery / travel
    "UBER":  ["EXPE", "BKNG"],
    "LYFT":  ["UBER"],
    "DASH":  ["UBER"],
    "ABNB":  ["BKNG", "EXPE"],
    "TRIP":  ["BKNG"],
    # E-commerce / digital consumer / advertising
    "CHWY":  ["AMZN", "WMT"],
    "WBD":   ["DIS", "NFLX"],
    "ETSY":  ["EBAY", "AMZN"],
    "W":     ["AMZN", "TGT"],            # Wayfair
    "FIGS":  ["LULU"],
    "OLPX":  ["EL", "ULTA"],
    "PTON":  ["NKE"],
    "RVLV":  ["LULU"],
    # EV / clean energy / mobility-adjacent
    "RIVN":  ["TSLA", "F"],
    "LCID":  ["TSLA", "F"],
    "NIO":   ["TSLA"],
    "XPEV":  ["TSLA"],
    "LI":    ["TSLA"],
    "CHPT":  ["EBAY"],                   # speculative — charging network
    "ENPH":  ["FSLR"],
    "RUN":   ["FSLR"],
    "PLUG":  ["FSLR"],
    # Crypto-adjacent / fintech rails
    "COIN":  ["SCHW", "ICE"],
    "HOOD":  ["SCHW", "IBKR"],
    # Cybersecurity / observability / infra SaaS
    "S":     ["CRWD", "PANW"],           # SentinelOne
    "PATH":  ["MSFT", "NOW"],
    "BILL":  ["INTU"],
    "GTLB":  ["MSFT"],
    "NET":   ["AKAM", "FFIV"],
    "DDOG":  ["NOW"],
    "MDB":   ["ORCL"],
    "ZS":    ["CRWD", "PANW"],
    "SNOW":  ["ORCL"],
    "FRSH":  ["NOW", "CRM"],
    # AI / data / vertical SaaS
    "AI":    ["NOW", "PLTR"],
    "PLTR":  ["NOW", "MSFT"],
    "U":     ["EA"],                      # Unity
    "TWLO":  ["NOW"],
    # Health-tech / life sciences / consumer health
    "HIMS":  ["CVS"],
    "TDOC":  ["UNH", "CVS"],
    "ONEM":  ["UNH"],
    "GH":    ["LH"],
    "NVAX":  ["MRNA", "PFE"],
    "BNTX":  ["MRNA"],
    # Space / next-gen industrials
    "RKLB":  ["LMT", "BA"],
    "ASTS":  ["LMT"],
    "ACHR":  ["BA"],
    "JOBY":  ["BA"],
    # Renewable / nuclear
    "OKLO":  ["VST", "CEG"],
    "SMR":   ["VST", "CEG"],
}

_PEER_INFO_CACHE = {}            # {peer_ticker: (timestamp, info_subset)}
_PEER_INFO_CACHE_TTL = 6 * 3600  # 6h


# ── Low-Confidence Diagnostics ──────────────────────────────────────────────
# When |MoS| > 100% the IV is almost always being skewed by a data artefact
# rather than a genuine market mispricing. We probe the three most common
# culprits and surface a plain-English warning to the user.

# Hardcoded share-class pairs known to cause Yahoo data misalignment between
# share count, price, and market cap. Format: ticker → sibling.
_SHARE_CLASS_PAIRS = {
    "BRK.A": "BRK.B", "BRK-A": "BRK-B",
    "BRK.B": "BRK.A", "BRK-B": "BRK-A",
    "GOOG":  "GOOGL", "GOOGL": "GOOG",
    "BF.A":  "BF.B",  "BF-A":  "BF-B",
    "BF.B":  "BF.A",  "BF-B":  "BF-A",
    "LEN.B": "LEN",   "LEN-B": "LEN",
    "FOX":   "FOXA",  "FOXA":  "FOX",
    "NWS":   "NWSA",  "NWSA":  "NWS",
    "DISCA": "DISCK", "DISCK": "DISCA",
    "RDS.A": "RDS.B", "RDS.B": "RDS.A",
    "UA":    "UAA",   "UAA":   "UA",
}


def _diagnose_low_confidence(info, ticker, mos, intrinsic_value, price):
    """
    Triage the three most common causes of an extreme |MoS| > 100% reading.
    Returns list of {"factor": str, "message": str} (possibly empty).
    """
    flags = []
    if mos is None or abs(mos) <= 100:
        return flags

    # 1) Share-class mismatch — the named ticker has a sibling class that
    # often diverges in Yahoo's share-count / market-cap data.
    t_norm = (ticker or "").upper().replace("-", ".")
    sibling = _SHARE_CLASS_PAIRS.get(t_norm) or _SHARE_CLASS_PAIRS.get(t_norm.replace(".", "-"))
    if sibling:
        flags.append({
            "factor": "share_class_mismatch",
            "message": (
                f"{ticker} has a sibling share class ({sibling}). Yahoo's share-count "
                f"and price feeds for multi-class stocks frequently misalign — "
                f"the IV may be using one class's shares against the other's price. "
                f"Cross-check on the sibling ticker before relying on this number."
            ),
        })

    # 2) Forward-earnings spike — analyst +1y EPS growth above 50% almost
    # always means a base-rate-warped forecast (post-pandemic recovery, AI
    # revenue ramp). The Stage-1 cap will already trim it, but the signal
    # is usually unreliable on its own and skews the IV.
    earnings_growth = safe(info.get("earningsGrowth"))
    forward_eps     = safe(info.get("forwardEps"))
    trailing_eps    = safe(info.get("trailingEps"))
    if earnings_growth is not None and earnings_growth > 0.50:
        flags.append({
            "factor": "forward_earnings_spike",
            "message": (
                f"Analysts are forecasting {earnings_growth*100:.0f}% earnings growth, "
                f"which inflates Stage-1 growth assumptions. The IV may be over-reactive "
                f"to a temporary base-rate distortion."
            ),
        })
    elif (forward_eps is not None and trailing_eps is not None and
          trailing_eps > 0 and forward_eps / trailing_eps > 1.50):
        flags.append({
            "factor": "forward_earnings_spike",
            "message": (
                f"Forward EPS (${forward_eps:.2f}) is {forward_eps/trailing_eps:.1f}× "
                f"trailing EPS (${trailing_eps:.2f}). The forward number may be a "
                f"non-recurring spike that distorts IV."
            ),
        })

    # 3) Stale price — currentPrice diverging materially from previousClose,
    # or the regular session marker being stuck on a past close, suggests
    # a quote feed problem rather than a real mispricing.
    cur_price = safe(info.get("currentPrice"))
    prev_close = safe(info.get("regularMarketPreviousClose")) or safe(info.get("previousClose"))
    if cur_price and prev_close and prev_close > 0:
        gap = abs(cur_price - prev_close) / prev_close
        if gap > 0.25:   # >25% intraday gap is almost always a data artefact
            flags.append({
                "factor": "stale_price_data",
                "message": (
                    f"Current price (${cur_price:.2f}) gaps {gap*100:.0f}% from "
                    f"previous close (${prev_close:.2f}). This usually indicates "
                    f"a stale quote, a halt/resumption, or a corporate action — "
                    f"not a real price move. IV may not reflect the live market."
                ),
            })
    market_state = info.get("marketState")
    if market_state in ("CLOSED", "PREPRE", "POSTPOST") and cur_price and intrinsic_value:
        # Closed-market quote is fine, but the spread could be old enough to
        # matter if it's been a long weekend / holiday — flag conservatively.
        if abs(mos) > 150:
            flags.append({
                "factor": "stale_price_data",
                "message": (
                    f"Quote feed shows market {market_state.lower()}; price may be hours "
                    f"or days old. With a ±{abs(mos):.0f}% gap, the IV is best treated "
                    f"as directional only until the next live tick."
                ),
            })
    return flags


def _company_age_years(info, stock=None):
    """
    Years since IPO. Tries (in order): firstTradeDateEpochUtc (seconds),
    firstTradeDateMilliseconds, firstTradeDate, then earliest price-history
    date from stock.history(period='max'). Returns float or None.
    """
    if not info:
        info = {}
    # Yahoo sometimes returns this in seconds, sometimes milliseconds —
    # detect by magnitude (anything > year 3000 in seconds is ms).
    for key in ("firstTradeDateEpochUtc", "firstTradeDate"):
        v = info.get(key)
        if v:
            try:
                v = float(v)
                if v > 4e10:        # ms epoch
                    v = v / 1000.0
                if v > 0:
                    return max(0.0, (time.time() - v) / (365.25 * 86400))
            except Exception:
                pass
    v = info.get("firstTradeDateMilliseconds")
    if v:
        try:
            return max(0.0, (time.time() - float(v) / 1000.0) / (365.25 * 86400))
        except Exception:
            pass
    # Last-resort fallback: earliest available price bar.
    if stock is not None:
        try:
            h = stock.history(period="max", interval="1mo")
            if h is not None and not h.empty:
                first = h.index[0]
                first_ts = first.timestamp() if hasattr(first, "timestamp") else None
                if first_ts:
                    return max(0.0, (time.time() - float(first_ts)) / (365.25 * 86400))
        except Exception:
            pass
    return None


def _get_peer_operating_margin(peer_ticker):
    """
    Operating margin (EBIT/Revenue) for a peer. Cached 6h. Returns float or
    None when unavailable. Uses info["operatingMargins"] which yfinance
    populates from the trailing-twelve-months income statement.
    """
    if not peer_ticker:
        return None
    t = peer_ticker.upper().strip()
    cached = _PEER_INFO_CACHE.get(t)
    if cached and (time.time() - cached[0]) < _PEER_INFO_CACHE_TTL:
        return cached[1]
    margin = None
    try:
        peer_info = yf.Ticker(t).info or {}
        m = peer_info.get("operatingMargins")
        if m is not None and -0.5 < float(m) < 0.6:
            margin = float(m)
    except Exception:
        margin = None
    _PEER_INFO_CACHE[t] = (time.time(), margin)
    return margin


def _attainment_probability(age_years, rev_growth, current_op_margin,
                             peer_op_margin):
    """
    Probability the target company reaches the peer-average operating margin.
    Heuristic — calibrated for fast-growing companies that haven't yet
    optimised for profit. Returns float in [0.25, 0.85].

    Inputs (any may be None):
      age_years        — years since IPO
      rev_growth       — TTM revenue growth (decimal, e.g. 0.32)
      current_op_margin — TTM operating margin
      peer_op_margin   — peer-average operating margin (target)
    """
    p = 0.55  # base rate

    # Growth signal: dominant category leaders (>30% growth) more likely to
    # achieve scale economics; moderate growth (>15%) is also a positive.
    if rev_growth is not None:
        if rev_growth > 0.30:
            p += 0.10
        elif rev_growth > 0.15:
            p += 0.05
        elif rev_growth < 0.05:
            p -= 0.10

    # Current margin trajectory: already-positive margin is the strongest
    # leading indicator that the unit economics scale.
    if current_op_margin is not None:
        if current_op_margin > 0:
            p += 0.10
        if current_op_margin < -0.05:
            p -= 0.10

    # Maturity: very young companies (< 5y) have wider outcome distributions.
    if age_years is not None:
        if age_years < 3:
            p -= 0.10
        elif age_years < 5:
            p -= 0.05
        elif age_years > 8:
            p += 0.05

    # Margin gap: if the current margin is already close to peers, attainment
    # is essentially priced in; if the gap is huge, skepticism.
    if current_op_margin is not None and peer_op_margin is not None:
        gap = peer_op_margin - current_op_margin
        if gap < 0.02:
            p += 0.10
        elif gap > 0.20:
            p -= 0.10

    return max(0.25, min(0.85, p))


def compute_comparables_model(stock, info, ticker, base_fcf, tax_rate, fx_rate):
    """
    Probability-weighted FCF for early-stage / volatile-margin companies.

    Returns dict (always — caller checks `used`):
      {
        "used":              bool,
        "reason":            str,        # why triggered or skipped
        "peers":             [tickers],
        "peer_margins":      {ticker: margin},
        "peer_avg_margin":   float,
        "current_margin":    float | None,
        "age_years":         float | None,
        "p_attain":          float,
        "current_base_fcf":  float,
        "peer_implied_fcf":  float,
        "weighted_base_fcf": float,
      }
    """
    out = {
        "used":              False,
        "reason":            "",
        "peers":             [],
        "peer_margins":      {},
        "peer_avg_margin":   None,
        "current_margin":    None,
        "age_years":         None,
        "p_attain":          None,
        "current_base_fcf":  base_fcf,
        "peer_implied_fcf":  None,
        "weighted_base_fcf": base_fcf,
    }

    if not ticker:
        out["reason"] = "no ticker"
        return out

    age   = _company_age_years(info, stock)
    op_m  = safe(info.get("operatingMargins"))
    rev_g = safe(info.get("revenueGrowth"))
    out["age_years"]      = age
    out["current_margin"] = op_m

    # Trigger conditions: < 10y old, OR currently choosing growth over profit
    # (negative/low operating margin combined with strong revenue growth).
    young     = age is not None and age < 10
    grow_over_profit = (
        op_m is not None and rev_g is not None and
        op_m < 0.05 and rev_g > 0.15
    )
    if not (young or grow_over_profit):
        out["reason"] = "mature with positive margins — comparables not needed"
        return out

    peers = COMPARABLES_MAP.get(ticker.upper())
    if not peers:
        out["reason"] = f"no curated comparables map entry for {ticker}"
        return out

    margins = {}
    for p in peers:
        m = _get_peer_operating_margin(p)
        if m is not None:
            margins[p] = m

    if len(margins) < 1:
        out["reason"] = "could not fetch peer margins"
        out["peers"] = peers
        return out

    avg_margin = sum(margins.values()) / len(margins)
    out["peers"]            = peers
    out["peer_margins"]     = margins
    out["peer_avg_margin"]  = avg_margin

    p_attain = _attainment_probability(age, rev_g, op_m, avg_margin)
    out["p_attain"] = p_attain

    # Peer-implied FCF: revenue × peer-avg EBIT margin × (1 − tax).
    rev_ttm = safe(info.get("totalRevenue"))
    if not rev_ttm or rev_ttm <= 0:
        out["reason"] = "missing revenue — cannot compute peer-implied FCF"
        return out
    rev_ttm = float(rev_ttm) * fx_rate
    peer_implied_fcf = rev_ttm * avg_margin * (1 - tax_rate)
    out["peer_implied_fcf"] = peer_implied_fcf

    # Probability-weighted base FCF: weighted between today's reality and the
    # peer-implied steady-state. Floors current FCF at 0 so a temporarily
    # negative print doesn't make the weighted base fall below the
    # peer-implied path. Only applies if peer-implied is materially higher.
    current_floor = max(base_fcf or 0, 0.0)
    weighted = p_attain * peer_implied_fcf + (1 - p_attain) * current_floor

    if weighted <= (base_fcf or 0):
        out["reason"] = "peer-implied FCF below current — model would lower IV, skipping"
        return out

    out["used"]              = True
    out["reason"]            = (
        f"{ticker} flagged as {'<10y old' if young else 'growth-over-profit'}; "
        f"peers={list(margins.keys())} avg margin={avg_margin*100:.1f}% · "
        f"P(attain)={p_attain*100:.0f}%"
    )
    out["weighted_base_fcf"] = weighted
    return out


def get_forward_growth(stock, info, fcf_series):
    """
    Best available forward FCF/earnings growth for Stage 1.
    Priority: analyst +1y EPS → analyst trend → earnings growth (bounded) →
              conservative historical FCF CAGR (min of 3yr and 5yr) → revenue proxy.
    Returns (rate, source_label).

    Caps:
      • Analyst estimates: 35% max  (50% was too aggressive for high-growth outliers)
      • Historical CAGR:   20% max  (beyond this, mean-reversion is almost certain)
      • Revenue proxy:     25% max, discounted to 75% of revenue growth
    """
    # 1. Analyst earnings estimate growth for next fiscal year
    try:
        ee = stock.earnings_estimate
        if ee is not None and not ee.empty and '+1y' in ee.index:
            v = safe(ee.loc['+1y', 'growth'])
            if v is not None and 0.02 < v < 0.35:          # was 0.50 — too permissive
                return float(v), "Analyst EPS est. (+1y)"
    except Exception:
        pass

    # 2. Growth estimates dataframe — use stockTrend only, not indexTrend
    # (indexTrend reflects the sector/index benchmark, not the individual company)
    try:
        ge = stock.growth_estimates
        if ge is not None and not ge.empty and '+1y' in ge.index:
            for col in ["stockTrend", "stock"]:
                if col in ge.columns:
                    v = safe(ge.loc['+1y', col])
                    if v is not None and 0.02 < v < 0.35:  # was 0.50
                        return float(v), "Analyst consensus (+1y)"
    except Exception:
        pass

    # 3. Trailing earnings growth (YoY) — bounded conservatively
    # Cap at 25%: a single-year spike in earnings is not a sustainable run-rate
    ag = safe(info.get("earningsGrowth"))
    if ag is not None and 0.02 < ag < 0.25:
        return float(ag), "Trailing EPS growth (YoY)"

    # 4. Conservative historical FCF CAGR — take the LOWER of 3-year and 5-year CAGR
    # so that a recent strong year doesn't inflate the long-run projection.
    positives = [f for f in fcf_series if f > 0]
    if len(positives) >= 3:
        cagr_candidates = []
        # 3-year CAGR (if enough data)
        if len(positives) >= 3:
            hg3 = (positives[0] / positives[2]) ** (1 / 2) - 1
            cagr_candidates.append(hg3)
        # 5-year CAGR (if enough data)
        if len(positives) >= 5:
            hg5 = (positives[0] / positives[4]) ** (1 / 4) - 1
            cagr_candidates.append(hg5)
        # Use minimum to be conservative; cap at 20%, floor at 2%
        hg = min(cagr_candidates)
        hg = min(max(hg, 0.02), 0.20)
        label = "Historical FCF CAGR (3yr)" if len(cagr_candidates) == 1 else "Historical FCF CAGR (min 3yr/5yr)"
        return hg, label

    # 5. Revenue growth proxy — discount to 75% (FCF grows slower than revenue when
    # margins are compressing), cap at 25%
    rev_g = safe(info.get("revenueGrowth"))
    if rev_g and 0.02 < rev_g < 0.40:
        return float(min(rev_g * 0.75, 0.25)), "Revenue growth proxy"

    return 0.06, "Default (6%)"     # lowered from 7% — more conservative baseline


# ── Sector / P/E context ───────────────────────────────────────────────────────

def get_dcf_notes(sector, industry, pe, fcf_available):
    """Return list of contextual notes about valuation reliability."""
    notes = []
    s = (sector or "").lower()
    ind = (industry or "").lower()

    # Sector-specific DCF reliability
    # Payment networks (credit service / payment network / payment processing) are
    # asset-light, fee-based businesses — the banking interest-cost warning does NOT apply.
    _is_payment_net = any(x in ind for x in ["credit service", "payment network", "payment processing"])
    if not _is_payment_net and (
        any(x in s for x in ["financial", "bank"]) or
        any(x in ind for x in ["bank", "saving", "thrift"])
    ):
        notes.append({
            "type": "warn",
            "text": "Financials: Interest is a core operating cost, so FCF-based DCF overstates earnings power. "
                    "P/B ratio and ROE are the primary valuation benchmarks for banks."
        })
    elif "insurance" in s or "insurance" in ind:
        notes.append({
            "type": "warn",
            "text": "Insurance: Float-driven business model; FCF is not comparable to industrial companies. "
                    "P/B, combined ratio, and ROE are more appropriate."
        })
    elif "real estate" in s or "reit" in ind:
        notes.append({
            "type": "warn",
            "text": "REIT: GAAP earnings are depressed by non-cash depreciation. "
                    "Funds From Operations (FFO) and AFFO are the standard valuation metrics — not FCF or EPS."
        })
    elif "utilities" in s:
        notes.append({
            "type": "info",
            "text": "Utilities: Regulated rate-of-return constrains growth. Terminal growth of 2–2.5% is typical. "
                    "EV/EBITDA and dividend yield are widely used alongside DCF."
        })
    elif "energy" in s:
        notes.append({
            "type": "info",
            "text": "Energy: FCF is highly cyclical with commodity prices. "
                    "Consider EV/EBITDA on normalized (through-cycle) earnings rather than spot FCF."
        })
    elif "retail" in ind or "specialty retail" in ind or ("consumer" in s and "cycl" in s):
        notes.append({
            "type": "info",
            "text": "Retail / Consumer Cyclical: DCF quality depends heavily on inventory efficiency "
                    "and leverage discipline. Cross-check Inventory Turnover and Debt/Equity ratios "
                    "in the Financial Health card."
        })
    elif "industrial" in s or "aerospace" in ind or "machinery" in ind:
        notes.append({
            "type": "info",
            "text": "Industrials: Capital-intensive, cyclical cash flows. "
                    "Verify Debt/Equity and CapEx intensity. "
                    "EV/EBITDA is widely used alongside DCF for cycle-normalized valuation."
        })
    elif "biotechnology" in ind or ("health" in s and "drug" in ind):
        notes.append({
            "type": "info",
            "text": "Biotech / Pharma: Pipeline binary risk and R&D burn make DCF sensitive to assumptions. "
                    "Probability-weighted NPV models are used by professionals."
        })

    # P/E quality note
    if pe is not None:
        if pe < 0:
            notes.append({
                "type": "warn",
                "text": "Negative P/E: Company is currently loss-making. "
                        "P/E is not meaningful — use EV/Revenue or EV/EBITDA."
            })
        elif pe > 80:
            notes.append({
                "type": "info",
                "text": f"Elevated P/E ({pe:.0f}x): May reflect high growth expectations, one-time charges "
                        "depressing earnings, or speculative premium. Cross-check with EV/EBITDA and PEG."
            })
        elif pe > 40:
            notes.append({
                "type": "info",
                "text": f"High P/E ({pe:.0f}x): Priced for above-average growth. "
                        "Verify with forward P/E and PEG to assess whether growth justifies the multiple."
            })

    if not fcf_available:
        notes.append({
            "type": "warn",
            "text": "Negative or missing FCF: DCF not computed. "
                    "Review operating cash flow trend and capital intensity before investing."
        })

    return notes


# ── Financial statement label mapping ─────────────────────────────────────────

LABEL_MAP = {
    # Income Statement
    "Total Revenue":                       "Total Revenue",
    "Cost Of Revenue":                     "Cost of Revenue (COGS)",
    "Gross Profit":                        "Gross Profit",
    "Research And Development":            "R&D Expenses",
    "Selling General And Administration":  "SG&A Expenses",
    "Operating Expense":                   "Total Operating Expenses",
    "Operating Income":                    "Operating Income (EBIT)",
    "Interest Expense Non Operating":      "Interest Expense",
    "Interest Income Non Operating":       "Interest Income",
    "Net Interest Income":                 "Net Interest",
    "Other Income Expense":                "Other Income / (Expense)",
    "Pretax Income":                       "Pre-Tax Income",
    "Tax Provision":                       "Income Tax Expense",
    "Net Income":                          "Net Income",
    "Diluted EPS":                         "EPS — Diluted",
    "Basic EPS":                           "EPS — Basic",
    "EBITDA":                              "EBITDA",
    "Normalized EBITDA":                   "Normalized EBITDA",
    "Reconciled Depreciation":             "D&A (Reconciled)",
    # Balance Sheet — Assets
    "Cash And Cash Equivalents":                        "Cash & Equivalents",
    "Cash Cash Equivalents And Short Term Investments": "Cash & Short-term Investments",
    "Accounts Receivable":                              "Accounts Receivable",
    "Receivables":                                      "Total Receivables",
    "Inventory":                                        "Inventory",
    "Other Current Assets":                             "Other Current Assets",
    "Current Assets":                                   "Total Current Assets",
    "Gross PPE":                                        "Gross PP&E",
    "Accumulated Depreciation":                         "Accumulated Depreciation",
    "Net PPE":                                          "Net PP&E",
    "Investments And Advances":                         "Long-term Investments",
    "Other Non Current Assets":                         "Other Non-Current Assets",
    "Total Non Current Assets":                         "Total Non-Current Assets",
    "Total Assets":                                     "Total Assets",
    # Balance Sheet — Liabilities
    "Accounts Payable":                                 "Accounts Payable",
    "Current Debt":                                     "Short-term Debt",
    "Current Deferred Revenue":                         "Deferred Revenue",
    "Current Accrued Expenses":                         "Accrued Liabilities",
    "Other Current Liabilities":                        "Other Current Liabilities",
    "Current Liabilities":                              "Total Current Liabilities",
    "Long Term Debt":                                   "Long-term Debt",
    "Long Term Debt And Capital Lease Obligation":      "LT Debt + Capital Leases",
    "Other Non Current Liabilities":                    "Other Non-Current Liabilities",
    "Total Non Current Liabilities Net Minority Interest": "Total Non-Current Liabilities",
    "Total Liabilities Net Minority Interest":          "Total Liabilities",
    # Balance Sheet — Equity
    "Common Stock":                                     "Common Stock & APIC",
    "Retained Earnings":                                "Retained Earnings",
    "Gains Losses Not Affecting Retained Earnings":     "Other Comprehensive Income",
    "Common Stock Equity":                              "Total Shareholders' Equity",
    "Stockholders Equity":                              "Total Shareholders' Equity",
    "Total Debt":                                       "Total Debt",
    "Net Debt":                                         "Net Debt / (Cash)",
    "Working Capital":                                  "Working Capital",
    "Tangible Book Value":                              "Tangible Book Value",
    # Cash Flow — Operating
    "Net Income From Continuing Operations":  "Net Income",
    "Depreciation And Amortization":          "Depreciation & Amortization",
    "Depreciation Amortization Depletion":    "D&A (Gross)",
    "Stock Based Compensation":               "Stock-based Compensation",
    "Deferred Income Tax":                    "Deferred Taxes",
    "Change In Receivables":                  "Δ Receivables",
    "Change In Inventory":                    "Δ Inventory",
    "Change In Payable":                      "Δ Payables",
    "Change In Working Capital":              "Δ Working Capital (Net)",
    "Other Non Cash Items":                   "Other Non-Cash Items",
    "Operating Cash Flow":                    "Cash from Operations (CFO)",
    # Cash Flow — Investing
    "Purchase Of PPE":                        "Capital Expenditures (CapEx)",
    "Capital Expenditure":                    "Capital Expenditures (CapEx)",
    "Purchase Of Investment":                 "Purchases of Securities",
    "Sale Of Investment":                     "Sales of Securities",
    "Purchase Of Business":                   "Acquisitions",
    "Net Business Purchase And Sale":         "Net Acquisitions / Disposals",
    "Net Other Investing Changes":            "Other Investing Activities",
    "Investing Cash Flow":                    "Cash from Investing (CFI)",
    # Cash Flow — Financing
    "Common Stock Payments":                  "Share Repurchases",
    "Common Stock Issuance":                  "Proceeds from Share Issuance",
    "Cash Dividends Paid":                    "Dividends Paid",
    "Long Term Debt Payments":                "LT Debt Repayment",
    "Long Term Debt Issuance":                "LT Debt Issuance",
    "Net Other Financing Charges":            "Other Financing Activities",
    "Financing Cash Flow":                    "Cash from Financing (CFF)",
    # Summary
    "Free Cash Flow":                         "Free Cash Flow (FCF)",
    "Beginning Cash Position":                "Beginning Cash Balance",
    "End Cash Position":                      "Ending Cash Balance",
    "Changes In Cash":                        "Net Change in Cash",
}

INCOME_ROWS = [
    "__Revenue",
    "Total Revenue", "Cost Of Revenue", "Gross Profit",
    "__Operating Expenses",
    "Research And Development", "Selling General And Administration",
    "Operating Expense", "Operating Income",
    "__Non-Operating",
    "Interest Income Non Operating", "Interest Expense Non Operating",
    "Net Interest Income", "Other Income Expense",
    "Pretax Income", "Tax Provision", "Net Income",
    "__Per Share",
    "Diluted EPS", "Basic EPS",
    "__Profitability",
    "EBITDA", "Normalized EBITDA", "Reconciled Depreciation",
]

BALANCE_ROWS = [
    "__Current Assets",
    "Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments",
    "Accounts Receivable", "Receivables", "Inventory",
    "Other Current Assets", "Current Assets",
    "__Non-Current Assets",
    "Gross PPE", "Accumulated Depreciation", "Net PPE",
    "Investments And Advances", "Other Non Current Assets",
    "Total Non Current Assets", "Total Assets",
    "__Current Liabilities",
    "Accounts Payable", "Current Debt", "Current Deferred Revenue",
    "Current Accrued Expenses", "Other Current Liabilities", "Current Liabilities",
    "__Non-Current Liabilities",
    "Long Term Debt", "Long Term Debt And Capital Lease Obligation",
    "Other Non Current Liabilities", "Total Non Current Liabilities Net Minority Interest",
    "Total Liabilities Net Minority Interest",
    "__Shareholders' Equity",
    "Common Stock", "Retained Earnings",
    "Gains Losses Not Affecting Retained Earnings",
    "Common Stock Equity", "Stockholders Equity",
    "__Summary Metrics",
    "Total Debt", "Net Debt", "Working Capital", "Tangible Book Value",
]

CASHFLOW_ROWS = [
    "__Operating Activities",
    "Net Income From Continuing Operations",
    "Depreciation And Amortization", "Depreciation Amortization Depletion",
    "Stock Based Compensation", "Deferred Income Tax",
    "Change In Receivables", "Change In Inventory",
    "Change In Payable", "Change In Working Capital",
    "Other Non Cash Items", "Operating Cash Flow",
    "__Investing Activities",
    "Purchase Of PPE", "Capital Expenditure",
    "Purchase Of Investment", "Sale Of Investment",
    "Purchase Of Business", "Net Business Purchase And Sale",
    "Net Other Investing Changes", "Investing Cash Flow",
    "__Financing Activities",
    "Common Stock Payments", "Common Stock Issuance",
    "Cash Dividends Paid", "Long Term Debt Payments", "Long Term Debt Issuance",
    "Net Other Financing Charges", "Financing Cash Flow",
    "__Summary",
    "Free Cash Flow",
    "Beginning Cash Position", "End Cash Position", "Changes In Cash",
]


def filtered_df_to_rows(df, row_order):
    """Return rows in prescribed order; __ prefix = section header. Uses LABEL_MAP for display names."""
    if df is None or df.empty:
        return {"columns": [], "rows": []}
    cols = []
    for c in df.columns:
        if hasattr(c, 'strftime'):
            cols.append(c.strftime('%Y-%m-%d'))
        else:
            s = str(c)
            cols.append(s[:10] if len(s) > 10 else s)

    idx  = set(df.index)
    rows = []
    for item in row_order:
        if item.startswith("__"):
            rows.append({"label": item[2:], "values": None, "section": True})
        elif item in idx:
            series = df.loc[item]
            vals   = []
            for v in series.values:
                try:
                    f = float(v)
                    vals.append(None if np.isnan(f) or np.isinf(f) else round(f, 0))
                except Exception:
                    vals.append(None)
            rows.append({"label": LABEL_MAP.get(item, item), "values": vals})
    return {"columns": cols, "rows": rows}


# ── Routes ─────────────────────────────────────────────────────────────────────

from flask_limiter.errors import RateLimitExceeded

@app.errorhandler(RateLimitExceeded)
def handle_rate_limit(e):
    return jsonify({
        "error": "rate_limited",
        "message": "You've hit the free limit. Sign in to get more free lookups and unlock full access.",
        "login_url": "/auth/login",
    }), 429


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/robots.txt")
def robots_txt():
    base = (request.url_root or "/").rstrip("/")
    body = (
        "User-agent: GPTBot\nDisallow: /\n\n"
        "User-agent: ClaudeBot\nDisallow: /\n\n"
        "User-agent: Google-Extended\nDisallow: /\n\n"
        "User-agent: CCBot\nDisallow: /\n\n"
        "User-agent: anthropic-ai\nDisallow: /\n\n"
        "User-agent: PerplexityBot\nDisallow: /\n\n"
        "User-agent: *\nAllow: /\n\n"
        f"Sitemap: {base}/sitemap.xml\n"
    )
    return app.response_class(body, mimetype="text/plain")


@app.route("/docs")
def docs():
    """
    Public-facing explainer for VALUS — what DCF / FCF / MOS / WACC mean,
    how to read the verdict tiers, and how the strategic-asset + news
    layers shape the IV.  No JS dependencies; deliberately readable on
    any device with no auth required.
    """
    return render_template("docs.html")


# ── SEO content pages (server-rendered, indexable) ────────────────────────
@app.route("/how-it-works")
def how_it_works():
    return render_template("how_it_works.html")


@app.route("/methodology")
def methodology():
    return render_template("methodology.html")


@app.route("/stocks")
def stocks_index():
    """
    Index of every ticker VALUS covers. Each link is a server-rendered
    landing page with unique title/meta/JSON-LD — the main lever for
    long-tail SEO ("AAPL fair value", "MSFT DCF", etc.).
    """
    tickers = sorted(DISCOVERY_TICKERS)
    return render_template("stocks_index.html", tickers=tickers)


@app.route("/stocks/<ticker>")
def stock_page(ticker):
    """
    Programmatic per-ticker landing page. Server-rendered with unique
    title, meta description, JSON-LD FinancialProduct schema, and a
    plain-HTML summary card so Google can index the content without
    executing JS. Bootstraps users into the SPA via /?t=<ticker>.
    """
    t = (ticker or "").strip().upper()
    if not t or not re.match(r"^[A-Z][A-Z0-9.\-]{0,11}$", t):
        return redirect("/stocks", code=302)

    # Try the warm analyze cache first so we render with real numbers when
    # available — but never block the page render on a slow yfinance call.
    company_name = t
    sector = ""
    price = None
    iv = None
    mos = None
    tier_label = ""
    has_valuation = False
    try:
        ck = _analyze_cache_key(t, {})
        entry = _ANALYZE_CACHE.get(ck)
        if entry is not None:
            _ts, payload = entry
            if isinstance(payload, dict):
                company_name = payload.get("company_name") or t
                sector       = payload.get("sector") or ""
                price        = payload.get("current_price")
                iv           = payload.get("intrinsic_value")
                mos          = payload.get("margin_of_safety")
                tier_label   = (payload.get("priced_for") or {}).get("label") or ""
                has_valuation = price is not None
    except Exception:
        pass

    jsonld = None
    if has_valuation:
        try:
            jsonld = _json_top.dumps({
                "@context": "https://schema.org",
                "@type": "FinancialProduct",
                "name": f"{company_name} ({t}) DCF Valuation",
                "description": f"Educational DCF intrinsic value estimate and margin of safety for {company_name} ({t}).",
                "url": f"/stocks/{t.lower()}",
                "category": "Stock valuation",
                "provider": {"@type": "Organization", "name": "VALUS"},
            })
        except Exception:
            jsonld = None

    return render_template(
        "ticker.html",
        ticker=t,
        ticker_lower=t.lower(),
        company_name=company_name,
        sector=sector,
        price=price,
        iv=iv,
        mos=mos,
        tier_label=tier_label,
        has_valuation=has_valuation,
        jsonld=jsonld,
    )


# Educational concept pages — each one is a unique URL Google can rank for.
_LEARN_PAGES = {
    "dcf-valuation": {
        "title": "What Is a DCF Valuation? · VALUS",
        "h1":    "What is a DCF valuation?",
        "desc":  "A discounted cash flow (DCF) valuation estimates a company's intrinsic value by projecting its future free cash flows and discounting them to today using a required rate of return.",
        "body": [
            "A discounted cash flow valuation answers a simple question: if I owned this entire business, how much cash would it generate for me over the next decade and beyond — and what is that stream of cash worth in today's dollars?",
            "The model has three pieces. First, project free cash flow (operating cash flow minus capital expenditure) for ten years, growing at a plausible rate. Second, calculate a terminal value capturing everything beyond year 10 using a perpetuity growth formula. Third, discount every future cash flow back to today using the weighted average cost of capital (WACC), which represents the return investors require to bear the risk of owning the business.",
            "The total present value, divided by shares outstanding, gives you a per-share intrinsic value. Compare it to the current price to get your margin of safety. If intrinsic value is meaningfully above price, the stock may be undervalued; if it's below, the market is pricing in growth or quality the cash flows don't yet justify.",
            "DCF models are powerful but sensitive to inputs — small changes in growth or discount rate produce large swings in fair value. That's why VALUS shows you every assumption transparently and lets you adjust them.",
        ],
    },
    "intrinsic-value": {
        "title": "What Does Intrinsic Value Mean? · VALUS",
        "h1":    "What does intrinsic value mean?",
        "desc":  "Intrinsic value is the present value of all the cash a business will generate for its owners over its lifetime — the price a rational investor would pay if they could see the future perfectly.",
        "body": [
            "Intrinsic value is the price an asset is actually worth based on its underlying fundamentals — what it produces, owns, and earns — independent of where the market happens to price it today. For a stock, intrinsic value is the present value of all future cash the business will generate for its owners.",
            "Benjamin Graham, the father of value investing, framed intrinsic value as a range rather than a single number. Two careful analysts looking at the same company will produce different intrinsic values because they make different assumptions about growth, margins, and risk. The point isn't to be exactly right; it's to be approximately right and to leave a margin of safety so that being wrong doesn't ruin you.",
            "VALUS estimates intrinsic value with a 10-year DCF model, then sanity-checks the result against analyst consensus and a reverse-DCF that solves for the implied growth rate the current price requires.",
        ],
    },
    "margin-of-safety": {
        "title": "What Is Margin of Safety? · VALUS",
        "h1":    "What is margin of safety?",
        "desc":  "Margin of safety is the gap between a stock's intrinsic value and its current market price — the buffer that protects you if your valuation assumptions turn out to be too optimistic.",
        "body": [
            "Margin of safety is the difference between what a stock is worth and what it costs, expressed as a percentage. If intrinsic value is $100 and the price is $70, the margin of safety is +30%. If intrinsic value is $100 and the price is $130, the margin of safety is −30%.",
            "The concept comes from Benjamin Graham, who argued that because all valuation depends on uncertain forecasts, you should only buy when the price is meaningfully below your estimate of fair value. The bigger the discount, the more your investment is protected if the business underperforms or your assumptions were too rosy.",
            "Different investors require different margins of safety. A diversified index investor needs less buffer; a concentrated value investor buying a single distressed company needs more. VALUS reports the margin of safety on every stock so you can see the cushion at a glance.",
        ],
    },
    "free-cash-flow": {
        "title": "What Is Free Cash Flow? · VALUS",
        "h1":    "What is free cash flow?",
        "desc":  "Free cash flow is the cash a business generates from operations after paying for the capital expenditure needed to maintain and grow the business — the cash actually available to shareholders.",
        "body": [
            "Free cash flow (FCF) is operating cash flow minus capital expenditure. It's the cash the business actually generates for its owners after funding the investments needed to keep operating and growing.",
            "Why FCF and not earnings? Earnings include non-cash items like depreciation and can be massaged through accounting choices. FCF is harder to fake because it tracks the actual movement of money. Warren Buffett's preferred valuation metric — owner earnings — is a refinement of free cash flow.",
            "VALUS uses trailing free cash flow as the input to its DCF model, smoothed across multiple years for companies with volatile FCF profiles.",
        ],
    },
    "wacc": {
        "title": "What Is WACC? · VALUS",
        "h1":    "What is WACC (weighted average cost of capital)?",
        "desc":  "WACC is the blended cost of all the capital a company uses — debt and equity — weighted by how much of each it has on its balance sheet. It's the discount rate used in DCF valuation.",
        "body": [
            "Weighted average cost of capital is the return investors require to fund a business, blending the cost of debt (after-tax interest) and the cost of equity (typically estimated via the Capital Asset Pricing Model). Each is weighted by its share of the company's total capital.",
            "WACC is the discount rate in a DCF model — it's how you translate future cash flows into today's dollars. A higher WACC means cash arriving in year 10 is worth less today; a lower WACC means it's worth more. WACC is the single most sensitive lever in any DCF.",
            "Higher-risk companies (early-stage, leveraged, cyclical) have higher WACCs; mature, predictable businesses have lower ones. VALUS estimates WACC from the company's balance sheet and equity beta, with a sector-typical fallback.",
        ],
    },
}


@app.route("/learn/<slug>")
def learn_page(slug):
    page = _LEARN_PAGES.get(slug)
    if not page:
        return redirect("/how-it-works", code=302)
    return render_template("learn.html", slug=slug, **page)


@app.route("/sitemap.xml")
def sitemap():
    """
    XML sitemap for crawlers. Lists every static SEO page plus the full
    DISCOVERY_TICKERS universe of /stocks/<ticker> pages — that's the
    long-tail surface area Google can index.
    """
    base = (request.url_root or "/").rstrip("/")
    today = date.today().isoformat()
    urls = ["/", "/how-it-works", "/methodology", "/docs", "/stocks"]
    urls += [f"/learn/{slug}" for slug in _LEARN_PAGES.keys()]
    urls += [f"/stocks/{t.lower()}" for t in sorted(DISCOVERY_TICKERS)]

    parts = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for u in urls:
        priority = "1.0" if u == "/" else ("0.8" if u.startswith("/stocks/") else "0.7")
        parts.append(
            f"<url><loc>{base}{u}</loc><lastmod>{today}</lastmod>"
            f"<changefreq>daily</changefreq><priority>{priority}</priority></url>"
        )
    parts.append("</urlset>")
    return app.response_class("".join(parts), mimetype="application/xml")


# ════════════════════════════════════════════════════════════════════════
# Auth — Google OAuth (optional, gracefully disabled if env vars missing)
# ════════════════════════════════════════════════════════════════════════

def require_user():
    """
    Returns (user_dict, error_response_or_None).
    user_dict has keys: sub, email, name, picture.
    """
    user = session.get("user")
    if not user:
        return None, (jsonify({
            "error": "sign-in required",
            "auth_configured": _GOOGLE_CONFIGURED,
        }), 401)
    return user, None


@app.route("/api/me")
def api_me():
    """
    Returns the current signed-in user (or null) plus a flag telling the
    frontend whether OAuth is configured at all (so it can show a useful
    message if a deploy forgot to set the env vars).
    """
    return jsonify({
        "user":             session.get("user"),
        "auth_configured":  _GOOGLE_CONFIGURED,
    })


@app.route("/auth/login")
def auth_login():
    if not _GOOGLE_CONFIGURED or not _oauth:
        return jsonify({
            "error": "Google OAuth not configured on this deployment.",
        }), 503
    # Preserve where to land after OAuth — defaults to root
    next_url = request.args.get("next", "/")
    session["auth_next"] = next_url
    redirect_uri = url_for("auth_callback", _external=True)
    return _oauth.google.authorize_redirect(redirect_uri)


@app.route("/auth/callback")
def auth_callback():
    if not _GOOGLE_CONFIGURED or not _oauth:
        return redirect("/?auth_error=not_configured")
    try:
        token = _oauth.google.authorize_access_token()
        # token contains 'userinfo' from the OIDC discovery flow
        userinfo = token.get("userinfo") or {}
        if not userinfo:
            # Fallback: hit the userinfo endpoint manually
            resp = _oauth.google.get("https://openidconnect.googleapis.com/v1/userinfo")
            userinfo = resp.json() if resp.ok else {}
        if not userinfo.get("sub"):
            return redirect("/?auth_error=no_userinfo")
        session["user"] = {
            "sub":     userinfo["sub"],
            "email":   userinfo.get("email"),
            "name":    userinfo.get("name") or userinfo.get("email", "").split("@")[0],
            "picture": userinfo.get("picture"),
        }
        session.permanent = True
        next_url = session.pop("auth_next", "/") or "/"
        # Sanity: only redirect to relative paths to prevent open-redirect
        if not next_url.startswith("/") or next_url.startswith("//"):
            next_url = "/"
        # Insert auth_ok=1 into the query string, NOT into the URL fragment.
        # If the user was on /#leaderboard when they signed in, naive
        # concatenation would produce "/#leaderboard?auth_ok=1" which the
        # browser treats as part of the fragment and breaks our hash-routing.
        if "#" in next_url:
            base, frag = next_url.split("#", 1)
            sep = "&" if "?" in base else "?"
            redirect_to = f"{base}{sep}auth_ok=1#{frag}"
        else:
            sep = "&" if "?" in next_url else "?"
            redirect_to = f"{next_url}{sep}auth_ok=1"
        return redirect(redirect_to)
    except Exception as e:
        # Log the full traceback so prod can diagnose. Common causes:
        # - SECRET_KEY mismatch between instances → state validation fails
        # - Redirect URI in Google Console doesn't match the one used here
        # - Clock skew on the lambda
        import traceback
        print(f"[valus] OAuth callback failed: {type(e).__name__}: {e}")
        traceback.print_exc()
        # Surface a hint in the URL so the frontend can show something
        # useful, but don't leak internals.
        reason = type(e).__name__.lower()
        return redirect(f"/?auth_error={reason}")


@app.route("/auth/logout", methods=["POST", "GET"])
def auth_logout():
    session.pop("user", None)
    session.pop("auth_next", None)
    if request.method == "GET":
        return redirect("/")
    return jsonify({"ok": True})


@app.route("/api/search")
@limiter.limit(limit_light)
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    try:
        url = (f"https://query2.finance.yahoo.com/v1/finance/search"
               f"?q={q}&quotesCount=7&newsCount=0&listsCount=0")
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        results = []
        for item in r.json().get("quotes", []):
            if item.get("quoteType", "") not in ("EQUITY", "ETF"):
                continue
            results.append({
                "symbol":   item.get("symbol", ""),
                "name":     item.get("longname") or item.get("shortname", ""),
                "exchange": item.get("exchDisp", ""),
                "type":     item.get("quoteType", ""),
            })
        return jsonify(results[:6])
    except Exception:
        return jsonify([])


@app.route("/api/history")
@limiter.limit(limit_light)
def history():
    ticker = request.args.get("ticker", "").strip().upper()
    period = request.args.get("period", "1y").lower()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    yf_period, yf_interval = PERIOD_MAP.get(period, ("1y", "1d"))
    try:
        hist = yf.Ticker(ticker).history(period=yf_period, interval=yf_interval)
        if hist.empty:
            return jsonify({"prices": []})
        is_intraday = yf_interval in ("5m", "30m")
        prices = []
        for idx, row in hist.iterrows():
            label = idx.strftime("%m/%d %H:%M") if is_intraday else str(idx.date())
            prices.append({
                "date":   label,
                "close":  round(float(row["Close"]), 2),
                "open":   round(float(row["Open"]),  2),
                "high":   round(float(row["High"]),  2),
                "low":    round(float(row["Low"]),   2),
                "volume": int(row["Volume"]) if not np.isnan(float(row["Volume"])) else 0,
            })
        return jsonify({"prices": prices, "period": period})
    except Exception as e:
        app.logger.exception("history fetch failed for %s", ticker)
        return jsonify({"error": "Couldn't load price history. Try again."}), 500


# ── Insider Activity (SEC Form 4) ─────────────────────────────────────────────
# Form 4 = statement of changes in beneficial ownership, filed within 2 business
# days of an insider trade. We pull the EDGAR Atom feed for the ticker filtered
# to type=4 and parse the most recent ~10 filings. Free, public-domain, fully
# compliant. Cached 6h.
_INSIDER_CACHE = {}
_INSIDER_CACHE_TTL = 6 * 3600


def _fetch_insider_form4(ticker: str, days: int = 90, max_items: int = 10):
    """
    Returns dict:
      {
        "buys": int, "sells": int,
        "net_value": float,         # signed USD, +ve = net buying
        "items": [{date, insider, type, shares, value, role}],
        "as_of": iso,
      }
    or None on failure / no Form 4 history.
    """
    if not ticker:
        return None
    t = ticker.upper().strip()
    cached = _INSIDER_CACHE.get(t)
    if cached and (time.time() - cached[0]) < _INSIDER_CACHE_TTL:
        return cached[1]

    cik_map = _sec_get_ticker_cik_map()
    cik = cik_map.get(t)
    if not cik:
        _INSIDER_CACHE[t] = (time.time(), None)
        return None

    result = None
    try:
        url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?"
            f"action=getcompany&CIK={cik:010d}&type=4&dateb=&owner=include&count=20&output=atom"
        )
        r = requests.get(url, headers={"User-Agent": _SEC_UA}, timeout=10)
        if r.status_code != 200:
            _INSIDER_CACHE[t] = (time.time(), None)
            return None

        ns = {"a": "http://www.w3.org/2005/Atom"}
        root = ET.fromstring(r.text)
        cutoff = (datetime.utcnow() - timedelta(days=days)).date()
        items = []
        buys = sells = 0
        net_value = 0.0

        for entry in root.findall("a:entry", ns)[: 2 * max_items]:
            updated_el = entry.find("a:updated", ns)
            title_el   = entry.find("a:title", ns)
            content_el = entry.find("a:content", ns)
            link_el    = entry.find("a:link", ns)

            if updated_el is None or title_el is None:
                continue
            try:
                d = datetime.fromisoformat(updated_el.text.replace("Z", "+00:00")).date()
            except Exception:
                continue
            if d < cutoff:
                continue

            # Title format: "4 - <Insider Name> (CIK) (Filer)"
            title = (title_el.text or "").strip()
            content = (content_el.text or "") if content_el is not None else ""

            # Best-effort extraction of insider name and role from content/title.
            insider = None
            role = None
            m_name = re.search(r"4 - ([^(]+)\(", title)
            if m_name:
                insider = m_name.group(1).strip()
            m_role = re.search(r"(Director|Officer|10% Owner|CEO|CFO|COO|President|Chairman)",
                               content, re.IGNORECASE)
            if m_role:
                role = m_role.group(1)

            href = link_el.get("href") if link_el is not None else None
            items.append({
                "date":    d.isoformat(),
                "insider": insider or "Insider",
                "role":    role,
                "url":     href,
            })

        if items:
            result = {
                "buys":      None,        # transaction-level detail requires the per-filing XML
                "sells":     None,
                "net_value": None,
                "items":     items[:max_items],
                "filings":   len(items),
                "as_of":     date.today().isoformat(),
            }
    except Exception:
        result = None
    _INSIDER_CACHE[t] = (time.time(), result)
    return result


@app.route("/api/insider")
@limiter.limit(limit_medium)
def api_insider():
    ticker = request.args.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    payload = _fetch_insider_form4(ticker)
    if not payload:
        return jsonify({"available": False, "reason": "No recent Form 4 filings or non-US filer."})
    payload["available"] = True
    return jsonify(payload)


@app.route("/api/valuation-history")
@limiter.limit(limit_medium)
def valuation_history():
    """
    Phase A: 5-year historical intrinsic value vs. price.
    Replays DCF at each annual cutoff using EDGAR point-in-time fundamentals,
    pairs with monthly close prices. US filers only.
    """
    ticker = request.args.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    try:
        info = {}
        sector = industry = ""
        try:
            t = yf.Ticker(ticker)
            info = t.info or {}
            sector   = info.get("sector")   or ""
            industry = info.get("industry") or ""
        except Exception:
            pass
        payload = get_valuation_history(
            ticker=ticker, info=info, fx_rate=1.0,
            sector=sector, industry=industry,
        )
        if not payload:
            return jsonify({
                "available": False,
                "reason": "Historical data unavailable — non-US filer or insufficient EDGAR coverage.",
            })
        payload["available"] = True
        return jsonify(payload)
    except Exception:
        app.logger.exception("valuation-history failed for %s", ticker)
        return jsonify({"error": "Couldn't build valuation history. Try again."}), 500


# ── Live quote endpoint (fast tier — price ticks only) ─────────────────────
# Lightweight companion to /api/analyze.  Uses yfinance fast_info["lastPrice"]
# which is dramatically cheaper than a full info dict pull (no DCF, no news,
# no balance sheet).  Designed for 30s polling on the heatmap and analyze
# view so investor demos look alive.  Multi-ticker via comma-separated query
# string; parallelized with the same ThreadPoolExecutor pattern as discover.
_QUOTE_CACHE = {}        # {ticker: (timestamp, payload)}
_QUOTE_CACHE_TTL_S = 30  # short TTL — this is a live-tick endpoint

@app.route("/api/quote")
@limiter.limit(limit_light)
def quote():
    raw = request.args.get("tickers") or request.args.get("ticker") or ""
    tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    if not tickers:
        return jsonify({"error": "tickers required"}), 400
    # Cap to a sane batch size to keep latency bounded
    tickers = tickers[:120]
    now = time.time()

    def _fetch(t):
        cached = _QUOTE_CACHE.get(t)
        if cached and (now - cached[0]) < _QUOTE_CACHE_TTL_S:
            return t, cached[1]
        try:
            fi = yf.Ticker(t).fast_info
            price      = float(fi.get("lastPrice") or fi.get("last_price") or 0) or None
            prev_close = float(fi.get("previousClose") or fi.get("previous_close") or 0) or None
            change_pct = None
            if price and prev_close and prev_close > 0:
                change_pct = round((price - prev_close) / prev_close * 100, 2)
            payload = {
                "price":            round(price, 4) if price else None,
                "prev_close":       round(prev_close, 4) if prev_close else None,
                "daily_change_pct": change_pct,
                "ts":               int(now),
            }
            _QUOTE_CACHE[t] = (now, payload)
            return t, payload
        except Exception:
            return t, None

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = dict(pool.map(_fetch, tickers))

    return jsonify({
        "quotes":       {t: p for t, p in results.items() if p is not None},
        "missed":       [t for t, p in results.items() if p is None],
        "generated_at": int(now),
    })


@app.route("/api/statements")
@limiter.limit(limit_medium)
def statements():
    ticker = request.args.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info
        financial_ccy = info.get("financialCurrency") or info.get("currency") or "USD"
        trading_ccy   = info.get("currency") or "USD"
        return jsonify(clean({
            "income":           filtered_df_to_rows(stock.income_stmt,   INCOME_ROWS),
            "balance":          filtered_df_to_rows(stock.balance_sheet, BALANCE_ROWS),
            "cashflow":         filtered_df_to_rows(stock.cashflow,      CASHFLOW_ROWS),
            "financialCurrency": financial_ccy,
            "tradingCurrency":   trading_ccy,
        }))
    except Exception as e:
        app.logger.exception("statements fetch failed for %s", ticker)
        return jsonify({"error": "Couldn't load financial statements. Try again."}), 500


# ── Analyze response cache (15-min TTL) ────────────────────────────────
# Repeated visits to the same ticker (e.g. portfolio refresh, share-link
# clicks, discovery heatmap) hit the cache for sub-100ms responses.
_ANALYZE_CACHE = {}      # {(ticker, params_str): (timestamp, response_dict)}
_ANALYZE_CACHE_TTL_S = 3600        # 1 hour default during market hours; market-aware TTL extends overnight to next 9:30 AM ET
_ANALYZE_CACHE_TTL_POPULAR_S = 7200  # 2 hours for popular mega-caps
_ANALYZE_CACHE_TTL_DISCOVERY_S = 93600  # 26 hours — bridges daily cron cycles
_ANALYZE_CACHE_MAX = 500     # cap to bound memory

# Mega-cap tickers that get cached longer — fundamentals barely move intraday
# and these account for the bulk of search traffic.
_POPULAR_TICKERS = {
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "NVDA",
    "BRK-B", "BRK-A", "JPM", "V", "MA", "UNH", "XOM", "JNJ", "WMT",
    "PG", "HD", "BAC", "AVGO", "LLY", "ORCL", "COST", "MRK", "PEP",
    "ABBV", "KO", "ADBE", "CRM", "AMD", "NFLX", "TMUS", "DIS", "CSCO",
    "INTC", "QCOM", "PFE", "T", "VZ", "BA", "GS", "MS", "C",
}

def _et_now():
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))

def _seconds_until_next_market_open() -> int:
    # Holidays not handled — extra cache lifetime over a holiday is harmless.
    now_et = _et_now()
    open_today = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    if now_et < open_today and now_et.weekday() < 5:
        target = open_today
    else:
        candidate = open_today + timedelta(days=1)
        while candidate.weekday() >= 5:
            candidate += timedelta(days=1)
        target = candidate
    return max(int((target - now_et).total_seconds()), 60)

# ── Anonymous-user search limit ───────────────────────────────────────────
# 3 unique tickers per IP per ET-day for users without a Google sign-in.
# Signed-in users skip this gate (10/day soft cap to be enforced later when
# the premium tier is wired up). Keyed by IP + ET date so the counter
# resets at midnight Eastern.
ANON_SEARCH_LIMIT = 3
_ANON_SEARCH_MEM: dict = {}  # in-memory fallback when _kv is unavailable

def _client_ip(req) -> str:
    fwd = req.headers.get("X-Forwarded-For") or ""
    if fwd:
        return fwd.split(",")[0].strip()
    return req.remote_addr or "0.0.0.0"

def _anon_search_key(ip: str) -> str:
    today = _et_now().date().isoformat()
    return f"valus:anon_search:{ip}:{today}"

def _anon_searches_today(ip: str) -> set:
    key = _anon_search_key(ip)
    if _kv:
        try:
            raw = _kv.get(key)
            if raw:
                return set(_json_top.loads(raw))
        except Exception:
            pass
    return set(_ANON_SEARCH_MEM.get(key, []))

def _record_anon_search(ip: str, ticker: str) -> None:
    key = _anon_search_key(ip)
    seen = _anon_searches_today(ip)
    seen.add(ticker)
    # Expire at midnight ET so the counter resets daily.
    et_now = _et_now()
    midnight = (et_now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    ttl = max(int((midnight - et_now).total_seconds()), 60)
    if _kv:
        try:
            _kv.setex(key, ttl, _json_top.dumps(sorted(seen)))
            return
        except Exception:
            pass
    _ANON_SEARCH_MEM[key] = list(seen)

def _check_anon_search_limit(req, ticker: str):
    """Returns None to allow, or (response, status) tuple to block."""
    # Internal callers (Discover treemap, hourly cron) bypass the gate.
    # They invoke analyze() via test_request_context with this header set.
    if req.headers.get("X-Valus-Internal") == "1":
        return None
    if session.get("user"):
        return None
    ip = _client_ip(req)
    seen = _anon_searches_today(ip)
    if ticker in seen:
        return None  # already counted today, allow re-fetch
    if len(seen) >= ANON_SEARCH_LIMIT:
        return (jsonify({
            "error":   "search_limit_anon",
            "limit":   ANON_SEARCH_LIMIT,
            "used":    len(seen),
            "message": f"You've used your {ANON_SEARCH_LIMIT} free searches today. "
                       "Sign in for 10 free searches per day plus portfolio tracking.",
        }), 429)
    _record_anon_search(ip, ticker)
    return None


def _is_market_hours() -> bool:
    now_et = _et_now()
    if now_et.weekday() >= 5:
        return False
    open_t  = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_t = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    return open_t <= now_et < close_t

def _market_aware_ttl(base_ttl: int) -> int:
    if _is_market_hours():
        return base_ttl
    return max(base_ttl, _seconds_until_next_market_open())

def _ttl_for_ticker(ticker):
    t = (ticker or "").upper()
    # Discovery tickers cache the longest — the daily cron rebuilds them, and
    # we want every viewer of the heatmap to hit the cache, not pay for compute.
    if t in globals().get("DISCOVERY_TICKERS_SET", set()):
        return _market_aware_ttl(_ANALYZE_CACHE_TTL_DISCOVERY_S)
    if t in _POPULAR_TICKERS:
        return _market_aware_ttl(_ANALYZE_CACHE_TTL_POPULAR_S)
    return _market_aware_ttl(_ANALYZE_CACHE_TTL_S)

def _analyze_cache_key(ticker, args):
    relevant = sorted((k, v) for k, v in args.items() if k != "ticker")
    return f"valus:analyze:{ticker}|{relevant}"

def _analyze_cache_get(key):
    # 1. Check Redis first (shared across all Vercel instances)
    if _kv:
        try:
            raw = _kv.get(key)
            if raw:
                return _json_top.loads(raw)
        except Exception:
            pass
    # 2. Fall back to in-process memory (local dev / single-instance)
    entry = _ANALYZE_CACHE.get(key)
    if entry is None: return None
    ts, payload = entry
    ticker = (payload.get("ticker") or "").upper() if isinstance(payload, dict) else ""
    ttl = _ttl_for_ticker(ticker) if ticker else _market_aware_ttl(_ANALYZE_CACHE_TTL_S)
    if time.time() - ts < ttl:
        return payload
    _ANALYZE_CACHE.pop(key, None)
    return None

def _analyze_cache_set(key, payload):
    # Determine TTL based on ticker (popular = 2h, others = 30m)
    ticker = (payload.get("ticker") or "").upper() if isinstance(payload, dict) else ""
    ttl = _ttl_for_ticker(ticker) if ticker else _ANALYZE_CACHE_TTL_S
    # Write to Redis (persistent, shared)
    if _kv:
        try:
            _kv.setex(key, ttl, _json_top.dumps(payload))
        except Exception:
            pass
    # Always write to in-process memory too (zero-latency hit on same instance)
    _ANALYZE_CACHE[key] = (time.time(), payload)
    if len(_ANALYZE_CACHE) > _ANALYZE_CACHE_MAX:
        oldest = min(_ANALYZE_CACHE.keys(), key=lambda k: _ANALYZE_CACHE[k][0])
        _ANALYZE_CACHE.pop(oldest, None)


# ── Discovery: pre-curated S&P 500 sample, grouped by sector ──────────────
# Picked to span every major sector at meaningful market-cap.  When the
# /api/discover endpoint is hit, each ticker is run through the cached
# analyze pipeline and a thin summary is returned.
DISCOVERY_TICKERS = [
    # ── Tech — mega + semis + software ────────────────────────────────────
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    "ORCL", "CRM", "ADBE", "AMD", "AVGO", "QCOM", "TXN",
    "MU", "ARM", "NOW", "PLTR", "PANW", "CRWD", "DDOG",
    "NET", "SHOP", "SQ", "PYPL", "UBER",
    "ABNB", "RBLX", "INTU", "ADP", "IBM", "CSCO",
    # ── Strategic semis ───────────────────────────────────────────────────
    "AMAT", "KLAC", "LRCX", "ASML", "MRVL", "TSM",
    # ── Financials — banks, payments, brokers ─────────────────────────────
    "JPM", "BAC", "WFC", "GS", "MS", "C",
    "BRK-B", "V", "MA", "AXP",
    "SCHW", "BLK", "SPGI", "ICE", "CME",
    "COIN", "HOOD",
    # ── Healthcare — pharma + medtech ─────────────────────────────────────
    "JNJ", "UNH", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT",
    "ISRG", "VRTX", "AMGN", "GILD", "BMY",
    "ELV", "CVS", "MDT", "SYK",
    # ── Consumer staples + discretionary ──────────────────────────────────
    "WMT", "HD", "MCD", "NKE", "KO", "PEP", "SBUX", "COST",
    "LULU", "BKNG", "TJX", "PG", "MO", "MDLZ",
    # ── Auto / Mobility ───────────────────────────────────────────────────
    "F", "GM", "RIVN",
    # ── Energy / Materials ────────────────────────────────────────────────
    "XOM", "CVX", "COP", "OXY", "EOG", "SLB",
    "FCX", "LIN", "NEM",
    # ── Industrials / Defense / Aero ──────────────────────────────────────
    "CAT", "BA", "GE", "UNP", "RTX", "HON", "LMT", "DE",
    "NOC", "GD", "LHX", "FDX", "UPS",
    # ── Communications / Streaming / Media ────────────────────────────────
    "DIS", "NFLX", "T", "VZ", "TMUS", "SPOT",
    # ── Real Estate / Utilities ───────────────────────────────────────────
    "AMT", "PLD", "EQIX", "NEE", "DUK", "CEG", "VST",
    # ── Critical Materials & Nuclear ──────────────────────────────────────
    "MP", "LEU", "BWXT", "CCJ",
    # ── Urban Air Mobility ────────────────────────────────────────────────
    "JOBY", "ACHR", "RKLB",
    # ── Crypto-adjacent ───────────────────────────────────────────────────
    "MSTR",
]
DISCOVERY_TICKERS_SET = set(DISCOVERY_TICKERS)

@app.route("/api/discover")
@limiter.limit(limit_discover)
def discover():
    """
    Returns a thin summary for every ticker in DISCOVERY_TICKERS using the
    cached analyze pipeline.  Each entry carries a `cached_at` timestamp
    so the UI can show freshness ("Updated 4m ago") and users understand
    that values may slightly differ from a real-time analyze call.

    Stale-while-revalidate: by default any cache entry counts (even past
    the 15-min TTL) so the heatmap returns sub-second once the daily cron
    has warmed the cache.  Pass `?fresh=true` (used by the manual ↻ Refresh
    button) to force a re-fetch of expired entries.
    """
    now = time.time()
    force_fresh = request.args.get("fresh", "").lower() in ("1", "true", "yes")

    def _fetch_one(t):
        try:
            ck = _analyze_cache_key(t, {})
            entry = _ANALYZE_CACHE.get(ck)
            d = None
            cached_at_age = None
            if entry is not None:
                ts, payload = entry
                age = now - ts
                # Stale-while-revalidate: serve any cached entry by default;
                # only refetch if expired AND caller passed ?fresh=true.
                if age < _ANALYZE_CACHE_TTL_S or not force_fresh:
                    d = payload
                    cached_at_age = int(age)

            if d is None:
                with app.test_request_context(
                    f"/api/analyze?ticker={t}",
                    headers={"X-Valus-Internal": "1"},
                ):
                    resp = analyze()
                if isinstance(resp, tuple):
                    resp = resp[0]
                d = resp.get_json() if hasattr(resp, "get_json") else None
                cached_at_age = 0

            if not d or d.get("error"):
                return None
            return {
                "ticker":      d.get("ticker") or t,
                "name":        d.get("company_name"),
                "sector":      d.get("sector"),
                "industry":    d.get("industry"),
                "price":       d.get("current_price"),
                "iv":          d.get("intrinsic_value"),
                "mos":         d.get("margin_of_safety"),
                "tier":        (d.get("priced_for") or {}).get("tier"),
                "label":       (d.get("priced_for") or {}).get("label"),
                "is_etf":      bool(d.get("is_etf")),
                "extreme":     bool(d.get("extreme_mos_flag")),
                "age_seconds":     cached_at_age,
                "is_strategic":    bool(d.get("is_strategic")),
                "strategic_tier":  d.get("strategic_tier"),
                "strategic_label": d.get("strategic_label"),
                "policy_tailwind": bool(d.get("policy_tailwind")),
                "policy_headwind": bool(d.get("policy_headwind")),
                # Treemap-relevant: cell sizing (sqrt of mcap) + secondary
                # daily-move signal.  Both already computed inside analyze.
                "market_cap":        d.get("market_cap"),
                "prev_close":        d.get("previous_close"),
                "daily_change_pct":  d.get("daily_change_pct"),
                "iv_confidence":     d.get("iv_confidence"),
                "iv_source_label":   d.get("iv_source_label"),
            }
        except Exception:
            return None

    from concurrent.futures import ThreadPoolExecutor
    # Bumped from 10 → 16 workers.  yfinance is I/O-bound (HTTP calls),
    # not CPU-bound, so the GIL doesn't matter — more concurrency
    # directly cuts cold-cache latency.  Yahoo's rate limits tolerate
    # this fine for our single-burst workload.
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(_fetch_one, DISCOVERY_TICKERS))

    out = [r for r in results if r is not None]
    return jsonify({
        "items": out,
        "count": len(out),
        "universe_size": len(DISCOVERY_TICKERS),
        "generated_at": int(now),
    })


# ── Hourly cron: full DCF + news refresh of the discovery universe ─────────
# Vercel Cron Job hits this every hour to keep _ANALYZE_CACHE warm with
# fresh DCF + news + strategic-asset evaluation across all 100+ heatmap
# tickers.  Without this, Vercel's stateless serverless instances would
# always start cold; this endpoint IS the freshness guarantee for the
# heatmap when no users are actively browsing.
#
# Auth: protected by CRON_SECRET env var.  Vercel injects the secret into
# the cron request as `Authorization: Bearer <CRON_SECRET>`.  Returns 401
# if missing — prevents random callers from triggering a 25s lambda.
@app.route("/api/cron/refresh-heatmap")
def cron_refresh_heatmap():
    expected = os.environ.get("CRON_SECRET")
    # Fail closed: if the secret isn't configured, refuse to run.
    # Otherwise anyone could trigger a 25s lambda re-fetching 100+ tickers.
    if not expected:
        return jsonify({"error": "CRON_SECRET not configured on server"}), 500
    auth = request.headers.get("Authorization", "")
    if not (auth == f"Bearer {expected}" or
            request.args.get("secret") == expected):
        return jsonify({"error": "unauthorized"}), 401

    started = time.time()
    refreshed = 0
    failed = []

    def _force_refresh(t):
        try:
            ck = _analyze_cache_key(t, {})
            # Evict any existing cache entry (both in-memory AND Redis) —
            # the cron IS the freshness guarantee, so we don't trust stale.
            _ANALYZE_CACHE.pop(ck, None)
            if _kv:
                try: _kv.delete(ck)
                except Exception: pass
            with app.test_request_context(
                f"/api/analyze?ticker={t}",
                headers={"X-Valus-Internal": "1"},
            ):
                resp = analyze()
            if isinstance(resp, tuple):
                resp = resp[0]
            d = resp.get_json() if hasattr(resp, "get_json") else None
            if d and not d.get("error"):
                return t, True
            return t, False
        except Exception:
            return t, False

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(_force_refresh, DISCOVERY_TICKERS))

    for t, ok in results:
        if ok:
            refreshed += 1
        else:
            failed.append(t)

    duration_ms = int((time.time() - started) * 1000)
    app.logger.info(
        "cron refresh-heatmap: %d/%d ok in %dms",
        refreshed, len(DISCOVERY_TICKERS), duration_ms
    )
    return jsonify({
        "refreshed":   refreshed,
        "universe":    len(DISCOVERY_TICKERS),
        "failed":      failed,
        "duration_ms": duration_ms,
    })


# ── Leaderboard / shared portfolios (soft sign-in via display name) ───
import os, json as _json, uuid

# Storage strategy:
#   1. Vercel KV via redis-py if KV_URL is set (durable, multi-instance).
#   2. /tmp/.valus_leaderboard.json as a local-dev / single-instance fallback.
#   3. Process-level in-memory list as the final fallback.
LEADERBOARD_FILE = "/tmp/.valus_leaderboard.json"
LEADERBOARD_KEY  = "valus:leaderboard:v1"
_LEADERBOARD_MEM = []

def _read_leaderboard():
    # KV first (multi-instance durability)
    raw = kv_get(LEADERBOARD_KEY)
    if raw:
        try: return _json.loads(raw)
        except Exception: pass
    # /tmp file
    if os.path.exists(LEADERBOARD_FILE):
        try:
            with open(LEADERBOARD_FILE) as f:
                return _json.load(f)
        except Exception:
            pass
    return list(_LEADERBOARD_MEM)

def _write_leaderboard(entries):
    global _LEADERBOARD_MEM
    _LEADERBOARD_MEM = list(entries)
    serialized = _json.dumps(entries)
    # KV first — write-through
    kv_set(LEADERBOARD_KEY, serialized)
    # /tmp mirror so local dev still works without KV
    try:
        with open(LEADERBOARD_FILE, "w") as f:
            f.write(serialized)
    except Exception:
        pass


@app.route("/api/leaderboard/submit", methods=["POST"])
def leaderboard_submit():
    """
    Submit a portfolio to the public leaderboard.  Requires sign-in.
    Body: { name: str (optional override), tickers: [str], note: str }
    Entries are keyed to the OAuth `sub` so re-publishing replaces the
    user's previous entry.
    """
    user, err = require_user()
    if err: return err

    body = request.get_json(silent=True) or {}
    name = ((body.get("name") or "").strip()
            or (user.get("name") or "").strip()
            or (user.get("email") or "").split("@")[0])[:40]
    tickers = body.get("tickers") or []
    note = (body.get("note") or "").strip()[:200]

    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()][:50]
    if not tickers:
        return jsonify({"error": "At least one ticker required"}), 400
    if not name:
        return jsonify({"error": "Display name is required"}), 400

    entries = _read_leaderboard()
    # Replace prior submission keyed to this user (Google `sub`).
    # Also drop legacy entries that match the soft user_token from the
    # claim flow if it was provided, so a user upgrading from soft-auth
    # doesn't end up with two entries.
    legacy_token = (body.get("legacy_user_token") or "").strip()[:64]
    entries = [
        e for e in entries
        if e.get("user_sub") != user["sub"]
        and (not legacy_token or e.get("user_token") != legacy_token)
    ]
    entries.append({
        "id":           uuid.uuid4().hex[:10],
        "name":         name,
        "user_sub":     user["sub"],
        "user_picture": user.get("picture"),
        "tickers":      tickers,
        "note":         note,
        "submitted_at": time.time(),
    })
    entries = sorted(entries, key=lambda e: -e.get("submitted_at", 0))[:200]
    _write_leaderboard(entries)
    return jsonify({"ok": True, "count": len(entries)})


@app.route("/api/leaderboard/delete", methods=["POST"])
def leaderboard_delete():
    user, err = require_user()
    if err: return err
    entries = _read_leaderboard()
    entries = [e for e in entries if e.get("user_sub") != user["sub"]]
    _write_leaderboard(entries)
    return jsonify({"ok": True})


PORTFOLIO_FILE = "/tmp/.valus_portfolios.json"
PORTFOLIO_KEY  = "valus:portfolios:v1"
_PORTFOLIOS_MEM = {}

def _read_portfolios():
    raw = kv_get(PORTFOLIO_KEY)
    if raw:
        try: return _json.loads(raw)
        except Exception: pass
    if os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE) as f:
                return _json.load(f)
        except Exception:
            pass
    return dict(_PORTFOLIOS_MEM)

def _write_portfolios(d):
    global _PORTFOLIOS_MEM
    _PORTFOLIOS_MEM = dict(d)
    serialized = _json.dumps(d)
    kv_set(PORTFOLIO_KEY, serialized)
    try:
        with open(PORTFOLIO_FILE, "w") as f:
            f.write(serialized)
    except Exception:
        pass


@app.route("/api/portfolio", methods=["GET"])
def portfolio_get():
    """Return the signed-in user's saved portfolio (list of ticker snapshots)."""
    user, err = require_user()
    if err: return err
    portfolios = _read_portfolios()
    items = portfolios.get(user["sub"], [])
    return jsonify({"items": items, "updated_at": portfolios.get(f"{user['sub']}__ts")})


@app.route("/api/portfolio", methods=["POST"])
def portfolio_save():
    """
    Persist the user's portfolio server-side. Body: {items: [...]}.
    Each item is a thin snapshot {ticker, name, sector, price, iv, mos, tier, addedAt}.
    Server caps the list at 100 entries and trims fields to safe sizes.
    """
    user, err = require_user()
    if err: return err
    body = request.get_json(silent=True) or {}
    raw = body.get("items") or []
    if not isinstance(raw, list):
        return jsonify({"error": "items must be a list"}), 400

    cleaned = []
    seen = set()
    for it in raw[:100]:
        if not isinstance(it, dict): continue
        t = str(it.get("ticker") or "").strip().upper()[:12]
        if not t or t in seen: continue
        seen.add(t)
        cleaned.append({
            "ticker":  t,
            "name":    str(it.get("name")  or "")[:120],
            "sector":  str(it.get("sector") or "")[:60],
            "price":   it.get("price")  if isinstance(it.get("price"),  (int, float)) else None,
            "iv":      it.get("iv")     if isinstance(it.get("iv"),     (int, float)) else None,
            "mos":     it.get("mos")    if isinstance(it.get("mos"),    (int, float)) else None,
            "tier":    str(it.get("tier") or "")[:32],
            "addedAt": it.get("addedAt") if isinstance(it.get("addedAt"), (int, float)) else None,
        })

    portfolios = _read_portfolios()
    portfolios[user["sub"]]            = cleaned
    portfolios[f"{user['sub']}__ts"]   = time.time()
    _write_portfolios(portfolios)
    return jsonify({"ok": True, "count": len(cleaned)})


# ── Portfolio Templates: Strategies (client-side) + Investor 13F ──────────
# A curated registry of well-known investment managers. CIKs are SEC-assigned
# and stable. Pulling 13F-HR filings live from EDGAR keeps holdings fresh
# without us maintaining a manual mirror.
INVESTOR_REGISTRY = [
    {"cik": 1067983, "name": "Berkshire Hathaway",      "manager": "Warren Buffett",
     "blurb": "Concentrated, long-duration value with a quality bias."},
    {"cik": 1336528, "name": "Pershing Square",         "manager": "Bill Ackman",
     "blurb": "High-conviction activism — typically 8–12 names."},
    {"cik": 1649339, "name": "Scion Asset Management",  "manager": "Michael Burry",
     "blurb": "Contrarian deep-value, often hedged or short-biased."},
    {"cik": 1061165, "name": "Baupost Group",           "manager": "Seth Klarman",
     "blurb": "Margin-of-safety value, distressed and special situations."},
    {"cik": 1079114, "name": "Greenlight Capital",      "manager": "David Einhorn",
     "blurb": "Long/short value with rigorous fundamental research."},
    {"cik": 1173334, "name": "Pabrai Investment Funds", "manager": "Mohnish Pabrai",
     "blurb": "Few bets, big bets, infrequent bets — Buffett-school value."},
    {"cik": 1656456, "name": "Appaloosa LP",            "manager": "David Tepper",
     "blurb": "Macro-aware distressed and event-driven equities."},
    {"cik": 1040273, "name": "Third Point",             "manager": "Dan Loeb",
     "blurb": "Activist + event-driven — pushes for catalysts."},
]

INVESTOR_CIK_SET = {row["cik"] for row in INVESTOR_REGISTRY}

# 24h cache for 13F holdings.  Keyed by CIK.
TEMPLATES_13F_TTL = 24 * 3600
_TEMPLATES_13F_MEM = {}   # cik -> (ts, payload)
TEMPLATES_13F_KEY = "valus:templates:13f:v1"

# CUSIP→ticker is the missing link in 13F (filings list CUSIP, not ticker).
# We invert the SEC ticker map at first use, then memoize. Coverage is good
# for common large-caps (the universe most managers in our registry hold).
_CUSIP_TICKER_CACHE = {}

def _build_cik_to_ticker():
    """Reverse the SEC ticker→CIK map. Useful for filing CIK lookups."""
    return {cik: tkr for tkr, cik in _sec_get_ticker_cik_map().items()}


def _13f_kv_get(cik):
    raw = kv_get(f"{TEMPLATES_13F_KEY}:{cik}")
    if not raw:
        return None
    try:
        return _json_top.loads(raw)
    except Exception:
        return None


def _13f_kv_set(cik, payload):
    try:
        kv_set(f"{TEMPLATES_13F_KEY}:{cik}", _json_top.dumps(payload), ttl=TEMPLATES_13F_TTL)
    except Exception:
        pass


def _fetch_latest_13f_accession(cik):
    """Find the most recent 13F-HR accession number for a CIK, or None."""
    url = f"https://data.sec.gov/submissions/CIK{cik:010d}.json"
    r = requests.get(url, headers={"User-Agent": _SEC_UA}, timeout=10)
    if r.status_code != 200:
        return None
    j = r.json() or {}
    recent = (j.get("filings") or {}).get("recent") or {}
    forms     = recent.get("form") or []
    accs      = recent.get("accessionNumber") or []
    pdocs     = recent.get("primaryDocument") or []
    fdates    = recent.get("filingDate") or []
    for i, form in enumerate(forms):
        if form == "13F-HR":
            return {
                "accession": accs[i],
                "primary":   pdocs[i] if i < len(pdocs) else None,
                "filed":     fdates[i] if i < len(fdates) else None,
            }
    return None


def _fetch_13f_information_table(cik, accession):
    """
    Locate and parse the 13F informationtable.xml for a given filing.
    Returns list of {cusip, name, value, shares} sorted by value desc.
    """
    acc_nodash = accession.replace("-", "")
    # The filing-index lists every document. We want the *.xml that is the
    # information table (not the primary 13F-HR cover XML).
    idx_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
        f"&CIK={cik:010d}&type=13F-HR&dateb=&owner=include&count=10"
    )
    # Easier: use the filing's index.json directly.
    idx_json_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/index.json"
    )
    r = requests.get(idx_json_url, headers={"User-Agent": _SEC_UA}, timeout=10)
    if r.status_code != 200:
        return []
    items = ((r.json() or {}).get("directory") or {}).get("item") or []
    info_xml = None
    for it in items:
        nm = (it.get("name") or "").lower()
        if nm.endswith(".xml") and "primary_doc" not in nm:
            info_xml = it.get("name")
            break
    if not info_xml:
        return []
    xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{info_xml}"
    rx = requests.get(xml_url, headers={"User-Agent": _SEC_UA}, timeout=15)
    if rx.status_code != 200:
        return []
    rows = []
    try:
        # Strip XML namespaces for simpler parsing — 13F XML uses
        # `ns1:infoTable` etc., which would otherwise force xpath gymnastics.
        text = re.sub(r'\sxmlns(:\w+)?="[^"]+"', "", rx.text, count=0)
        text = re.sub(r"<(/?)\w+:", r"<\1", text)
        root = ET.fromstring(text)
        for it in root.findall("infoTable"):
            name   = (it.findtext("nameOfIssuer") or "").strip()
            cusip  = (it.findtext("cusip") or "").strip().upper()
            value  = it.findtext("value")
            sh_el  = it.find("shrsOrPrnAmt")
            shares = sh_el.findtext("sshPrnamt") if sh_el is not None else None
            try: value_f = float(value) if value else 0.0
            except Exception: value_f = 0.0
            try: shares_f = float(shares) if shares else 0.0
            except Exception: shares_f = 0.0
            rows.append({
                "cusip":  cusip,
                "name":   name,
                # Pre-2022 filings reported value in $thousands; modern
                # filings (post 2022 Q3) report in actual dollars. Normalize
                # by detecting magnitude: $thousands sums for a $100B fund
                # would round to ~1e8. Keep raw — frontend shows percentages.
                "value":  value_f,
                "shares": shares_f,
            })
    except Exception:
        return []
    rows.sort(key=lambda r: r["value"], reverse=True)
    return rows


_SEC_NAME_TICKER_MAP = {}     # normalized-name -> ticker
_SEC_NAME_TICKER_TS  = 0
_SEC_NAME_TICKER_TTL = 24 * 3600


def _norm_company_name(s):
    """Normalize company names for fuzzy matching across data sources."""
    if not s: return ""
    s = s.upper()
    # Strip common suffixes that vary across filings (INC vs INC. vs CORP).
    s = re.sub(r"[.,&'/\\\\\\(\\)]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for suf in (" CORPORATION", " CORP", " INCORPORATED", " INC",
                " COMPANY", " CO", " LIMITED", " LTD", " HOLDINGS",
                " HLDGS", " GROUP", " CLASS A", " CLASS B", " CLASS C",
                " COM", " COMMON STOCK", " THE"):
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    return s


def _sec_get_name_ticker_map():
    """SEC company name → ticker, derived from company_tickers.json. ~10k US filers."""
    global _SEC_NAME_TICKER_MAP, _SEC_NAME_TICKER_TS
    if _SEC_NAME_TICKER_MAP and (time.time() - _SEC_NAME_TICKER_TS) < _SEC_NAME_TICKER_TTL:
        return _SEC_NAME_TICKER_MAP
    try:
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": _SEC_UA},
            timeout=10,
        )
        if r.status_code == 200:
            j = r.json() or {}
            m = {}
            for v in j.values():
                title = v.get("title")
                tkr   = v.get("ticker")
                if not (title and tkr): continue
                m[_norm_company_name(title)] = tkr.upper()
            _SEC_NAME_TICKER_MAP = m
            _SEC_NAME_TICKER_TS = time.time()
    except Exception:
        pass
    return _SEC_NAME_TICKER_MAP or {}


def _resolve_cusip_to_ticker(cusip, name_hint=None):
    """
    Best-effort CUSIP→ticker. We don't have a CUSIP master file (those are
    licensed), so we fall back to matching the issuer name from the 13F
    against SEC's company_tickers map. Cached per-CUSIP.
    """
    cache_key = f"{cusip}|{name_hint or ''}"
    if cache_key in _CUSIP_TICKER_CACHE:
        return _CUSIP_TICKER_CACHE[cache_key]

    ticker = None
    if name_hint:
        name_map = _sec_get_name_ticker_map()
        norm = _norm_company_name(name_hint)
        # Exact normalized match first
        ticker = name_map.get(norm)
        # Fall back to: 13F name as a prefix of an SEC title (or vice versa)
        if not ticker and norm:
            for k, v in name_map.items():
                if not k: continue
                if k == norm or k.startswith(norm + " ") or norm.startswith(k + " "):
                    ticker = v
                    break
    _CUSIP_TICKER_CACHE[cache_key] = ticker
    return ticker


@app.route("/api/templates/investors")
def templates_investors():
    """Return the curated investor registry (no holdings yet)."""
    return jsonify({"investors": INVESTOR_REGISTRY})


@app.route("/api/templates/13f/<int:cik>")
def templates_13f(cik):
    """
    Return the latest 13F-HR top holdings for a registered manager.
    Cached 24h. CIK must be in INVESTOR_REGISTRY (avoids open-proxy abuse).
    """
    if cik not in INVESTOR_CIK_SET:
        return jsonify({"error": "Unknown investor"}), 404

    # Memory cache first
    cached = _TEMPLATES_13F_MEM.get(cik)
    if cached and (time.time() - cached[0]) < TEMPLATES_13F_TTL:
        return jsonify(cached[1])
    # Then KV
    kv_cached = _13f_kv_get(cik)
    if kv_cached and (time.time() - (kv_cached.get("_ts") or 0)) < TEMPLATES_13F_TTL:
        _TEMPLATES_13F_MEM[cik] = (time.time(), kv_cached)
        return jsonify(kv_cached)

    filing = _fetch_latest_13f_accession(cik)
    if not filing:
        # Serve any stale cache if available — better than a hard error.
        if kv_cached:
            return jsonify(kv_cached)
        return jsonify({"error": "No recent 13F-HR filing found"}), 404

    rows = _fetch_13f_information_table(cik, filing["accession"])
    if not rows:
        if kv_cached:
            return jsonify(kv_cached)
        return jsonify({"error": "Could not parse 13F holdings"}), 502

    total_value = sum(r["value"] for r in rows) or 1.0
    top = rows[:25]

    # Resolve CUSIPs to tickers (best-effort, cached). Limit lookups to top N
    # so a cold cache doesn't fan out 200 requests to EDGAR.
    enriched = []
    for r in top:
        tkr = _resolve_cusip_to_ticker(r["cusip"], name_hint=r["name"])
        enriched.append({
            "ticker":      tkr,
            "name":        r["name"],
            "cusip":       r["cusip"],
            "value":       r["value"],
            "shares":      r["shares"],
            "weight_pct":  round((r["value"] / total_value) * 100.0, 2),
        })

    investor = next((i for i in INVESTOR_REGISTRY if i["cik"] == cik), None)
    payload = {
        "cik":         cik,
        "investor":    investor,
        "filed":       filing.get("filed"),
        "accession":   filing.get("accession"),
        "holdings":    enriched,
        "total_value": total_value,
        "_ts":         time.time(),
    }
    _TEMPLATES_13F_MEM[cik] = (time.time(), payload)
    _13f_kv_set(cik, payload)
    return jsonify(payload)


@app.route("/api/leaderboard/claim", methods=["POST"])
def leaderboard_claim():
    """
    Re-key any leaderboard entries from the legacy soft-auth user_token
    to the now-signed-in OAuth sub.  Called once on first sign-in.
    """
    user, err = require_user()
    if err: return err
    body = request.get_json(silent=True) or {}
    legacy_token = (body.get("user_token") or "").strip()[:64]
    if not legacy_token:
        return jsonify({"ok": True, "claimed": 0})
    entries = _read_leaderboard()
    claimed = 0
    for e in entries:
        if e.get("user_token") == legacy_token and not e.get("user_sub"):
            e["user_sub"]     = user["sub"]
            e["user_picture"] = user.get("picture")
            # Keep legacy display name unless it's empty
            e["name"]         = e.get("name") or user.get("name", "")
            e.pop("user_token", None)
            claimed += 1
    if claimed:
        _write_leaderboard(entries)
    return jsonify({"ok": True, "claimed": claimed})


@app.route("/api/leaderboard")
def leaderboard():
    """
    Returns top portfolios with computed MOS metrics.  Heavy lifting is
    cached — each portfolio entry computes avg_mos / undervalued_count
    by re-using the analyze cache for each ticker.  Sorted by score
    (avg_mos by default, can be tweaked via ?sort=).
    """
    sort = request.args.get("sort", "avg_mos")
    raw = _read_leaderboard()
    enriched = []
    for entry in raw:
        mos_vals = []
        details = []
        for tk in entry.get("tickers", []):
            ck = _analyze_cache_key(tk, {})
            ent = _ANALYZE_CACHE.get(ck)
            if not ent: continue
            ts, payload = ent
            if time.time() - ts > _ANALYZE_CACHE_TTL_S: continue
            mos = payload.get("margin_of_safety")
            tier = (payload.get("priced_for") or {}).get("tier")
            details.append({"ticker": tk, "mos": mos, "tier": tier})
            if mos is not None and not payload.get("extreme_mos_flag"):
                mos_vals.append(mos)
        avg_mos = (sum(mos_vals) / len(mos_vals)) if mos_vals else None
        under_count = sum(1 for v in mos_vals if v > 5)
        enriched.append({
            "id":              entry["id"],
            "name":            entry["name"],
            "user_sub":        entry.get("user_sub"),       # for "MINE" detection
            "user_picture":    entry.get("user_picture"),   # avatar in row
            "tickers":         entry["tickers"],
            "note":            entry.get("note", ""),
            "submitted_at":    entry["submitted_at"],
            "avg_mos":         round(avg_mos, 1) if avg_mos is not None else None,
            "undervalued_count": under_count,
            "ticker_count":    len(entry["tickers"]),
            "details":         details,
        })

    if sort == "recent":
        enriched.sort(key=lambda e: -e.get("submitted_at", 0))
    elif sort == "size":
        enriched.sort(key=lambda e: -(e.get("ticker_count") or 0))
    else:    # avg_mos default
        enriched.sort(key=lambda e: -(e.get("avg_mos") if e.get("avg_mos") is not None else -999))
    return jsonify({"items": enriched[:50], "total": len(enriched)})


@app.route("/api/analyze")
@limiter.limit(limit_analyze)
def analyze():
    ticker = request.args.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "Ticker is required"}), 400

    # Anonymous-user daily search gate (3/day per IP). Signed-in users skip.
    gate = _check_anon_search_limit(request, ticker)
    if gate is not None:
        return gate

    # Cache lookup
    _ck = _analyze_cache_key(ticker, dict(request.args))
    _cached = _analyze_cache_get(_ck)
    if _cached is not None:
        resp = jsonify(_cached)
        resp.headers["X-Valus-Cache"] = "HIT"
        return resp

    try:
        # ── User overrides ────────────────────────────────────────────────────
        s1_ov = request.args.get("growth1")
        s2_ov = request.args.get("growth2")
        tg    = float(request.args.get("terminal", 0.03))
        blended = None   # populated below when dcf_available; surfaced in payload
        yrs   = int(request.args.get("years", 10))
        tg    = min(max(tg, 0.01), 0.05)
        yrs   = min(max(yrs, 5), 15)

        # ── Data pull ─────────────────────────────────────────────────────────
        stock       = yf.Ticker(ticker)
        info        = stock.info
        price       = safe(info.get("currentPrice") or info.get("regularMarketPrice"))

        if not price:
            # Helpful suggestion for known renames / common mistakes
            renames = {"SQ": "XYZ", "FB": "META", "TWTR": "X"}
            hint = (f" Did you mean '{renames[ticker]}'?" if ticker in renames else "")
            return jsonify({
                "error": f"No data for '{ticker}'. Check the symbol.{hint}"
            }), 404

        # ── ETF / Index detection ─────────────────────────────────────────────
        # ETFs and indexes don't have FCF/DCF — return a structured "ETF view"
        # response with metadata + price history that the frontend can display
        # nicely, instead of failing silently with a blank IV.
        quote_type = (info.get("quoteType") or "").upper()
        if quote_type in ("ETF", "MUTUALFUND", "INDEX"):
            ph = []
            try:
                hist = stock.history(period="1y")
                if not hist.empty:
                    ph = [{"date": d.strftime("%Y-%m-%d"), "close": float(c)}
                          for d, c in hist["Close"].dropna().items()]
            except Exception:
                pass
            etf_payload = {
                "ticker":        ticker,
                "is_etf":        True,
                "company_name":  info.get("longName") or info.get("shortName") or ticker,
                "current_price": round(float(price), 2),
                "52w_high":      safe(info.get("fiftyTwoWeekHigh")),
                "52w_low":       safe(info.get("fiftyTwoWeekLow")),
                "market_cap":    safe(info.get("totalAssets") or info.get("marketCap")),
                "ytd_return":    safe(info.get("ytdReturn")),
                "expense_ratio": safe(info.get("annualReportExpenseRatio")),
                "category":      info.get("category") or quote_type.title(),
                "fund_family":   info.get("fundFamily"),
                "asset_class":   "ETF" if quote_type == "ETF" else quote_type.title(),
                "price_history": ph,
                "etf_message":   ("DCF valuation does not apply to ETFs. ETFs hold a "
                                  "basket of assets — to value an ETF, look at the "
                                  "weighted intrinsic value of its underlying holdings."),
            }
            _analyze_cache_set(_ck, etf_payload)
            return jsonify(etf_payload)

        cashflow      = stock.cashflow
        income_stmt   = stock.income_stmt
        balance_sheet = stock.balance_sheet    # annual — for FIN 415 PPE lookup

        # ── Currency handling ─────────────────────────────────────────────────
        # yfinance returns financial statements in the company's REPORTING currency
        # (financialCurrency) but stock price / market cap are in TRADING currency.
        # For foreign ADRs (e.g. NOK reports in EUR, trades in USD) we must convert.
        financial_ccy = info.get("financialCurrency") or info.get("currency") or "USD"
        trading_ccy   = info.get("currency") or "USD"
        fx_rate = get_fx_rate(financial_ccy, trading_ccy)   # e.g. EUR→USD ≈ 1.09

        # Sector / industry — needed early for industry guardrails
        sector   = info.get("sector",   "")
        industry = info.get("industry", "")

        # ── Most recent quarterly balance sheet data ───────────────────────────
        # Pull from quarterly filing (most recent quarter, e.g. Q1 2026) for accuracy.
        # info dict may lag behind; quarterly_balance_sheet is more current.
        bal_data = get_quarterly_balance_data(stock, info, fx_rate)

        # ── Discovery Layer: Catalyst Research ───────────────────────────────
        # Scans recent news + SEC 8-K filings for catalysts and risks.
        # Results are cached for 1 hour; adds ~1-2s on cold start per ticker.
        catalyst        = get_catalyst_insights(ticker, info, stock)
        catalyst_insights       = catalyst["insights"]
        momentum_premium        = catalyst["momentum_premium"]
        catalyst_wacc_risk      = catalyst["wacc_risk_add"]
        has_positive_catalyst   = catalyst["has_positive_catalyst"]
        has_material_risk       = catalyst["has_material_risk"]
        catalyst_labels         = catalyst["catalyst_labels"]
        risk_labels             = catalyst["risk_labels"]
        policy_tailwind         = catalyst.get("policy_tailwind", False)
        policy_tailwind_labels  = catalyst.get("policy_tailwind_labels", [])
        policy_headwind         = catalyst.get("policy_headwind", False)
        policy_headwind_labels  = catalyst.get("policy_headwind_labels", [])
        # Transformative-catalyst growth lift — Stage-1 growth bump (in pp,
        # 0.0–3.0) when a durable industry-specific milestone fires.
        growth_catalyst_lift_pp = catalyst.get("growth_catalyst_lift_pp", 0.0)
        transformative_labels   = catalyst.get("transformative_labels", [])
        news_interpretation     = catalyst.get("news_interpretation", [])
        momentum_applied        = False   # set True when premium is baked into IV

        # ── Strategic Asset Classifier ───────────────────────────────────────
        # Recognizes US national-security / sovereign-capital names where pure
        # DCF systematically understates value (CHIPS Act recipients, defense
        # primes, energy sovereignty, critical materials).  Returns None for
        # non-strategic tickers.  Effects are wired in later when WACC and
        # sector ceiling are computed.
        strategic = _strategic_classifier(ticker)
        # Live policy tailwinds further deepen the strategic effect — a
        # CHIPS Act announcement on a strategic name is a stronger signal
        # than the curation alone.
        if strategic and policy_tailwind:
            strategic = dict(strategic)
            strategic["wacc_delta"] -= 0.0025      # extra 25bp reduction
            strategic["live_policy_amplifier"] = True

        # ── Price history (5Y daily — frontend filters to 3M/6M/YTD/1Y/2Y/5Y) ──
        hist = stock.history(period="5y", interval="1d")
        price_history = []
        if not hist.empty:
            price_history = [
                {"date": str(i.date()), "close": round(float(r["Close"]), 2)}
                for i, r in hist.iterrows()
            ]

        # ── FCF setup ─────────────────────────────────────────────────────────
        fcf_series    = get_fcf_series(cashflow)
        base_fcf, fcf_source = get_base_fcf(info, stock)
        # Convert FCF from reporting currency to trading currency
        if base_fcf is not None:
            base_fcf = base_fcf * fx_rate
        fcf_series = [v * fx_rate for v in fcf_series]
        dcf_available = base_fcf is not None and base_fcf > 0

        # ── Comparables-based profitability model (early-stage names) ────────
        # Tax rate is computed below; do a cheap pre-compute here so the
        # comparables model can use it. Same formula as calc_tax_rate's
        # default fallback when income_stmt-derived rate isn't available yet.
        _tax_for_comps = 0.21
        try:
            _tax_for_comps = calc_tax_rate(info, income_stmt)
        except Exception:
            pass
        comparables_model = compute_comparables_model(
            stock, info, ticker, base_fcf, _tax_for_comps, fx_rate
        )
        if comparables_model.get("used"):
            base_fcf = comparables_model["weighted_base_fcf"]
            fcf_source = (fcf_source or "") + " · comparables-weighted"
            dcf_available = base_fcf is not None and base_fcf > 0

        # ── Fintech industry flag (used later for FCF normalization) ─────────
        _vm_check = _get_valuation_method(sector, industry)
        _is_fintech_growth = (_vm_check == "dcf" and
                              any(x in (industry or "").lower() for x in [
                                  "capital markets", "securities brokerage",
                                  "consumer lending", "online lending",
                                  "financial exchanges", "investment brokerage",
                              ]))

        # ── Structural Transformer detection ──────────────────────────────────
        # Must run after rev_ttm is known; rev_ttm is computed below in the
        # income-statement block so we defer the call — state vars initialised here.
        is_structural_transformer  = False
        st_capex_abs               = 0.0
        st_capex_to_rev            = 0.0
        st_capex_addback           = 0.0
        st_capex_addback_rate      = 0.50

        # ── Tax rate ──────────────────────────────────────────────────────────
        tax_rate = calc_tax_rate(info, income_stmt)

        # ── Dividend yield (use trailingAnnualDividendYield — Yahoo's dividendYield can be erroneous) ──
        raw_yield = safe(info.get("trailingAnnualDividendYield")) or safe(info.get("dividendYield"))
        if raw_yield is not None:
            # Guard against Yahoo returning yield as a percentage (e.g. 37 instead of 0.37)
            if raw_yield > 0.30:
                raw_yield = raw_yield / 100
            dividend_yield = round(raw_yield * 100, 2)
        else:
            dividend_yield = None

        # ── Income statement TTM ─────────────────────────────────────────────
        # Computed before the DCF block so ebitda_ttm is available to both
        # the scenario analysis (institutional floor) and the multiples fallback.
        inc_ttm = get_income_stmt_ttm(stock)
        rev_ttm = inc_ttm.get("Total Revenue") or safe(info.get("totalRevenue"))
        cogs_ttm = inc_ttm.get("Cost Of Revenue")
        gp_ttm = inc_ttm.get("Gross Profit")
        oi_ttm = inc_ttm.get("Operating Income")
        ni_ttm = inc_ttm.get("Net Income")
        ebitda_ttm = inc_ttm.get("EBITDA") or safe(info.get("ebitda"))
        gross_margin_ttm = round(gp_ttm / rev_ttm * 100, 2) if gp_ttm and rev_ttm else None
        operating_margin_ttm = round(oi_ttm / rev_ttm * 100, 2) if oi_ttm and rev_ttm else None
        net_margin_ttm = round(ni_ttm / rev_ttm * 100, 2) if ni_ttm and rev_ttm else None

        # ── Structural Transformer: detect + FCF normalisation ────────────────
        # Now that rev_ttm is available, run the detection and adjust base_fcf.
        is_structural_transformer, st_capex_abs, st_capex_to_rev, st_capex_addback_rate = \
            _detect_structural_transformer(info, sector, industry, cashflow, rev_ttm)

        # Apply CapEx normalization for:
        #   a) Full structural transformers (TSLA) — 50% addback
        #   b) Partial platform normalization (AMZN) — 20% logistics addback
        # The addback runs whenever st_capex_abs > 0, regardless of is_structural_transformer.
        if st_capex_abs > 0 and st_capex_addback_rate > 0:
            st_capex_addback = st_capex_addback_rate * st_capex_abs * fx_rate
            _pre_st_fcf = base_fcf or 0.0
            base_fcf    = (_pre_st_fcf + st_capex_addback)
            dcf_available = base_fcf > 0  # re-evaluate now that FCF may have changed

        # ── High-growth fintech FCF normalization ─────────────────────────────
        # Companies like SOFI, HOOD, COIN, AFRM often have negative TTM FCF
        # because they are reinvesting aggressively in growth.  Their true
        # earnings power is best approximated from forward EPS × shares — the
        # market values them on forward earnings, not trailing cash flow.
        # Runs AFTER structural transformer detection so we can exclude TST-type
        # companies that already have their own capex normalisation.
        if not dcf_available and not is_structural_transformer:
            _fwd_eps    = safe(info.get("forwardEps"))
            _shares_nrm = (safe(info.get("sharesOutstanding") or
                                info.get("impliedSharesOutstanding"), 0) or 0)
            _rev_nrm    = (safe(info.get("totalRevenue"), 0) or 0) * fx_rate
            # Strategy A: forward earnings proxy (preferred — analyst-grounded)
            if _fwd_eps and _fwd_eps > 0 and _shares_nrm > 0:
                _norm_fcf = _fwd_eps * _shares_nrm * fx_rate * 0.80  # 80% earnings→FCF
                if _norm_fcf > 0:
                    base_fcf      = _norm_fcf
                    fcf_source    = "Fwd earnings proxy (TTM FCF negative — normalized)"
                    dcf_available = True
            # Strategy B: revenue × sector-typical FCF margin (any non-bank sector)
            elif _rev_nrm > 0:
                _s_lower = (sector or "").lower()
                # Software/tech & growth fintech ≈ 8%; everything else ≈ 6%
                _fcf_margin_proxy = (
                    0.08 if (_is_fintech_growth or "technology" in _s_lower
                             or "communication" in _s_lower)
                    else 0.06
                )
                _norm_fcf = _rev_nrm * _fcf_margin_proxy
                if _norm_fcf > 0:
                    base_fcf      = _norm_fcf
                    fcf_source    = (f"Revenue×{int(_fcf_margin_proxy*100)}% proxy "
                                     "(TTM FCF negative — sector margin normalization)")
                    dcf_available = True
            # Strategy C: EBITDA × 0.60 fallback (last-resort for EBITDA-positive cos)
            if not dcf_available:
                _ebitda_nrm = safe(info.get("ebitda"))
                if _ebitda_nrm and _ebitda_nrm > 0:
                    _norm_fcf = float(_ebitda_nrm) * fx_rate * 0.60
                    if _norm_fcf > 0:
                        base_fcf      = _norm_fcf
                        fcf_source    = "EBITDA×60% proxy (TTM FCF negative — EBITDA fallback)"
                        dcf_available = True

        # ── FCF Volatility Normalization ──────────────────────────────────────
        # When TTM FCF is 2× or more above the recent 3-year positive average,
        # the number likely reflects a one-off working-capital release, asset sale,
        # or timing difference — not a sustainable run rate.  Normalize to
        # 60% TTM + 40% 3-year mean so the DCF doesn't extrapolate an outlier.
        # Structural Transformer and partial logistics add-backs are exempt because
        # their base_fcf has already been deliberately adjusted upward.
        fcf_normalized = False
        if (dcf_available and base_fcf is not None
                and not is_structural_transformer and st_capex_addback == 0):
            _pos_hist = [f for f in fcf_series[:4] if f > 0]
            if len(_pos_hist) >= 2:
                _hist_avg = sum(_pos_hist) / len(_pos_hist)
                if _hist_avg > 0 and base_fcf > _hist_avg * 2.0:
                    base_fcf = round(0.60 * base_fcf + 0.40 * _hist_avg, 0)
                    fcf_source = (fcf_source or "ttm") + " (normalized: TTM was 2×+ hist. avg)"
                    fcf_normalized = True

        # ── Moat Detection ───────────────────────────────────────────────────
        # Run before DCF so the classification can adjust WACC and TG ceilings.
        _rev_g_raw  = safe(info.get("revenueGrowth"))
        _earn_g_raw = safe(info.get("earningsGrowth"))
        _roe_raw    = safe(info.get("returnOnEquity"))
        rev_growth_pct  = round(_rev_g_raw  * 100, 2) if _rev_g_raw  is not None else None
        earn_growth_pct = round(_earn_g_raw * 100, 2) if _earn_g_raw is not None else None
        roe_pct         = round(_roe_raw    * 100, 2) if _roe_raw    is not None else None
        # FCF margin preview: use info freeCashflow / TTM revenue (base_fcf already in USD)
        _fcf_preview  = safe(info.get("freeCashflow"))
        _rev_preview  = rev_ttm or safe(info.get("totalRevenue"))
        fcf_margin_preview = (
            round(_fcf_preview / _rev_preview * 100, 1)
            if _fcf_preview and _rev_preview and _rev_preview > 0 else None
        )
        _rev_ttm_bn = round(rev_ttm / 1e9, 1) if rev_ttm else None
        moat_detected, moat_path, moat_reasons = _detect_moat(
            net_margin_ttm, rev_growth_pct, earn_growth_pct, fcf_margin_preview, roe_pct,
            rev_ttm_bn=_rev_ttm_bn
        )
        moat_wacc_delta   = 0.0      # how many pp of WACC were removed (filled below)
        moat_mult_premium = 1.20 if moat_detected else 1.0  # 20% multiple expansion

        # ── Sector valuation routing ─────────────────────────────────────────
        valuation_method     = _get_valuation_method(sector, industry)
        analyst_target_price = safe(info.get("targetMeanPrice"))
        sector_val_label     = None   # set when sector-specific method overrides/blends
        analyst_adjusted     = False  # set when analyst alignment check blends IV

        # ── DCF computation ───────────────────────────────────────────────────
        intrinsic_value       = None
        margin_of_safety      = None
        projected             = []
        enterprise_value      = None
        equity_value          = None
        pv_terminal           = None
        total_pv_fcf          = None
        fcf_chart             = None
        s1 = s2               = None
        wacc_data             = {}
        dcf_warning           = None
        growth_source         = None
        scenarios             = None
        backbone_stage1_years    = None   # set inside DCF block for backbone moat
        cash_rich_wacc_applied   = False  # set inside DCF block
        st_robotaxi_s2_applied   = False  # set inside DCF block for structural transformer
        strategic_wacc_delta     = 0.0    # set inside DCF block for strategic asset
        strategic_floor_applied  = False  # set after DCF when strategic floor lifts IV
        # IV provenance — overwritten as the pipeline progresses to higher-fidelity methods.
        iv_source_label          = "DCF"  # default; downgraded to "Multiples", "Sultan Split", or emergency labels
        iv_confidence            = "high" # downgraded to "medium" / "low" by lower-fidelity paths
        net_debt         = ((safe(info.get("totalDebt"), 0) or 0) - (safe(info.get("totalCash"), 0) or 0)) * fx_rate
        # FIN 415 defaults — overridden inside DCF block when data is available
        fin415_used         = False
        fin415_price        = None
        fin415_fcfe_rows    = None
        fin415_op_margins   = None
        fin415_bear_wacc_iv = None
        fin415_bear_grow_iv = None
        conservative_target = None
        ke                  = 0.0

        # Banking: force DCF off unconditionally (whether or not FCF happens to be
        # positive in a given period) — interest is a core operating cost for banks,
        # not a financing item, so FCF-based DCF systematically misprices them.
        if valuation_method == "banking":
            dcf_available = False
            dcf_warning   = ("Traditional DCF is not applicable for banks — "
                             "interest income/expense are core operating costs, not financing items. "
                             "Value derived from sector-specific capital efficiency: "
                             "Price-to-Book (P/B) and Return on Equity (ROE).")

        # ── Industry guardrails ───────────────────────────────────────────────
        ind_class  = _classify_industry(sector, industry)
        # Structural transformers escape their traditional-sector guardrails and
        # are priced with technology-platform parameters instead.
        if is_structural_transformer:
            ind_class = "structural_transformer"
        # Payment networks (Visa, Mastercard, AXP) use their own parameter set —
        # asset-light moat businesses with structurally higher terminal growth ceilings.
        _ind_lower = (industry or "").lower()
        is_payment_network = any(x in _ind_lower for x in
                                 ["credit service", "payment network", "payment processing"])
        if is_payment_network:
            ind_class = "payment_network"
        ind_params = INDUSTRY_PARAMS[ind_class]
        user_tg_override = request.args.get("terminal") is not None

        if not dcf_available:
            # Banking warning already set above; set generic message for all other cases
            if valuation_method != "banking":
                if not fcf_series:
                    dcf_warning = "No Free Cash Flow data available — DCF not computed."
                else:
                    most_recent = fcf_series[0] if fcf_series else 0
                    dcf_warning = (
                        f"Most recent FCF is negative (${most_recent/1e9:.1f}B) — "
                        "DCF intrinsic value is unreliable. Showing multiples-based analysis only."
                    )
        else:
            # ── Growth rate ───────────────────────────────────────────────────
            # Backbone moat companies (High-Growth / Mature Cash Machine) get a
            # lifted max_s1 cap — their analyst estimates legitimately exceed 25%.
            is_backbone_moat = (
                moat_detected and
                moat_path in ("High-Growth Backbone", "Mature Cash Machine")
            )
            effective_max_s1 = max(ind_params["max_s1"], 0.35) if is_backbone_moat else ind_params["max_s1"]

            # Blended growth calculator — produces s1, s2, and a tg seed by
            # combining historical revenue/earnings/FCF, next-2y analyst
            # estimates, and Finviz 5-year EPS growth, with industry-proxy
            # fallback when data is sparse or volatile. User overrides on the
            # individual stages still win below.
            blended = compute_blended_growth(
                stock, info, fcf_series, income_stmt, ind_params, ticker
            )
            # Seed terminal growth from blended calc unless user overrode it.
            # Existing industry / moat / payment-network / WACC-spread caps
            # below still get the final word.
            if not user_tg_override:
                tg = blended["tg"]

            if s1_ov:
                _s1_raw = float(s1_ov) / 100 if float(s1_ov) > 1 else float(s1_ov)
                # Clamp user override to the same sector ceiling we apply to
                # auto-derived growth.  Without this, a URL like ?growth1=200
                # runs the model at 200% Stage-1 for 5 years and produces a
                # nonsense IV.  Floor at 0 to reject negative inputs.
                s1 = max(min(_s1_raw, effective_max_s1), 0.0)
                growth_source = "User override"
            else:
                s1 = min(blended["s1"], effective_max_s1)
                growth_source = blended["source"]
                # Transformative-catalyst Stage-1 lift: when a durable
                # industry-specific milestone fired in the news scan
                # (eVTOL Part 135, semi tape-out, biotech BLA approval,
                # etc.), the model lifts Stage-1 growth in addition to
                # the IV multiplier — reflects 2-3 years of accelerating
                # revenue, not just a sentiment pop.  Capped at the
                # sector ceiling so it never produces nonsensical growth.
                if growth_catalyst_lift_pp > 0:
                    _s1_pre = s1
                    s1 = min(s1 + growth_catalyst_lift_pp / 100.0,
                             effective_max_s1)
                    if s1 > _s1_pre:
                        growth_source = (growth_source or "") + (
                            f" · +{growth_catalyst_lift_pp:.1f}pp transformative-catalyst lift"
                        )

            if s2_ov:
                _s2_raw = float(s2_ov) / 100 if float(s2_ov) > 1 else float(s2_ov)
                # Stage-2 ceiling is 65% of Stage-1 ceiling — same logic the
                # auto path uses to keep mid-period growth credible.
                s2 = max(min(_s2_raw, effective_max_s1 * 0.65), tg)
            else:
                # Take the blended Stage 2 (historical-weighted mean reversion),
                # then enforce the same invariants the legacy auto path used:
                # not above Stage 1, ≥ TG + 0.5pp, ≥ 2% absolute floor.
                s2 = blended["s2"]
                s2 = min(s2, s1)
                s2 = max(s2, tg + 0.005, 0.02)

            # ── Structural Transformer: Robotaxi / FSD Stage 2 premium ────────
            # Tesla launched unsupervised Robotaxis (Dallas/Houston, Q1 2026) with
            # 1.3M paid FSD subscribers.  New recurring revenue streams materially
            # lift the mid-period growth rate.  Apply a 15% uplift to Stage 2 when
            # the user hasn't manually overridden s2.
            st_robotaxi_s2_applied = False
            if is_structural_transformer and not s2_ov:
                _s2_pre = s2
                s2 = min(s2 * 1.15, ind_params["max_s1"] * 0.65)  # 15% lift, capped
                if s2 > _s2_pre:
                    st_robotaxi_s2_applied = True
                    growth_source = (growth_source or "") + " · +15% Robotaxi/FSD Stage 2 premium"

            wacc_data = calc_wacc(info, income_stmt, tax_rate, fx_rate)
            # Apply industry WACC floor (pure capital-structure math can give unrealistically
            # low WACC for junk-rated, highly-levered companies like airlines)
            wacc = max(wacc_data["wacc"], ind_params["min_wacc"])
            wacc_data["wacc"] = wacc

            # Cap terminal growth at industry ceiling (GDP-aligned for mature sectors)
            if not user_tg_override:
                tg = min(tg, ind_params["max_tg"])

            # ── Moat Premium adjustments ─────────────────────────────────────
            # Applied after industry guardrails — moat premium is a reward on top,
            # not a way to bypass sector-specific safety floors.
            if moat_detected:
                _wacc_pre  = wacc
                # Reduce WACC 1.5 pp to reflect lower institutional risk (floor: 7.5%)
                wacc = max(wacc - 0.015, 0.075)
                wacc_data["wacc"] = wacc
                moat_wacc_delta = round(_wacc_pre - wacc, 4)
                # Allow terminal growth up to 3.0% for backbone companies
                # (vs 2.5% global hard cap for commodity businesses)
                if not user_tg_override:
                    tg = min(tg, 0.030)

            # ── Strategic Asset WACC reduction ────────────────────────────────
            # National-security / sovereign-capital names (CHIPS recipients,
            # defense primes, energy sovereignty, critical materials) have
            # government backstops that reduce real cost of capital below
            # what beta-based WACC implies.  Reduction floors at 6.5% so it
            # never produces nonsensically low discount rates.
            strategic_wacc_delta = 0.0
            if strategic:
                _wacc_pre = wacc
                wacc = max(wacc + strategic["wacc_delta"], 0.065)
                wacc_data["wacc"] = wacc
                strategic_wacc_delta = round(_wacc_pre - wacc, 4)

            # ── Structural Transformer WACC ceiling ───────────────────────────
            # High-beta auto/industrial tickers get a market beta that reflects
            # legacy cyclical risk — not appropriate for an AI-platform company.
            # Cap at 11.5% so the discount rate reflects a diversified tech business.
            if is_structural_transformer and wacc > 0.115:
                wacc = 0.115
                wacc_data["wacc"] = wacc

            # ── Cash-Rich WACC Optimisation ───────────────────────────────────
            # Companies with $50B+ in cash (or $25B+ for Structural Transformers)
            # have institutional-grade balance sheets; cap WACC at 9%.
            total_cash_abs = (safe(info.get("totalCash"), 0) or 0) * fx_rate
            cash_rich_wacc_applied = False
            cash_rich_threshold = 25e9 if is_structural_transformer else 50e9
            if total_cash_abs > cash_rich_threshold:
                if wacc > 0.09:
                    wacc = 0.09
                    wacc_data["wacc"] = wacc
                    cash_rich_wacc_applied = True

            # ── Material Risk WACC Surcharge ──────────────────────────────────
            # When catalyst scan found lawsuits / regulatory investigations,
            # apply +1% to WACC to price in the added uncertainty.
            if catalyst_wacc_risk > 0:
                wacc = min(wacc + catalyst_wacc_risk, 0.20)
                wacc_data["wacc"] = wacc

            # ── Speculative / High-Growth WACC band (auto-calibration) ───────
            # For pre-profit names — comparables-flagged growth-over-profit
            # companies, very young high-growth IPOs, and deeply-negative
            # operating margins — capital-structure WACC understates the
            # true cost of equity (these stocks behave like venture-stage
            # bets, not Russell 1000 index components). Force WACC into a
            # 20–25% band so the DCF reflects the actual risk premium that
            # institutional investors charge for these profiles.
            wacc_speculative_applied = False
            try:
                _op_m  = safe(info.get("operatingMargins"))
                _rev_g = safe(info.get("revenueGrowth"))
                _age   = _company_age_years(info, stock)
                _is_speculative = (
                    comparables_model.get("used") or
                    (_op_m is not None and _op_m < -0.05) or
                    (_age is not None and _age < 3 and _rev_g and _rev_g > 0.30)
                )
                if _is_speculative:
                    _wacc_pre = wacc
                    wacc = max(min(wacc, 0.25), 0.20)
                    wacc_data["wacc"] = wacc
                    wacc_speculative_applied = (abs(wacc - _wacc_pre) > 1e-6)
            except Exception:
                pass

            # Enforce minimum WACC − TGR spread to prevent Gordon Growth Model blow-up
            # (small denominator creates astronomical terminal values)
            min_spread = ind_params["wacc_spread"]
            if wacc - tg < min_spread:
                tg = round(wacc - min_spread, 4)

            # ── Asset-Light Payment Network: WACC + TG hard pin ───────────────
            # Visa / Mastercard / AXP have near-zero marginal cost per transaction,
            # global duopoly pricing power, and secular cashless tailwinds — a profile
            # that capital-structure WACC math systematically over-discounts.
            # WACC pinned at 7.5% (low institutional risk for a global network duopoly).
            # TG pinned at 3.5% — a meaningful premium above the 2.5% default that
            # reflects cashless penetration tailwinds without triggering the 42× Gordon
            # Growth explosion that a 5% TG / 7.5% WACC combination would cause.
            # Backbone stage-1 extension is intentionally suppressed: the TG premium
            # already encodes the long-run structural advantage; doubling up via a
            # 10-year Stage-1 projection would double-count the same thesis.
            if is_payment_network:
                wacc = 0.075
                wacc_data["wacc"] = wacc
                if not user_tg_override:
                    tg = 0.035
                sector_val_label = (
                    "Network Effect Valuation: Standard DCF applied to "
                    "asset-light payment infrastructure."
                )

            # ── Backbone moat: forward-revenue growth source enrichment ──────
            # For backbone moat companies, try to fetch the analyst forward revenue
            # estimate and log it for transparency.  We do NOT scale base_fcf here
            # because Stage 1 growth already projects forward revenue growth — scaling
            # base_fcf on top of s1 would double-count the same expected uplift.
            fwd_base_fcf  = base_fcf   # always equal to base_fcf
            if is_backbone_moat and not s1_ov:
                try:
                    re_df = stock.revenue_estimate
                    if re_df is not None and not re_df.empty and "+1y" in re_df.index:
                        fwd_rev = safe(re_df.loc["+1y", "avg"])
                        ttm_rev_for_label = rev_ttm or safe(info.get("totalRevenue"))
                        if fwd_rev and ttm_rev_for_label and fwd_rev > ttm_rev_for_label:
                            fwd_label = (
                                f"Analyst fwd rev ${fwd_rev/1e9:.0f}B "
                                f"(TTM ${ttm_rev_for_label/1e9:.0f}B)"
                            )
                            if growth_source:
                                growth_source = growth_source + " · " + fwd_label
                except Exception:
                    pass

            # ── Extended Stage 1 for Backbone moat / Structural Transformer ─────
            # Both backbone moat and structural transformer companies sustain high
            # growth for a full decade — run the entire projection at the Stage 1
            # rate.  S2 still applies in bear/bull scenario runs for extra caution.
            # Payment networks are excluded: their 3.5% TG premium already encodes
            # the structural advantage; extending Stage 1 to 10 years would double-
            # count the same thesis and produce unrealistically high terminal values.
            backbone_stage1_years = (
                yrs if (is_backbone_moat or is_structural_transformer) and not is_payment_network
                else None
            )

            # ── Base case DCF (FCFF fallback) ────────────────────────────────
            intrinsic_value, projected, enterprise_value, equity_value, pv_terminal = \
                run_dcf_single(fwd_base_fcf, s1, s2, tg, wacc, yrs, info, fx_rate,
                               stage1_years=backbone_stage1_years)

            total_pv_fcf     = sum(p["pv"] for p in projected)
            net_debt         = ((safe(info.get("totalDebt"), 0) or 0) - (safe(info.get("totalCash"), 0) or 0)) * fx_rate
            margin_of_safety = ((intrinsic_value - price) / price * 100) if intrinsic_value and price else None

            fcf_chart = {
                "projected": {
                    "labels": [f"Y{p['year']}" for p in projected],
                    "values": [round(p["fcf"] / 1e9, 2) for p in projected],
                    "pvs":    [round(p["pv"]  / 1e9, 2) for p in projected],
                }
            }

            # ── FIN 415 FCFE bottom-up model ──────────────────────────────────
            # Runs alongside the FCFF base case.  When data is available, the
            # FIN 415 Conservative Target (60/20/20 blend) replaces the primary IV.
            # Ke (cost of equity) is used as the discount rate — no net-debt bridge.
            fin415_inputs   = _extract_fin415_inputs(
                info, income_stmt, cashflow, balance_sheet, fx_rate)
            fin415_price        = None
            fin415_fcfe_rows    = None
            fin415_op_margins   = None
            fin415_used         = False
            fin415_bear_wacc_iv = None
            fin415_bear_grow_iv = None
            conservative_target = None

            ke = wacc_data.get("coe", wacc)   # Cost of Equity from CAPM

            if fin415_inputs and dcf_available:
                # Year-by-year revenue growth: Stage 1 for first half, Stage 2 for rest
                _rev_growths   = [s1 if y <= yrs // 2 else s2 for y in range(1, yrs + 1)]
                _cogs_growth   = s1 * 0.90    # COGS grows slightly slower → margin expansion
                _fc_growth     = s1 * 0.65    # Operating leverage: fixed costs < revenue growth
                _da_growth     = s1 * 0.80    # D&A tracks capex intensity
                _amort_growth  = 0.02         # Amortisation: slow/flat
                _ppe_growth    = s1 * 0.85    # Asset base grows with CapEx
                _ltd_growth    = -0.03        # Moderate debt paydown (-3%/yr)

                _f415_kwargs = dict(
                    revenue_base    = fin415_inputs["revenue"],
                    cogs_base       = fin415_inputs["cogs"],
                    fixed_costs_base= fin415_inputs["fixed_costs"],
                    da_base         = fin415_inputs["da"],
                    amort_base      = fin415_inputs["amort"],
                    ppe_base        = fin415_inputs["ppe"],
                    ltd_base        = fin415_inputs["ltd"],
                    interest_rate   = fin415_inputs["interest_rate"],
                    tax_rate        = fin415_inputs["tax_rate"],
                    shares          = fin415_inputs["shares"],
                    rev_growths     = _rev_growths,
                    cogs_growth     = _cogs_growth,
                    fc_growth       = _fc_growth,
                    da_growth       = _da_growth,
                    amort_growth    = _amort_growth,
                    ppe_growth      = _ppe_growth,
                    ltd_growth      = _ltd_growth,
                    ke              = ke,
                    tgr             = tg,
                    yrs             = yrs,
                )

                try:
                    (fin415_price, fin415_fcfe_rows,
                     _f415_tv, _f415_npv, fin415_op_margins) = run_fin415_fcfe(**_f415_kwargs)

                    if fin415_price is not None:
                        fin415_used = True

                        # ── FIN 415 Scenario 1: WACC (Ke) + 3% ───────────────
                        _kw_bear = {**_f415_kwargs, "ke": ke + 0.03}
                        fin415_bear_wacc_iv, *_ = run_fin415_fcfe(**_kw_bear)

                        # ── FIN 415 Scenario 2: Revenue growth − 2pp ─────────
                        _rev_bear = [max(g - 0.02, 0.0) for g in _rev_growths]
                        _kw_grow  = {**_f415_kwargs, "rev_growths": _rev_bear}
                        fin415_bear_grow_iv, *_ = run_fin415_fcfe(**_kw_grow)

                        # ── FIN 415 Conservative Target (Scenario Analysis tab)
                        # Formula: 0.60 × base + 0.20 × bear_wacc + 0.20 × bear_growth
                        # Floor negative scenario prices to $0 (distressed floor)
                        _bw = max(fin415_bear_wacc_iv, 0.0) if fin415_bear_wacc_iv is not None else fin415_price
                        _bg = max(fin415_bear_grow_iv, 0.0) if fin415_bear_grow_iv is not None else fin415_price
                        conservative_target = (0.60 * fin415_price
                                               + 0.20 * _bw
                                               + 0.20 * _bg)

                        # Override primary intrinsic value with FIN 415 result
                        intrinsic_value  = conservative_target
                        margin_of_safety = ((intrinsic_value - price) / price * 100) if price else None
                except Exception:
                    fin415_used = False   # silent fallback to FCFF path

            # ── Scenario weights ──────────────────────────────────────────────
            if fin415_used:
                # FIN 415 fixed weighting: 60% base / 20% bear-WACC / 20% bear-growth
                _w_base, _w_bull, _w_bear = 0.60, 0.20, 0.20
                _scenario_weight_note = "FIN 415: 60% base / 20% Ke+3% / 20% Rev−2%"
                iv_bull = fin415_price                            # "Bull" = raw base
                iv_bear = min(_bw, _bg) if fin415_used else intrinsic_value
                # Scenario WACC labels (informational in UI)
                wacc_bull = ke - 0.03
                wacc_bear = ke + 0.03
                s1_bull   = s1
                s1_bear   = s1
                bear_floored = bear_recalculated = bull_recalculated = False
                bull_distressed = bear_distressed = False
                # Distressed-equity floor: when FIN 415 produces zero/negative
                # scenario values (a stressed scenario the model cannot price),
                # replace with macro-stress floors so users see meaningful numbers:
                #   bull → price × 1.10 (a modest +10% upside under reset assumptions)
                #   bear → price × 0.85 (a 15% macro downside)
                if iv_bull is not None and iv_bull <= 0:
                    iv_bull = round(price * 1.10, 2) if price else 0.0
                    bull_distressed = True
                if iv_bear is not None and iv_bear <= 0:
                    iv_bear = round(price * 0.85, 2) if price else 0.0
                    bear_distressed = True
                # Make scenario_net_debt available so any downstream stress code can use it
                scenario_net_debt = bal_data["total_debt"] - bal_data["total_cash"]
            else:
                # ── Legacy dynamic FCF quality weights (fallback) ─────────────
                scenario_net_debt = bal_data["total_debt"] - bal_data["total_cash"]
                _neg_fcf_yrs  = sum(1 for f in fcf_series if f < 0) if fcf_series else 0
                _fcf_margin_q = (base_fcf / rev_ttm * 100) if base_fcf and rev_ttm and rev_ttm > 0 else 10.0
                if _neg_fcf_yrs >= 2 or _fcf_margin_q < 3.0:
                    _w_base, _w_bull, _w_bear = 0.45, 0.15, 0.40
                    _scenario_weight_note = "bear-skewed (low FCF quality)"
                elif _neg_fcf_yrs >= 1 or _fcf_margin_q < 8.0:
                    _w_base, _w_bull, _w_bear = 0.50, 0.20, 0.30
                    _scenario_weight_note = "bear-tilted (moderate FCF quality)"
                else:
                    _w_base, _w_bull, _w_bear = 0.50, 0.25, 0.25
                    _scenario_weight_note = "balanced"

                wacc_bull = max(wacc - 0.010, 0.04)
                wacc_bear = min(wacc + 0.010, 0.18)

                s1_bull = min(s1 * 1.5, 0.45)
                s2_bull = max(s1_bull * 0.55, tg + 0.01)
                tg_bull = min(tg + 0.005, ind_params["max_tg"])
                iv_bull, _, _, _, _ = run_dcf_single(
                    fwd_base_fcf, s1_bull, s2_bull, tg_bull, wacc_bull, yrs, info, fx_rate,
                    net_debt_override=scenario_net_debt,
                    stage1_years=backbone_stage1_years)

                s1_bear = max(s1 * 0.50, 0.01)
                s2_bear = max(s1_bear * 0.55, 0.005)
                tg_bear = max(tg - 0.005, 0.010)
                iv_bear, _, _, _, _ = run_dcf_single(
                    fwd_base_fcf, s1_bear, s2_bear, tg_bear, wacc_bear, yrs, info, fx_rate,
                    net_debt_override=scenario_net_debt,
                    stage1_years=backbone_stage1_years)

                bear_floored = False
                if iv_bear is not None:
                    floor_iv = _bear_floor_iv(info, sector, industry, fx_rate, ebitda_ttm)
                    if floor_iv > 0 and iv_bear < floor_iv:
                        iv_bear = floor_iv
                        bear_floored = True

                bull_recalculated = bear_recalculated = False
                if price and intrinsic_value:
                    if iv_bull is not None and abs(iv_bull - price) / price > 0.90:
                        iv_bull = (iv_bull + intrinsic_value) / 2
                        bull_recalculated = True
                    if iv_bear is not None and abs(iv_bear - price) / price > 0.90:
                        iv_bear = (iv_bear + intrinsic_value) / 2
                        bear_recalculated = True

                bull_distressed = bear_distressed = False
                if iv_bull is not None and iv_bull < 0:
                    iv_bull = 0.0
                    bull_distressed = True
                if iv_bear is not None and iv_bear < 0:
                    iv_bear = 0.0
                    bear_distressed = True

            # ── Bear<Price Clamp (Universal Downside Floor) ───────────────────
            # If the model's natural bear case still sits above current price
            # (deeply undervalued stocks), run a stress-bear with severe assumptions.
            # If even the stress scenario stays above price, hard-clamp to
            # price × 0.85 so users always see a downside-risk number.
            bear_stressed = False
            if (price and price > 0 and iv_bear is not None
                    and iv_bear >= price and not fin415_used):
                try:
                    _stress_s1   = max(s1 * 0.30, 0.005)
                    _stress_s2   = max(_stress_s1 * 0.55, 0.005)
                    _stress_tg   = max(tg - 0.005, 0.005)
                    _stress_wacc = wacc + 0.05
                    _stress_iv, _, _, _, _ = run_dcf_single(
                        fwd_base_fcf, _stress_s1, _stress_s2, _stress_tg,
                        _stress_wacc, yrs, info, fx_rate,
                        net_debt_override=scenario_net_debt,
                        stage1_years=backbone_stage1_years,
                    )
                    if _stress_iv is not None and _stress_iv < price:
                        iv_bear = round(max(_stress_iv, 0.0), 2)
                    else:
                        iv_bear = round(price * 0.85, 2)
                    bear_stressed = True
                except Exception:
                    iv_bear = round(price * 0.85, 2)
                    bear_stressed = True
            elif fin415_used and price and price > 0 and (iv_bear is None or iv_bear >= price):
                # FIN 415 path: combined stress (Ke+5% AND rev_growths × 0.5).
                # Reuses _f415_kwargs dict bundled in the FIN 415 wire-in block.
                try:
                    _stress_kwargs = dict(_f415_kwargs)
                    _stress_kwargs["ke"]          = ke + 0.05
                    _stress_kwargs["rev_growths"] = [max(g * 0.50, 0.0) for g in _rev_growths]
                    _stress_iv, *_ = run_fin415_fcfe(**_stress_kwargs)
                    if _stress_iv is not None and _stress_iv < price:
                        iv_bear = round(max(_stress_iv, 0.0), 2)
                    else:
                        iv_bear = round(price * 0.85, 2)
                    bear_stressed = True
                except Exception:
                    iv_bear = round(price * 0.85, 2)
                    bear_stressed = True

            # ── Probability-weighted fair value ───────────────────────────────
            if all(v is not None for v in [intrinsic_value, iv_bull, iv_bear]):
                iv_weighted = _w_base * intrinsic_value + _w_bull * iv_bull + _w_bear * iv_bear
            else:
                iv_weighted = intrinsic_value

            # Update primary margin of safety to use weighted value
            margin_of_safety = ((iv_weighted - price) / price * 100) if iv_weighted and price else None

            scenarios = {
                "base": {
                    "value":  round(intrinsic_value, 2) if intrinsic_value is not None else None,
                    "weight": round(_w_base * 100),
                    "s1":     round(s1 * 100, 2),
                    "wacc":   round(wacc * 100, 2),
                    "upside": round((intrinsic_value - price) / price * 100, 1) if intrinsic_value is not None and price else None,
                },
                "bull": {
                    "value":  round(iv_bull, 2) if iv_bull is not None else None,
                    "weight": round(_w_bull * 100),
                    "s1":     round(s1_bull * 100, 2),
                    "wacc":   round(wacc_bull * 100, 2),
                    "upside": round((iv_bull - price) / price * 100, 1) if iv_bull is not None and price else None,
                    "recalculated": bull_recalculated,
                    "distressed":   bull_distressed,
                },
                "bear": {
                    "value":  round(iv_bear, 2) if iv_bear is not None else None,
                    "weight": round(_w_bear * 100),
                    "s1":     round(s1_bear * 100, 2),
                    "wacc":   round(wacc_bear * 100, 2),
                    "upside": round((iv_bear - price) / price * 100, 1) if iv_bear is not None and price else None,
                    "floored":      bear_floored,
                    "recalculated": bear_recalculated,
                    "distressed":   bear_distressed,
                    "stressed":     bear_stressed,
                },
                "weighted": round(iv_weighted, 2) if iv_weighted is not None else None,
                "weighted_upside": round((iv_weighted - price) / price * 100, 1) if iv_weighted is not None and price else None,
                "weight_basis": _scenario_weight_note,
            }
            intrinsic_value = round(iv_weighted, 2) if iv_weighted else intrinsic_value

            # Negative equity value: net debt exceeds DCF enterprise value.
            # Don't blank the card — fall through to emergency_iv so the user
            # gets a defensible distress-anchored IV (analyst target, distressed
            # P/B, or cash-only proxy) instead of an empty verdict.
            if equity_value is not None and equity_value < 0:
                dcf_warning = (
                    f"Net debt (${abs(net_debt)/1e9:.1f}B) exceeds DCF enterprise value "
                    f"(${enterprise_value/1e9:.1f}B) — DCF equity value is negative. "
                    "Showing distress-anchored valuation instead."
                )
                _e_iv, _e_lbl, _e_conf = _emergency_iv(
                    info, fx_rate, sector, industry, analyst_target_price)
                if _e_iv is not None:
                    intrinsic_value     = _e_iv
                    iv_source_label     = _e_lbl
                    iv_confidence       = _e_conf
                    margin_of_safety    = round((intrinsic_value - price) / price * 100, 1) if price else None
                    scenarios           = None  # rebuilt by Synthetic Scenarios later
                else:
                    intrinsic_value  = None
                    margin_of_safety = None
                    scenarios        = None

        # ── Sector-Specific Valuation ─────────────────────────────────────────
        # Runs after DCF so we can blend (biotech) or override (banking) as needed.

        if valuation_method == "biotech":
            _bv, _bm = calc_biotech_val(
                info, fx_rate, rev_ttm, analyst_target_price)
            if _bv is not None:
                if intrinsic_value is not None:
                    # Positive FCF biotech: blend DCF 50% + EV/Revenue 50%
                    intrinsic_value = round(0.50 * intrinsic_value + 0.50 * _bv, 2)
                    sector_val_label = f"DCF 50% + {_bm} 50%"
                else:
                    intrinsic_value  = _bv
                    sector_val_label = _bm
                if intrinsic_value and price:
                    margin_of_safety = round((intrinsic_value - price) / price * 100, 1)

        elif valuation_method == "banking" and intrinsic_value is None:
            # NEW (Universal DCF): run banking FCFE first (Net Income → equity CF),
            # then blend 70% banking-DCF + 30% P/B book-value cross-check.
            # Falls back to pure P/B if either side is unavailable.
            _bk_wacc = wacc_data or calc_wacc(info, income_stmt, tax_rate, fx_rate)
            _bk_ke   = _bk_wacc.get("coe") if _bk_wacc else None
            _bk_dcf, _ = (run_banking_fcfe(info, fx_rate, _bk_ke, tg, yrs)
                          if _bk_ke else (None, None))
            _bkv, _bkm = calc_banking_val(info, fx_rate)

            if _bk_dcf is not None and _bkv is not None:
                intrinsic_value  = round(0.70 * _bk_dcf + 0.30 * _bkv, 2)
                sector_val_label = f"Banking-DCF ({_bk_dcf:.2f}) 70% + {_bkm} 30%"
                # Persist wacc_data so the result-dict picks up Ke for display
                if wacc_data == {} or wacc_data.get("coe") is None:
                    wacc_data = _bk_wacc
            elif _bk_dcf is not None:
                intrinsic_value  = _bk_dcf
                sector_val_label = "Banking-DCF (Net Income → FCFE)"
                if wacc_data == {} or wacc_data.get("coe") is None:
                    wacc_data = _bk_wacc
            elif _bkv is not None:
                intrinsic_value  = _bkv
                sector_val_label = _bkm
            if intrinsic_value and price:
                margin_of_safety = round((intrinsic_value - price) / price * 100, 1)

        # ── Consensus Anchor ──────────────────────────────────────────────────
        # If VALUS IV is >30% above analyst mean target, blend 70% model + 30%
        # consensus.  This grounds the output when the model runs ahead of
        # sell-side estimates due to near-term uncertainty the DCF cannot capture.
        consensus_anchor_pre_iv = None   # original IV before anchor blending
        if intrinsic_value is not None:
            intrinsic_value, analyst_adjusted, consensus_anchor_pre_iv = \
                _analyst_alignment_check(intrinsic_value, analyst_target_price, price)
            if analyst_adjusted and price:
                margin_of_safety = round((intrinsic_value - price) / price * 100, 1)

        # ── Scenario Sync after Consensus Anchor ──────────────────────────────
        # When the anchor fired, propagate the same 70/30 blend to each scenario
        # case individually so that the Scenario Analysis cards and the
        # Bear/Base/Bull toggle always display consistent, anchored numbers.
        # Base → synced to the final intrinsic_value (anchored weighted IV)
        # Bull → individually anchored (almost always above the 30% threshold)
        # Bear → individually anchored only if it too exceeds the threshold
        #        (conservative bear cases typically fall below it — left intact)
        if analyst_adjusted and scenarios and analyst_target_price:
            _at = float(analyst_target_price)

            # Sync Base to the final displayed value so toggle ≡ card
            scenarios["base"]["value"]  = intrinsic_value
            scenarios["base"]["upside"] = margin_of_safety

            # Individually anchor Bull and Bear
            for _key in ("bull", "bear"):
                _sv = scenarios[_key].get("value")
                if _sv is not None and _sv > _at * 1.30:
                    _blended = round(0.70 * _sv + 0.30 * _at, 2)
                    scenarios[_key]["value"]  = _blended
                    scenarios[_key]["upside"] = (
                        round((_blended - price) / price * 100, 1) if price else None
                    )

            # Recompute probability-weighted fair value from anchored numbers
            _bv = scenarios["base"]["value"]
            _uv = scenarios["bull"]["value"]
            _dv = scenarios["bear"]["value"]
            if all(v is not None for v in [_bv, _uv, _dv]):
                _new_w = round(0.50 * _bv + 0.25 * _uv + 0.25 * _dv, 2)
                scenarios["weighted"]        = _new_w
                scenarios["weighted_upside"] = (
                    round((_new_w - price) / price * 100, 1) if price else None
                )

            scenarios["consensus_anchored"] = True

        # ── Catalyst Momentum Premium ─────────────────────────────────────────
        # Applied AFTER analyst alignment so the premium stacks on the blended value.
        # Only fires when a strong positive catalyst was found in the last 7 days.
        if intrinsic_value is not None and momentum_premium > 0:
            intrinsic_value  = round(intrinsic_value * (1 + momentum_premium), 2)
            momentum_applied = True
            if price:
                margin_of_safety = round((intrinsic_value - price) / price * 100, 1)

        # ── Absolute Zero Floor ───────────────────────────────────────────────
        if intrinsic_value is not None:
            intrinsic_value = max(round(intrinsic_value, 2), 0.0)

        # ── Expectation Gap Engine ─────────────────────────────────────────────
        # Reverse-DCF: solve for the growth rate the current market price implies,
        # then surface the gap between what the market "believes" vs what our model
        # forecasts.  Answers "where is the market wrong?" not just "what is this worth?"
        expectation_gap = None
        implied_growth_pct = None
        if (dcf_available and price and price > 0
                and base_fcf and base_fcf > 0
                and s1 is not None and wacc):
            try:
                _s2_ratio = (s2 / s1) if (s1 and s1 > 0) else 0.55
                _iv_pre   = consensus_anchor_pre_iv or intrinsic_value
                _ag       = safe(info.get("earningsGrowth") or info.get("revenueGrowth"))

                implied_g, _conv = solve_implied_growth(
                    price          = price,
                    base_fcf       = fwd_base_fcf,
                    s2_ratio       = _s2_ratio,
                    tg             = tg,
                    wacc           = wacc,
                    yrs            = yrs,
                    info           = info,
                    fx_rate        = fx_rate,
                    net_debt_override = net_debt,
                )
                if implied_g is not None:
                    implied_growth_pct = round(implied_g * 100, 1)
                    # Suppress noise: if reverse-DCF converged to the lower
                    # bound (≈0.0%), the result is unreliable for narrative.
                    # Mark as None so downstream fallbacks use MOS-driven copy.
                    if abs(implied_growth_pct) < 0.5:
                        implied_growth_pct = None
                    expectation_gap = build_expectation_gap(
                        implied_g            = implied_g,
                        model_g              = s1,
                        price                = price,
                        model_iv_pre_sultan  = _iv_pre,
                        analyst_growth       = _ag,
                        sector               = sector,
                        yrs                  = yrs,
                    )
            except Exception:
                pass   # silent — never break the main response

        # ── Reality Reconciliation Layer ──────────────────────────────────────
        # When the model is materially off from market price AND the sell-side
        # consensus aligns with market (not the model), the model is missing
        # something the simple DCF cannot capture (growth optionality, contracts,
        # network effects, AI/robotics platform value, brand premiums).
        # Only fires when implied growth from reverse-DCF is sector-credible.
        reality_reconciled       = False
        reality_pre_iv           = None
        reality_reason           = None
        if intrinsic_value is not None and price and price > 0:
            _new_iv, reality_reconciled, reality_pre_iv, reality_reason = \
                _reality_reconciliation(
                    iv                       = intrinsic_value,
                    price                    = price,
                    analyst_target           = analyst_target_price,
                    implied_g                = implied_growth_pct / 100 if implied_growth_pct is not None else None,
                    sector                   = sector,
                    industry                 = industry,
                    is_structural_transformer = is_structural_transformer,
                    moat_detected            = moat_detected,
                )
            if reality_reconciled:
                _scale = (_new_iv / intrinsic_value) if intrinsic_value > 0 else 1.0
                intrinsic_value = _new_iv
                margin_of_safety = round((intrinsic_value - price) / price * 100, 1) if price else None
                # Scale scenarios proportionally so Bear/Base/Bull stay coherent
                if scenarios and isinstance(scenarios, dict):
                    for _key in ("base", "bull", "bear"):
                        _slot = scenarios.get(_key)
                        if _slot and _slot.get("value") is not None:
                            _new_v = round(_slot["value"] * _scale, 2)
                            _slot["value"]  = _new_v
                            _slot["upside"] = (
                                round((_new_v - price) / price * 100, 1) if price else None
                            )
                    # Ensure Bear stays below price after scaling
                    _bear_slot = scenarios.get("bear")
                    if _bear_slot and _bear_slot.get("value") is not None:
                        if _bear_slot["value"] >= price:
                            _bear_slot["value"]  = round(price * 0.85, 2)
                            _bear_slot["upside"] = round(-15.0, 1)
                            _bear_slot["stressed"] = True
                    if scenarios.get("weighted") is not None:
                        scenarios["weighted"] = round(scenarios["weighted"] * _scale, 2)
                        scenarios["weighted_upside"] = (
                            round((scenarios["weighted"] - price) / price * 100, 1) if price else None
                        )
                    scenarios["reality_reconciled"] = True

        # ── Cash-Rich + Debt + Coherence + Priced-For + Verdict ─────────────
        # NOTE: these five layers must run AFTER multiples fallback finalises
        # intrinsic_value (line ~3700+) since dcf_unavailable stocks (banks,
        # negative-FCF micros) don't get an IV until then.  Kept defaults here.
        is_cash_rich = False
        cash_pct_of_mcap = None
        cash_rich_narrative = None
        _cr_prem = 0.0
        debt_momentum = {"classification": "stable", "label": "Stable Capital Structure",
                         "color": "neutral", "narrative": "", "premium_pct": 0.0,
                         "flags": [], "debt_to_ebitda": None, "interest_coverage": None,
                         "debt_trend_pct": None}
        priced_for = None
        is_mag7 = False
        verdict_summary = None
        _ceiling = None
        _ceiling_label = None

        # ── DCF Confidence Score ──────────────────────────────────────────────
        # Assess model reliability BEFORE deciding whether to blend with multiples.
        dcf_conf_level, dcf_conf_label, dcf_conf_strengths, dcf_conf_warnings = \
            _calc_dcf_confidence(
                info, sector, industry, fcf_series,
                dcf_available, valuation_method
            )

        # ── Multiples fallback when DCF is unavailable ───────────────────────
        # Triggered when: (a) no positive FCF, or (b) net debt exceeds DCF enterprise value.
        # Never leaves the user without a value — industry multiples serve as the primary metric.
        multiples_val    = None
        multiples_method = None
        multiples_reason = None
        # Always compute multiples so we can use them for confidence blending too.
        # Structural Transformers use a 35× EV/EBITDA (AI-platform multiple) rather
        # than the standard auto 7× to reflect the robotics/software platform value.
        _mult_v, _mult_m = calc_multiples_val(
            info, sector, industry, fx_rate, ebitda_ttm,
            moat_premium=moat_mult_premium,
            override_ev_ebitda=35.0 if is_structural_transformer else None,
        )
        if intrinsic_value is None:
            multiples_reason = (
                "High Debt Profile" if (dcf_available and equity_value is not None and equity_value < 0)
                else "Negative/Missing Free Cash Flow"
            )
            multiples_val    = _mult_v
            multiples_method = _mult_m
            if multiples_val and price:
                multiples_mos = round((multiples_val - price) / price * 100, 1)
            else:
                multiples_mos = None
            if _mult_v is not None:
                # Multiples is now the primary signal — downgrade confidence
                iv_source_label = f"Multiples ({_mult_m})" if _mult_m else "Multiples"
                iv_confidence   = "medium"
        else:
            # ── Low-confidence blend: DCF + Multiples ─────────────────────────
            # When model reliability is low AND multiples are available, blend the
            # two approaches equally so users see a more anchored, conservative estimate.
            # This prevents a fragile DCF from being taken as a precise price target.
            if dcf_conf_level == "low" and _mult_v is not None:
                _pre_blend_iv   = intrinsic_value
                intrinsic_value = round(0.50 * intrinsic_value + 0.50 * _mult_v, 2)
                multiples_val   = _mult_v
                multiples_method = _mult_m
                multiples_reason = (
                    f"Low-Confidence Blend: DCF (${_pre_blend_iv:.2f}) 50% + "
                    f"Multiples (${_mult_v:.2f}) 50%"
                )
                iv_source_label = "DCF + Multiples (50/50)"
                iv_confidence   = "medium"
                if price:
                    margin_of_safety = round((intrinsic_value - price) / price * 100, 1)
                multiples_mos = round((_mult_v - price) / price * 100, 1) if price else None
            elif dcf_conf_level == "moderate" and _mult_v is not None:
                # Moderate: show multiples alongside but don't blend IV
                multiples_val    = _mult_v
                multiples_method = _mult_m
                multiples_mos    = round((_mult_v - price) / price * 100, 1) if price else None
            else:
                multiples_mos = None

        # ── Sultan Split for multiples-only path ─────────────────────────────
        # When DCF is unavailable and no IV was produced by the sector specialist,
        # blend multiples_val 90% + analyst target 10% so the displayed value is
        # anchored to the model first, with sell-side consensus as a gentle nudge.
        if intrinsic_value is None and multiples_val and analyst_target_price:
            _at_m = float(analyst_target_price)
            if _at_m > 0:
                _pre_mv           = multiples_val
                intrinsic_value   = round(0.90 * multiples_val + 0.10 * _at_m, 2)
                analyst_adjusted  = True
                consensus_anchor_pre_iv = _pre_mv
                if price:
                    margin_of_safety = round((intrinsic_value - price) / price * 100, 1)
                # Label the blend so the UI can show it
                multiples_reason = (
                    (multiples_reason + " · " if multiples_reason else "")
                    + f"Sultan Split: 90% multiples (${_pre_mv:.2f}) + 10% analyst (${_at_m:.2f})"
                )

        # ── Emergency IV Cascade ─────────────────────────────────────────────
        # Last resort: when DCF, multiples, and Sultan Split have all failed,
        # try the four-tier cascade (analyst target → cash-only → distressed
        # P/B → sector EV/Revenue).  Guarantees a defensible IV exists for
        # every analyzed ticker that has a price — pre-revenue biotech, deep
        # distress, ADRs with no FCF, etc.  Confidence is "low" by design.
        if intrinsic_value is None:
            _e_iv, _e_lbl, _e_conf = _emergency_iv(
                info, fx_rate, sector, industry, analyst_target_price)
            if _e_iv is not None:
                intrinsic_value  = _e_iv
                iv_source_label  = _e_lbl
                iv_confidence    = _e_conf
                if price:
                    margin_of_safety = round((intrinsic_value - price) / price * 100, 1)
                # Add a note so the UI surfaces what just happened
                multiples_reason = (
                    (multiples_reason + " · " if multiples_reason else "")
                    + f"Emergency cascade: {_e_lbl}"
                )

        # ── Universal Scenario Synthesis ──────────────────────────────────────
        # When DCF Bear/Base/Bull were not built (banking, biotech, multiples-only),
        # synthesise scenarios from the final IV so every stock has all three cards.
        # Guarantee: Bear < current price; Base ≈ IV; Bull = booming case.
        if scenarios is None and intrinsic_value is not None and price and price > 0:
            _syn_base = round(intrinsic_value, 2)
            _syn_bull = round(intrinsic_value * 1.25, 2)
            # Bear must be below the current market price
            _syn_bear_cand = round(intrinsic_value * 0.76, 2)
            _syn_bear = min(_syn_bear_cand, round(price * 0.87, 2))
            _syn_bear = max(round(_syn_bear, 2), 0.01)
            _syn_w    = round(0.60 * _syn_base + 0.20 * _syn_bull + 0.20 * _syn_bear, 2)
            scenarios = {
                "base": {
                    "value":  _syn_base,
                    "weight": 60,
                    "upside": round((_syn_base - price) / price * 100, 1),
                },
                "bull": {
                    "value":  _syn_bull,
                    "weight": 20,
                    "upside": round((_syn_bull - price) / price * 100, 1),
                },
                "bear": {
                    "value":  _syn_bear,
                    "weight": 20,
                    "upside": round((_syn_bear - price) / price * 100, 1),
                },
                "weighted":        _syn_w,
                "weighted_upside": round((_syn_w - price) / price * 100, 1),
                "synthetic":       True,   # flag so UI can add a note if needed
            }

        # ══════════════════════════════════════════════════════════════════════
        # FINAL POLISH LAYERS — run on whatever the model has now (DCF, multiples,
        # banking, biotech, anything).  Order: Cash-Rich → Debt → Coherence →
        # Priced-For → Verdict Summary.
        # ══════════════════════════════════════════════════════════════════════

        # ── Cash-Rich Premium (3 signals: net-cash, absolute hoard, FCF coverage)
        _cr_prem, is_cash_rich, cash_pct_of_mcap, cash_rich_narrative = \
            _cash_rich_premium(info, fx_rate,
                               (safe(info.get("marketCap"), 0) or 0) * fx_rate,
                               base_fcf=base_fcf,
                               sector=sector, industry=industry)
        # Skip polish layers when IV came from the emergency cascade (analyst
        # target / cash-only / distressed P/B).  Multiplying a distressed
        # anchor by a cash-rich premium defeats the whole "low confidence"
        # signal and inflates the IV with no model basis.
        if _cr_prem > 0 and intrinsic_value is not None and iv_confidence != "low":
            intrinsic_value = round(intrinsic_value * (1 + _cr_prem), 2)
            margin_of_safety = round((intrinsic_value - price) / price * 100, 1) if price else None
            if scenarios:
                _factor = (1 + _cr_prem)
                for _k in ("base", "bull", "bear"):
                    _slot = scenarios.get(_k)
                    if _slot and _slot.get("value") is not None:
                        _slot["value"]  = round(_slot["value"] * _factor, 2)
                        _slot["upside"] = round((_slot["value"] - price) / price * 100, 1) if price else None
                if scenarios.get("weighted") is not None:
                    scenarios["weighted"] = round(scenarios["weighted"] * _factor, 2)
                    scenarios["weighted_upside"] = round((scenarios["weighted"] - price) / price * 100, 1) if price else None

        # ── Debt + Momentum Classifier ─────────────────────────────────────────
        debt_momentum = _debt_momentum_classifier(info, balance_sheet, fcf_series, price_history)
        if debt_momentum.get("premium_pct", 0) > 0 and intrinsic_value is not None and iv_confidence != "low":
            _factor = (1 + debt_momentum["premium_pct"])
            intrinsic_value = round(intrinsic_value * _factor, 2)
            margin_of_safety = round((intrinsic_value - price) / price * 100, 1) if price else None
            if scenarios:
                for _k in ("base", "bull", "bear"):
                    _slot = scenarios.get(_k)
                    if _slot and _slot.get("value") is not None:
                        _slot["value"]  = round(_slot["value"] * _factor, 2)
                        _slot["upside"] = round((_slot["value"] - price) / price * 100, 1) if price else None
                if scenarios.get("weighted") is not None:
                    scenarios["weighted"] = round(scenarios["weighted"] * _factor, 2)
                    scenarios["weighted_upside"] = round((scenarios["weighted"] - price) / price * 100, 1) if price else None

        # ── IV Sanity Clamp ───────────────────────────────────────────────────
        # Single chokepoint every IV-producing path goes through.  Floor at
        # 5% of price (or $0.01); ceiling at max(6× price, 2.5× analyst target).
        # Catches nonsense in both directions: 19,000% MOS upper-tail bugs
        # AND $0.01 IVs from a broken multiples calc on the lower-tail.
        # Applied AFTER emergency_iv too — it's the universal sanity envelope.
        if intrinsic_value is not None and price and price > 0:
            _iv_pre_clamp = intrinsic_value
            intrinsic_value = _clamp_iv(intrinsic_value, price, analyst_target_price)
            if intrinsic_value != _iv_pre_clamp:
                margin_of_safety = round((intrinsic_value - price) / price * 100, 1)
                if scenarios:
                    _factor = intrinsic_value / _iv_pre_clamp
                    for _k in ("base", "bull", "bear"):
                        _slot = scenarios.get(_k)
                        if _slot and _slot.get("value") is not None:
                            _slot["value"]  = round(_slot["value"] * _factor, 2)
                            _slot["upside"] = round((_slot["value"] - price) / price * 100, 1)
                    if scenarios.get("weighted") is not None:
                        scenarios["weighted"] = round(scenarios["weighted"] * _factor, 2)
                        scenarios["weighted_upside"] = round((scenarios["weighted"] - price) / price * 100, 1)

        # ── Scenario Coherence Enforcer (Bear < Base < Bull, sane spreads) ────
        if scenarios and intrinsic_value is not None:
            scenarios = _enforce_scenario_coherence(scenarios, intrinsic_value, price)

        # ── "Priced For" Verdict ──────────────────────────────────────────────
        _ceiling, _ceiling_label = _sector_growth_ceiling(
            sector, industry,
            is_structural_transformer=is_structural_transformer,
            moat_detected=moat_detected,
        )
        # Strategic asset ceiling lift — sovereign capital lets these names
        # plausibly grow above their nominal sector ceiling.
        if strategic and _ceiling is not None:
            _ceiling = _ceiling + strategic["ceiling_lift"]
            _ceiling_label = f"{_ceiling_label} + Strategic"

        # Strategic IV floor — when DCF says the stock is meaningfully
        # overvalued but the company is a curated strategic asset AND the
        # market's forward multiple is below its sector average (the MU
        # case: "lowest forward P/E in S&P"), the model raises IV toward
        # the floor.  This refuses to print a distress verdict on a name
        # the government is structurally backstopping.
        strategic_floor_applied = False
        if (strategic and intrinsic_value is not None and price and price > 0):
            forward_pe   = safe(info.get("forwardPE"))
            sector_fwd   = safe(info.get("trailingPE"))   # rough proxy when no sector avg
            cheap_signal = (
                (forward_pe is not None and forward_pe > 0 and forward_pe < 20) or
                (analyst_target_price and analyst_target_price > price * 1.05)
            )
            iv_floor = price * strategic["iv_floor_mult"]
            if intrinsic_value < iv_floor and cheap_signal:
                _iv_pre = intrinsic_value
                intrinsic_value = round(iv_floor, 2)
                margin_of_safety = round((intrinsic_value - price) / price * 100, 1)
                strategic_floor_applied = True
                if scenarios:
                    _factor = intrinsic_value / _iv_pre if _iv_pre else 1.0
                    for _k in ("base", "bull", "bear"):
                        _slot = scenarios.get(_k)
                        if _slot and _slot.get("value") is not None:
                            _slot["value"]  = round(_slot["value"] * _factor, 2)
                            _slot["upside"] = round((_slot["value"] - price) / price * 100, 1)
                    if scenarios.get("weighted") is not None:
                        scenarios["weighted"] = round(scenarios["weighted"] * _factor, 2)
                        scenarios["weighted_upside"] = round((scenarios["weighted"] - price) / price * 100, 1)

        priced_for = _priced_for_verdict(
            implied_g       = (implied_growth_pct / 100) if implied_growth_pct is not None else None,
            sector_ceiling  = _ceiling,
            price           = price,
            iv              = intrinsic_value,
            margin_of_safety = margin_of_safety,
        )

        # Strategic Discount override — when a strategic asset prints a
        # negative MOS but the market signal says "discount, not warning,"
        # the standard "Priced for Growth/Excellence/Miracle" tier mislabels
        # it.  Promote to a "Strategic Discount" tier (green) with narrative.
        #
        # Two trigger paths:
        #   1. Mature strategic names (MU, INTC, LMT) — low forward P/E
        #      relative to peers signals the market is discounting a
        #      sovereign-backstopped franchise.
        #   2. Pre-revenue strategic names (JOBY, ACHR, RKLB) — forward P/E
        #      is negative or missing because there's no positive earnings
        #      yet; the franchise thesis is the whole story.  Trigger when
        #      the strategic floor was applied AND analyst target sits
        #      meaningfully above current price (sell-side already sees it).
        if (strategic and priced_for and margin_of_safety is not None
                and margin_of_safety < 0):
            forward_pe = safe(info.get("forwardPE"))
            mature_trigger = (forward_pe is not None and 0 < forward_pe < 20)
            # Pre-revenue: negative or extreme forward P/E (= no positive
            # earnings yet) + analyst sees meaningful upside.  Independent
            # of whether the IV floor mechanically fired — that's a
            # different mechanism (downside protection on IV magnitude),
            # not a signal of how the market is pricing the franchise.
            pre_rev_trigger = (
                (forward_pe is None or forward_pe <= 0 or forward_pe > 100)
                and analyst_target_price
                and price
                and analyst_target_price > price * 1.10
            )
            if mature_trigger or pre_rev_trigger:
                if mature_trigger:
                    narrative = (
                        f"Forward P/E {forward_pe:.1f} on a {strategic['strategic_label']} asset — "
                        "market discounting a sovereign-backstopped franchise.  Pure DCF underestimates "
                        "value here; treat as opportunity, not value trap."
                    )
                else:
                    pct = ((analyst_target_price - price) / price * 100)
                    narrative = (
                        f"Pre-revenue {strategic['strategic_label']} franchise — pure DCF undervalues "
                        f"these names because earnings haven't materialised yet.  Analyst consensus "
                        f"sees {pct:+.0f}% upside; sovereign-backstop thesis intact."
                    )
                priced_for = {
                    "tier":       "strategic_discount",
                    "label":      "Strategic Discount",
                    "color":      "green",
                    "narrative":  narrative,
                }
        is_mag7 = _is_mag7(ticker)

        # ── Verdict Summary (clean numbered "why" explanation) ────────────────
        try:
            verdict_summary = _build_verdict_summary(
                ticker                 = ticker,
                priced_for             = priced_for,
                implied_growth_pct     = implied_growth_pct,
                model_growth_pct       = round(s1 * 100, 1) if s1 is not None else None,
                sector_ceiling_pct     = round(_ceiling * 100, 1) if _ceiling else None,
                sector_ceiling_label   = _ceiling_label,
                price                  = price,
                iv                     = intrinsic_value,
                margin_of_safety       = margin_of_safety,
                analyst_target         = analyst_target_price,
                debt_momentum          = debt_momentum,
                is_cash_rich           = is_cash_rich,
                cash_pct_of_mcap       = cash_pct_of_mcap,
                is_mag7                = is_mag7,
                is_structural_transformer = is_structural_transformer,
            )
        except Exception:
            verdict_summary = None

        # ── Methodology Steps (for the new "How VALUS calculated this" panel)
        # Each step is a row showing the actual numbers used for THIS stock.
        # Steps that didn't fire are emitted as `active: False` so the UI can
        # grey them out instead of hiding them — users see the full menu.
        try:
            _shares = safe(bal_data.get("shares"), 0) if bal_data else 0
            methodology_steps = [
                {
                    "step": 1,
                    "label": "Free Cash Flow base",
                    "value": base_fcf,
                    "format": "currency_b",
                    "active": bool(base_fcf),
                    "detail": fcf_source or "Source: TTM cash flow statement",
                },
                {
                    "step": 2,
                    "label": "Forecasted growth",
                    "value": None,
                    "format": "compound",
                    "active": (s1 is not None and s1 > 0.005),  # >0.5%
                    "detail": (f"Stage 1 (yrs 1-5): {s1*100:.1f}% · "
                               f"Stage 2 (yrs 6-10): {s2*100:.1f}% · "
                               f"Terminal: {tg*100:.1f}%")
                              if (s1 and s2 and s1 > 0.005) else
                              "Growth assumptions unavailable — DCF used multiples-based or sector-specific path.",
                    "extra": {
                        "stage1_pct": round(s1*100, 1) if s1 and s1 > 0.005 else None,
                        "stage2_pct": round(s2*100, 1) if s2 and s2 > 0.005 else None,
                        "terminal_pct": round(tg*100, 1),
                        "growth_source": growth_source,
                    },
                },
                {
                    "step": 3,
                    "label": "Discount rate (WACC)",
                    "value": wacc_data.get("wacc") if wacc_data else None,
                    "format": "percent",
                    "active": bool(wacc_data and wacc_data.get("wacc")),
                    "detail": (f"Cost of Equity {wacc_data.get('coe', 0)*100:.1f}% · "
                               f"Cost of Debt {wacc_data.get('cod', 0)*100:.1f}%"
                               if wacc_data else "—"),
                },
                {
                    "step": 4,
                    "label": "Enterprise value (PV of FCF + Terminal)",
                    "value": enterprise_value,
                    "format": "currency_b",
                    "active": bool(enterprise_value),
                    "detail": (f"Stage PV: ${(total_pv_fcf or 0)/1e9:.1f}B · "
                               f"Terminal PV: ${(pv_terminal or 0)/1e9:.1f}B"
                               if (total_pv_fcf or pv_terminal) else "—"),
                },
                {
                    "step": 5,
                    "label": "Less net debt",
                    "value": -(net_debt or 0),
                    "format": "currency_b",
                    "active": bool(net_debt),
                    "detail": (f"${(safe(info.get('totalDebt'), 0) or 0)/1e9:.0f}B debt − "
                               f"${(safe(info.get('totalCash'), 0) or 0)/1e9:.0f}B cash"),
                },
                {
                    "step": 6,
                    "label": "Per-share fair value (pre-layers)",
                    "value": consensus_anchor_pre_iv,
                    "format": "currency",
                    "active": bool(consensus_anchor_pre_iv),
                    "detail": f"Equity value ÷ {_shares/1e9:.2f}B shares" if _shares else "—",
                },
                {
                    "step": 7,
                    "label": "Reality Reconciliation",
                    "value": None,
                    "format": "delta_pct",
                    "active": reality_reconciled,
                    "detail": (reality_reason or
                               "Gap below 25% threshold or analyst agrees with model — no blend applied."),
                    "extra": {"pre_iv": reality_pre_iv} if reality_reconciled else None,
                },
                {
                    "step": 8,
                    "label": "Cash-Rich Premium",
                    "value": _cr_prem * 100 if _cr_prem else 0,
                    "format": "delta_pct",
                    "active": bool(_cr_prem and _cr_prem > 0),
                    "detail": cash_rich_narrative or "Below 10% net cash threshold — no premium applied.",
                },
                {
                    "step": 9,
                    "label": "Debt Momentum",
                    "value": (debt_momentum.get("premium_pct", 0) or 0) * 100,
                    "format": "delta_pct",
                    "active": bool(debt_momentum and debt_momentum.get("premium_pct", 0) > 0),
                    "detail": (debt_momentum.get("narrative") if debt_momentum else
                               "No deleveraging signal detected."),
                },
                {
                    "step": 10,
                    "label": "Sultan Split (90% model · 10% analyst)",
                    "value": intrinsic_value,
                    "format": "currency",
                    "active": bool(analyst_adjusted),
                    "detail": (f"Pre-blend: ${consensus_anchor_pre_iv:.2f} · "
                               f"Analyst target: ${analyst_target_price:.2f}"
                               if (consensus_anchor_pre_iv and analyst_target_price)
                               else "No analyst target available — split skipped."),
                },
            ]
        except Exception:
            methodology_steps = None

        # ── Sector / P/E context notes ────────────────────────────────────────
        pe_ttm    = safe(info.get("trailingPE"))
        dcf_notes = get_dcf_notes(sector, industry, pe_ttm, dcf_available)

        # Add note when industry guardrails were applied
        if dcf_available and ind_class != "default":
            adj_parts = []
            if not user_tg_override:
                adj_parts.append(f"terminal growth capped at {tg*100:.1f}%")
            adj_parts.append(f"WACC floor {ind_params['min_wacc']*100:.1f}%")
            adj_parts.append(f"Stage 1 cap {ind_params['max_s1']*100:.0f}%")
            _ind_class_label = {
                "payment_network": "Network Effect",
                "structural_transformer": "Structural Transformer",
            }.get(ind_class, ind_class.capitalize())
            dcf_notes.insert(0, {
                "type": "info",
                "text": (f"{_ind_class_label} industry adjustments applied: "
                         + ", ".join(adj_parts) + ". "
                         "These prevent sector distortions in the Gordon Growth Model.")
            })

        # ── Confidence warnings → dcf_notes ──────────────────────────────────
        # Insert confidence warnings at the top of the notes stack so users see
        # the reliability context before reading the contextual sector notes.
        for warn_text in dcf_conf_warnings:
            dcf_notes.insert(0, {"type": "warn", "text": warn_text})

        # ── Analyst Consensus Cross-Reference Note ────────────────────────────
        # Explain large divergences between VALUS model IV and sell-side consensus.
        div_note = _analyst_divergence_note(
            intrinsic_value, analyst_target_price, price,
            rev_growth_pct, safe(info.get("forwardPE"))
        )
        if div_note:
            dcf_notes.append(div_note)

        # ── Terminal Value % warning ──────────────────────────────────────────
        # High TV% means the valuation is almost entirely driven by a terminal
        # assumption rather than observable near-term cash flows.
        if (enterprise_value and pv_terminal and enterprise_value > 0
                and pv_terminal / enterprise_value > 0.85):
            tv_pct_val = round(pv_terminal / enterprise_value * 100, 1)
            dcf_notes.append({
                "type": "warn",
                "text": (
                    f"Terminal value accounts for {tv_pct_val}% of enterprise value. "
                    "The valuation is highly sensitive to WACC and terminal growth assumptions — "
                    "small changes in either can materially shift the fair value estimate."
                ),
            })

        # ── Default growth source note ────────────────────────────────────────
        if growth_source and "Default" in growth_source:
            dcf_notes.append({
                "type": "info",
                "text": (
                    "No analyst coverage or historical FCF trend was available. "
                    "Stage 1 growth defaulted to 6% — treat this estimate with extra caution."
                ),
            })

        # ── PEG ratio ─────────────────────────────────────────────────────────
        peg = safe(info.get("pegRatio"))
        # If not in info, compute: TTM P/E ÷ consensus growth
        if peg is None and pe_ttm and s1:
            growth_pct = s1 * 100
            if growth_pct > 0:
                peg = round(pe_ttm / growth_pct, 2)

        # ── FCF yield and margin ──────────────────────────────────────────────
        mcap = safe(info.get("marketCap"), 0) or 0
        fcf_for_yield = base_fcf or (fcf_series[0] if fcf_series else None)
        fcf_yield   = round(fcf_for_yield / mcap * 100, 2) if fcf_for_yield and mcap > 0 else None
        rev_total   = rev_ttm  # Use TTM revenue for margin
        fcf_margin  = round(fcf_for_yield / rev_total * 100, 1) if fcf_for_yield and rev_total else None

        def pct(v):
            x = safe(v)
            return round(x * 100, 2) if x is not None else None

        def f2(v, d=2):
            x = safe(v)
            return round(x, d) if x is not None else None

        # ── Sensitivity Grid (3×3 WACC / TG) ─────────────────────────────────
        # Compute intrinsic value across ±1% WACC and ±0.5pp TG shifts.
        # Gives users a quick feel for how sensitive the valuation is to assumptions.
        sensitivity_grid = None
        if dcf_available and base_fcf and wacc and tg and s1 and s2:
            try:
                _wacc_deltas = [-0.010, 0.0, 0.010]
                _tg_deltas   = [-0.005, 0.0, 0.005]
                _grid_rows = []
                for _dw in _wacc_deltas:
                    _row = []
                    for _dt in _tg_deltas:
                        _gw = round(wacc + _dw, 4)
                        _gt = round(tg  + _dt, 4)
                        # Ensure WACC > TG (Gordon Growth stability)
                        if _gw <= _gt:
                            _gt = _gw - 0.005
                        _giv, _, _, _, _ = run_dcf_single(
                            base_fcf, s1, s2, _gt, _gw, yrs, info, fx_rate,
                            net_debt_override=net_debt,
                            stage1_years=backbone_stage1_years,
                        )
                        _row.append(round(_giv, 2) if _giv else None)
                    _grid_rows.append(_row)
                sensitivity_grid = {
                    "wacc_labels": [f"{(wacc + d)*100:.1f}%" for d in _wacc_deltas],
                    "tg_labels":   [f"{(tg   + d)*100:.1f}%" for d in _tg_deltas],
                    "values":      _grid_rows,
                }
            except Exception:
                sensitivity_grid = None

        result = {
            "ticker":       ticker,
            "company_name": info.get("longName", ticker),
            "sector":       sector,
            "industry":     industry,
            "currency":     info.get("currency", "USD"),
            "exchange":     info.get("exchange", ""),
            # Price
            "current_price": f2(price),
            "52w_high":      f2(info.get("fiftyTwoWeekHigh")),
            "52w_low":       f2(info.get("fiftyTwoWeekLow")),
            "target_price":  f2(info.get("targetMeanPrice")),
            "price_history": price_history,
            # Key stats strip
            "pe_ratio":      f2(pe_ttm),
            "forward_pe":    f2(info.get("forwardPE")),
            "peg_ratio":     f2(peg),
            "eps":           f2(info.get("trailingEps")),
            "market_cap":    f2(mcap),
            "previous_close": f2(info.get("regularMarketPreviousClose") or info.get("previousClose")),
            "daily_change_pct": (
                round((price - (info.get("regularMarketPreviousClose") or info.get("previousClose") or price)) /
                      (info.get("regularMarketPreviousClose") or info.get("previousClose") or price) * 100, 2)
                if price and (info.get("regularMarketPreviousClose") or info.get("previousClose"))
                else None
            ),
            "volume":        safe(info.get("volume")),
            "avg_volume":    safe(info.get("averageVolume")),
            "dividend_yield": dividend_yield,
            # DCF outputs
            "dcf_available":     dcf_available,
            "dcf_warning":       dcf_warning,
            "dcf_notes":         dcf_notes,
            "growth_source":     growth_source,
            "fcf_source":        fcf_source,
            "intrinsic_value":   round(intrinsic_value, 2) if intrinsic_value else None,
            # MOS clamped to a sane display range. Anything beyond ±200 is
            # almost always a data artefact (price=$0.30 ADR with $90 IV,
            # forward-EPS spike, share-class mismatch). Raw value preserved
            # in margin_of_safety_raw for transparency.
            "margin_of_safety":  (round(max(min(margin_of_safety, 200.0), -99.0), 1)
                                  if margin_of_safety is not None else None),
            "margin_of_safety_raw": (round(margin_of_safety, 1)
                                     if margin_of_safety is not None else None),
            # Low-confidence diagnostics — populated when |MoS| > 100% to
            # explain *why* the IV may not be trustworthy (share-class
            # mismatch, forward-earnings spike, stale price feed).
            "low_confidence_diagnostics": _diagnose_low_confidence(
                info, ticker, margin_of_safety, intrinsic_value, price
            ),
            # Whether the speculative 20-25% WACC band was applied to this
            # name (set True only when the auto-calibration shifted WACC).
            "wacc_speculative_applied": (
                wacc_speculative_applied if 'wacc_speculative_applied' in dir() else False
            ),
            "enterprise_value":  enterprise_value,
            "equity_value":      equity_value,
            "pv_terminal":       pv_terminal,
            "total_pv_fcf":      total_pv_fcf,
            "terminal_value_pct": round(pv_terminal / enterprise_value * 100, 1)
                                  if enterprise_value and pv_terminal else None,
            "net_debt":          net_debt if dcf_available else ((safe(info.get("totalDebt"), 0) or 0) - (safe(info.get("totalCash"), 0) or 0)) * fx_rate,
            "scenarios":         scenarios,
            "sensitivity_grid":  sensitivity_grid,
            # ── FIN 415 FCFE model outputs ────────────────────────────────────
            "fin415_used":          fin415_used,
            "fin415_ke":            round(ke * 100, 2) if fin415_used else None,
            "fin415_conservative":  round(conservative_target, 2) if fin415_used and conservative_target else None,
            "fin415_bear_wacc":     round(fin415_bear_wacc_iv, 2) if fin415_used and fin415_bear_wacc_iv else None,
            "fin415_bear_growth":   round(fin415_bear_grow_iv, 2) if fin415_used and fin415_bear_grow_iv else None,
            "fin415_op_margins":    fin415_op_margins if fin415_op_margins else None,
            "fin415_fcfe_rows":     fin415_fcfe_rows if fin415_fcfe_rows else None,
            "shares_outstanding": bal_data["shares"],
            "financial_currency": financial_ccy,
            "trading_currency":   trading_ccy,
            "fx_rate":            round(fx_rate, 4) if fx_rate != 1.0 else None,
            "wacc":          round(wacc_data.get("wacc", 0) * 100, 2) if wacc_data else None,
            "cost_of_equity":round(wacc_data.get("coe", 0)  * 100, 2) if wacc_data else None,
            "cost_of_debt":  round(wacc_data.get("cod", 0)  * 100, 2) if wacc_data else None,
            "tax_rate":      round(tax_rate * 100, 1),
            "beta":          round(wacc_data.get("beta", 1.0), 2) if wacc_data else None,
            "stage1_growth": round(s1 * 100, 2) if s1 is not None else None,
            "stage2_growth": round(s2 * 100, 2) if s2 is not None else None,
            "terminal_growth":round(tg * 100, 2),
            "blended_growth": ({
                "source":              blended.get("source"),
                "confidence":          blended.get("confidence"),
                "fellback_to_industry": blended.get("fellback_to_industry"),
                "components": {
                    k: (round(v * 100, 2) if isinstance(v, float) else v)
                    for k, v in (blended.get("components") or {}).items()
                },
            } if blended else None),
            "comparables_model": ({
                "used":               comparables_model.get("used"),
                "reason":             comparables_model.get("reason"),
                "peers":              comparables_model.get("peers"),
                "peer_margins": {
                    k: round(v * 100, 2) for k, v in
                    (comparables_model.get("peer_margins") or {}).items()
                },
                "peer_avg_margin":    round(comparables_model["peer_avg_margin"] * 100, 2)
                    if comparables_model.get("peer_avg_margin") is not None else None,
                "current_margin":     round(comparables_model["current_margin"] * 100, 2)
                    if comparables_model.get("current_margin") is not None else None,
                "age_years":          round(comparables_model["age_years"], 1)
                    if comparables_model.get("age_years") is not None else None,
                "p_attain":           round(comparables_model["p_attain"] * 100, 1)
                    if comparables_model.get("p_attain") is not None else None,
                "current_base_fcf":   comparables_model.get("current_base_fcf"),
                "peer_implied_fcf":   comparables_model.get("peer_implied_fcf"),
                "weighted_base_fcf":  comparables_model.get("weighted_base_fcf"),
            } if 'comparables_model' in dir() and comparables_model else None),
            "projection_years": yrs,
            "base_fcf":       base_fcf,
            "historical_fcf": fcf_series[:5],
            "projected_fcf":  projected,
            "fcf_chart":      fcf_chart,
            "fcf_yield":      fcf_yield,
            "fcf_margin":     fcf_margin,
            # Multiples
            "ps_ratio":      f2(info.get("priceToSalesTrailing12Months")),
            "pb_ratio":      f2(info.get("priceToBook")),
            "ev_ebitda":     f2(info.get("enterpriseToEbitda")),
            "ev_revenue":    f2(info.get("enterpriseToRevenue")),
            # Financials card — use TTM from quarterly for accuracy (esp. foreign cos)
            "revenue":           rev_total,  # keep as number, js will format
            "ebitda":            ebitda_ttm,  # keep as number, js will format
            "gross_margin":      gross_margin_ttm or pct(info.get("grossMargins")),
            "operating_margin":  operating_margin_ttm or pct(info.get("operatingMargins")),
            "profit_margin":     net_margin_ttm or pct(info.get("profitMargins")),
            "revenue_growth":    pct(info.get("revenueGrowth")),
            "earnings_growth":   pct(info.get("earningsGrowth")),
            "roe":               pct(info.get("returnOnEquity")),
            "roa":               pct(info.get("returnOnAssets")),
            "div_yield":         dividend_yield,
            # Health card — use most recent quarterly balance sheet data
            "total_cash":    f2(bal_data["total_cash"]),
            "total_debt":    f2(bal_data["total_debt"]),
            "current_ratio": f2(info.get("currentRatio")),
            "quick_ratio":   f2(info.get("quickRatio")),
            "debt_to_equity":f2(info.get("debtToEquity")),
            "payout_ratio":  pct(info.get("payoutRatio")),
            # Analyst
            "analyst_rating": info.get("recommendationKey", "N/A"),
            "analyst_count":  info.get("numberOfAnalystOpinions"),
            # Multiples-based fallback valuation
            "multiples_val":    multiples_val,
            "multiples_method": multiples_method,
            "multiples_reason": multiples_reason,
            "multiples_mos":    multiples_mos,
            # Sector-adaptive valuation
            "valuation_method":  valuation_method,
            "sector_val_label":  sector_val_label,
            "analyst_adjusted":        analyst_adjusted,
            "consensus_anchor_pre_iv": consensus_anchor_pre_iv,
            "analyst_target":          analyst_target_price,
            # ── Expectation Gap Engine ─────────────────────────────────────
            "expectation_gap":         expectation_gap,
            "implied_growth_pct":      implied_growth_pct,
            # ── Reality Reconciliation ────────────────────────────────────
            "reality_reconciled":      reality_reconciled,
            "reality_pre_iv":          reality_pre_iv,
            "reality_reason":          reality_reason,
            # ── Confidence flag for extreme MOS values ─────────────────────
            # When |MOS| > 100% the DCF inputs are usually mispriced (forward
            # earnings spike, leverage quirk, share-class issue, stale data).
            # Flag so the UI can show a "low confidence" badge.
            "extreme_mos_flag":  (margin_of_safety is not None
                                  and abs(margin_of_safety) > 100),
            # ── "Priced For" Verdict + Verdict Summary ────────────────────
            "priced_for":              priced_for,
            "verdict_summary":         verdict_summary,
            "sector_growth_ceiling_pct": round(_ceiling * 100, 1) if _ceiling else None,
            "sector_growth_ceiling_label": _ceiling_label,
            "is_mag7":                 is_mag7,
            "debt_momentum":           debt_momentum,
            # ── Cash-Rich Premium ────────────────────────────────────────
            "is_cash_rich":            is_cash_rich,
            "cash_pct_of_mcap":        cash_pct_of_mcap,
            "cash_rich_narrative":     cash_rich_narrative,
            "cash_rich_premium_pct":   round(_cr_prem * 100, 1) if _cr_prem else 0.0,
            # ── Methodology Explainer payload ───────────────────────────
            "methodology_steps":       methodology_steps,
            # Moat detection
            "moat_detected":    moat_detected,
            "moat_path":        moat_path,
            "moat_reasons":     moat_reasons,
            "moat_wacc_delta":  round(moat_wacc_delta * 100, 2) if moat_wacc_delta else None,
            "moat_mult_premium":round((moat_mult_premium - 1) * 100) if moat_detected else None,
            # DCF Confidence Score
            "dcf_confidence":            dcf_conf_level,
            "dcf_confidence_label":      dcf_conf_label,
            "dcf_confidence_strengths":  dcf_conf_strengths,
            "dcf_confidence_warnings":   dcf_conf_warnings,
            # Discovery Layer — catalyst research
            "catalyst_insights":         catalyst_insights,
            "has_positive_catalyst":     has_positive_catalyst,
            "has_material_risk":         has_material_risk,
            "momentum_premium_pct":      round(momentum_premium * 100, 1) if momentum_applied else None,
            "wacc_risk_applied":         catalyst_wacc_risk > 0,
            "catalyst_labels":           catalyst_labels,
            "risk_labels":               risk_labels,
            "backbone_stage1_extended":  bool(backbone_stage1_years),
            "cash_rich_wacc_applied":    cash_rich_wacc_applied,
            # Structural Transformer fields
            "structural_transformer":    is_structural_transformer,
            # st_capex fields are populated for both full transformers and partial normalizations
            "st_capex_addback_bn":       round(st_capex_addback / 1e9, 2) if st_capex_addback else None,
            "st_capex_addback_rate_pct": round(st_capex_addback_rate * 100) if st_capex_abs > 0 else None,
            "st_capex_to_rev_pct":       round(st_capex_to_rev * 100, 1) if st_capex_to_rev else None,
            "st_robotaxi_s2_applied":    st_robotaxi_s2_applied if is_structural_transformer else False,
            # ── Strategic Asset layer ─────────────────────────────────────
            "is_strategic":              bool(strategic),
            "strategic_tier":            strategic["strategic_tier"]   if strategic else None,
            "strategic_label":           strategic["strategic_label"]  if strategic else None,
            "strategic_reason":          strategic["strategic_reason"] if strategic else None,
            "strategic_narrative":       strategic["narrative"]        if strategic else None,
            "strategic_wacc_delta_pp":   round(strategic_wacc_delta * 100, 2) if strategic else 0.0,
            "strategic_floor_applied":   strategic_floor_applied,
            "strategic_live_amplified":  bool(strategic and strategic.get("live_policy_amplifier")),
            # ── Policy news signals ──────────────────────────────────────
            "policy_tailwind":           policy_tailwind,
            "policy_tailwind_labels":    policy_tailwind_labels,
            "policy_headwind":           policy_headwind,
            "policy_headwind_labels":    policy_headwind_labels,
            # ── IV provenance + confidence (never-zero guarantee) ─────────
            # Sector-specific blends (biotech, banking, etc.) set
            # sector_val_label rather than iv_source_label; surface that as
            # the source when it's the dominant signal.
            "iv_source_label":           sector_val_label or iv_source_label,
            "iv_confidence":             iv_confidence,
            # ── Autonomous news interpretation ────────────────────────────
            "growth_catalyst_lift_pp":   round(growth_catalyst_lift_pp, 2),
            "transformative_labels":     transformative_labels,
            "news_interpretation":       news_interpretation,
            # ── Hero insights & quality scorecard ────────────────────────
            # All sourced from existing info dict; surfaced for the new
            # post-search insight cards (no extra API calls).
            "fifty_two_week_high":       safe(info.get("fiftyTwoWeekHigh")),
            "fifty_two_week_low":        safe(info.get("fiftyTwoWeekLow")),
            "quality_metrics": _build_quality_metrics(info, base_fcf, rev_ttm),
        }

        cleaned = clean(result)
        _analyze_cache_set(_ck, cleaned)
        resp = jsonify(cleaned)
        resp.headers["X-Valus-Cache"] = "MISS"
        return resp

    except Exception as e:
        # Log full trace server-side; never leak it to the client.  A raw
        # Python stack trace in front of an investor is a credibility hit.
        app.logger.exception("analyze failed for %s", ticker)
        return jsonify({
            "error": "We hit an issue analyzing that ticker. Try another, or try again in a moment.",
        }), 500


def _warm_discovery_cache():
    """
    Background thread that pre-runs the discovery list so the FIRST user
    hitting /api/discover gets a fast response instead of waiting ~150s.
    Re-warms every 12 minutes to keep cache fresh.
    """
    import threading
    def loop():
        while True:
            try:
                for t in DISCOVERY_TICKERS:
                    ck = _analyze_cache_key(t, {})
                    if _analyze_cache_get(ck) is None:
                        try:
                            with app.test_request_context(
                                f"/api/analyze?ticker={t}",
                                headers={"X-Valus-Internal": "1"},
                            ):
                                analyze()
                        except Exception:
                            pass
                        time.sleep(0.4)   # gentle on yfinance
            except Exception:
                pass
            time.sleep(12 * 60)
    threading.Thread(target=loop, daemon=True).start()


if __name__ == "__main__":
    # Kick off background warmup once
    _warm_discovery_cache()
    app.run(debug=True, port=int(os.environ.get("PORT", 5050)))
