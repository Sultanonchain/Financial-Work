from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import yfinance as yf
import numpy as np
import pandas as pd
import requests
import traceback
import time
import re
import xml.etree.ElementTree as ET
from datetime import datetime

app = Flask(__name__)
CORS(app)

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
    seven_days_ago    = now - 7 * 86400

    # ── 1. yfinance news ─────────────────────────────────────────────────────
    try:
        news_items = getattr(stock, "news", None) or []
        recent = sorted(
            [n for n in news_items if n.get("providerPublishTime", 0) > seven_days_ago],
            key=lambda x: x.get("providerPublishTime", 0), reverse=True
        )
        for item in recent[:10]:
            raw_title   = item.get("title",   "") or ""
            raw_snippet = item.get("summary", "") or ""
            text = (raw_title + " " + raw_snippet).lower()

            is_strong = any(kw in text for kw in _CATALYST_STRONG)
            is_mod    = any(kw in text for kw in _CATALYST_MODERATE)
            is_risk   = any(kw in text for kw in _RISK_KEYWORDS)

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

            if len(insights) < 3:
                pub_ts   = item.get("providerPublishTime", 0)
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

    result = {
        "insights":              insights[:3],
        "momentum_premium":      round(momentum_premium, 3),
        "wacc_risk_add":         round(wacc_risk_add, 3),
        "has_positive_catalyst": has_positive,
        "has_material_risk":     has_risk,
        "catalyst_labels":       catalyst_labels[:2],
        "risk_labels":           risk_labels[:2],
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
    Sultan Split: unconditionally blend 70% VALUS model IV + 30% analyst
    consensus target whenever an analyst target is available.

    Rationale (FIN 415 template): The model captures long-run intrinsic value;
    the analyst consensus anchors the near-term Street view.  A fixed 70/30
    split gives users a single price that is both fundamentally grounded and
    cross-referenceable with professional estimates.

    Returns (adjusted_iv, was_adjusted: bool, pre_blend_iv).
    """
    if not iv or not analyst_target or not price or price <= 0:
        return iv, False, iv
    at = float(analyst_target)
    if at <= 0:
        return iv, False, iv
    # Unconditional Sultan Split: 70% model + 30% analyst (always fires)
    blended = round(max(0.70 * iv + 0.30 * at, 0.0), 2)
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
                          low=0.0, high=3.0, tol=5e-5):
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

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/search")
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
        return jsonify({"error": str(e)}), 500


@app.route("/api/statements")
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
        return jsonify({"error": str(e)}), 500


@app.route("/api/analyze")
def analyze():
    ticker = request.args.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "Ticker is required"}), 400

    try:
        # ── User overrides ────────────────────────────────────────────────────
        s1_ov = request.args.get("growth1")
        s2_ov = request.args.get("growth2")
        tg    = float(request.args.get("terminal", 0.03))
        yrs   = int(request.args.get("years", 10))
        tg    = min(max(tg, 0.01), 0.05)
        yrs   = min(max(yrs, 5), 15)

        # ── Data pull ─────────────────────────────────────────────────────────
        stock       = yf.Ticker(ticker)
        info        = stock.info
        price       = safe(info.get("currentPrice") or info.get("regularMarketPrice"))

        if not price:
            return jsonify({"error": f"No data for '{ticker}'. Check the symbol."}), 404

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
        momentum_applied        = False   # set True when premium is baked into IV

        # ── Price history (1Y default) ────────────────────────────────────────
        hist = stock.history(period="1y", interval="1d")
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

            if s1_ov:
                s1 = float(s1_ov) / 100 if float(s1_ov) > 1 else float(s1_ov)
                growth_source = "User override"
            else:
                s1, growth_source = get_forward_growth(stock, info, fcf_series)
                s1 = min(s1, effective_max_s1)

            if s2_ov:
                s2 = float(s2_ov) / 100 if float(s2_ov) > 1 else float(s2_ov)
            else:
                # Stage 2 = 55% of Stage 1 (standard mean-reversion taper), with:
                #   • floor of (TG + 0.5pp) — prevents S2 falling below terminal rate
                #   • absolute floor of 2% — even mature companies grow at nominal GDP pace
                s2 = max(s1 * 0.55, tg + 0.005, 0.02)

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

            # ── Probability-weighted fair value ───────────────────────────────
            if all(v is not None for v in [intrinsic_value, iv_bull, iv_bear]):
                iv_weighted = _w_base * intrinsic_value + _w_bull * iv_bull + _w_bear * iv_bear
            else:
                iv_weighted = intrinsic_value

            # Update primary margin of safety to use weighted value
            margin_of_safety = ((iv_weighted - price) / price * 100) if iv_weighted and price else None

            scenarios = {
                "base": {
                    "value":  round(intrinsic_value, 2) if intrinsic_value else None,
                    "weight": round(_w_base * 100),
                    "s1":     round(s1 * 100, 2),
                    "wacc":   round(wacc * 100, 2),
                    "upside": round((intrinsic_value - price) / price * 100, 1) if intrinsic_value and price else None,
                },
                "bull": {
                    "value":  round(iv_bull, 2) if iv_bull else None,
                    "weight": round(_w_bull * 100),
                    "s1":     round(s1_bull * 100, 2),
                    "wacc":   round(wacc_bull * 100, 2),
                    "upside": round((iv_bull - price) / price * 100, 1) if iv_bull and price else None,
                    "recalculated": bull_recalculated,
                    "distressed":   bull_distressed,
                },
                "bear": {
                    "value":  round(iv_bear, 2) if iv_bear else None,
                    "weight": round(_w_bear * 100),
                    "s1":     round(s1_bear * 100, 2),
                    "wacc":   round(wacc_bear * 100, 2),
                    "upside": round((iv_bear - price) / price * 100, 1) if iv_bear and price else None,
                    "floored":      bear_floored,
                    "recalculated": bear_recalculated,
                    "distressed":   bear_distressed,
                },
                "weighted": round(iv_weighted, 2) if iv_weighted else None,
                "weighted_upside": round((iv_weighted - price) / price * 100, 1) if iv_weighted and price else None,
                "weight_basis": _scenario_weight_note,
            }
            intrinsic_value = round(iv_weighted, 2) if iv_weighted else intrinsic_value

            # Negative equity value: net debt exceeds DCF enterprise value
            if equity_value is not None and equity_value < 0:
                dcf_warning = (
                    f"Net debt (${abs(net_debt)/1e9:.1f}B) exceeds DCF enterprise value "
                    f"(${enterprise_value/1e9:.1f}B) — equity value is technically negative. "
                    "This means the company's debt load overwhelms the modelled cash flows. "
                    "Use EV/EBITDA or forward earnings multiples instead of DCF for this security."
                )
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
            # DCF was forced off; use P/B + P/E banking model
            _bkv, _bkm = calc_banking_val(info, fx_rate)
            if _bkv is not None:
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
            "margin_of_safety":  round(margin_of_safety, 1) if margin_of_safety is not None else None,
            "enterprise_value":  enterprise_value,
            "equity_value":      equity_value,
            "pv_terminal":       pv_terminal,
            "total_pv_fcf":      total_pv_fcf,
            "terminal_value_pct": round(pv_terminal / enterprise_value * 100, 1)
                                  if enterprise_value and pv_terminal else None,
            "net_debt":          net_debt if dcf_available else ((safe(info.get("totalDebt"), 0) or 0) - (safe(info.get("totalCash"), 0) or 0)) * fx_rate,
            "scenarios":         scenarios if dcf_available else None,
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
        }

        return jsonify(clean(result))

    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5050)
