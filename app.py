from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import yfinance as yf
import numpy as np
import pandas as pd
import requests
import traceback

app = Flask(__name__)
CORS(app)

RISK_FREE_RATE   = 0.045   # 10-yr US Treasury proxy
EQUITY_RISK_PREM = 0.055   # Damodaran ERP

# FX rate cache (in-memory, lives for the process lifetime — good enough for a session)
_fx_cache: dict = {}

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
    beta = safe(info.get("beta"), 1.0) or 1.0
    beta = min(max(beta, 0.3), 3.0)
    coe  = RISK_FREE_RATE + beta * EQUITY_RISK_PREM

    cod = 0.05
    try:
        # iexp and debt are both in reporting currency, so the ratio is currency-neutral
        iexp = None
        if income_stmt is not None and not income_stmt.empty:
            for lbl in ["Interest Expense Non Operating", "Interest Expense"]:
                if lbl in income_stmt.index:
                    v = safe(income_stmt.loc[lbl].iloc[0])
                    if v is not None:
                        iexp = abs(v); break
        debt = safe(info.get("totalDebt"), 0) or 0
        if iexp and debt > 0:
            c = iexp / debt
            if 0.01 < c < 0.20:
                cod = c
    except Exception:
        pass

    mcap = safe(info.get("marketCap"), 0) or 0          # in trading currency
    debt = (safe(info.get("totalDebt"), 0) or 0) * fx_rate  # convert to trading currency
    tc   = mcap + debt
    we, wd = (mcap / tc, debt / tc) if tc > 0 else (0.85, 0.15)

    wacc = we * coe + wd * cod * (1 - tax_rate)
    if not wacc or np.isnan(wacc):
        wacc = coe
    return {
        "wacc": min(max(wacc, 0.05), 0.18),
        "coe": coe, "cod": cod,
        "tax": tax_rate, "beta": beta,
        "we": we, "wd": wd,
    }


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
    Priority: analyst +1y EPS → analyst trend → earnings growth (bounded) → historical FCF CAGR.
    Returns (rate, source_label).
    """
    # 1. Analyst earnings estimate growth for next fiscal year
    try:
        ee = stock.earnings_estimate
        if ee is not None and not ee.empty and '+1y' in ee.index:
            v = safe(ee.loc['+1y', 'growth'])
            if v is not None and 0.02 < v < 0.50:
                return float(v), "Analyst EPS est. (+1y)"
    except Exception:
        pass

    # 2. Growth estimates dataframe (analyst consensus)
    try:
        ge = stock.growth_estimates
        if ge is not None and not ge.empty and '+1y' in ge.index:
            for col in ge.columns:
                v = safe(ge.loc['+1y', col])
                if v is not None and 0.02 < v < 0.50:
                    return float(v), "Analyst consensus (+1y)"
    except Exception:
        pass

    # 3. Trailing earnings growth (YoY) — bounded conservatively
    ag = safe(info.get("earningsGrowth"))
    if ag is not None and 0.02 < ag < 0.30:
        return float(ag), "Trailing EPS growth (YoY)"

    # 4. Historical FCF CAGR from annual statement
    positives = [f for f in fcf_series if f > 0]
    if len(positives) >= 3:
        span = len(positives) - 1
        hg = (positives[0] / positives[-1]) ** (1 / span) - 1
        hg = min(max(hg, 0.02), 0.18)
        return hg, "Historical FCF CAGR"

    # 5. Default
    rev_g = safe(info.get("revenueGrowth"))
    if rev_g and 0.02 < rev_g < 0.30:
        return float(rev_g * 0.8), "Revenue growth proxy"

    return 0.07, "Default (7%)"


# ── Sector / P/E context ───────────────────────────────────────────────────────

def get_dcf_notes(sector, industry, pe, fcf_available):
    """Return list of contextual notes about valuation reliability."""
    notes = []
    s = (sector or "").lower()
    ind = (industry or "").lower()

    # Sector-specific DCF reliability
    if any(x in s for x in ["financial", "bank"]) or any(x in ind for x in ["bank", "saving", "thrift"]):
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

        cashflow    = stock.cashflow
        income_stmt = stock.income_stmt

        # ── Currency handling ─────────────────────────────────────────────────
        # yfinance returns financial statements in the company's REPORTING currency
        # (financialCurrency) but stock price / market cap are in TRADING currency.
        # For foreign ADRs (e.g. NOK reports in EUR, trades in USD) we must convert.
        financial_ccy = info.get("financialCurrency") or info.get("currency") or "USD"
        trading_ccy   = info.get("currency") or "USD"
        fx_rate = get_fx_rate(financial_ccy, trading_ccy)   # e.g. EUR→USD ≈ 1.09

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

        # ── DCF computation ───────────────────────────────────────────────────
        intrinsic_value  = None
        margin_of_safety = None
        projected        = []
        enterprise_value = None
        equity_value     = None
        pv_terminal      = None
        total_pv_fcf     = None
        fcf_chart        = None
        s1 = s2          = None
        wacc_data        = {}
        dcf_warning      = None
        growth_source    = None

        if not dcf_available:
            if not fcf_series:
                dcf_warning = "No Free Cash Flow data available — DCF not computed."
            else:
                most_recent = fcf_series[0] if fcf_series else 0
                dcf_warning = (
                    f"Most recent FCF is negative (${most_recent/1e9:.1f}B) — "
                    "DCF intrinsic value is unreliable. Showing multiples-based analysis only."
                )
        else:
            # Growth rate
            if s1_ov:
                s1 = float(s1_ov) / 100 if float(s1_ov) > 1 else float(s1_ov)
                growth_source = "User override"
            else:
                s1, growth_source = get_forward_growth(stock, info, fcf_series)
                s1 = min(s1, 0.25)   # hard cap at 25%

            if s2_ov:
                s2 = float(s2_ov) / 100 if float(s2_ov) > 1 else float(s2_ov)
            else:
                s2 = max(s1 * 0.55, tg + 0.005)   # Stage 2 fades to near-terminal

            # WACC
            wacc_data = calc_wacc(info, income_stmt, tax_rate, fx_rate)
            wacc      = wacc_data["wacc"]
            half      = yrs // 2
            fcf_run   = base_fcf

            # Ensure WACC > terminal growth (Gordon Growth Model requires this)
            if wacc <= tg:
                tg = wacc - 0.01

            for y in range(1, yrs + 1):
                g       = s1 if y <= half else s2
                fcf_run = fcf_run * (1 + g)
                pv      = fcf_run / ((1 + wacc) ** y)
                projected.append({"year": y, "fcf": fcf_run, "pv": pv, "growth": g})

            # Terminal value — Gordon Growth Model on Year-N FCF
            tv_fcf       = projected[-1]["fcf"] * (1 + tg)
            terminal_val = tv_fcf / (wacc - tg)
            pv_terminal  = terminal_val / ((1 + wacc) ** yrs)

            total_pv_fcf     = sum(p["pv"] for p in projected)
            enterprise_value = total_pv_fcf + pv_terminal

            # Convert balance sheet items from reporting → trading currency
            cash         = (safe(info.get("totalCash"), 0) or 0) * fx_rate
            debt         = (safe(info.get("totalDebt"), 0) or 0) * fx_rate
            net_debt     = debt - cash
            equity_value = enterprise_value - net_debt

            shares = safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0) or 0
            intrinsic_value  = (equity_value / shares) if shares > 0 else None
            margin_of_safety = ((intrinsic_value - price) / price * 100) if intrinsic_value and price else None

            fcf_chart = {
                "projected": {
                    "labels": [f"Y{p['year']}" for p in projected],
                    "values": [round(p["fcf"] / 1e9, 2) for p in projected],
                    "pvs":    [round(p["pv"]  / 1e9, 2) for p in projected],
                }
            }

        # ── Sector / P/E context notes ────────────────────────────────────────
        pe_ttm   = safe(info.get("trailingPE"))
        sector   = info.get("sector", "")
        industry = info.get("industry", "")
        dcf_notes = get_dcf_notes(sector, industry, pe_ttm, dcf_available)

        # ── PEG ratio ─────────────────────────────────────────────────────────
        peg = safe(info.get("pegRatio"))
        # If not in info, compute: TTM P/E ÷ consensus growth
        if peg is None and pe_ttm and s1:
            growth_pct = s1 * 100
            if growth_pct > 0:
                peg = round(pe_ttm / growth_pct, 2)

        # ── Income statement TTM (prefer quarterly for foreign companies) ──────
        inc_ttm = get_income_stmt_ttm(stock)
        rev_ttm = inc_ttm.get("Total Revenue") or safe(info.get("totalRevenue"))
        cogs_ttm = inc_ttm.get("Cost Of Revenue")
        gp_ttm = inc_ttm.get("Gross Profit")
        oi_ttm = inc_ttm.get("Operating Income")
        ni_ttm = inc_ttm.get("Net Income")
        ebitda_ttm = inc_ttm.get("EBITDA") or safe(info.get("ebitda"))

        # Compute margins from TTM data
        gross_margin_ttm = round(gp_ttm / rev_ttm * 100, 2) if gp_ttm and rev_ttm else None
        operating_margin_ttm = round(oi_ttm / rev_ttm * 100, 2) if oi_ttm and rev_ttm else None
        net_margin_ttm = round(ni_ttm / rev_ttm * 100, 2) if ni_ttm and rev_ttm else None

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
            "net_debt":          ((safe(info.get("totalDebt"), 0) or 0) - (safe(info.get("totalCash"), 0) or 0)) * fx_rate,
            "shares_outstanding": safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0),
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
            # Health card — convert balance sheet items to trading currency
            "total_cash":    f2((safe(info.get("totalCash")) or 0) * fx_rate),
            "total_debt":    f2((safe(info.get("totalDebt")) or 0) * fx_rate),
            "current_ratio": f2(info.get("currentRatio")),
            "quick_ratio":   f2(info.get("quickRatio")),
            "debt_to_equity":f2(info.get("debtToEquity")),
            "payout_ratio":  pct(info.get("payoutRatio")),
            # Analyst
            "analyst_rating": info.get("recommendationKey", "N/A"),
            "analyst_count":  info.get("numberOfAnalystOpinions"),
        }

        return jsonify(clean(result))

    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5050)
