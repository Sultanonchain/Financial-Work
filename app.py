from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import yfinance as yf
import numpy as np
import pandas as pd
import requests
import traceback

app = Flask(__name__)
CORS(app)

RISK_FREE_RATE    = 0.045
EQUITY_RISK_PREM  = 0.055

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


def fmt_num(v):
    """Format large numbers for display."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    v = float(v)
    return round(v, 2)


# ── WACC ───────────────────────────────────────────────────────────────────────

def calc_wacc(info, income_stmt):
    beta = safe(info.get("beta"), 1.0) or 1.0
    beta = min(max(beta, 0.3), 3.0)
    coe  = RISK_FREE_RATE + beta * EQUITY_RISK_PREM

    cod = 0.05
    try:
        iexp = None
        if income_stmt is not None and not income_stmt.empty:
            for lbl in ["Interest Expense", "Interest Expense Non Operating"]:
                if lbl in income_stmt.index:
                    v = safe(income_stmt.loc[lbl].iloc[0])
                    if v is not None:
                        iexp = abs(v); break
        debt = safe(info.get("totalDebt"), 0) or 0
        if iexp and debt > 0:
            c = iexp / debt
            if not (np.isnan(c) or np.isinf(c)):
                cod = min(max(c, 0.02), 0.15)
    except Exception:
        pass

    tax  = safe(info.get("effectiveTaxRate"), 0.21) or 0.21
    tax  = min(max(tax, 0.05), 0.40)
    mcap = safe(info.get("marketCap"), 0) or 0
    debt = safe(info.get("totalDebt"), 0) or 0
    tc   = mcap + debt
    we, wd = (mcap/tc, debt/tc) if tc > 0 else (0.8, 0.2)

    wacc = we * coe + wd * cod * (1 - tax)
    if not wacc or np.isnan(wacc): wacc = coe
    return {"wacc": min(max(wacc, 0.05), 0.20), "coe": coe, "cod": cod,
            "tax": tax, "beta": beta, "we": we, "wd": wd}


# ── FCF series ─────────────────────────────────────────────────────────────────

def get_fcf(cashflow):
    if cashflow is None or cashflow.empty:
        return []
    try:
        if "Free Cash Flow" in cashflow.index:
            vals = cashflow.loc["Free Cash Flow"].values
        else:
            ok  = next((k for k in cashflow.index if "Operating" in k and "Cash" in k), None)
            ck  = next((k for k in cashflow.index if "Capital Expenditure" in k), None)
            if not ok:
                return []
            ocf  = cashflow.loc[ok].values
            capx = cashflow.loc[ck].values if ck else np.zeros(len(ocf))
            vals = ocf + capx
        return [float(v) for v in vals if v is not None and not np.isnan(float(v))]
    except Exception:
        return []


# ── Format a DataFrame as a list of {label, values…} rows ─────────────────────

def df_to_rows(df, max_rows=35):
    """df should have dates as columns and line items as index (yfinance default)."""
    if df is None or df.empty:
        return {"columns": [], "rows": []}
    try:
        # Format column dates cleanly (strip time component)
        cols = []
        for c in df.columns:
            if hasattr(c, 'strftime'):
                cols.append(c.strftime('%Y-%m-%d'))
            else:
                s = str(c)
                cols.append(s[:10] if len(s) > 10 else s)

        rows = []
        for label, series in df.iterrows():
            vals = []
            for v in series.values:
                try:
                    f = float(v)
                    vals.append(None if np.isnan(f) or np.isinf(f) else round(f, 0))
                except Exception:
                    vals.append(None)
            rows.append({"label": str(label), "values": vals})
        return {"columns": cols, "rows": rows[:max_rows]}
    except Exception:
        return {"columns": [], "rows": []}


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/search")
def search():
    q = request.args.get("q", "").strip()
    if not q or len(q) < 1:
        return jsonify([])
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={q}&quotesCount=7&newsCount=0&listsCount=0"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        data = r.json()
        results = []
        for item in data.get("quotes", []):
            qtype = item.get("quoteType", "")
            if qtype not in ("EQUITY", "ETF"):
                continue
            results.append({
                "symbol":   item.get("symbol", ""),
                "name":     item.get("longname") or item.get("shortname", ""),
                "exchange": item.get("exchDisp", ""),
                "type":     qtype,
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
        stock = yf.Ticker(ticker)
        hist  = stock.history(period=yf_period, interval=yf_interval)
        if hist.empty:
            return jsonify({"prices": []})

        is_intraday = yf_interval in ("5m", "30m")
        prices = []
        for idx, row in hist.iterrows():
            if is_intraday:
                label = idx.strftime("%m/%d %H:%M")
            else:
                label = str(idx.date())
            prices.append({"date": label, "close": round(float(row["Close"]), 2),
                           "open": round(float(row["Open"]), 2),
                           "high": round(float(row["High"]), 2),
                           "low":  round(float(row["Low"]), 2),
                           "volume": int(row["Volume"]) if not np.isnan(float(row["Volume"])) else 0})
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

        def safe_df(df):
            # yfinance returns df with dates as columns, items as index — use as-is
            return df_to_rows(df)

        return jsonify(clean({
            "income":  safe_df(stock.income_stmt),
            "balance": safe_df(stock.balance_sheet),
            "cashflow":safe_df(stock.cashflow),
        }))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/analyze")
def analyze():
    ticker  = request.args.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "Ticker is required"}), 400

    try:
        s1_ov   = request.args.get("growth1")
        s2_ov   = request.args.get("growth2")
        tg      = float(request.args.get("terminal", 0.03))
        yrs     = int(request.args.get("years", 10))
        tg      = min(max(tg, 0.01), 0.05)
        yrs     = min(max(yrs, 5), 15)

        stock   = yf.Ticker(ticker)
        info    = stock.info
        price   = safe(info.get("currentPrice") or info.get("regularMarketPrice"), 0)

        if not price:
            return jsonify({"error": f"No data for '{ticker}'. Check the symbol."}), 404

        cashflow    = stock.cashflow
        income_stmt = stock.income_stmt

        # ── 1Y price history ───────────────────────────────────────────────────
        hist = stock.history(period="1y", interval="1d")
        price_history = []
        if not hist.empty:
            price_history = [{"date": str(i.date()), "close": round(float(r["Close"]), 2)}
                             for i, r in hist.iterrows()]

        # ── FCF ────────────────────────────────────────────────────────────────
        fcf_vals = get_fcf(cashflow)
        dcf_available = bool(fcf_vals) and fcf_vals[0] > 0

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

        if not fcf_vals:
            dcf_warning = "No Free Cash Flow data available — DCF not computed."
        elif fcf_vals[0] <= 0:
            dcf_warning = f"Most recent FCF is negative (${fcf_vals[0]/1e9:.1f}B) — DCF intrinsic value is unreliable for this company. Showing multiples-based analysis only."
        else:
            base_fcf = fcf_vals[0]
            positive = [f for f in fcf_vals if f > 0]
            if len(positive) >= 2:
                span = len(positive) - 1
                hg   = (positive[0] / positive[-1]) ** (1/span) - 1
                hg   = min(max(hg, -0.30), 0.50)
            else:
                hg = 0.07

            ag = safe(info.get("earningsGrowth") or info.get("revenueGrowth"))
            s1 = float(s1_ov) if s1_ov else (ag if ag and -0.3 < ag < 0.5 else min(hg, 0.25))
            s2 = float(s2_ov) if s2_ov else max(s1 * 0.5, tg + 0.01)

            wacc_data = calc_wacc(info, income_stmt)
            wacc      = wacc_data["wacc"]
            half      = yrs // 2
            fcf       = base_fcf

            for y in range(1, yrs + 1):
                g   = s1 if y <= half else s2
                fcf = fcf * (1 + g)
                pv  = fcf / ((1 + wacc) ** y)
                projected.append({"year": y, "fcf": fcf, "pv": pv, "growth": g})

            tv_fcf       = projected[-1]["fcf"] * (1 + tg)
            tv           = tv_fcf / (wacc - tg)
            pv_terminal  = tv / ((1 + wacc) ** yrs)
            total_pv_fcf = sum(p["pv"] for p in projected)
            enterprise_value = total_pv_fcf + pv_terminal

            cash         = safe(info.get("totalCash"), 0) or 0
            debt         = safe(info.get("totalDebt"), 0) or 0
            net_debt     = debt - cash
            equity_value = enterprise_value - net_debt
            shares       = safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0) or 0
            intrinsic_value  = (equity_value / shares) if shares > 0 else None
            margin_of_safety = ((intrinsic_value - price) / price * 100) if intrinsic_value and price else None

            fcf_chart = {
                "projected": {
                    "labels": [f"Y{p['year']}" for p in projected],
                    "values": [round(p["fcf"]/1e9, 2) for p in projected],
                    "pvs":    [round(p["pv"]/1e9, 2)  for p in projected],
                }
            }

        def pct(v):
            x = safe(v); return round(x*100, 2) if x is not None else None
        def f2(v, d=2):
            x = safe(v); return round(x, d)     if x is not None else None

        result = {
            "ticker":        ticker,
            "company_name":  info.get("longName", ticker),
            "sector":        info.get("sector", "N/A"),
            "industry":      info.get("industry", "N/A"),
            "currency":      info.get("currency", "USD"),
            "exchange":      info.get("exchange", ""),
            "description":   info.get("longBusinessSummary", ""),
            # Price
            "current_price": f2(price),
            "52w_high":      f2(info.get("fiftyTwoWeekHigh")),
            "52w_low":       f2(info.get("fiftyTwoWeekLow")),
            "target_price":  f2(info.get("targetMeanPrice")),
            "price_history": price_history,
            # Key stats for strip
            "pe_ratio":      f2(info.get("trailingPE")),
            "forward_pe":    f2(info.get("forwardPE")),
            "eps":           f2(info.get("trailingEps")),
            "market_cap":    f2(info.get("marketCap")),
            "volume":        safe(info.get("volume")),
            "avg_volume":    safe(info.get("averageVolume")),
            "dividend_yield":pct(info.get("dividendYield")),
            # DCF
            "dcf_available":     dcf_available,
            "dcf_warning":       dcf_warning,
            "intrinsic_value":   round(intrinsic_value, 2) if intrinsic_value else None,
            "margin_of_safety":  round(margin_of_safety, 1) if margin_of_safety is not None else None,
            "enterprise_value":  enterprise_value,
            "equity_value":      equity_value,
            "pv_terminal":       pv_terminal,
            "total_pv_fcf":      total_pv_fcf,
            "terminal_value_pct":round(pv_terminal/enterprise_value*100,1) if enterprise_value and pv_terminal else None,
            "net_debt":          (safe(info.get("totalDebt"),0) or 0) - (safe(info.get("totalCash"),0) or 0),
            "shares_outstanding":safe(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0),
            "wacc":          round(wacc_data.get("wacc",0)*100, 2) if wacc_data else None,
            "cost_of_equity":round(wacc_data.get("coe", 0)*100,  2) if wacc_data else None,
            "cost_of_debt":  round(wacc_data.get("cod", 0)*100,  2) if wacc_data else None,
            "tax_rate":      round(wacc_data.get("tax", 0)*100,  1) if wacc_data else None,
            "beta":          round(wacc_data.get("beta",1.0),    2) if wacc_data else None,
            "stage1_growth": round(s1*100, 2) if s1 is not None else None,
            "stage2_growth": round(s2*100, 2) if s2 is not None else None,
            "terminal_growth":round(tg*100, 2),
            "projection_years": yrs,
            "base_fcf":       fcf_vals[0] if fcf_vals else None,
            "historical_fcf": fcf_vals[:5],
            "projected_fcf":  projected,
            "fcf_chart":      fcf_chart,
            # Multiples
            "ps_ratio":      f2(info.get("priceToSalesTrailing12Months")),
            "pb_ratio":      f2(info.get("priceToBook")),
            "ev_ebitda":     f2(info.get("enterpriseToEbitda")),
            "ev_revenue":    f2(info.get("enterpriseToRevenue")),
            # Financials
            "revenue":           f2(info.get("totalRevenue")),
            "ebitda":            f2(info.get("ebitda")),
            "gross_margin":      pct(info.get("grossMargins")),
            "operating_margin":  pct(info.get("operatingMargins")),
            "profit_margin":     pct(info.get("profitMargins")),
            "revenue_growth":    pct(info.get("revenueGrowth")),
            "earnings_growth":   pct(info.get("earningsGrowth")),
            "roe":               pct(info.get("returnOnEquity")),
            "roa":               pct(info.get("returnOnAssets")),
            # Health
            "total_cash":    f2(info.get("totalCash")),
            "total_debt":    f2(info.get("totalDebt")),
            "current_ratio": f2(info.get("currentRatio")),
            "quick_ratio":   f2(info.get("quickRatio")),
            "debt_to_equity":f2(info.get("debtToEquity")),
            "payout_ratio":  pct(info.get("payoutRatio")),
            # Analyst
            "analyst_rating":info.get("recommendationKey","N/A"),
            "analyst_count": info.get("numberOfAnalystOpinions"),
        }

        return jsonify(clean(result))

    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5050)
