from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import yfinance as yf
import numpy as np
import traceback

app = Flask(__name__)
CORS(app)

RISK_FREE_RATE = 0.045
EQUITY_RISK_PREMIUM = 0.055


def safe_float(val, default=None):
    try:
        if val is None:
            return default
        f = float(val)
        return None if (np.isnan(f) or np.isinf(f)) else f
    except Exception:
        return default


def clean_nan(v):
    """Recursively replace NaN/Inf with None so JSON stays valid."""
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    if isinstance(v, list):
        return [clean_nan(i) for i in v]
    if isinstance(v, dict):
        return {k: clean_nan(val) for k, val in v.items()}
    return v


def calculate_wacc(info, income_stmt):
    beta = safe_float(info.get("beta"), 1.0)
    if not beta or beta <= 0:
        beta = 1.0
    beta = min(max(beta, 0.3), 3.0)

    cost_of_equity = RISK_FREE_RATE + beta * EQUITY_RISK_PREMIUM

    cost_of_debt = 0.05
    try:
        interest_exp = None
        if income_stmt is not None and not income_stmt.empty:
            for label in ["Interest Expense", "Interest Expense Non Operating"]:
                if label in income_stmt.index:
                    v = safe_float(income_stmt.loc[label].iloc[0])
                    if v is not None:
                        interest_exp = abs(v)
                        break
        total_debt = safe_float(info.get("totalDebt"), 0) or 0
        if interest_exp and total_debt > 0:
            cd = interest_exp / total_debt
            if not np.isnan(cd) and not np.isinf(cd):
                cost_of_debt = min(max(cd, 0.02), 0.15)
    except Exception:
        pass

    tax_rate = safe_float(info.get("effectiveTaxRate"), 0.21) or 0.21
    tax_rate = min(max(tax_rate, 0.05), 0.40)

    market_cap = safe_float(info.get("marketCap"), 0) or 0
    total_debt = safe_float(info.get("totalDebt"), 0) or 0
    total_capital = market_cap + total_debt

    if total_capital > 0:
        we = market_cap / total_capital
        wd = total_debt / total_capital
    else:
        we, wd = 0.8, 0.2

    wacc = we * cost_of_equity + wd * cost_of_debt * (1 - tax_rate)
    if wacc is None or np.isnan(wacc) or np.isinf(wacc):
        wacc = cost_of_equity
    wacc = min(max(wacc, 0.05), 0.20)

    return {
        "wacc": wacc,
        "cost_of_equity": cost_of_equity,
        "cost_of_debt": cost_of_debt,
        "tax_rate": tax_rate,
        "beta": beta,
        "weight_equity": we,
        "weight_debt": wd,
    }


def get_fcf_series(cashflow):
    if cashflow is None or cashflow.empty:
        return []
    try:
        if "Free Cash Flow" in cashflow.index:
            vals = cashflow.loc["Free Cash Flow"].values
        else:
            ocf_key = next((k for k in cashflow.index if "Operating" in k and "Cash" in k), None)
            capex_key = next((k for k in cashflow.index if "Capital Expenditure" in k or "Capex" in k), None)
            if ocf_key:
                ocf = cashflow.loc[ocf_key].values
                capex = cashflow.loc[capex_key].values if capex_key else np.zeros(len(ocf))
                vals = ocf + capex
            else:
                return []
        return [float(v) for v in vals if v is not None and not np.isnan(float(v))]
    except Exception:
        return []


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/analyze", methods=["GET"])
def analyze():
    ticker = request.args.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "Ticker is required"}), 400

    try:
        stage1_growth_override = request.args.get("growth1")
        stage2_growth_override = request.args.get("growth2")
        terminal_growth = float(request.args.get("terminal", 0.03))
        projection_years = int(request.args.get("years", 10))
        terminal_growth = min(max(terminal_growth, 0.01), 0.05)
        projection_years = min(max(projection_years, 5), 15)

        stock = yf.Ticker(ticker)
        info = stock.info

        current_price = safe_float(info.get("currentPrice") or info.get("regularMarketPrice"), 0)
        if not current_price:
            return jsonify({"error": f"Could not find data for '{ticker}'. Check the symbol and try again."}), 404

        cashflow = stock.cashflow
        income_stmt = stock.income_stmt

        # ── Historical price data (1 year, weekly) ─────────────────────────
        hist = stock.history(period="1y", interval="1d")
        price_history = []
        if not hist.empty:
            price_history = [
                {"date": str(idx.date()), "close": round(float(row["Close"]), 2)}
                for idx, row in hist.iterrows()
            ]

        # ── FCF ────────────────────────────────────────────────────────────
        fcf_values = get_fcf_series(cashflow)
        if not fcf_values:
            return jsonify({"error": "No Free Cash Flow data available. This ticker may not have financial statements (ETFs, indices, etc.)."}), 422

        base_fcf = fcf_values[0]

        positive = [f for f in fcf_values if f > 0]
        if len(positive) >= 2:
            years_span = len(positive) - 1
            hist_growth = (positive[0] / positive[-1]) ** (1 / years_span) - 1
            hist_growth = min(max(hist_growth, -0.30), 0.50)
        else:
            hist_growth = 0.07

        analyst_growth = safe_float(info.get("earningsGrowth") or info.get("revenueGrowth"))

        if stage1_growth_override is not None:
            s1 = float(stage1_growth_override)
        else:
            s1 = analyst_growth if analyst_growth and -0.3 < analyst_growth < 0.5 else min(hist_growth, 0.25)

        if stage2_growth_override is not None:
            s2 = float(stage2_growth_override)
        else:
            s2 = max(s1 * 0.5, terminal_growth + 0.01)

        # ── WACC ───────────────────────────────────────────────────────────
        wacc_data = calculate_wacc(info, income_stmt)
        wacc = wacc_data["wacc"]

        # ── DCF projection ─────────────────────────────────────────────────
        half = projection_years // 2
        projected = []
        fcf = base_fcf

        for y in range(1, projection_years + 1):
            g = s1 if y <= half else s2
            fcf = fcf * (1 + g)
            pv = fcf / ((1 + wacc) ** y)
            projected.append({"year": y, "fcf": fcf, "pv": pv, "growth": g})

        terminal_fcf = projected[-1]["fcf"] * (1 + terminal_growth)
        terminal_value = terminal_fcf / (wacc - terminal_growth)
        pv_terminal = terminal_value / ((1 + wacc) ** projection_years)

        total_pv_fcf = sum(p["pv"] for p in projected)
        enterprise_value = total_pv_fcf + pv_terminal

        cash = safe_float(info.get("totalCash"), 0) or 0
        debt = safe_float(info.get("totalDebt"), 0) or 0
        net_debt = debt - cash
        equity_value = enterprise_value - net_debt

        shares = safe_float(info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0) or 0
        intrinsic_value = (equity_value / shares) if shares > 0 else 0

        margin_of_safety = (
            ((intrinsic_value - current_price) / current_price * 100)
            if current_price and current_price > 0 else None
        )

        def pct(val):
            v = safe_float(val)
            return round(v * 100, 2) if v is not None else None

        def fmt(val, digits=2):
            v = safe_float(val)
            return round(v, digits) if v is not None else None

        # ── FCF chart data (labels + values) ───────────────────────────────
        fcf_chart = {
            "historical": {
                "labels": [f"FY-{i}" if i > 0 else "TTM" for i in range(len(fcf_values[:5]))],
                "values": [round(v / 1e9, 2) for v in fcf_values[:5]],
            },
            "projected": {
                "labels": [f"Y{p['year']}" for p in projected],
                "values": [round(p["fcf"] / 1e9, 2) for p in projected],
                "pvs":    [round(p["pv"]  / 1e9, 2) for p in projected],
            }
        }

        result = {
            "ticker": ticker,
            "company_name": info.get("longName", ticker),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "currency": info.get("currency", "USD"),
            "exchange": info.get("exchange", ""),
            # Price & history
            "current_price": fmt(current_price),
            "52w_high": fmt(info.get("fiftyTwoWeekHigh")),
            "52w_low": fmt(info.get("fiftyTwoWeekLow")),
            "target_price": fmt(info.get("targetMeanPrice")),
            "price_history": price_history,
            # DCF output
            "intrinsic_value": round(intrinsic_value, 2),
            "margin_of_safety": round(margin_of_safety, 1) if margin_of_safety is not None else None,
            "enterprise_value": enterprise_value,
            "equity_value": equity_value,
            "pv_terminal": pv_terminal,
            "total_pv_fcf": total_pv_fcf,
            "terminal_value_pct": round(pv_terminal / enterprise_value * 100, 1) if enterprise_value else None,
            # DCF assumptions
            "wacc": round(wacc * 100, 2),
            "cost_of_equity": round(wacc_data["cost_of_equity"] * 100, 2),
            "cost_of_debt": round(wacc_data["cost_of_debt"] * 100, 2),
            "tax_rate": round(wacc_data["tax_rate"] * 100, 1),
            "beta": round(wacc_data["beta"], 2),
            "stage1_growth": round(s1 * 100, 2),
            "stage2_growth": round(s2 * 100, 2),
            "terminal_growth": round(terminal_growth * 100, 2),
            "projection_years": projection_years,
            # FCF data
            "base_fcf": base_fcf,
            "historical_fcf": fcf_values[:5],
            "historical_growth": round(hist_growth * 100, 2),
            "projected_fcf": projected,
            "net_debt": net_debt,
            "shares_outstanding": shares,
            "fcf_chart": fcf_chart,
            # Valuation multiples
            "market_cap": fmt(info.get("marketCap")),
            "pe_ratio": fmt(info.get("trailingPE")),
            "forward_pe": fmt(info.get("forwardPE")),
            "ps_ratio": fmt(info.get("priceToSalesTrailing12Months")),
            "pb_ratio": fmt(info.get("priceToBook")),
            "ev_ebitda": fmt(info.get("enterpriseToEbitda")),
            "ev_revenue": fmt(info.get("enterpriseToRevenue")),
            # Financials
            "revenue": fmt(info.get("totalRevenue")),
            "ebitda": fmt(info.get("ebitda")),
            "profit_margin": pct(info.get("profitMargins")),
            "operating_margin": pct(info.get("operatingMargins")),
            "gross_margin": pct(info.get("grossMargins")),
            "revenue_growth": pct(info.get("revenueGrowth")),
            "earnings_growth": pct(info.get("earningsGrowth")),
            # Balance sheet
            "total_cash": fmt(info.get("totalCash")),
            "total_debt": fmt(info.get("totalDebt")),
            "debt_to_equity": fmt(info.get("debtToEquity")),
            "current_ratio": fmt(info.get("currentRatio")),
            "quick_ratio": fmt(info.get("quickRatio")),
            # Returns
            "roe": pct(info.get("returnOnEquity")),
            "roa": pct(info.get("returnOnAssets")),
            # Dividends
            "dividend_yield": pct(info.get("dividendYield")),
            "payout_ratio": pct(info.get("payoutRatio")),
            # Analyst
            "analyst_rating": info.get("recommendationKey", "N/A"),
            "analyst_count": info.get("numberOfAnalystOpinions"),
        }

        return jsonify(clean_nan(result))

    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5050)
