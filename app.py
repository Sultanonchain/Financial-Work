from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import yfinance as yf
import numpy as np
import traceback

app = Flask(__name__)
CORS(app)

RISK_FREE_RATE = 0.045   # 10-yr treasury approx
EQUITY_RISK_PREMIUM = 0.055  # Standard ERP


def safe_float(val, default=None):
    try:
        if val is None:
            return default
        f = float(val)
        return None if (np.isnan(f) or np.isinf(f)) else f
    except Exception:
        return default


def calculate_wacc(info, income_stmt):
    beta = safe_float(info.get("beta"), 1.0)
    if not beta or beta <= 0:
        beta = 1.0
    beta = min(max(beta, 0.3), 3.0)

    cost_of_equity = RISK_FREE_RATE + beta * EQUITY_RISK_PREMIUM

    try:
        interest_exp = None
        if income_stmt is not None and not income_stmt.empty:
            for label in ["Interest Expense", "Interest Expense Non Operating"]:
                if label in income_stmt.index:
                    raw = income_stmt.loc[label].iloc[0]
                    v = safe_float(raw)
                    if v is not None:
                        interest_exp = abs(v)
                        break
        total_debt = safe_float(info.get("totalDebt"), 0) or 0
        if interest_exp and total_debt > 0:
            cost_of_debt = interest_exp / total_debt
            cost_of_debt = min(max(cost_of_debt, 0.02), 0.15)
        else:
            cost_of_debt = 0.05
    except Exception:
        cost_of_debt = 0.05

    # Always ensure cost_of_debt is a clean float
    if cost_of_debt is None or np.isnan(cost_of_debt):
        cost_of_debt = 0.05

    tax_rate = safe_float(info.get("effectiveTaxRate"), 0.21)
    if not tax_rate or tax_rate <= 0:
        tax_rate = 0.21
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
    if wacc is None or np.isnan(wacc):
        wacc = cost_of_equity  # fallback to unlevered cost of equity
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
    """Return list of FCF values (most recent first)."""
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
        # User-supplied overrides (optional)
        stage1_growth_override = request.args.get("growth1")
        stage2_growth_override = request.args.get("growth2")
        terminal_growth = float(request.args.get("terminal", 0.03))
        projection_years = int(request.args.get("years", 10))

        terminal_growth = min(max(terminal_growth, 0.01), 0.05)
        projection_years = min(max(projection_years, 5), 15)

        stock = yf.Ticker(ticker)
        info = stock.info

        if not info or info.get("regularMarketPrice") is None and info.get("currentPrice") is None:
            return jsonify({"error": f"Could not find data for ticker '{ticker}'. Please check the symbol."}), 404

        cashflow = stock.cashflow
        income_stmt = stock.income_stmt
        balance_sheet = stock.balance_sheet

        # ── FCF ──────────────────────────────────────────────────────────────
        fcf_values = get_fcf_series(cashflow)
        if not fcf_values:
            return jsonify({"error": "No Free Cash Flow data available for this ticker."}), 422

        base_fcf = fcf_values[0]

        # Historical growth rate
        positive = [f for f in fcf_values if f > 0]
        if len(positive) >= 2:
            years_span = len(positive) - 1
            hist_growth = (positive[0] / positive[-1]) ** (1 / years_span) - 1
            hist_growth = min(max(hist_growth, -0.30), 0.50)
        else:
            hist_growth = 0.07

        # Analyst estimates if available
        analyst_growth = safe_float(info.get("earningsGrowth") or info.get("revenueGrowth"), None)

        # Determine stage growth rates
        if stage1_growth_override is not None:
            s1 = float(stage1_growth_override)
        else:
            s1 = analyst_growth if analyst_growth and -0.3 < analyst_growth < 0.5 else min(hist_growth, 0.25)

        if stage2_growth_override is not None:
            s2 = float(stage2_growth_override)
        else:
            s2 = max(s1 * 0.5, terminal_growth + 0.01)

        # ── WACC ─────────────────────────────────────────────────────────────
        wacc_data = calculate_wacc(info, income_stmt)
        wacc = wacc_data["wacc"]

        # ── DCF projection ───────────────────────────────────────────────────
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

        # ── Equity value per share ────────────────────────────────────────────
        cash = safe_float(info.get("totalCash"), 0)
        debt = safe_float(info.get("totalDebt"), 0)
        net_debt = (debt or 0) - (cash or 0)
        equity_value = enterprise_value - net_debt

        shares = safe_float(
            info.get("sharesOutstanding") or info.get("impliedSharesOutstanding"), 0
        )
        intrinsic_value = (equity_value / shares) if shares and shares > 0 else 0

        current_price = safe_float(
            info.get("currentPrice") or info.get("regularMarketPrice"), 0
        )
        margin_of_safety = (
            ((intrinsic_value - current_price) / current_price * 100)
            if current_price and current_price > 0
            else None
        )

        # ── Additional metrics ────────────────────────────────────────────────
        def pct(val):
            v = safe_float(val)
            return round(v * 100, 2) if v is not None else None

        def fmt(val, digits=2):
            return round(safe_float(val), digits) if safe_float(val) is not None else None

        result = {
            "ticker": ticker,
            "company_name": info.get("longName", ticker),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "currency": info.get("currency", "USD"),
            "exchange": info.get("exchange", ""),
            # Price
            "current_price": fmt(current_price),
            "52w_high": fmt(info.get("fiftyTwoWeekHigh")),
            "52w_low": fmt(info.get("fiftyTwoWeekLow")),
            "target_price": fmt(info.get("targetMeanPrice")),
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
            # Balance sheet / health
            "total_cash": fmt(info.get("totalCash")),
            "total_debt": fmt(info.get("totalDebt")),
            "debt_to_equity": fmt(info.get("debtToEquity")),
            "current_ratio": fmt(info.get("currentRatio")),
            "quick_ratio": fmt(info.get("quickRatio")),
            # Returns
            "roe": pct(info.get("returnOnEquity")),
            "roa": pct(info.get("returnOnAssets")),
            "roic": pct(info.get("returnOnCapital")),
            # Dividends
            "dividend_yield": pct(info.get("dividendYield")),
            "payout_ratio": pct(info.get("payoutRatio")),
            # Analyst
            "analyst_rating": info.get("recommendationKey", "N/A"),
            "analyst_count": info.get("numberOfAnalystOpinions"),
        }

        # Sanitize: replace any remaining NaN/Inf with None so JSON is valid
        def clean(v):
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                return None
            if isinstance(v, list):
                return [clean(i) for i in v]
            if isinstance(v, dict):
                return {k: clean(val) for k, val in v.items()}
            return v

        return jsonify(clean(result))

    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5050)
