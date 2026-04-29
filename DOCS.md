# How VALUS Works

*An institutional-grade discounted cash flow engine, made transparent for everyone.*

---

## What VALUS Is

VALUS is a research tool that values public stocks the same way professional analysts do — by projecting how much cash a company will generate over the next decade, discounting that cash back to today's dollars, and comparing the result to the current share price.  The output is an **intrinsic value** per share and a **margin of safety** that tells you how far the market is from the model's estimate.

Most retail tools either over-simplify (one P/E ratio, no growth modeling) or hide their assumptions behind a black box.  VALUS shows you every input, every override, and every reason the verdict is what it is.  When the model deviates from pure DCF — for strategic assets, news catalysts, or sector quirks — it explains why.

> **Not financial advice.**  VALUS is an educational research tool.  Use it to inform your thinking; don't use it as a substitute for your own diligence.

---

## The Core Concepts

### FCF — Free Cash Flow

Free Cash Flow is the cash a business actually puts in its pocket each year, after paying for everything it needs to keep running:

```
FCF = Operating Cash Flow − Capital Expenditures (capex)
```

FCF is harder to manipulate than reported earnings.  A company can boost EPS with accounting tricks (depreciation schedules, stock-based compensation classification, one-time charges), but FCF is the literal dollars that landed in the bank account.  This is why VALUS values stocks based on FCF, not earnings.

### DCF — Discounted Cash Flow

DCF is the foundational valuation framework: a stock is worth the sum of every future dollar the company will return to shareholders, with each future dollar shrunk to today's value (because a dollar tomorrow is worth less than a dollar today).

VALUS runs a three-stage DCF:

- **Stage 1 (Years 1–5)**: high growth based on analyst estimates, recent FCF trend, or sector ceiling — capped to keep things realistic.
- **Stage 2 (Years 6–10)**: mean-reversion taper — usually 55% of Stage 1 growth, floored at GDP-pace.
- **Terminal value**: the value of all cash flows beyond Year 10, calculated assuming the company grows at GDP pace forever.  Capped at industry-specific ceilings to prevent the math from blowing up.

Each year's FCF is discounted to present value using the company's WACC, then summed and divided by shares outstanding.  That's the per-share intrinsic value.

### WACC — Weighted Average Cost of Capital

WACC is the discount rate — how much each future dollar shrinks per year.  It reflects the blended cost of the company's debt and equity, weighted by the proportion of each in its capital structure.  A higher WACC means the model penalizes future cash flows more aggressively, which lowers intrinsic value.

VALUS adjusts WACC for several real-world factors:

- **Industry minimums** — a junk-rated airline shouldn't get a 5% WACC just because its capital structure looks favorable on paper.
- **Moat premiums** — companies with durable competitive advantages get a 1.5pp reduction.
- **Strategic-asset reductions** — CHIPS Act recipients get a 1pp reduction.
- **Material-risk surcharges** — active SEC investigations add 1pp.

### MOS — Margin of Safety

Margin of safety is the gap between intrinsic value and current price, expressed as a percentage of price:

```
MOS = (Intrinsic Value − Price) / Price × 100
```

A positive MOS means the model thinks the stock is undervalued; negative means overvalued.  VALUS clamps the displayed MOS to ±200% to prevent data quirks (forward-EPS spikes, share-class quirks, ADR FX glitches) from producing nonsensical "+19,000% upside" numbers.  When this happens, the chip says "Low Conf" and tells you which fallback method produced the IV.

---

## How to Read a Verdict

Every analyzed ticker gets one of these verdict tiers, based primarily on MOS and secondarily on implied growth vs sector ceiling:

| Tier | When it fires |
|---|---|
| 🟢 **Priced for Deep Discount** | MOS ≥ +40% — market overly pessimistic; high conviction undervalued |
| 🟢 **Priced for Discount** | MOS +15% to +40% — market mildly pessimistic; undervalued |
| 🟢 **Strategic Discount** | Strategic asset trading below model fair value; sovereign-capital backstop intact |
| 🔵 **Priced for Fair Value** | MOS −10% to +15% — model and market are aligned |
| 🟡 **Priced for Growth** | MOS −10% to −25% — market paying a growth premium; modestly overvalued |
| 🟡 **Priced for Excellence** | MOS −25% to −50% — market expecting flawless execution; meaningfully overvalued |
| 🔴 **Priced for Miracle** | MOS < −50% OR implied growth blows past the sector ceiling; speculative |

---

## The Strategic Asset Layer

Pure DCF systematically undervalues US national-security and sovereign-capital names.  The discount rate doesn't reflect government backstops; the sector ceiling doesn't reflect policy-driven capital flows.  A stock like MU — sole US producer of HBM memory, $6.1B CHIPS Act grant — should not be valued the same way as a generic memory chip company.

VALUS recognizes 25+ curated tickers across five tiers.  Each entry has a per-ticker reason citing the specific government program, certification, or sole-supplier role:

- **Semiconductor Sovereignty** — MU, INTC, TXN, ADI, MCHP, ON, AMAT, KLAC, LRCX.  CHIPS Act recipients + export-control-protected cap equipment monopoly.
- **Defense Primes** — LMT, RTX, NOC, GD, LHX, HII.  Sole-source contracts, multi-decade backlogs.
- **Energy Sovereignty** — CEG, VST, NEE, OXY.  AI-datacenter PPAs, nuclear renaissance, Permian dominance.
- **Critical Materials** — MP, LEU, BWXT.  Rare earths, uranium enrichment, naval reactors.
- **Urban Air Mobility** — JOBY, ACHR, RKLB.  Pre-revenue franchise bets with FAA Part 135 certifications and government contracts.

For these names, the model lowers WACC modestly (50–100bp), lifts the sector growth ceiling, applies an IV floor (so distress signals don't override the franchise thesis), and surfaces the curation reason in a banner above the verdict.

---

## The News Engine

VALUS scans three news sources in parallel for every ticker:

- **Yahoo Finance news** (when available)
- **SEC EDGAR 8-K filings** (material corporate disclosures)
- **Google News RSS** (recent headlines, the most reliable source)

Each headline gets a numeric score in `[−1.0, +1.0]` based on keyword matches modulated by sector context.  A "first commercial passenger flight" headline scores higher on JOBY (eVTOL sector) than on AAPL (consumer electronics).  Older headlines decay smoothly — a 1-day-old article gets full weight, a 6-day-old gets ~14%.

When a transformative catalyst fires (FAA Part 135 cert, semi tape-out, BLA filing, PPA signed), the model bumps Stage-1 growth +0 to +3pp on top of the IV multiplier.  This reflects the 2-3 year revenue acceleration from a real launch event, not just a sentiment pop.

With `ANTHROPIC_API_KEY` configured, ambiguous headlines (heuristic score in `[−0.2, +0.2]`) get sent to Claude Haiku for structured interpretation.  This is the "model decides on its own" path — the AI reads what a keyword scanner can't.

---

## What We Don't Use

We deliberately stick to free + transparent data sources.  No paid news APIs, no proprietary "alpha factors," no hidden weights.  Specifically:

- **Apple News** — has no public API for content syndication; articles are paywalled and can't be redistributed.
- **Bloomberg / Refinitiv** — paywalled and licensing-restricted.
- **Anonymous "alpha" signals** — opaque scores from black-box vendors don't belong in a transparent model.

---

## Verdict Layers in Order

For the curious — here's the order in which VALUS produces a verdict:

1. **Data pull**: yfinance for price, financials, balance sheet, cash flow.
2. **Sector classification**: industry-specific WACC floors, growth ceilings, terminal-growth caps.
3. **FCF series**: TTM → quarterly → annual fallback.  Single-period IPO names get extra scrutiny.
4. **Catalyst scan**: news from three sources, scored on `[−1, +1]` with sector context and age decay.
5. **Strategic classifier**: per-ticker registry lookup with WACC delta, ceiling lift, IV floor.
6. **DCF computation**: Stage 1 → Stage 2 → terminal, with reality reconciliation against analyst consensus.
7. **Sector overrides**: banking-DCF, biotech blend, network-effect, distressed P/B fallback.
8. **Premium stacking**: moat, cash-rich, debt-momentum, momentum-catalyst (gated when low confidence).
9. **Sanity envelope**: clamp IV to `[price × 0.05, max(price × 6, analyst_target × 2.5)]`.
10. **Verdict tier**: based on MOS + implied growth vs sector ceiling, with Strategic Discount override for low-multiple sovereign names.
11. **Confidence chip**: high (DCF), medium (multiples blend), low (emergency cascade — analyst target / cash-only / distressed P/B / P/Revenue).

---

## Disclaimer

VALUS is a research and education tool, not investment advice.  All valuations are model outputs subject to data quality, news interpretation, and assumption errors.  Past performance of similar tools or models does not guarantee future results.  Always do your own diligence; consult a licensed financial advisor before making investment decisions.  Market data is sourced from public APIs and may be delayed, incomplete, or contain errors.  We do not warrant the accuracy or timeliness of any output.
