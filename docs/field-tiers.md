# Field tier audit, VALUS ticker page

Audit only. No code, markup or copy was changed to produce this.

**What was audited.** Everything rendered for a ticker today:

| Surface | Source |
|---|---|
| Verdict screen (the real ticker page, reached from `/?t=TICKER`) | `templates/index.html` 1086-1725, rendered by `static/js/main.js` |
| Assumption inputs that parameterise it | `templates/index.html` 394-427 (home screen, typed) |
| SEO landing page `/stocks/<ticker>` | `templates/ticker.html`, served by `app.py:6768` |

**Tier rules applied.**

- `headline` — at most 6 fields on the whole verdict screen.
- `summary` — plain language only. No raw ratios, no Greek letters.
- `detail` — everything else, lives in the assumptions drawer.

**One assumption, flag it if you disagree.** Company name and ticker are treated
as page chrome, not as headline fields, so they do not spend the budget of 6.
Everything that carries a number or a judgement does.

**Where it stands today.** The hero card alone renders up to 28 elements plus 3
buttons before the drawer is opened. The verdict is currently stated four
separate ways in that card: the tier label, the A-F grade, the verdict line and
the Lynch Lens plain-English line.

## Decisions taken in review, 2026-09-16

- **The A-F grade is cut, not demoted.** A letter grade reads as a
  recommendation, and it was the fourth restatement of a verdict the card
  already gives three times.
- **Only the fair-value range is a headline number.** The point value, the
  probability-weighted figure and the sanity-check figure all move to detail,
  so one fair-value number sits above the drawer instead of four.
- **The assumption editor is the paid tier, not a deletion.** Free users get
  the verdict and the method; "Customize DCF assumptions" is what Pro unlocks.
  Tiered as detail and flagged "pro surface" below.
- Confirmed as written: company name and ticker are chrome and do not spend the
  budget of six, and margin of safety sits at summary rather than headline.

---

## Proposed headline six

| # | Field | Today's id | Why it earns the slot |
|---|---|---|---|
| 1 | Verdict tier label | `vTierLabel` | The answer the page exists to give |
| 2 | Fair value range | `vIvBandHeadline` | "Worth roughly $X to $Y a share", already plain |
| 3 | Current price | `vPrice` | The number being compared |
| 4 | One-line plain verdict | `vVerdict` | The reason, in a sentence |
| 5 | Confidence | `viConfidence` | Whether to trust the other five |
| 6 | Valuation method | not rendered today | What produced the number: a DCF, a banking blend, an analyst target. The agent layer already computes this (`valuationMethod`, `valuationBasis`) and nothing on the page shows it |

Everything else moves down, and one thing moves out. Demoted to summary or
detail: margin of safety, implied growth and the 52-week range. Cut entirely:
the A-F grade. Moved to detail: the single-point fair value (`vIV`), the
probability-weighted figure (`scWeighted`) and the sanity-check figure
(`sanityValusIv`), leaving the range as the only fair-value number on the
verdict screen.

---

## Hero verdict card

`templates/index.html` 1101-1250.

| Field | Renders today | Proposed tier | Actionable unaided? | Note |
|---|---|---|---|---|
| `vName` company name | Hero head | chrome | Yes | Identity, not a field |
| `vTicker` | Pill under name | chrome | Yes | Identity |
| `vSector` | Beside ticker | summary | Yes | Plain word |
| `vRange` "52W $lo, $hi" | Beside sector | detail | No | Duplicates the `viRange` bar lower in the same card |
| `vPrice` current price | Price/IV row, left | **headline** | Yes | |
| `vMosPct` + `vMosFill` margin of safety | Centre bar | summary | No | "Margin of safety" is jargon. Needs a plain label such as "how far today's price sits below the estimate" |
| `vIV` fair value | Price/IV row, right | detail | Partly | The cent-precise point estimate. Moved to the drawer in review: the band is the answer, and a point estimate beside it implies precision the model does not have |
| `vIvBandHeadline` | Under price row | **headline** | Yes | "VALUS thinks this is worth roughly $X to $Y a share" |
| `vIvBandSpan/Mid/Price` band bar | Under headline | summary | Yes | Visual, reads without finance vocabulary |
| `vIvBandFoot` | Under the bar | summary | Yes | Explains the band's width in plain words; mentions discount rate and long-run growth without naming them as ratios |
| `vIvFloorBreakdown` "DCF model $X · Strategic floor $Y · Used $Z" | Conditional, under band | detail | No | Three intermediate model values |
| `vAiIvBreakdown` "DCF: $X · VALUS: $Y (+Z%)" | Conditional | detail | No | Exposes an internal model disagreement with no guidance on what to do with it |
| `vIvScenarioText` "Market is pricing X% growth · VALUS forecasts Y%" | Under band | summary | Partly | Good framing, but two growth rates in one line is the third place implied growth appears |
| `vTierLabel` + `vTierBadge` | Badge | **headline** | Yes | Button opens "Why this verdict?" |
| `vValusGradeBadge` A-F | Grade row | **cut** | Yes | A letter grade reads as a recommendation, and it is the fourth restatement of the verdict. Removed, not demoted |
| `vValusGradeExplain` | Beside badge | **cut** | Yes | Goes with the badge |
| `vValusGradeWhy` "How are grades calculated?" | Control | **cut** | Yes | Goes with the badge, along with the grade explainer modal it opens |
| `vStrategicLabel` / `vStrategicReason` / `vStrategicChips` / `vStrategicLive` | Conditional banner | summary | Yes | Plain, and materially changes the read |
| `vReasons` reason chips | Under banner | summary | Yes | Keep 3 max |
| `vNewsCount` + `vNewsList` | News chip, expandable | summary | Yes | Collapsed by default already |
| `vVerdict` verdict line | Under news | **headline** | Yes | |
| `viConfidence` | Insight row | **headline** | Yes | Low/medium/high reads unaided |
| `viConfidenceHint` first warning | Beside confidence | summary | No | Raw model warnings, e.g. "Thin FCF margin (2.7%), a 1pp change in cost assumptions can move IV by 20%+" |
| `viImplied` implied growth % | Insight row | detail | No | Reverse-DCF output; meaningless without the concept |
| `viImpliedHint` "vs X% sector ceiling" | Beside it | detail | No | |
| `viRangeLow` / `viRangeHigh` / `viRangeMarker` | Insight row | summary | Yes | Visual position in the year's range |
| `addPortfolioBtn`, `addWatchlistBtn`, `copyBtn` | Hero actions | summary | Yes | Controls |

## Scenario analysis

`templates/index.html` 1253-1264, built in `main.js` 1477+.

| Field | Renders today | Proposed tier | Actionable unaided? | Note |
|---|---|---|---|---|
| Section title "Scenario analysis" | Card head | summary | Yes | |
| `scWeightNote` "Weights: ..." | Card head | detail | No | Probability weights |
| `scGrid` bear/base/bull value | Three cards | summary | Yes | Three prices |
| `scGrid` upside % per case | In each card | summary | Yes | |
| `scGrid` weight % per case | In each card | detail | No | |
| `scGrid` case narrative | In each card | summary | Yes | Written in plain sector language |
| `scGrid` empty-state reason | Replaces grid | summary | Yes | Says why there is no range |
| `scWeighted` probability-weighted fair value | Bar under grid | detail | No | Confirmed detail in review: one of the three fair-value numbers leaving the verdict screen |
| `scWeightedDelta` | Beside it | detail | No | |

## Verdict intelligence row

`templates/index.html` 1267-1317.

| Field | Renders today | Proposed tier | Actionable unaided? | Note |
|---|---|---|---|---|
| `sanityTier` pill | Sanity card head | summary | Yes | |
| `sanityValusIv` | Sanity card | detail | Partly | Repeats fair value. Confirmed detail in review |
| `sanityAnalyst` analyst consensus | Sanity card | summary | Yes | |
| `sanityGap` | Sanity card | detail | No | |
| `sanityNarrative` | Sanity card | summary | Yes | |
| `flipList` rows: label, what it needs, delta % | What-would-change card | summary | Yes | The most actionable block on the page |
| `rdcfImplied` market-implied growth | Reverse-DCF card | detail | No | |
| `rdcfCeiling` sector ceiling | Reverse-DCF card | detail | No | |
| `reverseDcfTier` pill | Card head | detail | Partly | |
| `rdcfNarrative` | Reverse-DCF card | summary | Yes | Plain sentence; the numbers above it are not |

## Mini stats

`templates/index.html` 1320-1337.

| Field | Renders today | Proposed tier | Actionable unaided? | Note |
|---|---|---|---|---|
| `mMcap` market cap | Stat strip | summary | Yes | |
| `mPE` P/E (TTM) | Stat strip | detail | No | Raw ratio, barred from summary by the rules |
| `mDiv` dividend yield | Stat strip | detail | Partly | A ratio; "pays X% a year" would be summary |
| `mTarget` analyst target | Stat strip | summary | Yes | A price |

## Valuation history

`templates/index.html` 1340-1366.

| Field | Renders today | Proposed tier | Actionable unaided? | Note |
|---|---|---|---|---|
| `vhSub` "5-year price vs intrinsic value" | Card head | summary | Yes | |
| `vhStats` verdict pill + "IV $X · Px $Y" | Card head | detail | No | "IV" and "Px" are abbreviations |
| `vhCaveatText` cyclical caveat | Conditional | summary | Yes | |
| Chart + legend | Card body | summary | Yes | The legend already explains the mint line |
| `vhEmpty` | Fallback | detail | Yes | |

## Lynch Lens

`templates/index.html` 1369-1397.

| Field | Renders today | Proposed tier | Actionable unaided? | Note |
|---|---|---|---|---|
| `lynchCategory` | Pill row | summary | Yes | Stalwart, fast grower, cyclical |
| `lynchVerdict` | Pill row | summary | Yes | Fourth statement of the verdict |
| `lynchRegime` | Pill row | detail | No | Tape regime jargon |
| `lynchAsOf` | Card head | detail | Yes | |
| `lynchPlain` | Card body | summary | Yes | Competes with `vVerdict` for the same job |
| `lynchThesis` | Card body | summary | Partly | Analyst register by design |
| `lynchBulls` / `lynchBears` | Two columns | summary | Yes | |
| `lynchBackstop` | Conditional | summary | Yes | |

## Insights grid

`templates/index.html` 1400-1494.

| Field | Renders today | Proposed tier | Actionable unaided? | Note |
|---|---|---|---|---|
| `qualityScoreNum` 0-100 + `qualityScoreGrade` | Quality card | summary | Yes | Composite reads unaided |
| `qualityGrid` metric rows (ROIC, FCF margin, net debt/EBITDA, ...) | Quality card | detail | No | Backend-labelled ratios |
| `moatStrength` + `moatList` | Moat card | summary | Yes | Plain-language moat sources |
| `buffettScore` + `buffettList` | Buffett card | summary | Yes | Checklist reads as pass/fail |
| `momentumTier` | Momentum card | summary | Yes | |
| `momPrice`, `momMa50`, `momMa50Gap`, `momMa200`, `momMa200Gap` | Momentum card | detail | No | Moving averages |
| `eqTier` + `eqNarrative` | Earnings quality | summary | Partly | Narrative yes, tier label needs the concept |
| `riskSummary`, `riskChips`, `riskBullets` | Risk card | summary | Yes | |
| `insiderSummary` + `insiderList` | Insider card | summary | Yes | |
| `congressSummary` + `congressList` | Congress card | summary | Yes | |
| `newsSummaryBody` | News card | summary | Yes | |

## Drawer, "View detailed analysis"

`templates/index.html` 1497-1722. Everything here is `detail` by definition;
the column below flags the ones that are currently mislabelled for a lay reader
even inside the drawer.

| Field | Renders today | Proposed tier | Actionable unaided? | Note |
|---|---|---|---|---|
| `drawerTrigger` | Above drawer | summary | Yes | Control |
| `cdValusIV` / `cdYourIV` / `cdYourDelta` | Try-your-own-assumptions | detail | Yes | The comparison itself is clear |
| `cdS1` + `cdS1Val` "Stage 1 growth · g₁" | Slider | detail | No | Greek letter; must never be promoted to summary as labelled |
| `cdS2` + `cdS2Val` "Stage 2 growth · g₂" | Slider | detail | No | Greek letter |
| `cdWacc` + `cdWaccVal` "Discount rate · WACC" | Slider | detail | No | Acronym |
| `cdTg` + `cdTgVal` "Terminal growth · g_T" | Slider | detail | No | Greek letter |
| `cdResetBtn` | Control | detail | Yes | |
| `cmpProb`, `cmpReason`, `cmpAge`, `cmpCurMargin`, `cmpPeerAvg`, `cmpPAttain`, `cmpPeers`, `cmpFcfNow`, `cmpFcfPeer`, `cmpFcfWeighted` | Comparables card | detail | No | Whole card is modelling internals |
| `dcfScenarioToggle` bear/base/bull | Chart head | detail | Yes | Control |
| `dyYearN`, `dyGrowth`, `dyFcf`, `dyPv`, `dyDiscNote` | Year card | detail | No | |
| `dcfSlider` year scrubber + ticks | Chart | detail | Yes | View control, not a model input |
| `dcfChart`, `dcfChartFooter` | Chart | detail | Partly | |
| `assumptionsGrid`: WACC, Cost of Equity, Cost of Debt, Beta, Stage 1 growth, Stage 2 growth, Terminal growth, Tax rate, Base FCF, Net debt, Shares out, Years projected | Assumptions card (12 rows) | detail | No | Each has a tooltip; none reads unaided |
| `notesList` model notes, `dcf_warning`, low-confidence diagnostics | Notes card | detail, with one exception | No | **Exception:** `dcf_warning` carries method disclosures such as "Traditional DCF is not applicable for banks". That belongs at summary beside the method field, not buried in the drawer |
| `priceChartStats`, range tabs 3M-5Y, `priceChart` | Price history | detail | Yes | |
| Projection table: Year, FCF, Growth, PV (discounted) | Table | detail | No | |
| Financial statements: Income / Balance / Cash Flow tabs | Table | detail | No | |

## SEO landing page, `/stocks/<ticker>`

`templates/ticker.html`. Server-rendered from the warm analyze cache; separate
from the verdict screen and currently not tiered at all.

| Field | Renders today | Proposed tier | Actionable unaided? | Note |
|---|---|---|---|---|
| Current price | Stat card | headline | Yes | |
| Intrinsic value | Stat card | headline | Partly | Single point, no range, no confidence, no method |
| Margin of safety | Stat card | summary | No | Same jargon as the app |
| VALUS verdict (`tier_label`) | Stat card | headline | Yes | |
| Explanatory paragraph | Under stats | summary | Yes | Names WACC and reverse-DCF |
| "Open full analysis" CTA | Button | summary | Yes | |
| "How VALUS values X" prose | Section | summary | Partly | Mentions WACC, terminal value, reverse-DCF |
| Learn links | Section | summary | Yes | |

This page shows a cent-precise intrinsic value with no band and no confidence,
which is the opposite of the app's own position that the range is the answer.

---

## Flagged: pro surface, not removed

These are the fields a user must **type** today that the model already derives.
They are not being deleted. They become the paid tier: free users get the
verdict and the method, and the assumption editor is what Pro unlocks. Every
one stays auto-calibrated by default, so none of them is ever required input,
for a free user or a paying one.

| Input | Where | Placeholder today | Model already derives it? | Verdict |
|---|---|---|---|---|
| `advS1` Stage 1 growth (%) | Home screen, "Customize DCF assumptions" | `auto` | Yes. Analyst consensus blended with the sector ceiling (`blended_growth`) | **Detail, pro surface** |
| `advS2` Stage 2 growth (%) | Same panel | `auto` | Yes. Tapers from Stage 1 toward terminal | **Detail, pro surface** |
| `advTg` Terminal growth (%) | Same panel | `2.5` | Yes. Default 2.5%, pinned per sector (3.5% for payment networks, `min_spread` enforced against WACC) | **Detail, pro surface** |
| `advYrs` Projection years | Same panel | `10` | Yes. 10 by default, extended to 10 for backbone-moat names | **Detail, pro surface** |

Notes on the rest of the list you named:

- **Discount rate.** No typed input exists today; it is already auto-calibrated
  (CAPM cost of equity, after-tax cost of debt, sector WACC band, plus pins for
  payment networks). It is exposed only as the `cdWacc` slider in the drawer.
  If it ever becomes editable it belongs in the same Pro surface, never in the
  free path to a number.
- **Terminal multiple.** Not applicable. VALUS terminates on a Gordon growth
  rate, not an exit multiple, and no input for one exists.
- **The four drawer sliders** (`cdS1`, `cdS2`, `cdWacc`, `cdTg`) are dragged,
  not typed, and they answer "what if" rather than feeding the headline. They
  are the same Pro surface as the four fields above: `detail`, never the way a
  value is produced. Their labels carry the Greek letters that the summary tier
  bars, so they cannot be lifted as they are.

---

## Other findings for the same review

1. **The verdict is stated four times** in the hero: `vTierLabel`, the A-F grade
   (`vValusGradeBadge` + explanation), `vVerdict`, and `lynchPlain` /
   `lynchVerdict` below. Settled in review: the grade is cut, `vTierLabel` and
   `vVerdict` hold the headline, and the Lynch lines stay at summary.
2. **Four different fair-value numbers** can appear on one screen: `vIV`, the
   band (`vIvBandHeadline`), `scWeighted` (probability-weighted), and
   `sanityValusIv`. Plus `cdValusIV` in the drawer. Settled in review: the band
   is the only one above the drawer; the other three move to detail.
3. **Implied growth appears three times**: `vIvScenarioText`, `viImplied` with
   its ceiling hint, and the reverse-DCF card.
4. **52-week range appears twice** in the hero: as `vRange` text and as the
   `viRange` bar.
5. **The method that produced the value is never shown.** The engine reaches its
   number by a banking blend, an analyst target or a sector method on some
   tickers, and the page presents all of them identically as a DCF result. The
   agent layer already derives this (`valuationMethod`, `valuationBasis`,
   `valuationReliability`); nothing in the template renders it. That is the
   proposed sixth headline field.
6. **Confidence is rendered but its reason is raw.** `viConfidenceHint` prints
   the engine's own warning string verbatim, e.g. "Thin FCF margin (2.7%), a 1pp
   change in cost assumptions can move IV by 20%+".
