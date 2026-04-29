/* ════════════════════════════════════════════════════════════════════════
   VALUS — Frontend (immersive institutional redesign)
   ════════════════════════════════════════════════════════════════════════ */

const $ = (id) => document.getElementById(id);

const escHtml = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
}[c]));

const fmt = (n, d = 2) => {
  if (n == null || isNaN(n)) return "—";
  return Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
};

const fmtPrice = (n) => n == null || isNaN(n) ? "—" : `$${fmt(n, 2)}`;

const fmtPct = (n, d = 1) => {
  if (n == null || isNaN(n)) return "—";
  const sign = n > 0 ? "+" : "";
  return `${sign}${fmt(n, d)}%`;
};

const fmtBig = (n) => {
  if (n == null || isNaN(n)) return "—";
  const abs = Math.abs(n);
  if (abs >= 1e12) return `$${fmt(n / 1e12, 2)}T`;
  if (abs >= 1e9)  return `$${fmt(n / 1e9, 2)}B`;
  if (abs >= 1e6)  return `$${fmt(n / 1e6, 1)}M`;
  if (abs >= 1e3)  return `$${fmt(n / 1e3, 0)}K`;
  return `$${fmt(n, 2)}`;
};

const fmtX = (n) => n == null ? "—" : `${fmt(n, 1)}×`;

/* ════════════════════════════════════════════════════════════════════════
   Tier color mapping
   ════════════════════════════════════════════════════════════════════════ */

const TIER_CLASSES = {
  deep_discount:      "tier-positive",
  discount:           "tier-positive",
  fair_value:         "tier-info",
  growth:             "tier-warning",
  excellence:         "tier-warning",
  miracle:            "tier-negative",
  decline:            "tier-negative",
  distress:           "tier-positive",  // distressed = market overly pessimistic = opportunity
  strategic_discount: "tier-positive",  // strategic asset trading at low forward multiple
};

function tierClassFor(tier) {
  return TIER_CLASSES[tier] || "tier-info";
}

/* ════════════════════════════════════════════════════════════════════════
   Cursor-tracking card glow (desktop only)
   ════════════════════════════════════════════════════════════════════════ */

function attachCardGlow() {
  if (window.matchMedia("(max-width: 768px)").matches) return;
  document.querySelectorAll(".card, .hero-verdict").forEach(card => {
    card.addEventListener("mousemove", (e) => {
      const rect = card.getBoundingClientRect();
      card.style.setProperty("--mx", `${e.clientX - rect.left}px`);
      card.style.setProperty("--my", `${e.clientY - rect.top}px`);
    });
  });
}

/* ════════════════════════════════════════════════════════════════════════
   Number count-up animation
   ════════════════════════════════════════════════════════════════════════ */

function animateNumber(el, from, to, durationMs, formatter) {
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
    el.textContent = formatter(to);
    return;
  }
  const start = performance.now();
  function step(now) {
    const t = Math.min(1, (now - start) / durationMs);
    const eased = 1 - Math.pow(1 - t, 4);  // easeOutQuart
    const v = from + (to - from) * eased;
    el.textContent = formatter(v);
    if (t < 1) requestAnimationFrame(step);
    else el.textContent = formatter(to);
  }
  requestAnimationFrame(step);
}

/* ════════════════════════════════════════════════════════════════════════
   API layer
   ════════════════════════════════════════════════════════════════════════ */

// ── Crypto / BTC routing ──────────────────────────────────────────────────
function isBTCTicker(t) {
  const u = (t || "").toUpperCase().trim().replace(/-USD$/, "").replace(/^\$/, "");
  return u === "BTC" || u === "BITCOIN" || u === "₿" || u === "XBT";
}
function normalizeBTCTicker(t) { return isBTCTicker(t) ? "BTC-USD" : t; }

async function analyze(ticker, params = {}) {
  // Disable the Analyze button + swap label so a slow Yahoo response can't
  // be double-fired by an impatient click.
  const _btn = document.getElementById("analyzeBtn");
  const _btnLabel = _btn ? _btn.textContent : null;
  if (_btn) { _btn.disabled = true; _btn.textContent = "Analyzing…"; }
  showLoading();
  hideError();
  $("results").classList.add("hidden");
  $("btcHero").classList.add("hidden");
  $("etfHero")?.classList.add("hidden");
  $("portfolioPage")?.classList.add("hidden");
  $("discoverPage")?.classList.add("hidden");
  $("leaderboardPage")?.classList.add("hidden");
  // Make sure hero search section is visible (in case we came from portfolio)
  document.querySelector(".hero")?.classList.remove("hidden");

  const isBTC = isBTCTicker(ticker);
  const fetchTicker = normalizeBTCTicker(ticker);

  const url = new URL("/api/analyze", window.location.origin);
  url.searchParams.set("ticker", fetchTicker);
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== "") url.searchParams.set(k, v);
  }

  try {
    const res = await fetch(url);
    const data = await res.json();
    if (!res.ok || data.error) {
      throw new Error(data.error || `HTTP ${res.status}`);
    }
    hideLoading();
    pushTickerToURL(ticker);
    if (isBTC) {
      renderBTCHero(data);
    } else if (data.is_etf) {
      renderETFHero(data);
    } else {
      renderResults(data);
    }
  } catch (err) {
    hideLoading();
    showError(err.message || "Failed to load analysis");
  } finally {
    if (_btn) { _btn.disabled = false; _btn.textContent = _btnLabel || "Analyze"; }
  }
}

async function searchSuggestions(q) {
  if (!q || q.length < 1) return [];
  try {
    const res = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
    if (!res.ok) return [];
    const data = await res.json();
    // Endpoint returns an array directly (not wrapped in {results: ...})
    return Array.isArray(data) ? data : (data.results || []);
  } catch {
    return [];
  }
}

function showLoading() { $("loading").classList.remove("hidden"); }
function hideLoading() { $("loading").classList.add("hidden"); }
function showError(msg) {
  $("errorMsg").textContent = msg;
  $("error").classList.remove("hidden");
}
function hideError() { $("error").classList.add("hidden"); }

/* ════════════════════════════════════════════════════════════════════════
   Main render
   ════════════════════════════════════════════════════════════════════════ */

let _LAST_DATA = null;

// Polling handle for live-tick on the analyze view (single ticker).
let _ANALYZE_TICK_TIMER = null;

function renderResults(d) {
  _LAST_DATA = d;
  $("results").classList.remove("hidden");

  // Live-tick the analyze view: poll /api/quote every 30s for the displayed
  // ticker and patch price + MOS in place.  IV is stable until the next
  // hourly cron refresh, so client-side recompute is cheap and accurate.
  if (_ANALYZE_TICK_TIMER) clearInterval(_ANALYZE_TICK_TIMER);
  if (d && d.ticker) {
    _ANALYZE_TICK_TIMER = setInterval(() => refreshAnalyzeTick(d.ticker), 30000);
  }

  // Reset scenario chart back to base for each new analysis
  _DCF_CHART_SCENARIO = "base";
  document.querySelectorAll("[data-dcf-sc]").forEach(b =>
    b.classList.toggle("active", b.dataset.dcfSc === "base")
  );

  // If the drawer was open from a previous analysis, collapse it so the user
  // sees the new fresh hero+verdict at the top instead of being stuck deep
  // inside another stock's drilldown.  They can re-expand if they want.
  const drawer  = $("drawer");
  const trigger = $("drawerTrigger");
  if (drawer && drawer.classList.contains("open")) {
    drawer.classList.remove("open");
    if (trigger) {
      trigger.classList.remove("open");
      trigger.setAttribute("aria-expanded", "false");
      const txt = $("drawerTriggerTxt");
      if (txt) txt.textContent = "View detailed analysis";
    }
  }

  renderHeroVerdict(d);
  renderMethodology(d);
  renderScenarios(d);
  renderMiniStats(d);
  renderDrawerContent(d);
  syncAddPortfolioButtonForCurrent();
  if (typeof window._cdReset === "function") window._cdReset();

  attachCardGlow();
  $("results").scrollIntoView({ behavior: "smooth", block: "start" });
}

/* ════════════════════════════════════════════════════════════════════════
   ETF / Index hero — special view for non-equity tickers
   ════════════════════════════════════════════════════════════════════════ */

let etfChartInstance = null;

function renderETFHero(d) {
  _LAST_DATA = d;
  // Hide every other view and show only the ETF hero
  $("results").classList.add("hidden");
  $("btcHero").classList.add("hidden");
  $("portfolioPage")?.classList.add("hidden");
  $("etfHero").classList.remove("hidden");

  $("etfName").textContent  = d.company_name || d.ticker;
  $("etfClass").textContent = d.asset_class || "ETF";
  $("etfPrice").textContent = d.current_price != null ? fmtPrice(d.current_price) : "—";
  $("etfHigh").textContent  = d["52w_high"] != null ? fmtPrice(d["52w_high"]) : "—";
  $("etfLow").textContent   = d["52w_low"]  != null ? fmtPrice(d["52w_low"])  : "—";
  $("etfYtd").textContent   = d.ytd_return != null ? fmtPct(d.ytd_return * 100) : "—";
  $("etfExpense").textContent = d.expense_ratio != null ? `${fmt(d.expense_ratio * 100, 2)}%` : "—";
  $("etfMessage").textContent = d.etf_message || "";

  // Render price chart (theme by YTD direction)
  const canvas = $("etfChart");
  if (canvas && d.price_history && d.price_history.length > 0) {
    const ctx = canvas.getContext("2d");
    if (etfChartInstance) etfChartInstance.destroy();
    const hist = d.price_history;
    const up = hist[hist.length - 1].close >= hist[0].close;
    const color = up ? "#34d399" : "#f87171";
    etfChartInstance = new Chart(ctx, {
      type: "line",
      data: {
        labels: hist.map(h => h.date),
        datasets: [{
          data: hist.map(h => h.close),
          borderColor: color, borderWidth: 2,
          backgroundColor: up ? "rgba(52,211,153,0.10)" : "rgba(248,113,113,0.10)",
          fill: true, tension: 0.25, pointRadius: 0, pointHoverRadius: 4
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: {
          backgroundColor: "#11151d", borderColor: "rgba(255,255,255,0.08)", borderWidth: 1,
          titleColor: "#f5f7fa", bodyColor: "#b6bdcb",
          callbacks: { label: c => `$${fmt(c.parsed.y, 2)}` }
        } },
        scales: {
          x: { ticks: { color: "#6b7382", maxTicksLimit: 6, autoSkip: true }, grid: { display: false } },
          y: { ticks: { color: "#6b7382", callback: v => `$${fmt(v, 0)}` }, grid: { color: "rgba(255,255,255,0.04)" } }
        }
      }
    });
  }
  $("etfHero").scrollIntoView({ behavior: "smooth", block: "start" });
}

/* ════════════════════════════════════════════════════════════════════════
   BTC HODL hero — special view for ₿ tickers
   ════════════════════════════════════════════════════════════════════════ */

let btcChartInstance = null;

function renderBTCHero(d) {
  _LAST_DATA = d;
  $("btcHero").classList.remove("hidden");
  $("results").classList.add("hidden");

  const price = d.current_price;
  const hist  = d.price_history || [];
  const high  = d["52w_high"];
  const low   = d["52w_low"];
  const mcap  = d.market_cap;

  // Animate price count-up
  animateNumber($("btcPrice"), 0, price || 0, 700, v => fmtPrice(v));

  // 24h delta — last vs second-to-last close
  if (hist.length >= 2) {
    const last = hist[hist.length - 1].close;
    const yest = hist[hist.length - 2].close;
    if (yest > 0) {
      const d24 = (last - yest) / yest * 100;
      const el = $("btcDelta24h");
      el.textContent = `24h ${fmtPct(d24)}`;
      el.classList.toggle("positive", d24 >= 0);
      el.classList.toggle("negative", d24 < 0);
    }
  }

  // 1y delta
  if (hist.length >= 30) {
    const last = hist[hist.length - 1].close;
    const first = hist[0].close;
    if (first > 0) {
      const d1y = (last - first) / first * 100;
      const el = $("btcDelta1y");
      el.textContent = `1y ${fmtPct(d1y)}`;
      el.classList.toggle("positive", d1y >= 0);
      el.classList.toggle("negative", d1y < 0);
    }
  }

  $("btcMcap").textContent = mcap != null ? fmtBig(mcap) : "—";
  $("btcHigh").textContent = high != null ? fmtPrice(high) : "—";
  $("btcLow").textContent  = low  != null ? fmtPrice(low)  : "—";

  // Render chart with orange BTC theme
  const canvas = $("btcChart");
  if (canvas && hist.length > 0) {
    const ctx = canvas.getContext("2d");
    if (btcChartInstance) btcChartInstance.destroy();
    const labels = hist.map(h => h.date);
    const prices = hist.map(h => h.close);
    btcChartInstance = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [{
          data: prices,
          borderColor: "#f7931a",
          borderWidth: 2.5,
          backgroundColor: (ctx) => {
            const chart = ctx.chart;
            const { ctx: c, chartArea } = chart;
            if (!chartArea) return "rgba(247, 147, 26, 0.10)";
            const grad = c.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
            grad.addColorStop(0, "rgba(247, 147, 26, 0.45)");
            grad.addColorStop(1, "rgba(247, 147, 26, 0)");
            return grad;
          },
          fill: true, tension: 0.3, pointRadius: 0, pointHoverRadius: 5,
          pointHoverBackgroundColor: "#ffd685", pointHoverBorderColor: "#f7931a"
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "#1a1408", borderColor: "#f7931a",
            borderWidth: 1, titleColor: "#ffd685", bodyColor: "#d9c8a5",
            callbacks: { label: c => `$${fmt(c.parsed.y, 2)}` }
          }
        },
        scales: {
          x: { ticks: { color: "#b8a37a", maxTicksLimit: 6, autoSkip: true }, grid: { display: false } },
          y: { ticks: { color: "#b8a37a", callback: v => `$${fmt(v / 1000, 0)}k` }, grid: { color: "rgba(247, 147, 26, 0.06)" } }
        }
      }
    });
  }

  // Add to portfolio button
  const btcAdd = $("btcAddPortfolio");
  if (btcAdd) {
    const updateBtcAddState = () => {
      if (pfHas("BTC-USD")) {
        btcAdd.classList.add("starred");
        btcAdd.textContent = "★ In Portfolio";
      } else {
        btcAdd.classList.remove("starred");
        btcAdd.textContent = "★ Add to Portfolio";
      }
    };
    btcAdd.onclick = () => {
      if (pfHas("BTC-USD")) {
        pfRemove("BTC-USD");
      } else {
        pfAdd({
          ticker: "BTC-USD",
          name: "Bitcoin",
          sector: "Crypto",
          price: d.current_price,
          iv: null,
          mos: null,
          tier: "HODL",
        });
      }
      updateBtcAddState();
    };
    updateBtcAddState();
  }

  // Back to stocks
  const back = $("btcBack");
  if (back) back.onclick = () => {
    $("btcHero").classList.add("hidden");
    $("tickerInput").focus();
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  $("btcHero").scrollIntoView({ behavior: "smooth", block: "start" });
}

/* ════════════════════════════════════════════════════════════════════════
   Hero verdict card
   ════════════════════════════════════════════════════════════════════════ */

function renderHeroVerdict(d) {
  const hero = $("heroVerdict");
  const vs   = d.verdict_summary || {};
  const pf   = d.priced_for || {};
  const tier = pf.tier || "fair_value";
  const tierCls = tierClassFor(tier);

  // Reset tier classes, apply new
  hero.classList.remove("tier-positive", "tier-info", "tier-warning", "tier-negative");
  hero.classList.add(tierCls);

  // Company info
  $("vName").textContent = d.company_name || d.ticker;
  $("vTicker").textContent = d.ticker;
  $("vSector").textContent = d.sector || "—";
  $("vRange").textContent = d["52w_low"] && d["52w_high"]
    ? `52W $${fmt(d["52w_low"])} – $${fmt(d["52w_high"])}`
    : "";

  // Price + IV with count-up
  const price = d.current_price || 0;
  const iv    = d.intrinsic_value || 0;
  animateNumber($("vPrice"), 0, price, 600, v => fmtPrice(v));
  animateNumber($("vIV"),    0, iv,    600, v => fmtPrice(v));

  // MOS
  const mos = d.margin_of_safety;
  if (mos != null) {
    const fillEl = $("vMosFill");
    const pctEl  = $("vMosPct");
    // Append a "low confidence" chip when |MOS| > 100% (extreme outliers
    // typically indicate a data quirk: stale price, share-class mismatch,
    // forward-earnings spike, leverage non-monotonicity).
    if (d.extreme_mos_flag) {
      const raw = d.margin_of_safety_raw;
      const label = (raw != null && raw > 200) ? "200%+"
                  : (raw != null && raw < -99) ? "−99%"
                  : fmtPct(mos);
      pctEl.innerHTML = `${label}<span class="mos-confidence-chip" title="MOS clamped — raw model output exceeds ±100%. Inputs may be mispriced (forward-earnings spike, share-class mismatch, stale data). Treat as low-confidence.">Low Conf</span>`;
    } else if (d.iv_confidence === "low" || d.iv_confidence === "medium") {
      // Surface emergency / multiples-only IV provenance — investors should
      // know when the model is leaning on analyst targets / cash-only / P/B
      // rather than full DCF.
      const conf = d.iv_confidence === "low" ? "Low Conf" : "Medium";
      const tip  = d.iv_source_label
        ? `Source: ${d.iv_source_label}. DCF was unavailable or low-confidence — IV anchored to a fallback method.`
        : "IV from a fallback method (multiples, analyst target, distressed P/B, etc.).";
      pctEl.innerHTML = `${fmtPct(mos)}<span class="mos-confidence-chip" title="${tip.replace(/"/g, "&quot;")}">${conf}</span>`;
    } else {
      pctEl.textContent = fmtPct(mos);
    }
    // MOS bar fills outward from center toward the side that wins
    const cap = Math.min(Math.abs(mos), 100);
    const widthPct = cap / 2;  // half of total bar
    if (mos >= 0) {
      fillEl.style.left = "50%";
      fillEl.style.right = "auto";
      fillEl.style.width = `${widthPct}%`;
      fillEl.classList.add("positive"); fillEl.classList.remove("negative");
    } else {
      fillEl.style.right = "50%";
      fillEl.style.left = "auto";
      fillEl.style.width = `${widthPct}%`;
      fillEl.classList.add("negative"); fillEl.classList.remove("positive");
    }
  } else {
    $("vMosPct").textContent = "—";
    $("vMosFill").style.width = "0%";
  }

  // Tier badge
  const tierBadge = $("vTierBadge");
  tierBadge.classList.remove("tier-positive","tier-info","tier-warning","tier-negative");
  tierBadge.classList.add(tierCls);
  $("vTierLabel").textContent = pf.label || "Verdict pending";

  // ── Strategic Asset banner ───────────────────────────────────────────
  // Renders only when the backend tags this ticker as a strategic asset
  // (CHIPS Act recipient, defense prime, energy sovereignty, critical
  // material).  Includes per-ticker reason and any live policy tailwinds
  // detected from the news scanner.
  const stratBanner = $("vStrategicBanner");
  if (stratBanner) {
    if (d.is_strategic && d.strategic_label) {
      stratBanner.classList.remove("hidden");
      stratBanner.dataset.tier = d.strategic_tier || "";
      $("vStrategicLabel").textContent = d.strategic_label;
      $("vStrategicReason").textContent = d.strategic_reason || "";
      $("vStrategicLive").classList.toggle("hidden", !d.strategic_live_amplified);
      const chips = [];
      if (d.policy_tailwind) {
        const headlines = (d.policy_tailwind_labels || []).slice(0, 2)
          .map(t => escHtml(t)).join("</span><span class='strategic-chip'>");
        chips.push(`<span class="strategic-chip strategic-chip--up">⬈ Policy tailwind</span>`);
        if (headlines) chips.push(`<span class="strategic-chip strategic-chip--news">${headlines}</span>`);
      }
      if (d.policy_headwind) {
        chips.push(`<span class="strategic-chip strategic-chip--down">⬊ Policy headwind</span>`);
      }
      if (d.strategic_floor_applied) {
        chips.push(`<span class="strategic-chip strategic-chip--info" title="Pure DCF said overvalued; strategic floor lifted IV toward sovereign-backstop level.">IV floor applied</span>`);
      }
      if (d.strategic_wacc_delta_pp) {
        chips.push(`<span class="strategic-chip strategic-chip--info" title="WACC reduced to reflect government backstop on cost of capital.">WACC −${Math.abs(d.strategic_wacc_delta_pp).toFixed(2)}pp</span>`);
      }
      $("vStrategicChips").innerHTML = chips.join("");
    } else {
      stratBanner.classList.add("hidden");
    }
  }

  // Reasons
  const reasonsHtml = (vs.reasons || []).map((r, i) =>
    `<div class="reason"><span class="reason__num">${i+1}</span><span class="reason__txt">${escHtml(r)}</span></div>`
  ).join("");
  $("vReasons").innerHTML = reasonsHtml;

  // ── News block — visible chip + expandable list of catalyst headlines.
  // Renders for *every* ticker with catalysts (not just strategic ones)
  // so investors see the news that's affecting valuation.  Color-coded:
  //   green  = positive catalyst, no material risk
  //   amber  = both positive AND risk present
  //   red    = material risk only
  //   neutral = generic news, no scoring signal
  renderNewsBlock(d);

  // Verdict line
  $("vVerdict").textContent = vs.verdict || pf.narrative || "";

  // Scenario toggle values
  const sc = d.scenarios || {};
  document.querySelector('[data-sc-val="bear"]').textContent = sc.bear?.value != null ? fmtPrice(sc.bear.value) : "—";
  document.querySelector('[data-sc-val="base"]').textContent = sc.base?.value != null ? fmtPrice(sc.base.value) : "—";
  document.querySelector('[data-sc-val="bull"]').textContent = sc.bull?.value != null ? fmtPrice(sc.bull.value) : "—";

  // Bind toggle (animate IV cross-fade between scenarios)
  document.querySelectorAll(".scenario-toggle__btn").forEach(btn => {
    btn.onclick = () => activateScenario(btn.dataset.sc);
  });
  activateScenarioVisual("base");
}

// Renders the news chip + expandable list on the verdict card.  Called
// from renderHeroVerdict.  Hides itself gracefully when no catalysts exist.
function renderNewsBlock(d) {
  const block = document.getElementById("vNewsBlock");
  const chip  = document.getElementById("vNewsChip");
  const cnt   = document.getElementById("vNewsCount");
  const list  = document.getElementById("vNewsList");
  if (!block || !chip || !cnt || !list) return;

  const insights = d.catalyst_insights || [];
  const interp   = d.news_interpretation || [];
  const transformative = new Set((d.transformative_labels || []).map(s => (s || "").toLowerCase().slice(0, 50)));

  if (insights.length === 0 && interp.length === 0) {
    block.classList.add("hidden");
    return;
  }
  block.classList.remove("hidden");

  // Chip color tier
  chip.classList.remove("news-chip--up", "news-chip--down", "news-chip--mixed");
  if (d.has_positive_catalyst && d.has_material_risk) chip.classList.add("news-chip--mixed");
  else if (d.has_positive_catalyst) chip.classList.add("news-chip--up");
  else if (d.has_material_risk)     chip.classList.add("news-chip--down");

  // Choose the dataset to render: prefer scored news_interpretation when
  // available (richer per-headline detail), fall back to catalyst_insights
  // when interp is empty (e.g. analyst-fallback path).
  const rows = (interp.length > 0 ? interp : insights.map(s => ({ title: s }))).slice(0, 6);
  cnt.textContent = `${rows.length} catalyst${rows.length === 1 ? "" : "s"} in past 7 days`;

  list.innerHTML = rows.map(row => {
    const title = (row.title || "").toString();
    const score = row.score;
    const dur   = row.durability;
    const isTransform = transformative.has(title.toLowerCase().slice(0, 50));
    let scoreChip = "";
    if (score != null && Math.abs(score) > 0.05) {
      const sign = score > 0 ? "+" : "";
      const cls  = score > 0 ? "news-row__score--up" : "news-row__score--down";
      scoreChip = `<span class="news-row__score ${cls}">${sign}${score.toFixed(1)}${dur ? ` · ${escHtml(dur)}` : ""}</span>`;
    }
    const marker = isTransform ? `<span class="news-row__marker" title="Transformative catalyst — bumped Stage-1 growth">🚀</span>` : "";
    const ageStr = (row.age_days != null && !isNaN(row.age_days))
      ? `<span class="news-row__age">${Math.round(row.age_days)}d ago</span>`
      : "";
    return `<div class="news-row">${marker}<span class="news-row__title">${escHtml(title)}</span>${ageStr}${scoreChip}</div>`;
  }).join("");

  // Toggle expanded state
  chip.onclick = () => {
    const open = list.classList.toggle("hidden") === false;
    chip.setAttribute("aria-expanded", String(open));
    chip.classList.toggle("news-chip--open", open);
  };
}

// Sector-aware bear / bull case reasons.  Returns a 3-item array of
// numbered narratives explaining what would have to happen for that
// scenario to materialise.
function generateScenarioReasons(which, slot, d) {
  if (which === "base" || !slot || slot.value == null) {
    return (d.verdict_summary || {}).reasons || [];
  }
  const price   = d.current_price;
  const value   = slot.value;
  const upside  = (value - price) / price * 100;
  const sect    = (d.sector || "").toLowerCase();
  const ind     = (d.industry || "").toLowerCase();
  const isTech  = sect.includes("technology") || sect.includes("communication");
  const isFin   = sect.includes("financial");
  const isEnergy= sect.includes("energy");
  const isHC    = sect.includes("healthcare");
  const isStruct= !!d.structural_transformer;
  const isCons  = sect.includes("consumer");
  const isInd   = sect.includes("industrial");

  const ivStr   = fmtPrice(value);
  const upStr   = fmtPct(upside);

  if (which === "bull") {
    if (isStruct)  return [
      `For the bull case at ${ivStr} (${upStr}), platform optionality (AI, robotics, or autonomy) materialises faster than the market expects and revenue compounds at the upper sector ceiling.`,
      `Operating margins expand 200-500bps as platform scale kicks in; capex efficiency improves and the network effect deepens the moat.`,
      `Multiple expansion follows execution — the market re-rates the equity to reflect the optionality value embedded in the platform business.`,
    ];
    if (isTech)    return [
      `For the bull case at ${ivStr} (${upStr}), the AI tailwind compounds — cloud / software / silicon demand accelerates and operating leverage drives margins higher.`,
      `Free cash flow conversion stays at 25%+ of revenue; capital allocation continues to favor buybacks and high-ROIC R&D.`,
      `Multiple expansion comes from durable growth — the market keeps paying for moat-tech businesses with secular tailwinds.`,
    ];
    if (isFin)     return [
      `For the bull case at ${ivStr} (${upStr}), the rate environment supports net interest margin and loan growth accelerates 8%+.`,
      `Credit losses stay contained, capital ratios hold at regulatory comfort, and ROE expands toward sector-leading levels.`,
      `Capital return ramps — buybacks and dividends compound book value per share faster than peers.`,
    ];
    if (isEnergy)  return [
      `For the bull case at ${ivStr} (${upStr}), commodity prices hold elevated and capital discipline drives free-cash-flow yield to double-digits.`,
      `Production stays flat-to-up while capex remains restrained — the FCF flywheel converts directly to dividends + buybacks.`,
      `Any recession is shallow and short; demand recovery + supply constraints push margins to cycle-high levels.`,
    ];
    if (isHC)      return [
      `For the bull case at ${ivStr} (${upStr}), pipeline assets reach commercial milestones and existing franchises hold pricing power.`,
      `R&D efficiency improves and approvals/launches drive double-digit revenue growth with expanding gross margins.`,
      `Multiple expansion follows scientific success — the market pays a premium for proven execution against unmet medical needs.`,
    ];
    if (isInd || isCons) return [
      `For the bull case at ${ivStr} (${upStr}), demand stays robust through any cycle softness; pricing power offsets input-cost inflation.`,
      `Operational efficiency gains and disciplined capital deployment expand operating margins 100-300bps.`,
      `Capital return — buybacks, dividends, debt paydown — drives per-share intrinsic value higher faster than reported earnings.`,
    ];
    return [
      `For the bull case at ${ivStr} (${upStr}), revenue grows at the top of the sector band and operating margins expand on improving mix.`,
      `Capital allocation creates additional shareholder value via buybacks, dividend growth, or accretive M&A.`,
      `Multiple holds or expands — the market continues to reward execution and cash-flow durability.`,
    ];
  }

  // Bear case
  if (isStruct)  return [
    `For the bear case at ${ivStr} (${upStr}), the platform thesis stalls — AI / robotics / autonomy roadmap slips and capex stays elevated without payoff.`,
    `Growth reverts toward the base business as competition catches up; the speculative premium evaporates.`,
    `Multiple compresses as the market re-prices on actual cash flows rather than long-tail optionality.`,
  ];
  if (isTech)    return [
    `For the bear case at ${ivStr} (${upStr}), AI / cloud demand normalises and growth halves as the category matures.`,
    `Competition compresses operating margins; pricing power weakens and unit economics deteriorate.`,
    `Multiple compresses materially — the market re-rates from "growth" to "GARP" valuation framework.`,
  ];
  if (isFin)     return [
    `For the bear case at ${ivStr} (${upStr}), rates roll over and net interest margin compresses 50-100bps.`,
    `Credit losses spike on commercial real estate or consumer lending; loan-loss provisions weigh on earnings.`,
    `Regulatory capital requirements rise, restricting buybacks and forcing equity dilution at the wrong moment.`,
  ];
  if (isEnergy)  return [
    `For the bear case at ${ivStr} (${upStr}), commodity prices fall sharply on demand softness or excess supply.`,
    `Capex commitments stay fixed but cash flow contracts — dividends face risk and net debt creeps higher.`,
    `Multiple compresses on cycle fears — the market discounts a longer downturn than priced today.`,
  ];
  if (isHC)      return [
    `For the bear case at ${ivStr} (${upStr}), key pipeline assets fail or face surprise regulatory / pricing pressure.`,
    `Revenue concentration creates patent-cliff risk; competition from biosimilars or generics accelerates.`,
    `Multiple compresses as investors discount future R&D productivity and policy headwinds.`,
  ];
  if (isInd || isCons) return [
    `For the bear case at ${ivStr} (${upStr}), demand softens on macro deceleration; volumes decline mid-single-digits.`,
    `Input-cost inflation outpaces pricing power; operating margins compress 100-200bps.`,
    `Capital return slows — buybacks pause, debt paydown takes priority, and the multiple re-rates lower.`,
  ];
  return [
    `For the bear case at ${ivStr} (${upStr}), macro headwinds and competitive pressure cut revenue growth meaningfully below trend.`,
    `Operating margins compress as fixed costs absorb a smaller revenue base; cash conversion deteriorates.`,
    `Multiple re-rates lower as the market discounts a longer / deeper slowdown than currently priced.`,
  ];
}

const TIER_BY_SCENARIO = {
  bear: "tier-negative",
  base: null,           // restore original priced_for tier
  bull: "tier-positive",
};

function activateScenario(which) {
  if (!_LAST_DATA) return;
  const sc = _LAST_DATA.scenarios || {};
  const slot = sc[which];
  if (!slot || slot.value == null) return;

  // Animate IV value
  const ivEl = $("vIV");
  const cur  = parseFloat(ivEl.textContent.replace(/[^0-9.-]/g,"")) || 0;
  animateNumber(ivEl, cur, slot.value, 380, v => fmtPrice(v));

  // Swap hero card tier color (red for bear, green for bull, original for base)
  const hero = $("heroVerdict");
  hero.classList.remove("tier-positive", "tier-info", "tier-warning", "tier-negative");
  if (which === "base") {
    const orig = tierClassFor((_LAST_DATA.priced_for || {}).tier || "fair_value");
    hero.classList.add(orig);
    // Restore tier badge color too
    const tb = $("vTierBadge");
    tb.classList.remove("tier-positive", "tier-info", "tier-warning", "tier-negative");
    tb.classList.add(orig);
  } else {
    const cls = TIER_BY_SCENARIO[which];
    hero.classList.add(cls);
    const tb = $("vTierBadge");
    tb.classList.remove("tier-positive", "tier-info", "tier-warning", "tier-negative");
    tb.classList.add(cls);
  }

  // Update tier label
  const tierLabelMap = { bear: "Bear case scenario", bull: "Bull case scenario" };
  if (which === "base") {
    $("vTierLabel").textContent = (_LAST_DATA.priced_for || {}).label || "—";
  } else {
    $("vTierLabel").textContent = tierLabelMap[which];
  }

  // Update MOS
  if (_LAST_DATA.current_price && slot.value) {
    const newMos = (slot.value - _LAST_DATA.current_price) / _LAST_DATA.current_price * 100;
    $("vMosPct").textContent = fmtPct(newMos);
    const fillEl = $("vMosFill");
    const cap = Math.min(Math.abs(newMos), 100);
    const widthPct = cap / 2;
    if (newMos >= 0) {
      fillEl.style.left = "50%"; fillEl.style.right = "auto";
      fillEl.classList.add("positive"); fillEl.classList.remove("negative");
    } else {
      fillEl.style.right = "50%"; fillEl.style.left = "auto";
      fillEl.classList.add("negative"); fillEl.classList.remove("positive");
    }
    fillEl.style.width = `${widthPct}%`;
  }

  // Swap reasons + verdict line
  const reasons = generateScenarioReasons(which, slot, _LAST_DATA);
  const reasonsHtml = reasons.map((r, i) =>
    `<div class="reason"><span class="reason__num">${i+1}</span><span class="reason__txt">${escHtml(r)}</span></div>`
  ).join("");
  const reasonsEl = $("vReasons");
  reasonsEl.style.opacity = "0";
  setTimeout(() => {
    reasonsEl.innerHTML = reasonsHtml;
    reasonsEl.style.opacity = "1";
  }, 180);

  // Swap verdict line based on scenario
  const vline = $("vVerdict");
  if (which === "bear") {
    vline.textContent = `What would have to be true for the bear case to play out — and how the model would re-rate from there.`;
  } else if (which === "bull") {
    vline.textContent = `What would have to be true for the bull case to play out — and the upside the market hasn't yet priced in.`;
  } else {
    vline.textContent = (_LAST_DATA.verdict_summary || {}).verdict || (_LAST_DATA.priced_for || {}).narrative || "";
  }

  activateScenarioVisual(which);
}

function activateScenarioVisual(which) {
  document.querySelectorAll(".scenario-toggle__btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.sc === which);
  });
}

/* ════════════════════════════════════════════════════════════════════════
   Methodology explainer
   ════════════════════════════════════════════════════════════════════════ */

function fmtMethodValue(s) {
  const v = s.value;
  if (v == null) return "—";
  switch (s.format) {
    case "currency_b": return fmtBig(v);
    case "currency":   return fmtPrice(v);
    case "percent":    return fmt(v * 100, 1) + "%";
    case "delta_pct":  return (v > 0 ? "+" : "") + fmt(v, 1) + "%";
    default:           return String(v);
  }
}

function valueClassFor(s) {
  if (s.format === "delta_pct" && s.value > 0) return "positive";
  if (s.format === "currency_b" && s.value < 0) return "negative";
  if (!s.active) return "muted";
  return "";
}

function renderMethodology(d) {
  const steps = d.methodology_steps || [];
  const container = $("methodSteps");
  container.innerHTML = "";

  if (steps.length === 0) {
    container.innerHTML = `<div class="text-muted" style="padding: 16px 0; text-align: center;">Methodology breakdown unavailable for this stock.</div>`;
    return;
  }

  // Render each step
  const rows = steps.map(s => {
    const valClass = valueClassFor(s);
    const valStr = fmtMethodValue(s);
    return `
      <div class="method-step ${s.active ? '' : 'inactive'}">
        <div class="method-step__num">${s.step}</div>
        <div class="method-step__body">
          <div class="method-step__label">${escHtml(s.label)}</div>
          <div class="method-step__detail">${escHtml(s.detail || '')}</div>
        </div>
        <div class="method-step__value ${valClass}">${valStr}</div>
      </div>
    `;
  });

  // Final fair value row
  if (d.intrinsic_value != null) {
    rows.push(`
      <div class="method-step final">
        <div class="method-step__num">✓</div>
        <div class="method-step__body">
          <div class="method-step__label">Final fair value</div>
          <div class="method-step__detail">After all layers applied</div>
        </div>
        <div class="method-step__value">${fmtPrice(d.intrinsic_value)}</div>
      </div>
    `);
  }

  container.innerHTML = rows.join("");

  // Bind expand/collapse
  const card = $("methodology");
  const head = $("methodologyHead");
  const toggleMethod = () => {
    const isOpen = card.classList.toggle("expanded");
    head.setAttribute("aria-expanded", isOpen ? "true" : "false");
    if (isOpen) {
      // Re-trigger animation by clearing and re-applying
      container.querySelectorAll(".method-step").forEach(el => {
        el.style.animation = "none";
        // eslint-disable-next-line no-unused-expressions
        el.offsetHeight;
        el.style.animation = "";
      });
    }
  };
  head.onclick = toggleMethod;
  head.onkeydown = (e) => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggleMethod(); }
  };
}

/* ════════════════════════════════════════════════════════════════════════
   Scenarios
   ════════════════════════════════════════════════════════════════════════ */

function renderScenarios(d) {
  const sc = d.scenarios || {};
  const grid = $("scGrid");

  $("scWeightNote").textContent = sc.weight_basis ? `Weights: ${sc.weight_basis}` : "";

  // Sector-aware case-narratives so users understand what each scenario assumes
  const ind = (d.industry || "").toLowerCase();
  const sect = (d.sector || "").toLowerCase();
  const isTech    = sect.includes("technology") || sect.includes("communication");
  const isFin     = sect.includes("financial");
  const isEnergy  = sect.includes("energy");
  const isStruct  = !!d.structural_transformer;

  function bullCase() {
    if (isStruct) return "Best case: platform optionality (AI, robotics, or autonomy) materialises faster than expected and drives a re-rating. Margins expand, growth stays at sector ceiling.";
    if (isTech)   return "Best case: AI tailwind compounds, operating leverage kicks in, growth holds at the upper band of the sector ceiling.";
    if (isFin)    return "Best case: rate environment supports NIM, loan growth accelerates, credit losses stay contained, ROE expands.";
    if (isEnergy) return "Best case: commodity prices hold elevated, capital discipline drives free-cash-flow yield higher, dividends and buybacks compound.";
    return "Best case: revenue grows at the top of the sector band, margins expand, capital allocation creates additional shareholder value.";
  }
  function bearCase() {
    if (isStruct) return "Worst case: platform thesis stalls, capex stays elevated without payoff, growth reverts to the base business and the premium evaporates.";
    if (isTech)   return "Worst case: AI demand normalises, competition compresses margins, growth halves and the multiple compresses with it.";
    if (isFin)    return "Worst case: rates roll over, credit losses spike, regulatory capital requirements rise, ROE compresses.";
    if (isEnergy) return "Worst case: commodity prices fall sharply, capex commitments stay fixed, FCF is squeezed and dividends face risk.";
    return "Worst case: macro headwinds, market-share losses, or margin compression cut growth meaningfully and the multiple re-rates lower.";
  }
  function baseCase() {
    return "VALUS's central forecast — Stage 1 growth tapers to Stage 2, then to terminal at sector ceiling. Discounted at the model WACC.";
  }

  const meta = {
    bear: { label: "Bear case",  case: bearCase(), valClass: "negative", priorityIdx: 0 },
    base: { label: "Base case",  case: baseCase(), valClass: "neutral",  priorityIdx: 1 },
    bull: { label: "Bull case",  case: bullCase(), valClass: "positive", priorityIdx: 2 },
  };

  const cards = ["bear", "base", "bull"].map(key => {
    const slot = sc[key] || {};
    const v = slot.value;
    const w = slot.weight ?? 33;
    const upside = slot.upside;
    const m = meta[key];

    const barW = Math.min(Math.max(w, 5), 100);
    const s1   = slot.s1   != null ? `<span><strong>g₁</strong> ${fmt(slot.s1, 1)}%</span>` : "";
    const wacc = slot.wacc != null ? `<span><strong>WACC</strong> ${fmt(slot.wacc, 1)}%</span>` : "";

    return `
      <div class="sc-card ${key}">
        <div class="sc-card__head">
          <span class="sc-card__label">${m.label}</span>
          <span class="sc-card__weight">${w}% weight</span>
        </div>
        <div class="sc-card__value ${m.valClass}">${v != null ? fmtPrice(v) : "—"}</div>
        <div class="sc-card__delta">${upside != null ? fmtPct(upside) + " vs current" : "—"}</div>
        <div class="sc-card__bar"><div class="sc-card__bar-fill" style="--bar-width: ${barW}%; width: ${barW}%;"></div></div>
        <div class="sc-card__case">${escHtml(m.case)}</div>
        <div class="sc-card__assumptions">${s1}${wacc}</div>
      </div>
    `;
  });

  grid.innerHTML = cards.join("");

  const w = sc.weighted;
  const wd = sc.weighted_upside;
  $("scWeighted").textContent = w != null ? fmtPrice(w) : "—";
  $("scWeightedDelta").textContent = wd != null ? fmtPct(wd) + " potential" : "";
}

/* ════════════════════════════════════════════════════════════════════════
   Mini stats
   ════════════════════════════════════════════════════════════════════════ */

function renderMiniStats(d) {
  $("mMcap").textContent   = d.market_cap != null ? fmtBig(d.market_cap) : "—";
  $("mPE").textContent     = d.pe_ratio != null ? fmtX(d.pe_ratio) : "—";
  const dy = d.dividend_yield;
  $("mDiv").textContent    = dy != null ? `${fmt(dy, 2)}%` : "—";
  const tgt = d.target_price;
  $("mTarget").textContent = tgt != null ? fmtPrice(tgt) : "—";

  // Color analyst target if it differs significantly from current price
  const tgtEl = $("mTarget");
  tgtEl.classList.remove("positive", "negative");
  if (tgt && d.current_price) {
    const diff = (tgt - d.current_price) / d.current_price * 100;
    if (diff > 10) tgtEl.classList.add("positive");
    else if (diff < -10) tgtEl.classList.add("negative");
  }
}

/* ════════════════════════════════════════════════════════════════════════
   Drawer content (DCF assumptions + notes + charts + tables)
   ════════════════════════════════════════════════════════════════════════ */

function renderDrawerContent(d) {
  // ── Drawer toggle binding (FIRST, so it works even if other parts fail)
  const trigger = $("drawerTrigger");
  const drawer  = $("drawer");
  const triggerTxt = $("drawerTriggerTxt");
  if (trigger && drawer) {
    trigger.onclick = () => {
      const isOpen = drawer.classList.toggle("open");
      trigger.classList.toggle("open", isOpen);
      trigger.setAttribute("aria-expanded", isOpen ? "true" : "false");
      if (triggerTxt) triggerTxt.textContent = isOpen ? "Hide detailed analysis" : "View detailed analysis";
      // Lazy-load financial statements on first open (Yahoo-sourced)
      if (isOpen && !drawer.dataset.statementsLoaded) {
        renderFinancialsTabs(d).catch(() => {});
        drawer.dataset.statementsLoaded = "1";
      }
    };
  }

  // ── Each renderer is wrapped so a single failure doesn't take down others
  try {
    const ag = $("assumptionsGrid");
    if (ag) {
      const rows = [
        ["WACC",            d.wacc != null ? `${fmt(d.wacc, 1)}%` : "—"],
        ["Cost of Equity",  d.cost_of_equity != null ? `${fmt(d.cost_of_equity, 1)}%` : "—"],
        ["Cost of Debt",    d.cost_of_debt != null ? `${fmt(d.cost_of_debt, 1)}%` : "—"],
        ["Beta",            d.beta != null ? fmt(d.beta, 2) : "—"],
        ["Stage 1 growth",  d.stage1_growth != null ? `${fmt(d.stage1_growth, 1)}%` : "—"],
        ["Stage 2 growth", d.stage2_growth != null ? `${fmt(d.stage2_growth, 1)}%` : "—"],
        ["Terminal growth", d.terminal_growth != null ? `${fmt(d.terminal_growth, 1)}%` : "—"],
        ["Tax rate",        d.tax_rate != null ? `${fmt(d.tax_rate, 1)}%` : "—"],
        ["Base FCF",        d.base_fcf != null ? fmtBig(d.base_fcf) : "—"],
        ["Net debt",        d.net_debt != null ? fmtBig(d.net_debt) : "—"],
        ["Shares out",      d.shares_outstanding != null ? fmtBig(d.shares_outstanding).replace("$","") : "—"],
        ["Years projected", d.projection_years || 10],
      ];
      ag.innerHTML = rows.map(([l, v]) =>
        `<div class="assumption"><span class="assumption__label">${l}</span><span class="assumption__value numeric">${v}</span></div>`
      ).join("");
    }
  } catch (e) { console.error("[assumptions]", e); }

  try { renderNotes(d); } catch (e) { console.error("[notes]", e); }
  try { if (d.price_history) renderPriceChart(d.price_history); } catch (e) { console.error("[priceChart]", e); }
  try { if (d.fcf_chart)     renderDcfChart(d.fcf_chart); }       catch (e) { console.error("[dcfChart]", e); }
  try { renderProjectionTable(d); } catch (e) { console.error("[projTable]", e); }
  // Financial statements are now lazy-loaded on first drawer open (see above).
  // Reset the flag for each new analysis so a fresh ticker re-fetches.
  if (drawer) delete drawer.dataset.statementsLoaded;
}

function renderNotes(d) {
  const list = $("notesList");
  const items = [];

  // Pull all the auxiliary info into note items
  if (d.dcf_warning) items.push({ type: "warn", text: d.dcf_warning });
  if (d.expectation_gap) {
    const eg = d.expectation_gap;
    items.push({ type: "info",
      text: `<strong>Expectation Gap (${eg.score}/10)</strong>: ${eg.primary_narrative}` });
  }
  if (d.fin415_used) {
    items.push({ type: "info",
      text: `<strong>FIN 415 FCFE Model</strong> active — Ke = ${d.fin415_ke}%, Conservative target $${d.fin415_conservative}` });
  }
  if (d.reality_reconciled) {
    items.push({ type: "info",
      text: `<strong>Reality Reconciliation</strong>: ${escHtml(d.reality_reason)}` });
  }
  if (d.is_cash_rich && d.cash_rich_narrative) {
    items.push({ type: "good", text: `<strong>Cash Rich</strong> — ${escHtml(d.cash_rich_narrative)}` });
  }
  if (d.is_mag7) {
    items.push({ type: "warn",
      text: `<strong>Mag 7 Member</strong> — concentration risk: 6 of 7 Mag 7 stocks correlate with the AI productivity thesis. A 10–15% pullback could unwind multiple positions simultaneously.` });
  }
  if (d.debt_momentum && d.debt_momentum.classification && d.debt_momentum.classification !== "stable") {
    const dm = d.debt_momentum;
    const colorMap = { deleveraging: "good", speculative_distress: "risk", recovery_watch: "warn", healthy_leverage: "info" };
    items.push({ type: colorMap[dm.classification] || "info",
      text: `<strong>${escHtml(dm.label)}</strong> — ${escHtml(dm.narrative)}` });
  }
  if (d.moat_detected) {
    items.push({ type: "good",
      text: `<strong>${escHtml(d.moat_path)}</strong> moat detected — WACC adjustment ${d.moat_wacc_delta}pp.` });
  }
  if (d.structural_transformer) {
    items.push({ type: "info",
      text: `<strong>Structural Transformer</strong> — platform optionality (AI/robotics/autonomy) priced on top of base.` });
  }
  if (d.has_positive_catalyst) items.push({ type: "good", text: `<strong>Positive catalyst</strong> in recent filings.` });
  if (d.has_material_risk)     items.push({ type: "risk", text: `<strong>Material risk</strong> in recent filings.` });
  (d.dcf_notes || []).forEach(n => items.push(n));

  if (items.length === 0) {
    list.innerHTML = `<div class="text-muted" style="padding: 8px 0;">No additional notes for this stock.</div>`;
    return;
  }

  list.innerHTML = items.map(n =>
    `<div class="note-item ${n.type || 'info'}">${n.text}</div>`
  ).join("");
}

/* ════════════════════════════════════════════════════════════════════════
   Charts
   ════════════════════════════════════════════════════════════════════════ */

let priceChartInstance, dcfChartInstance;

function renderPriceChart(history) {
  const canvas = $("priceChart");
  if (!canvas || !history || history.length === 0) return;
  const ctx = canvas.getContext("2d");
  if (priceChartInstance) priceChartInstance.destroy();

  const labels = history.map(h => h.date);
  const prices = history.map(h => h.close);

  // Line color: green if up, red if down over period
  const up = prices[prices.length - 1] >= prices[0];
  const color = up ? "#34d399" : "#f87171";

  priceChartInstance = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [{
        data: prices, borderColor: color, borderWidth: 2,
        backgroundColor: up ? "rgba(52,211,153,0.10)" : "rgba(248,113,113,0.10)",
        fill: true, tension: 0.25, pointRadius: 0, pointHoverRadius: 4
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: {
        backgroundColor: "#11151d", borderColor: "rgba(255,255,255,0.08)",
        borderWidth: 1, titleColor: "#f5f7fa", bodyColor: "#b6bdcb",
        callbacks: { label: (c) => `$${fmt(c.parsed.y, 2)}` }
      } },
      scales: {
        x: { ticks: { color: "#6b7382", maxRotation: 0, autoSkip: true, maxTicksLimit: 6 }, grid: { display: false } },
        y: { ticks: { color: "#6b7382", callback: v => `$${fmt(v, 0)}` }, grid: { color: "rgba(255,255,255,0.04)" } }
      }
    }
  });
}

// Compute a 10-year projection client-side for any scenario.
// Uses base_fcf, scenario s1 + wacc, global s2/yrs.
// Returns values in $B to match the server-shipped base shape.
function projectScenario(d, scenario) {
  const sc = (d.scenarios || {})[scenario] || {};
  const baseFcfRaw = d.base_fcf || 0;
  if (!baseFcfRaw) return null;

  const yrs = d.projection_years || 10;
  const s1Pct   = sc.s1   != null ? sc.s1   : (d.stage1_growth || 0);
  const waccPct = sc.wacc != null ? sc.wacc : (d.wacc || 9);
  const s1 = s1Pct / 100;
  const s2Pct = d.stage2_growth != null ? d.stage2_growth : Math.max(s1Pct * 0.55, 2);
  const s2 = s2Pct / 100;
  const wacc = waccPct / 100;

  const stage1Years = Math.ceil(yrs / 2);
  const labels = [], projected = [], discounted = [];
  let fcf = baseFcfRaw;   // raw $ for math
  for (let y = 1; y <= yrs; y++) {
    const g = y <= stage1Years ? s1 : s2;
    fcf = fcf * (1 + g);
    const pv = fcf / Math.pow(1 + wacc, y);
    labels.push(`Y${y}`);
    projected.push(fcf / 1e9);    // convert to $B for display
    discounted.push(pv / 1e9);
  }
  const totalFcf = projected.reduce((a, b) => a + b, 0);
  const totalPv  = discounted.reduce((a, b) => a + b, 0);
  return { labels, projected, discounted, s1Pct, waccPct, s2Pct, totalFcf, totalPv };
}

let _DCF_CHART_SCENARIO = "base";

function renderDcfChart(fcfData) {
  // fcfData is the server-shipped { projected: { labels, values, pvs } } for base.
  // We override with per-scenario client-computed data when toggled.
  const canvas = $("dcfChart");
  if (!canvas || !_LAST_DATA) return;
  const ctx = canvas.getContext("2d");
  if (dcfChartInstance) dcfChartInstance.destroy();

  const which = _DCF_CHART_SCENARIO;
  let labels, projected, discounted, footerLine;

  if (which === "base" && fcfData && fcfData.projected && fcfData.projected.values) {
    // Backend already ships these in $B — do NOT divide again.
    labels     = fcfData.projected.labels || [];
    projected  = (fcfData.projected.values || []).slice();
    discounted = (fcfData.projected.pvs    || []).slice();
    const tot = projected.reduce((a, b) => a + b, 0);
    const pv  = discounted.reduce((a, b) => a + b, 0);
    footerLine = `<span><strong>g₁</strong> ${fmt(_LAST_DATA.stage1_growth || 0, 1)}%</span>` +
                 `<span><strong>WACC</strong> ${fmt(_LAST_DATA.wacc || 0, 1)}%</span>` +
                 `<span><strong>Total FCF (10y)</strong> $${fmt(tot, 1)}B</span>` +
                 `<span><strong>Total PV</strong> $${fmt(pv, 1)}B</span>`;
  } else {
    // Compute client-side for bear or bull
    const sim = projectScenario(_LAST_DATA, which);
    if (!sim) return;
    labels = sim.labels;
    projected = sim.projected;
    discounted = sim.discounted;
    footerLine = `<span><strong>g₁</strong> ${fmt(sim.s1Pct, 1)}%</span>` +
                 `<span><strong>WACC</strong> ${fmt(sim.waccPct, 1)}%</span>` +
                 `<span><strong>Total FCF (10y)</strong> $${fmt(sim.totalFcf, 1)}B</span>` +
                 `<span><strong>Total PV</strong> $${fmt(sim.totalPv, 1)}B</span>`;
  }

  if (!labels || labels.length === 0 || projected.length === 0) return;

  // Color theme per scenario
  const themes = {
    base: { proj: "#5eead4", projBg: "rgba(94,234,212,0.20)", disc: "#60a5fa", discBg: "rgba(96,165,250,0.20)" },
    bull: { proj: "#34d399", projBg: "rgba(52,211,153,0.20)",  disc: "#5eead4", discBg: "rgba(94,234,212,0.20)" },
    bear: { proj: "#f87171", projBg: "rgba(248,113,113,0.20)", disc: "#fbbf24", discBg: "rgba(251,191,36,0.20)" },
  };
  const t = themes[which];

  $("dcfChartFooter").innerHTML = footerLine;

  // ── Wire up the year scrubber ─────────────────────────────────────────
  const slider     = $("dcfSlider");
  const ticksEl    = $("dcfSliderTicks");
  const yearCard   = $("dcfYearCard");
  const dyYear     = $("dyYearN");
  const dyFcf      = $("dyFcf");
  const dyPv       = $("dyPv");
  const dyGrowth   = $("dyGrowth");
  const dyDiscNote = $("dyDiscNote");

  if (slider && yearCard) {
    slider.max = labels.length;
    if (parseInt(slider.value, 10) > labels.length) slider.value = "1";

    // Apply scenario color class
    yearCard.classList.remove("bear", "base", "bull");
    yearCard.classList.add(which);
    slider.classList.remove("bear", "base", "bull");
    slider.classList.add(which);

    // Build tick labels Y1..Y10
    if (ticksEl && ticksEl.children.length !== labels.length) {
      ticksEl.innerHTML = labels.map((l, i) =>
        `<span data-tick="${i+1}">${escHtml(l)}</span>`).join("");
    }

    // Compute per-year growth from successive values (or use base growth)
    const baseFcf = (_LAST_DATA?.base_fcf || 0) / 1e9;  // base in $B
    function growthFor(idx) {
      if (idx === 0) {
        const g = baseFcf > 0 ? (projected[0] - baseFcf) / baseFcf : null;
        return g;
      }
      const prev = projected[idx - 1];
      return prev > 0 ? (projected[idx] - prev) / prev : null;
    }

    function update(yearIdx) {
      const y = yearIdx;  // 1..N
      const i = y - 1;
      const fcf = projected[i];
      const pv  = discounted[i];
      const g   = growthFor(i);
      const wacc = (which === "base" && fcfData?.projected)
        ? (_LAST_DATA?.wacc || 0)
        : ((_LAST_DATA?.scenarios?.[which]?.wacc) ?? (_LAST_DATA?.wacc || 0));
      const discFactor = wacc > 0 ? Math.pow(1 + wacc / 100, y) : 1;

      dyYear.textContent = y;
      dyFcf.textContent  = `$${fmt(fcf, 2)}B`;
      dyPv.textContent   = `$${fmt(pv,  2)}B`;
      dyGrowth.textContent = g != null
        ? `${(g >= 0 ? "+" : "")}${fmt(g * 100, 1)}% growth`
        : "—";
      dyDiscNote.textContent =
        `Discount factor: 1 / (1 + ${fmt(wacc, 1)}%)^${y} = ${fmt(1 / discFactor, 4)} · ` +
        `Year-${y} growth applied: ${g != null ? fmt(g * 100, 1) + "%" : "—"}`;

      // Slider-fill % for the colored runnable track
      const pct = ((y - 1) / Math.max(labels.length - 1, 1)) * 100;
      slider.style.setProperty("--slider-pct", `${pct}%`);

      // Highlight tick label
      ticksEl?.querySelectorAll("span").forEach(s =>
        s.classList.toggle("active", parseInt(s.dataset.tick, 10) === y));
    }

    // Initial render + bind input event (replaces any prior handler)
    slider.oninput = (e) => update(parseInt(e.target.value, 10));
    update(parseInt(slider.value, 10) || 1);
  }

  dcfChartInstance = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        { label: "Projected FCF",      data: projected,
          backgroundColor: t.projBg, borderColor: t.proj,
          borderWidth: 1, borderRadius: 4 },
        { label: "Discounted FCF (PV)", data: discounted,
          backgroundColor: t.discBg, borderColor: t.disc,
          borderWidth: 1, borderRadius: 4 },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 600, easing: "easeOutQuart" },
      plugins: {
        legend: { labels: { color: "#b6bdcb", font: { size: 11 }, usePointStyle: true } },
        tooltip: {
          backgroundColor: "#11151d", borderColor: "rgba(255,255,255,0.08)",
          borderWidth: 1, padding: 12,
          titleColor: "#f5f7fa", bodyColor: "#b6bdcb",
          callbacks: { label: c => `${c.dataset.label}: $${fmt(c.parsed.y, 2)}B` }
        }
      },
      scales: {
        x: { ticks: { color: "#6b7382" }, grid: { display: false } },
        y: { ticks: { color: "#6b7382",
                      callback: v => v >= 1000 ? `$${fmt(v/1000, 1)}T`
                                              : v >= 1    ? `$${fmt(v, 0)}B`
                                              : `$${fmt(v * 1000, 0)}M` },
             grid: { color: "rgba(255,255,255,0.04)" } }
      }
    }
  });

  // Bind scenario toggle (idempotent — replaces existing handlers)
  document.querySelectorAll("[data-dcf-sc]").forEach(btn => {
    btn.onclick = () => {
      _DCF_CHART_SCENARIO = btn.dataset.dcfSc;
      document.querySelectorAll("[data-dcf-sc]").forEach(b =>
        b.classList.toggle("active", b === btn)
      );
      renderDcfChart(_LAST_DATA?.fcf_chart);
    };
  });
}

/* ════════════════════════════════════════════════════════════════════════
   Projection table
   ════════════════════════════════════════════════════════════════════════ */

function renderProjectionTable(d) {
  const body = $("projBody");
  const foot = $("projFoot");
  const proj = d.projected_fcf || [];

  if (proj.length === 0) {
    body.innerHTML = `<tr><td colspan="4" class="text-muted">No projection data.</td></tr>`;
    foot.innerHTML = "";
    return;
  }

  body.innerHTML = proj.map(r =>
    `<tr>
      <td>Year ${r.year}</td>
      <td>${fmtBig(r.fcf)}</td>
      <td>${r.growth != null ? `${fmt(r.growth * 100, 1)}%` : '—'}</td>
      <td>${fmtBig(r.pv)}</td>
    </tr>`
  ).join("");

  if (d.total_pv_fcf || d.pv_terminal) {
    foot.innerHTML = `
      <tr><td colspan="3">PV of explicit period</td><td>${fmtBig(d.total_pv_fcf)}</td></tr>
      <tr><td colspan="3">PV of terminal value</td><td>${fmtBig(d.pv_terminal)}</td></tr>
      <tr><td colspan="3">Enterprise value</td><td>${fmtBig(d.enterprise_value)}</td></tr>
      <tr><td colspan="3">Less: net debt</td><td>${fmtBig(-(d.net_debt || 0))}</td></tr>
      <tr><td colspan="3"><strong>Equity value</strong></td><td><strong>${fmtBig(d.equity_value)}</strong></td></tr>
    `;
  }
}

/* ════════════════════════════════════════════════════════════════════════
   Financial statements (income / balance / cashflow)
   ════════════════════════════════════════════════════════════════════════ */

// Financial statements are loaded lazily from /api/statements on first
// drawer open.  Cached on the data dict to avoid re-fetching.
let _STATEMENTS_CACHE = {};

async function renderFinancialsTabs(d) {
  const body = $("finBody");
  if (!body) return;
  body.innerHTML = `<tr><td class="text-muted">Loading financial statements…</td></tr>`;

  let statements = _STATEMENTS_CACHE[d.ticker];
  if (!statements) {
    try {
      const res = await fetch(`/api/statements?ticker=${encodeURIComponent(d.ticker)}`);
      statements = await res.json();
      if (statements && !statements.error) _STATEMENTS_CACHE[d.ticker] = statements;
    } catch (e) {
      body.innerHTML = `<tr><td class="text-muted">Could not load statements.</td></tr>`;
      return;
    }
  }
  if (!statements || statements.error) {
    body.innerHTML = `<tr><td class="text-muted">No statement data available.</td></tr>`;
    return;
  }

  const ccy = statements.financialCurrency || "USD";

  function pickTab(key) {
    const stmt = statements[key] || {};
    const rows = stmt.rows || [];
    const cols = stmt.columns || [];
    if (rows.length === 0) {
      body.innerHTML = `<tr><td class="text-muted">No ${key} statement available.</td></tr>`;
      return;
    }
    // Format column headers — keep just YYYY-MM-DD or year for compactness
    const headers = cols.map(c => {
      const s = String(c);
      // If it looks like a date, show "FY 2025" style
      const m = s.match(/^(\d{4})/);
      return m ? `FY ${m[1]}` : s;
    });

    const headerRow = `<tr><th style="text-align:left">Item (${escHtml(ccy)})</th>${headers.map(h => `<th>${escHtml(h)}</th>`).join("")}</tr>`;
    const bodyRows = rows.map(r => {
      if (r.section) {
        // Section header row — visually distinct
        return `<tr class="fin-section-row"><td colspan="${1 + headers.length}" style="text-align:left; padding-top: 14px; color: var(--accent); font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; font-size: 11px;">${escHtml(r.label)}</td></tr>`;
      }
      return `<tr><td style="text-align:left">${escHtml(r.label)}</td>${(r.values || []).map(v => `<td>${v == null ? "—" : fmtBig(v)}</td>`).join("")}</tr>`;
    }).join("");
    body.innerHTML = headerRow + bodyRows;
  }

  document.querySelectorAll("[data-fin-tab]").forEach(btn => {
    btn.onclick = () => {
      document.querySelectorAll("[data-fin-tab]").forEach(b => b.classList.toggle("active", b === btn));
      pickTab(btn.dataset.finTab);
    };
  });
  pickTab("income");
}

/* ════════════════════════════════════════════════════════════════════════
   Search + suggestions
   ════════════════════════════════════════════════════════════════════════ */

function setupSearch() {
  const input = $("tickerInput");
  const form  = $("searchForm");
  const dd    = $("searchDropdown");
  let timer = null;

  input.addEventListener("input", () => {
    clearTimeout(timer);
    const q = input.value.trim();
    if (!q) { dd.classList.add("hidden"); return; }
    timer = setTimeout(async () => {
      const results = await searchSuggestions(q);
      if (results.length === 0) { dd.classList.add("hidden"); return; }
      dd.innerHTML = results.slice(0, 8).map(r =>
        `<div class="search-result" data-ticker="${escHtml(r.symbol || r.ticker)}">
          <span class="search-result__ticker">${escHtml(r.symbol || r.ticker)}</span>
          <span class="search-result__name">${escHtml(r.name || r.shortname || '')}</span>
        </div>`
      ).join("");
      dd.classList.remove("hidden");
      dd.querySelectorAll(".search-result").forEach(el => {
        el.onclick = () => {
          input.value = el.dataset.ticker;
          dd.classList.add("hidden");
          submit();
        };
      });
    }, 180);
  });

  input.addEventListener("blur", () => setTimeout(() => dd.classList.add("hidden"), 200));

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    submit();
  });

  function submit() {
    const t = input.value.trim().toUpperCase();
    if (!t) return;
    // Cancel any pending debounced suggestion fetch — without this, a
    // setTimeout fired ~180ms after Enter would re-open the dropdown
    // on top of the freshly-loaded analysis.
    if (timer) { clearTimeout(timer); timer = null; }
    dd.classList.add("hidden");
    input.blur();
    const params = {};
    const yrs = $("advYrs").value;
    const s1  = $("advS1").value;
    const s2  = $("advS2").value;
    const tg  = $("advTg").value;
    if (yrs) params.years = yrs;
    if (s1)  params.growth1 = s1;
    if (s2)  params.growth2 = s2;
    if (tg)  params.terminal = tg;
    analyze(t, params);
  }
}

function setupAdvancedToggle() {
  const btn = $("advancedToggle");
  const panel = $("advancedPanel");
  btn.onclick = () => panel.classList.toggle("hidden");
}

function setupCopyButton() {
  const btn = $("copyBtn");
  const txt = $("copyBtnTxt");
  btn.onclick = () => {
    if (!_LAST_DATA) return;
    const d = _LAST_DATA;
    const summary = [
      `${d.company_name} (${d.ticker})`,
      `Price: ${fmtPrice(d.current_price)}  ·  VALUS Fair Value: ${fmtPrice(d.intrinsic_value)}`,
      `Margin of Safety: ${fmtPct(d.margin_of_safety)}`,
      `Verdict: ${d.priced_for?.label || ""}`,
      d.verdict_summary?.verdict || "",
    ].filter(Boolean).join("\n");
    navigator.clipboard.writeText(summary).then(() => {
      btn.classList.add("success");
      txt.textContent = "✓ Copied";
      setTimeout(() => { btn.classList.remove("success"); txt.textContent = "Copy summary"; }, 1500);
    });
  };
}

/* ════════════════════════════════════════════════════════════════════════
   Portfolio (localStorage-backed watchlist)
   ════════════════════════════════════════════════════════════════════════ */

const PF_KEY = "valus.portfolio.v1";

function pfRead() {
  try { return JSON.parse(localStorage.getItem(PF_KEY) || "[]"); }
  catch { return []; }
}
function pfWrite(items) {
  localStorage.setItem(PF_KEY, JSON.stringify(items));
  pfUpdateBadge();
}
function pfHas(ticker) {
  return pfRead().some(it => it.ticker === ticker);
}
function pfAdd(snap) {
  const items = pfRead();
  if (items.some(it => it.ticker === snap.ticker)) return;
  items.push({ ...snap, addedAt: Date.now() });
  pfWrite(items);
}
function pfRemove(ticker) {
  pfWrite(pfRead().filter(it => it.ticker !== ticker));
}
function pfUpdateBadge() {
  const n = pfRead().length;
  const el = $("portfolioCount");
  if (!el) return;
  if (n > 0) { el.textContent = n; el.hidden = false; }
  else       { el.hidden = true; }
}

function setupAddPortfolioButton() {
  const btn = $("addPortfolioBtn");
  const txt = $("addPortfolioTxt");
  if (!btn) return;
  btn.onclick = async () => {
    if (!_LAST_DATA) return;
    const t = _LAST_DATA.ticker;
    // Removing is allowed without auth (the user already added it pre-auth);
    // adding requires sign-in to associate the entry with an identity.
    if (pfHas(t)) {
      pfRemove(t);
      btn.classList.remove("starred");
      txt.textContent = "★ Add to Portfolio";
      return;
    }
    if (!(await requireAuthWithRedirectIntent("portfolio"))) return;
    pfAdd({
      ticker: t,
      name: _LAST_DATA.company_name || t,
      sector: _LAST_DATA.sector || "",
      price: _LAST_DATA.current_price,
      iv: _LAST_DATA.intrinsic_value,
      mos: _LAST_DATA.margin_of_safety,
      tier: _LAST_DATA.priced_for?.label || "",
    });
    btn.classList.add("starred");
    txt.textContent = "★ In Portfolio";
  };
}

function syncAddPortfolioButtonForCurrent() {
  const btn = $("addPortfolioBtn");
  const txt = $("addPortfolioTxt");
  if (!btn || !_LAST_DATA) return;
  if (pfHas(_LAST_DATA.ticker)) {
    btn.classList.add("starred");
    txt.textContent = "★ In Portfolio";
  } else {
    btn.classList.remove("starred");
    txt.textContent = "★ Add to Portfolio";
  }
}

// ── Portfolio page (full-screen view, replaces modal) ─────────────────
let _PF_SORT = "added";   // ticker | mos | added

function openPortfolioPage() {
  // Hide ALL other views — portfolio is its own dedicated page
  hideAllViews?.();
  $("results").classList.add("hidden");
  $("btcHero")?.classList.add("hidden");
  $("etfHero")?.classList.add("hidden");
  $("discoverPage")?.classList.add("hidden");
  document.querySelector(".hero")?.classList.add("hidden");
  $("loading")?.classList.add("hidden");
  $("error")?.classList.add("hidden");
  $("portfolioPage").classList.remove("hidden");
  // Restore the user's own-portfolio chrome (in case the page is currently
  // showing a shared view from a /?p=... deep link)
  _IS_SHARED_VIEW = false;
  $("pfSharedBanner")?.classList.add("hidden");
  $("pfPageTitle").textContent = "My Portfolio";
  $("pfPageTagline").textContent = "Real-time DCF tracking across your watchlist.";
  $("pfRefreshBtn").style.display = "";
  $("pfShareBtn").style.display = "";
  $("pfAllocationCard").style.display = "";
  renderPortfolioPage();
  setViewHash("portfolio");
  window.scrollTo({ top: 0, behavior: "instant" });
}
function closePortfolioPage() {
  $("portfolioPage").classList.add("hidden");
  // Restore the hero (search) and the last view, if any
  document.querySelector(".hero")?.classList.remove("hidden");
  if (_LAST_DATA && isBTCTicker(_LAST_DATA.ticker)) {
    $("btcHero").classList.remove("hidden");
  } else if (_LAST_DATA) {
    $("results").classList.remove("hidden");
  }
  setViewHash("");
}

const SECTOR_PALETTE = [
  "#5eead4", "#34d399", "#60a5fa", "#a78bfa",
  "#fbbf24", "#f472b6", "#f87171", "#fb923c",
];

function getSectorColor(idx) { return SECTOR_PALETTE[idx % SECTOR_PALETTE.length]; }

function renderPortfolioPage() {
  const itemsRaw = pfRead();
  const list  = $("portfolioList");
  const empty = $("portfolioEmpty");

  // Apply sort
  const items = [...itemsRaw];
  if (_PF_SORT === "ticker") {
    items.sort((a, b) => (a.ticker || "").localeCompare(b.ticker || ""));
  } else if (_PF_SORT === "mos") {
    items.sort((a, b) => (b.mos ?? -Infinity) - (a.mos ?? -Infinity));
  } else {
    // 'added' — most recent first
    items.sort((a, b) => (b.addedAt || 0) - (a.addedAt || 0));
  }

  // Empty state
  if (items.length === 0) {
    empty.classList.remove("hidden");
    list.innerHTML = "";
    $("pfAllocationCard").style.display = "none";
    $("pfCount").textContent = "0";
    $("pfAvgMos").textContent = "—";
    $("pfUnderCount").textContent = "0 / 0";
    $("pfBest").textContent = "—";
    return;
  }
  empty.classList.add("hidden");
  $("pfAllocationCard").style.display = "";

  // Render holdings
  list.innerHTML = items.map(it => {
    const mosClass = it.mos == null ? "neutral" : (it.mos > 5 ? "positive" : (it.mos < -5 ? "negative" : "neutral"));
    return `
      <div class="pf-item" data-pf-ticker="${escHtml(it.ticker)}">
        <span class="pf-item__ticker">${escHtml(it.ticker)}</span>
        <span class="pf-item__name">${escHtml(it.name || "")}</span>
        <span class="pf-item__price">${it.price != null ? fmtPrice(it.price) : "—"}</span>
        <span class="pf-item__mos ${mosClass}">${it.mos != null ? fmtPct(it.mos) : "—"}</span>
        <button class="pf-item__remove" data-pf-remove="${escHtml(it.ticker)}" aria-label="Remove">✕</button>
      </div>
    `;
  }).join("");

  list.querySelectorAll(".pf-item").forEach(row => {
    row.onclick = (e) => {
      if (e.target.closest("[data-pf-remove]")) return;
      closePortfolioPage();
      const t = row.dataset.pfTicker;
      $("tickerInput").value = t;
      analyze(t);
    };
  });
  list.querySelectorAll("[data-pf-remove]").forEach(btn => {
    btn.onclick = (e) => {
      e.stopPropagation();
      pfRemove(btn.dataset.pfRemove);
      renderPortfolioPage();
      syncAddPortfolioButtonForCurrent();
    };
  });

  // Summary metrics
  $("pfCount").textContent = items.length;
  const mosVals = items.filter(it => it.mos != null).map(it => it.mos);
  const avgMos = mosVals.length ? mosVals.reduce((a, b) => a + b, 0) / mosVals.length : null;
  const avgEl = $("pfAvgMos");
  avgEl.textContent = avgMos != null ? fmtPct(avgMos) : "—";
  avgEl.classList.toggle("text-positive", avgMos != null && avgMos > 5);
  avgEl.classList.toggle("text-negative", avgMos != null && avgMos < -5);

  const underCount = items.filter(it => it.mos != null && it.mos > 5).length;
  $("pfUnderCount").textContent = `${underCount} / ${items.length}`;

  const best = items.filter(it => it.mos != null).sort((a, b) => b.mos - a.mos)[0];
  $("pfBest").textContent = best ? `${best.ticker} ${fmtPct(best.mos)}` : "—";

  // Sector allocation
  const bySector = {};
  items.forEach(it => {
    const s = it.sector || "Other";
    bySector[s] = (bySector[s] || 0) + 1;
  });
  const sectors = Object.entries(bySector);
  const total = items.length;
  const bar = $("pfAllocationBar");
  const legend = $("pfAllocationLegend");
  bar.innerHTML = sectors.map(([s, n], idx) => {
    const pct = (n / total) * 100;
    const color = getSectorColor(idx);
    return `<div class="pf-alloc-segment" style="width:${pct}%; background:${color};" title="${escHtml(s)}: ${n}"></div>`;
  }).join("");
  legend.innerHTML = sectors.map(([s, n], idx) => {
    const color = getSectorColor(idx);
    const pct = (n / total) * 100;
    return `<span class="pf-legend-item"><span class="pf-legend-dot" style="background:${color};"></span>${escHtml(s)} · ${n} (${pct.toFixed(0)}%)</span>`;
  }).join("");
}

async function refreshPortfolioPrices() {
  const items = pfRead();
  if (items.length === 0) return;
  const refreshBtn = $("pfRefreshBtn");
  if (refreshBtn) refreshBtn.textContent = "↻ Refreshing…";

  const updated = await Promise.all(items.map(async it => {
    try {
      const res = await fetch(`/api/analyze?ticker=${encodeURIComponent(it.ticker)}`);
      const d = await res.json();
      if (d.error) return it;
      return {
        ...it,
        price: d.current_price,
        iv: d.intrinsic_value,
        mos: d.margin_of_safety,
        tier: d.priced_for?.label || it.tier,
        sector: d.sector || it.sector,
        name: d.company_name || it.name,
      };
    } catch { return it; }
  }));
  pfWrite(updated);
  renderPortfolioPage();
  if (refreshBtn) refreshBtn.textContent = "↻ Refresh prices";
}

function setupPortfolioPage() {
  const btn = $("portfolioBtn");
  if (btn) btn.onclick = openPortfolioPage;

  const back = $("pfBackBtn");
  if (back) back.onclick = closePortfolioPage;

  const refresh = $("pfRefreshBtn");
  if (refresh) refresh.onclick = refreshPortfolioPrices;

  const start = $("pfStartBtn");
  if (start) start.onclick = () => {
    closePortfolioPage();
    $("tickerInput").focus();
  };

  // Sort toggle
  document.querySelectorAll("[data-pf-sort]").forEach(btn => {
    btn.onclick = () => {
      _PF_SORT = btn.dataset.pfSort;
      document.querySelectorAll("[data-pf-sort]").forEach(b =>
        b.classList.toggle("active", b === btn)
      );
      renderPortfolioPage();
    };
  });
}

// Tier metadata for the focused glossary modal — single source of truth
const TIER_META = {
  distress:      { label: "Priced for Distress",      mos: "MOS > +50%",            color: "tier-positive",
                   desc: "Market is overly pessimistic. Trading well below fair value, often during macro fear or sector rotation. Meaningful upside if fundamentals stabilise." },
  deep_discount: { label: "Priced for Deep Discount", mos: "MOS +40% to +50%",      color: "tier-positive",
                   desc: "Significantly undervalued — strong signal if VALUS's growth assumptions hold. Worth a quality check to avoid value traps." },
  discount:      { label: "Priced for Discount",      mos: "MOS +15% to +40%",      color: "tier-positive",
                   desc: "Trading below fundamental value. Modest opportunity zone — model and analysts both see room above current price." },
  fair_value:    { label: "Priced for Fair Value",    mos: "MOS −10% to +15%",      color: "tier-info",
                   desc: "Market and VALUS aligned. The price reflects fundamentals as the model sees them — no clear edge in either direction." },
  growth:        { label: "Priced for Growth",        mos: "MOS −10% to −25%",      color: "tier-warning",
                   desc: "Market is paying a growth premium — modestly overvalued by VALUS. Acceptable if you believe execution will deliver above-base growth." },
  excellence:    { label: "Priced for Excellence",    mos: "MOS −25% to −50%",      color: "tier-warning",
                   desc: "Market expects flawless execution. Premium pricing requires growth, margins, and capital discipline to all work together — limited margin of error if any leg slips." },
  miracle:       { label: "Priced for Miracle",       mos: "MOS < −50% or speculative growth", color: "tier-negative",
                   desc: "Market is pricing in extraordinary outcomes that fewer than 1% of public companies achieve over a decade. Speculative — driven by momentum, not fundamentals." },
};

function setupTierGlossary() {
  const tierBtn = $("vTierBadge");
  const modal   = $("tierGlossaryModal");
  if (tierBtn && modal) {
    tierBtn.onclick = () => openTierModal();
  }
  // Expand/collapse "see all 7 tiers"
  const expandBtn = $("tgExpandBtn");
  const fullList  = $("tgFullList");
  if (expandBtn && fullList) {
    expandBtn.onclick = () => {
      const expanded = expandBtn.getAttribute("aria-expanded") === "true";
      const next = !expanded;
      expandBtn.setAttribute("aria-expanded", String(next));
      fullList.classList.toggle("hidden", !next);
      expandBtn.querySelector("span").textContent =
        next ? "Hide other tiers" : "See all 7 verdict tiers";
    };
  }
}

function openTierModal() {
  if (!_LAST_DATA) return;
  const pf = _LAST_DATA.priced_for || {};
  const tier = pf.tier;
  const meta = TIER_META[tier];
  const currentEl = $("tgCurrent");
  const fullList  = $("tgFullList");
  const expandBtn = $("tgExpandBtn");

  if (!meta || !currentEl) return;

  // Reset full list to hidden each time
  fullList.classList.add("hidden");
  if (expandBtn) {
    expandBtn.setAttribute("aria-expanded", "false");
    expandBtn.querySelector("span").textContent = "See all 7 verdict tiers";
  }

  const mos = _LAST_DATA.margin_of_safety;
  const iv  = _LAST_DATA.intrinsic_value;
  const px  = _LAST_DATA.current_price;
  const ig  = _LAST_DATA.implied_growth_pct;
  const ceil = _LAST_DATA.sector_growth_ceiling_pct;
  const ceilLbl = _LAST_DATA.sector_growth_ceiling_label;

  // Inline numbers tailored to this stock
  const numsHtml = `
    <div class="tg-current__numbers">
      <div class="tg-num">
        <span class="tg-num__label">Margin of safety</span>
        <span class="tg-num__value">${mos != null ? fmtPct(mos) : "—"}</span>
      </div>
      <div class="tg-num">
        <span class="tg-num__label">Fair value</span>
        <span class="tg-num__value">${iv != null ? fmtPrice(iv) : "—"}</span>
      </div>
      <div class="tg-num">
        <span class="tg-num__label">Sector ceiling</span>
        <span class="tg-num__value">${ceil != null ? `${ceil}% (${escHtml(ceilLbl || "—")})` : "—"}</span>
      </div>
    </div>
  `;

  // Mark up the focused current-tier card with the right tier color class
  currentEl.className = `tg-current ${meta.color}`;
  currentEl.innerHTML = `
    <div class="tg-current__header">
      <span class="tg-current__dot"></span>
      <span class="tg-current__label">${escHtml(meta.label)}</span>
      <span class="tg-current__mos">${escHtml(meta.mos)}</span>
    </div>
    <div class="tg-current__desc">${escHtml(meta.desc)}</div>
    ${numsHtml}
  `;

  // Highlight the matching row in the full list
  fullList.querySelectorAll(".tg-row").forEach(row => {
    row.classList.toggle("is-current", row.dataset.tier === tier);
  });

  $("tierGlossaryModal").classList.remove("hidden");
}

function closeAllModals() {
  document.querySelectorAll(".modal").forEach(m => m.classList.add("hidden"));
}

function setupModalDismiss() {
  // Universal close handler — works for any modal with [data-modal-close]
  document.addEventListener("click", (e) => {
    if (e.target.matches("[data-modal-close]")) {
      closeAllModals();
    }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeAllModals();
  });
}

/* ════════════════════════════════════════════════════════════════════════
   Boot
   ════════════════════════════════════════════════════════════════════════ */

// ── Custom DCF — live recompute from user-controlled sliders ──────────
function recomputeCustomIV(d, s1Pct, s2Pct, waccPct, tgPct) {
  const baseFcf = d.base_fcf || 0;
  if (!baseFcf) return null;
  const yrs = d.projection_years || 10;
  const stage1Years = Math.ceil(yrs / 2);

  const s1   = s1Pct  / 100;
  const s2   = s2Pct  / 100;
  const wacc = waccPct / 100;
  const tg   = tgPct  / 100;

  // Guard: WACC must exceed terminal growth for Gordon Growth stability
  if (wacc <= tg) return null;

  let totalPv = 0;
  let fcf = baseFcf;
  for (let y = 1; y <= yrs; y++) {
    const g = y <= stage1Years ? s1 : s2;
    fcf = fcf * (1 + g);
    totalPv += fcf / Math.pow(1 + wacc, y);
  }
  // Gordon Growth terminal value
  const terminalFcf = fcf * (1 + tg);
  const tv = terminalFcf / (wacc - tg);
  const tvPv = tv / Math.pow(1 + wacc, yrs);

  const enterprise = totalPv + tvPv;
  const netDebt    = d.net_debt || 0;
  const equity     = enterprise - netDebt;
  const shares     = d.shares_outstanding || 1;
  return equity / shares;
}

function updateSliderFill(slider) {
  const min = parseFloat(slider.min);
  const max = parseFloat(slider.max);
  const val = parseFloat(slider.value);
  const pct = ((val - min) / (max - min)) * 100;
  slider.style.setProperty("--slider-pct", `${pct}%`);
}

function setupCustomDCFSliders() {
  const ids = ["cdS1", "cdS2", "cdWacc", "cdTg"];
  const sliders = ids.map($).filter(Boolean);
  if (sliders.length !== 4) return;

  const reset = () => {
    if (!_LAST_DATA) return;
    const d = _LAST_DATA;
    // Prefer the exact base-scenario assumptions the model used so the
    // "Your Fair Value" lines up with the displayed Base case before any
    // user interaction. Fall back to top-level DCF inputs, then to defaults.
    const base = (d.scenarios && d.scenarios.base) || {};
    $("cdS1").value   = base.s1   ?? d.stage1_growth   ?? 10;
    $("cdS2").value   = d.stage2_growth   ?? 6;
    $("cdWacc").value = base.wacc ?? d.wacc            ?? 9;
    $("cdTg").value   = d.terminal_growth ?? 2.5;
    sliders.forEach(updateSliderFill);
    update();
  };

  const update = () => {
    if (!_LAST_DATA) return;
    const d = _LAST_DATA;
    const s1   = parseFloat($("cdS1").value);
    const s2   = parseFloat($("cdS2").value);
    const wacc = parseFloat($("cdWacc").value);
    const tg   = parseFloat($("cdTg").value);

    $("cdS1Val").textContent   = `${fmt(s1, 1)}%`;
    $("cdS2Val").textContent   = `${fmt(s2, 1)}%`;
    $("cdWaccVal").textContent = `${fmt(wacc, 1)}%`;
    $("cdTgVal").textContent   = `${fmt(tg, 1)}%`;

    const valusIv = d.intrinsic_value;
    $("cdValusIV").textContent = valusIv != null ? fmtPrice(valusIv) : "—";

    const yourIv = recomputeCustomIV(d, s1, s2, wacc, tg);
    if (yourIv != null && isFinite(yourIv) && yourIv > 0) {
      $("cdYourIV").textContent = fmtPrice(yourIv);
      const deltaEl = $("cdYourDelta");
      if (d.current_price && d.current_price > 0) {
        const newMos = (yourIv - d.current_price) / d.current_price * 100;
        deltaEl.textContent = `${fmtPct(newMos)} vs current price`;
        deltaEl.classList.toggle("positive", newMos > 0);
        deltaEl.classList.toggle("negative", newMos < 0);
      } else {
        deltaEl.textContent = "";
      }
    } else {
      $("cdYourIV").textContent = "—";
      $("cdYourDelta").textContent = "";
    }
  };

  // Bind input handlers
  sliders.forEach(s => {
    s.oninput = () => { updateSliderFill(s); update(); };
  });
  const resetBtn = $("cdResetBtn");
  if (resetBtn) resetBtn.onclick = reset;

  // Expose for renderResults to call after analysis
  window._cdReset = reset;
}

// ── Low confidence explainer ───────────────────────────────────────────
function setupLowConfExplainer() {
  // Delegated click — the chip is rendered dynamically inside the MOS pill
  document.addEventListener("click", (e) => {
    if (e.target.closest(".mos-confidence-chip")) {
      $("lowConfModal").classList.remove("hidden");
    }
  });
}

// ── Auth state machine ─────────────────────────────────────────────────
// _ME holds the OAuth-derived identity ({sub, name, picture, email})
// or null when signed out.  _AUTH_CONFIGURED is false when the deploy
// has no GOOGLE_CLIENT_ID — in that case the sign-in flow is disabled
// gracefully (modal explains, no broken redirect).
const VALUS_USER_KEY = "valus.user.v1";   // legacy soft-token, kept for claim flow
let _ME = null;
let _AUTH_CONFIGURED = false;
let _PENDING_INTENT = null;   // string set when an action prompted sign-in;
                              // re-fired on auth_ok=1 redirect.

function getValusUser() {
  try {
    const raw = localStorage.getItem(VALUS_USER_KEY);
    if (raw) return JSON.parse(raw);
  } catch {}
  return null;
}
function ensureValusUser(name) {
  let u = getValusUser();
  if (!u) {
    const tok = "u_" + Array.from(crypto.getRandomValues(new Uint8Array(12)))
      .map(b => b.toString(16).padStart(2, "0")).join("");
    u = { token: tok, name: name || "" };
  }
  if (name) u.name = name;
  localStorage.setItem(VALUS_USER_KEY, JSON.stringify(u));
  return u;
}

async function refreshMe() {
  try {
    const r = await fetch("/api/me", { credentials: "same-origin" });
    const data = await r.json();
    _ME = data.user || null;
    _AUTH_CONFIGURED = !!data.auth_configured;
  } catch {
    _ME = null;
    _AUTH_CONFIGURED = false;
  }
  updateAuthControl();
  // First-time sign-in: claim any legacy soft-token entries
  if (_ME && !sessionStorage.getItem("valus.claimed")) {
    const legacy = getValusUser();
    if (legacy?.token) {
      try {
        await fetch("/api/leaderboard/claim", {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ user_token: legacy.token }),
        });
      } catch {}
    }
    sessionStorage.setItem("valus.claimed", "1");
  }
}

function updateAuthControl() {
  const signInBtn = $("signInBtn");
  const avatar    = $("authAvatar");
  const avatarImg = $("authAvatarImg");
  const avatarTxt = $("authAvatarName");
  const menu      = $("authMenu");
  const email     = $("authMenuEmail");
  if (!signInBtn || !avatar) return;
  if (_ME) {
    signInBtn.classList.add("hidden");
    avatar.classList.remove("hidden");
    if (_ME.picture) {
      avatarImg.src = _ME.picture;
      avatarImg.style.display = "";
    } else {
      avatarImg.style.display = "none";
    }
    avatarTxt.textContent = _ME.name || "Account";
    if (email) email.textContent = _ME.email || "";
  } else {
    signInBtn.classList.remove("hidden");
    avatar.classList.add("hidden");
    if (menu) menu.classList.add("hidden");
  }
}

// Gate a protected action on auth.  If signed in, returns true; else
// opens the sign-in modal with an intent-specific explainer and stores
// the intent so it can fire after the OAuth roundtrip.
async function requireAuth(intent) {
  if (_ME) return true;
  _PENDING_INTENT = intent;
  showSignInModal(intent);
  return false;
}

const INTENT_COPY = {
  portfolio: "Sign in to save your portfolio across devices.",
  publish:   "Sign in to publish your portfolio to the public leaderboard.",
  default:   "Sign in to continue.",
};

function showSignInModal(intent) {
  const modal = $("signInModal");
  if (!modal) return;
  $("signInIntent").textContent = INTENT_COPY[intent] || INTENT_COPY.default;
  // If OAuth isn't configured on this deploy, dim the Google button and
  // surface a clear explainer instead of a confusing redirect.
  const notConfigured = $("signInNotConfigured");
  const googleBtn = $("googleSignInBtn");
  if (_AUTH_CONFIGURED) {
    notConfigured.classList.add("hidden");
    googleBtn.disabled = false;
    googleBtn.style.opacity = "1";
  } else {
    notConfigured.classList.remove("hidden");
    googleBtn.disabled = true;
    googleBtn.style.opacity = "0.4";
  }
  modal.classList.remove("hidden");
}

function setupAuthControl() {
  const signInBtn  = $("signInBtn");
  const avatar     = $("authAvatar");
  const menu       = $("authMenu");
  const signOutBtn = $("authSignOutBtn");
  const googleBtn  = $("googleSignInBtn");

  if (signInBtn) signInBtn.onclick = () => showSignInModal("default");
  if (googleBtn) googleBtn.onclick = () => {
    if (!_AUTH_CONFIGURED) return;
    // Preserve the current URL — including the hash — so the user lands
    // back on the same page after the Google round-trip.  Without the
    // hash, signing in from the Leaderboard or Discover page would dump
    // the user on the search hero.
    const next = encodeURIComponent(
      window.location.pathname + window.location.search + window.location.hash
    );
    window.location.href = `/auth/login?next=${next}`;
  };
  if (avatar && menu) {
    avatar.onclick = (e) => {
      e.stopPropagation();
      menu.classList.toggle("hidden");
    };
    document.addEventListener("click", (e) => {
      if (!menu.contains(e.target) && !avatar.contains(e.target)) {
        menu.classList.add("hidden");
      }
    });
  }
  if (signOutBtn) signOutBtn.onclick = async () => {
    try {
      await fetch("/auth/logout", { method: "POST", credentials: "same-origin" });
    } catch {}
    sessionStorage.removeItem("valus.claimed");
    _ME = null;
    updateAuthControl();
  };

  // After OAuth roundtrip, URL has ?auth_ok=1 — replay any pending intent
  if (window.location.search.includes("auth_ok=1")) {
    // Strip the marker from the URL but keep the rest
    const url = new URL(window.location.href);
    url.searchParams.delete("auth_ok");
    window.history.replaceState({}, "", url);
    // The intent (if any) was preserved in sessionStorage by the modal flow
    const pendingIntent = sessionStorage.getItem("valus.pending_intent");
    sessionStorage.removeItem("valus.pending_intent");
    setTimeout(() => {
      if (pendingIntent === "publish") {
        // Open the publish modal automatically
        $("pfPublishBtn")?.click();
      } else if (pendingIntent === "portfolio") {
        // Re-trigger the add-to-portfolio click from where they were
        $("addPortfolioBtn")?.click();
      }
    }, 400);
  }
}

// Wrap requireAuth to also persist intent across the OAuth redirect
async function requireAuthWithRedirectIntent(intent) {
  if (_ME) return true;
  sessionStorage.setItem("valus.pending_intent", intent);
  return requireAuth(intent);
}

// ── Submit-to-leaderboard modal ────────────────────────────────────────
function setupSubmitToLeaderboard() {
  const publishBtn = $("pfPublishBtn");
  const submitBtn  = $("lbSubmitBtn");
  const modal      = $("submitLbModal");
  const nameEl     = $("lbName");
  const noteEl     = $("lbNote");
  if (!publishBtn || !modal) return;

  publishBtn.onclick = async () => {
    const items = pfRead();
    if (items.length === 0) {
      publishBtn.textContent = "Add stocks first ★";
      setTimeout(() => { publishBtn.textContent = "🏆 Publish"; }, 1500);
      return;
    }
    // Gate on real auth — sign-in modal is shown if not signed in.
    if (!(await requireAuthWithRedirectIntent("publish"))) return;
    // Pre-populate display name from the OAuth identity
    if (_ME?.name) nameEl.value = _ME.name;
    modal.classList.remove("hidden");
    setTimeout(() => nameEl.focus(), 50);
  };

  submitBtn.onclick = async () => {
    const name = nameEl.value.trim();
    if (!name) {
      nameEl.style.borderColor = "var(--negative)";
      setTimeout(() => { nameEl.style.borderColor = ""; }, 1200);
      return;
    }
    const items = pfRead();
    const tickers = items.map(it => it.ticker);
    const note = noteEl.value.trim();
    // Pass the legacy soft-token so the backend can dedupe a user who
    // had a pre-auth submission under that token.
    const legacy = getValusUser();

    submitBtn.textContent = "Publishing…";
    submitBtn.disabled = true;
    try {
      const res = await fetch("/api/leaderboard/submit", {
        method: "POST",
        credentials: "same-origin",   // send the auth cookie
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name, tickers, note,
          legacy_user_token: legacy?.token || null,
        }),
      });
      const data = await res.json();
      if (res.status === 401) {
        // Session expired between modal-open and submit; re-prompt.
        modal.classList.add("hidden");
        submitBtn.textContent = "Publish";
        submitBtn.disabled = false;
        showSignInModal("publish");
        return;
      }
      if (data.error) throw new Error(data.error);
      submitBtn.textContent = "✓ Published";
      setTimeout(() => {
        modal.classList.add("hidden");
        submitBtn.textContent = "Publish";
        submitBtn.disabled = false;
        openLeaderboardPage();
      }, 700);
    } catch (e) {
      submitBtn.textContent = "Try again";
      submitBtn.disabled = false;
      setTimeout(() => { submitBtn.textContent = "Publish"; }, 1200);
    }
  };
}

// ── Leaderboard page ───────────────────────────────────────────────────
let _LB_SORT = "avg_mos";
let _LB_POLL_TIMER = null;

async function openLeaderboardPage() {
  hideAllViews();
  $("leaderboardPage").classList.remove("hidden");
  setViewHash("leaderboard");
  window.scrollTo({ top: 0, behavior: "instant" });
  // Reset seen-IDs and fingerprint so first render shows everything
  _LB_SEEN_IDS = new Set();
  _LB_LAST_FINGERPRINT = "";
  await loadLeaderboard();
  // Light polling: refresh every 20s while on the leaderboard page so
  // new submissions from other users automatically appear.
  if (_LB_POLL_TIMER) clearInterval(_LB_POLL_TIMER);
  _LB_POLL_TIMER = setInterval(() => {
    if (!$("leaderboardPage").classList.contains("hidden")) {
      loadLeaderboard(false);   // silent — no spinner flash
    } else {
      clearInterval(_LB_POLL_TIMER);
      _LB_POLL_TIMER = null;
    }
  }, 20000);
}
function closeLeaderboardPage() {
  $("leaderboardPage").classList.add("hidden");
  document.querySelector(".hero")?.classList.remove("hidden");
  if (_LB_POLL_TIMER) {
    clearInterval(_LB_POLL_TIMER);
    _LB_POLL_TIMER = null;
  }
  setViewHash("");
}

let _LB_LAST_FINGERPRINT = "";

async function loadLeaderboard(showSpinner = true) {
  if (showSpinner) {
    $("lbLoading").classList.remove("hidden");
    $("lbEmpty").classList.add("hidden");
    $("lbList").innerHTML = "";
  }
  try {
    const res = await fetch(`/api/leaderboard?sort=${encodeURIComponent(_LB_SORT)}`);
    const data = await res.json();
    const items = data.items || [];
    // Skip re-render if nothing changed (preserves scroll position on polls).
    // Sort key must be part of the fingerprint — without it, switching from
    // "Avg MOS" to "Most Recent" produces the same id+avg+count tuple in a
    // different order, the early-return fires, but lbList was already
    // cleared by the spinner branch above → user sees an empty leaderboard.
    const fp = JSON.stringify({
      sort: _LB_SORT,
      items: items.map(e => [e.id, e.avg_mos, e.ticker_count]),
    });
    if (fp === _LB_LAST_FINGERPRINT) return;
    _LB_LAST_FINGERPRINT = fp;
    renderLeaderboard(items);
  } catch {
    renderLeaderboard([]);
  } finally {
    $("lbLoading").classList.add("hidden");
  }
}

// Track which entries have been seen so newly-arrived submissions
// can briefly highlight when they show up via polling.
let _LB_SEEN_IDS = new Set();

function renderLeaderboard(items) {
  const list  = $("lbList");
  const empty = $("lbEmpty");
  if (items.length === 0) {
    list.innerHTML = "";
    empty.classList.remove("hidden");
    return;
  }
  empty.classList.add("hidden");

  // Track current user — prefer the OAuth `sub` (rock-solid identity);
  // fall back to a name match for legacy entries that haven't been claimed.
  const mySub  = _ME?.sub || null;
  const myName = (_ME?.name || "").trim().toLowerCase();

  // Diff: which IDs are NEW since last render?
  const newIds = new Set();
  items.forEach(it => {
    if (!_LB_SEEN_IDS.has(it.id)) newIds.add(it.id);
  });
  // Don't flash brand-new on first ever render
  const isFirstRender = _LB_SEEN_IDS.size === 0;
  items.forEach(it => _LB_SEEN_IDS.add(it.id));

  list.innerHTML = items.map((entry, idx) => {
    const avg = entry.avg_mos;
    const avgClass = avg == null ? "" : (avg > 5 ? "positive" : (avg < -5 ? "negative" : ""));
    const tickersStr = (entry.tickers || []).slice(0, 6).join(" · ") +
                       (entry.tickers.length > 6 ? ` +${entry.tickers.length - 6} more` : "");
    const isMine = (mySub && entry.user_sub === mySub) ||
                   (myName && entry.name && entry.name.trim().toLowerCase() === myName);
    const isFresh = !isFirstRender && newIds.has(entry.id);
    return `
      <div class="lb-row ${isMine ? 'is-mine' : ''} ${isFresh ? 'is-fresh' : ''}"
           data-lb-id="${escHtml(entry.id)}"
           data-lb-tickers="${escHtml((entry.tickers || []).join(','))}">
        <span class="lb-rank">${idx + 1}</span>
        <div class="lb-info">
          <div class="lb-name">${escHtml(entry.name)}${isMine ? '<span class="lb-name__mine">MINE</span>' : ''}</div>
          <div class="lb-tickers">${escHtml(tickersStr)}</div>
          ${entry.note ? `<div class="lb-note">"${escHtml(entry.note)}"</div>` : ""}
        </div>
        <div class="lb-stat">
          <span class="lb-stat__label">Avg MOS</span>
          <span class="lb-stat__value ${avgClass}">${avg != null ? fmtPct(avg) : "—"}</span>
        </div>
        <div class="lb-stat">
          <span class="lb-stat__label">Underv.</span>
          <span class="lb-stat__value">${entry.undervalued_count}/${entry.ticker_count}</span>
        </div>
        <div class="lb-stat">
          <span class="lb-stat__label">Picks</span>
          <span class="lb-stat__value">${entry.ticker_count}</span>
        </div>
      </div>
    `;
  }).join("");

  list.querySelectorAll(".lb-row").forEach(row => {
    row.onclick = () => {
      const tickers = row.dataset.lbTickers.split(",").filter(Boolean);
      if (tickers.length === 0) return;
      closeLeaderboardPage();
      openSharedPortfolio(tickers);
    };
  });

  // Scroll to user's own row if it's not in viewport
  const mine = list.querySelector(".lb-row.is-mine");
  if (mine && !isFirstRender) {
    // Only scroll on first render or after publish — not on every poll
  } else if (mine && isFirstRender) {
    setTimeout(() => mine.scrollIntoView({ behavior: "smooth", block: "center" }), 400);
  }
}

function setupLeaderboard() {
  // Header Leaderboard button removed; the entry point is now the
  // 🏆 Leaderboard → button on the Portfolio page.  Keep null-safe wiring
  // in case the header button gets restored later.
  const btn = $("leaderboardBtn");
  if (btn) btn.onclick = openLeaderboardPage;
  $("pfViewLeaderboardBtn")?.addEventListener("click", openLeaderboardPage);
  // Leaderboard "Back" returns to the Portfolio page (not the search hero)
  // so the user lands where they entered.
  $("lbBackBtn")?.addEventListener("click", () => {
    closeLeaderboardPage();
    openPortfolioPage();
  });
  $("lbRefreshBtn")?.addEventListener("click", loadLeaderboard);
  // Restore the persisted sort (sessionStorage) so refresh keeps the user's
  // current filter — defaults to avg_mos otherwise.
  try {
    const saved = sessionStorage.getItem("valus.lb.sort");
    if (saved && ["avg_mos", "recent", "size"].includes(saved)) {
      _LB_SORT = saved;
      document.querySelectorAll("[data-lb-sort]").forEach(x =>
        x.classList.toggle("active", x.dataset.lbSort === saved));
    }
  } catch (e) {}
  document.querySelectorAll("[data-lb-sort]").forEach(b => {
    b.onclick = () => {
      _LB_SORT = b.dataset.lbSort;
      try { sessionStorage.setItem("valus.lb.sort", _LB_SORT); } catch (e) {}
      document.querySelectorAll("[data-lb-sort]").forEach(x =>
        x.classList.toggle("active", x === b));
      loadLeaderboard();
    };
  });
}

// ── Discovery page ────────────────────────────────────────────────────
let _DISC_DATA = [];
let _DISC_TIER_FILTER = "all";
let _DISC_SORT = "mos";

function tierBucket(tier) {
  if (!tier) return "neutral";
  if (["distress", "deep_discount", "discount", "strategic_discount"].includes(tier)) return "undervalued";
  if (tier === "fair_value") return "fair";
  if (["growth", "excellence"].includes(tier)) return "overvalued";
  if (tier === "miracle") return "speculative";
  return "neutral";
}

function tierColor(tier) {
  const bucket = tierBucket(tier);
  const map = {
    undervalued: { accent: "#34d399", strong: "rgba(52,211,153,0.35)", glow: "rgba(52,211,153,0.30)" },
    fair:        { accent: "#60a5fa", strong: "rgba(96,165,250,0.35)", glow: "rgba(96,165,250,0.25)" },
    overvalued:  { accent: "#fbbf24", strong: "rgba(251,191,36,0.35)", glow: "rgba(251,191,36,0.25)" },
    speculative: { accent: "#f87171", strong: "rgba(248,113,113,0.35)", glow: "rgba(248,113,113,0.30)" },
    neutral:     { accent: "#6b7382", strong: "rgba(107,115,130,0.35)", glow: "rgba(107,115,130,0.20)" },
  };
  return map[bucket] || map.neutral;
}

// Live-refresh interval handles for the heatmap.  Set in openDiscoverPage,
// cleared in closeDiscoverPage so polling stops when the user leaves.
let _DISC_PRICE_TIMER = null;        // 30s fast-tier price ticks
let _DISC_DEEP_TIMER  = null;        // 60min deep-tier full /api/discover refresh
let _DISC_PRICES_LAST_UPDATE = 0;   // for the "Prices: Xs ago" badge

// localStorage cache for the Discover heatmap snapshot.  Renders previous
// snapshot instantly on repeat visits — bypasses the Vercel cold-lambda
// problem where every fresh function instance starts with an empty
// _ANALYZE_CACHE and has to refetch all 109 tickers from yfinance.
//
// TTL is generous (6h) because: (a) backend stale-while-revalidate will
// swap in fresh values within seconds of the page load anyway, and
// (b) DCF outputs don't move much hour-to-hour — only price (which is
// updated by the 30s /api/quote polling on top of this).
const _DISC_LS_KEY = "valus.discover.snapshot.v1";
const _DISC_LS_TTL_MS = 6 * 60 * 60 * 1000;

function loadDiscoverFromLocalStorage() {
  try {
    const raw = localStorage.getItem(_DISC_LS_KEY);
    if (!raw) return null;
    const obj = JSON.parse(raw);
    if (!obj || !obj.ts || !Array.isArray(obj.items)) return null;
    if (Date.now() - obj.ts > _DISC_LS_TTL_MS) return null;
    return obj.items;
  } catch (e) { return null; }
}

function saveDiscoverToLocalStorage(items) {
  try {
    localStorage.setItem(_DISC_LS_KEY, JSON.stringify({ ts: Date.now(), items }));
  } catch (e) { /* quota exceeded — non-fatal */ }
}

async function openDiscoverPage() {
  hideAllViews();
  $("discoverPage").classList.remove("hidden");
  setViewHash("discover");
  window.scrollTo({ top: 0, behavior: "instant" });
  // Try localStorage first — instant render of last snapshot if any.
  // Subsequent loadDiscover() will swap in fresh data when it returns.
  if (_DISC_DATA.length === 0) {
    const cached = loadDiscoverFromLocalStorage();
    if (cached && cached.length > 0) {
      _DISC_DATA = cached;
      renderDiscover();   // sub-50ms render, no skeleton flash
    }
    // Kick off the network fetch regardless — it'll repaint when fresh
    // data arrives.  No await here so the UI doesn't block.
    loadDiscover();
  } else {
    renderDiscover();
  }
  // Fast tier: poll /api/quote every 30s while the heatmap is open.
  if (_DISC_PRICE_TIMER) clearInterval(_DISC_PRICE_TIMER);
  _DISC_PRICE_TIMER = setInterval(refreshDiscoverPrices, 30000);
  // Deep tier: full /api/discover re-fetch every hour to pick up the
  // hourly cron's fresh DCF + news evaluation.
  if (_DISC_DEEP_TIMER) clearInterval(_DISC_DEEP_TIMER);
  _DISC_DEEP_TIMER = setInterval(loadDiscover, 60 * 60 * 1000);
  // Update the freshness badge once a second so "Prices: Xs ago" ticks.
  startFreshnessTicker();
}

function closeDiscoverPage() {
  $("discoverPage").classList.add("hidden");
  document.querySelector(".hero")?.classList.remove("hidden");
  if (_DISC_PRICE_TIMER) { clearInterval(_DISC_PRICE_TIMER); _DISC_PRICE_TIMER = null; }
  if (_DISC_DEEP_TIMER)  { clearInterval(_DISC_DEEP_TIMER);  _DISC_DEEP_TIMER  = null; }
  if (_FRESHNESS_TICKER) { clearInterval(_FRESHNESS_TICKER); _FRESHNESS_TICKER = null; }
  setViewHash("");
}

// Static mirror of DISCOVERY_TICKERS in app.py used to seed skeleton cells
// while /api/discover is in flight.  Order roughly matches the backend list
// so the skeleton layout matches the post-load layout closely enough that
// cells don't visibly jump.  Out-of-sync drift is harmless — the swap is
// based on per-cell ticker keys, not position.
const _DISCOVERY_TICKERS_HINT = [
  "AAPL","MSFT","GOOGL","GOOG","AMZN","META","NVDA","TSLA",
  "ORCL","CRM","ADBE","AMD","INTC","AVGO","QCOM","TXN",
  "MU","ARM","NOW","SNOW","PLTR","PANW","CRWD",
  "ADI","MCHP","ON","AMAT","KLAC","LRCX",
  "JPM","BAC","WFC","GS","MS","BRK-B","V","MA","AXP",
  "SCHW","BLK","SPGI","COIN","HOOD",
  "JNJ","UNH","LLY","PFE","ABBV","MRK","TMO","ABT","DHR",
  "ISRG","VRTX","REGN",
  "WMT","HD","MCD","NKE","KO","PEP","SBUX","COST","TGT",
  "LULU","CMG","ABNB","BKNG",
  "F","GM","UBER",
  "XOM","CVX","COP","OXY","FCX","LIN",
  "CAT","BA","GE","UNP","RTX","HON","LMT","DE",
  "NOC","GD","LHX","HII",
  "DIS","NFLX","CMCSA","T","VZ","TMUS","SPOT",
  "AMT","PLD","EQIX","NEE","DUK","CEG","VST",
  "MP","LEU","BWXT",
  "JOBY","ACHR","RKLB",
];

// Renders ~109 placeholder skeleton cells the moment Discover opens, so the
// user sees scaffolding instantly instead of a blank screen.  Cells fill
// in as /api/discover returns; layout doesn't reflow because skeletons
// share the same structure as real cells.
function renderDiscoverSkeleton() {
  const grid = $("discGrid");
  if (!grid) return;
  grid.classList.remove("disc-grid--treemap");
  // Build minimum-viable cells that take the same shape as the real ones.
  grid.innerHTML = _DISCOVERY_TICKERS_HINT.map(t => `
    <div class="disc-cell disc-cell--skeleton" data-disc-ticker="${escHtml(t)}">
      <span class="disc-cell__shimmer"></span>
      <div class="disc-cell__head">
        <span class="disc-cell__ticker">${escHtml(t)}</span>
        <span class="disc-cell__mos">—</span>
      </div>
      <div class="disc-cell__name">&nbsp;</div>
      <div class="disc-cell__tier">&nbsp;</div>
      <div class="disc-cell__bar"><div class="disc-cell__bar-fill" style="width:0%"></div></div>
    </div>
  `).join("");
}

async function loadDiscover(opts = {}) {
  const { fresh = false } = opts;
  // Show skeleton scaffolding immediately if the grid is empty.  When the
  // user reopens Discover with cached data, we skip skeleton and let the
  // existing cells stay visible until the new fetch swaps in.
  if (_DISC_DATA.length === 0) {
    renderDiscoverSkeleton();
  }
  $("discLoading").classList.add("hidden");   // skeleton replaces the spinner
  let failed = false;
  try {
    const url = fresh ? "/api/discover?fresh=true" : "/api/discover";
    const res = await fetch(url);
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    _DISC_DATA = data.items || [];
    // Persist the latest snapshot so the next page open renders instantly
    // even if Vercel spins up a fresh cold lambda.
    if (_DISC_DATA.length > 0) saveDiscoverToLocalStorage(_DISC_DATA);
  } catch {
    if (_DISC_DATA.length === 0) {
      _DISC_DATA = [];
    }
    failed = true;
  }
  renderDiscover(failed);
}

function renderDiscover(failed = false) {
  const grid = $("discGrid");
  if (!grid) return;
  if (_DISC_DATA.length === 0) {
    grid.innerHTML = `
      <div class="disc-empty" style="grid-column:1/-1;text-align:center;padding:48px 16px;color:var(--text-muted,#888);">
        <div style="font-size:32px;margin-bottom:12px;">${failed ? "⚠" : "✦"}</div>
        <div style="font-weight:600;margin-bottom:6px;">
          ${failed ? "Couldn't load Discover" : "No undervalued ideas surfaced"}
        </div>
        <div style="font-size:14px;">
          ${failed ? "Try refresh in a moment — Yahoo may be rate-limiting." : "Check back after the next discovery sweep."}
        </div>
        ${failed ? '<button onclick="loadDiscover()" style="margin-top:16px;padding:8px 20px;border:1px solid currentColor;background:transparent;color:inherit;cursor:pointer;border-radius:6px;">Retry</button>' : ""}
      </div>`;
    return;
  }
  // Filter out extreme-MOS outliers from sorting comparisons (BRK-B share-class
  // quirk gives +19,661% which would always dominate the top spot).  We still
  // show them, just don't let them control the order.
  let items = [..._DISC_DATA];
  if (_DISC_TIER_FILTER !== "all") {
    items = items.filter(it => tierBucket(it.tier) === _DISC_TIER_FILTER);
  }
  function sortMos(it) {
    if (it.extreme || it.mos == null) return -Infinity;
    return it.mos;
  }
  if (_DISC_SORT === "mos") {
    items.sort((a, b) => sortMos(b) - sortMos(a));
  } else if (_DISC_SORT === "ticker") {
    items.sort((a, b) => (a.ticker || "").localeCompare(b.ticker || ""));
  } else if (_DISC_SORT === "sector") {
    items.sort((a, b) => (a.sector || "").localeCompare(b.sector || ""));
  }

  function ageTxt(ageS) {
    if (ageS == null) return "";
    if (ageS < 30) return "now";
    if (ageS < 60) return `${Math.round(ageS)}s`;
    if (ageS < 3600) return `${Math.round(ageS/60)}m ago`;
    return `${Math.round(ageS/3600)}h ago`;
  }

  // Renders a single ticker cell.  Reused by both the treemap and the
  // mobile-grid fallback so the cell content + interactions are identical.
  function renderCell(it, sizeMode = "grid", sizeBasis = null) {
    const c = tierColor(it.tier);
    const mos = it.mos != null ? fmtPct(it.mos) : "—";
    const cap = it.mos != null ? Math.min(Math.abs(it.mos), 100) : 50;
    const age = ageTxt(it.age_seconds);
    const stratIcon  = it.is_strategic
      ? `<span class="disc-cell__strategic" title="${escHtml(it.strategic_label || 'Strategic Asset')} — sovereign-capital backstop">⛨</span>`
      : "";
    const policyChip = it.policy_tailwind
      ? `<span class="disc-cell__policy" title="Live policy tailwind in recent news">⬈ POLICY</span>`
      : it.policy_headwind
      ? `<span class="disc-cell__policy disc-cell__policy--down" title="Policy headwind in recent news">⬊ POLICY</span>`
      : "";
    // sizeMode === "treemap": cell flex-basis is proportional to sqrt(mcap)
    // so a $3T name is ~4× the area of a $200B name; linear scaling crushes
    // small caps to single pixels.  Min-width ensures readability at all sizes.
    const sizeStyle = (sizeMode === "treemap" && sizeBasis != null)
      ? `flex:${sizeBasis} 1 ${Math.max(120, sizeBasis * 30)}px;min-width:120px;min-height:${Math.max(86, Math.min(180, 60 + sizeBasis * 8))}px;`
      : "";
    return `
      <div class="disc-cell ${sizeMode === "treemap" ? "disc-cell--tm" : ""}" data-disc-ticker="${escHtml(it.ticker)}"
           style="--tier-accent:${c.accent};--tier-strong:${c.strong};--tier-glow:${c.glow};${sizeStyle}">
        ${stratIcon}
        ${policyChip}
        ${age ? `<span class="disc-cell__age" title="Cached snapshot">${escHtml(age)}</span>` : ""}
        <div class="disc-cell__head">
          <span class="disc-cell__ticker">${escHtml(it.ticker)}</span>
          <span class="disc-cell__mos">${mos}</span>
        </div>
        <div class="disc-cell__name">${escHtml(it.name || "—")}</div>
        <div class="disc-cell__tier">${escHtml(it.label || "—")}</div>
        <div class="disc-cell__bar"><div class="disc-cell__bar-fill" style="width:${cap}%"></div></div>
      </div>
    `;
  }

  // Treemap mode: group by sector, size cells by sqrt(market_cap).  Falls
  // back to the uniform grid when (a) viewport is narrow (<640px), or
  // (b) market_cap data isn't available on the items.
  const isMobile = window.matchMedia("(max-width: 640px)").matches;
  const haveMcap = items.some(it => it.market_cap != null && it.market_cap > 0);
  const useTreemap = haveMcap && !isMobile && _DISC_SORT !== "ticker";

  if (useTreemap) {
    // Group by sector while preserving sort order within each group.
    const bySector = new Map();
    for (const it of items) {
      const sec = it.sector || "Other";
      if (!bySector.has(sec)) bySector.set(sec, []);
      bySector.get(sec).push(it);
    }
    // Order sectors by total mcap descending so the biggest groups go top.
    const sectorList = [...bySector.entries()]
      .map(([sec, arr]) => [sec, arr, arr.reduce((s, it) => s + (it.market_cap || 0), 0)])
      .sort((a, b) => b[2] - a[2]);
    grid.classList.add("disc-grid--treemap");
    grid.innerHTML = sectorList.map(([sec, arr]) => {
      // Compute flex bases as sqrt(mcap), normalized so cells are comparable
      // across sectors (a $3T Tech and $3T Healthcare should look similar).
      const cells = arr.map(it => {
        const basis = it.market_cap ? Math.sqrt(it.market_cap / 1e9) : 1.0;
        return renderCell(it, "treemap", Math.max(0.5, basis));
      }).join("");
      return `
        <div class="disc-sector">
          <div class="disc-sector__head">
            <span class="disc-sector__name">${escHtml(sec)}</span>
            <span class="disc-sector__count">${arr.length}</span>
          </div>
          <div class="disc-sector__cells">${cells}</div>
        </div>`;
    }).join("");
  } else {
    grid.classList.remove("disc-grid--treemap");
    grid.innerHTML = items.map(it => renderCell(it, "grid")).join("");
  }

  grid.querySelectorAll(".disc-cell").forEach(cell => {
    cell.onclick = () => {
      closeDiscoverPage();
      const t = cell.dataset.discTicker;
      $("tickerInput").value = t;
      analyze(t);
    };
  });
}

// Fast-tier refresh: hits /api/quote, recomputes MOS client-side using the
// stable IV from _DISC_DATA, patches each cell's price + MOS in place
// without a relayout (treemap sizing stays anchored to mcap, not MOS).
async function refreshDiscoverPrices() {
  if (_DISC_DATA.length === 0) return;
  const tickers = _DISC_DATA.map(it => it.ticker).filter(Boolean);
  if (tickers.length === 0) return;
  try {
    const res = await fetch(`/api/quote?tickers=${encodeURIComponent(tickers.join(","))}`);
    if (!res.ok) return;
    const data = await res.json();
    const quotes = data.quotes || {};
    let touched = 0;
    for (const it of _DISC_DATA) {
      const q = quotes[it.ticker];
      if (!q || q.price == null) continue;
      it.price = q.price;
      it.daily_change_pct = q.daily_change_pct;
      if (it.iv != null && q.price > 0) {
        it.mos = Math.max(Math.min((it.iv - q.price) / q.price * 100, 200), -99);
      }
      touched++;
      // Patch the visible cell in place — no full re-render.
      const cell = document.querySelector(`.disc-cell[data-disc-ticker="${it.ticker}"]`);
      if (cell) {
        const mosEl = cell.querySelector(".disc-cell__mos");
        if (mosEl && it.mos != null) mosEl.textContent = fmtPct(it.mos);
        const fill = cell.querySelector(".disc-cell__bar-fill");
        if (fill && it.mos != null) fill.style.width = `${Math.min(Math.abs(it.mos), 100)}%`;
      }
    }
    if (touched > 0) {
      _DISC_PRICES_LAST_UPDATE = Date.now();
      updateFreshnessBadge();
    }
  } catch { /* swallow — next tick will retry */ }
}

// Live-tick on the analyze view.  Hits /api/quote for the single displayed
// ticker, recomputes MOS using the cached IV, and patches the hero card
// (price, MOS%, MOS bar) without re-running the full DCF.
async function refreshAnalyzeTick(ticker) {
  if (!ticker || !_LAST_DATA) return;
  // ETF / BTC hero cards have a different rendering path and no IV;
  // skip the live tick so we don't blank their hero or re-render incorrectly.
  if (_LAST_DATA.is_etf || _LAST_DATA.is_btc) return;
  try {
    const res = await fetch(`/api/quote?ticker=${encodeURIComponent(ticker)}`);
    if (!res.ok) return;
    const data = await res.json();
    const q = (data.quotes || {})[ticker];
    if (!q || q.price == null) return;
    const iv = _LAST_DATA.intrinsic_value;
    if (iv == null) return;
    const newMos = Math.max(Math.min((iv - q.price) / q.price * 100, 200), -99);
    _LAST_DATA.current_price = q.price;
    _LAST_DATA.margin_of_safety = newMos;
    // Patch the visible header in place.  No animateNumber call — the
    // periodic update should be subtle, not draw the eye every tick.
    const priceEl = document.getElementById("vPrice");
    if (priceEl) priceEl.textContent = fmtPrice(q.price);
    const pctEl = document.getElementById("vMosPct");
    if (pctEl && !_LAST_DATA.extreme_mos_flag) pctEl.textContent = fmtPct(newMos);
    const fillEl = document.getElementById("vMosFill");
    if (fillEl) {
      const cap = Math.min(Math.abs(newMos), 100);
      fillEl.style.width = `${cap / 2}%`;
      if (newMos >= 0) {
        fillEl.style.left = "50%"; fillEl.style.right = "auto";
        fillEl.classList.add("positive"); fillEl.classList.remove("negative");
      } else {
        fillEl.style.right = "50%"; fillEl.style.left = "auto";
        fillEl.classList.add("negative"); fillEl.classList.remove("positive");
      }
    }
  } catch { /* silent — next tick retries */ }
}

// Updates "Prices: Xs ago" + "Model: Xm ago" badge text.  Called by
// startFreshnessTicker every second and after each quote refresh.
function updateFreshnessBadge() {
  const badge = document.getElementById("discFreshness");
  if (!badge) return;
  const priceAge = _DISC_PRICES_LAST_UPDATE
    ? Math.round((Date.now() - _DISC_PRICES_LAST_UPDATE) / 1000)
    : null;
  const modelAgeS = _DISC_DATA.length
    ? Math.min(...(_DISC_DATA.map(it => it.age_seconds).filter(a => a != null)))
    : null;
  const fmtAge = (s) => {
    if (s == null || isNaN(s)) return "—";
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.round(s / 60)}m ago`;
    return `${Math.round(s / 3600)}h ago`;
  };
  // Color the model freshness chip green/amber/red based on the cron health.
  const modelCls = modelAgeS == null ? "freshness--unknown"
                 : modelAgeS < 30 * 60 ? "freshness--good"
                 : modelAgeS < 90 * 60 ? "freshness--warn"
                 : "freshness--stale";
  badge.innerHTML = `
    <span class="freshness-chip">Prices: ${fmtAge(priceAge)}</span>
    <span class="freshness-chip ${modelCls}">Model: ${fmtAge(modelAgeS)}</span>
  `;
}

let _FRESHNESS_TICKER = null;
function startFreshnessTicker() {
  if (_FRESHNESS_TICKER) clearInterval(_FRESHNESS_TICKER);
  _FRESHNESS_TICKER = setInterval(updateFreshnessBadge, 1000);
  updateFreshnessBadge();
}

function setupDiscover() {
  const btn = $("discoverBtn");
  if (btn) btn.onclick = openDiscoverPage;
  const back = $("discBackBtn");
  if (back) back.onclick = closeDiscoverPage;
  const refresh = $("discRefreshBtn");
  if (refresh) refresh.onclick = () => loadDiscover({ fresh: true });
  document.querySelectorAll("[data-disc-tier]").forEach(b => {
    b.onclick = () => {
      _DISC_TIER_FILTER = b.dataset.discTier;
      document.querySelectorAll("[data-disc-tier]").forEach(x =>
        x.classList.toggle("active", x === b));
      renderDiscover();
    };
  });
  document.querySelectorAll("[data-disc-sort]").forEach(b => {
    b.onclick = () => {
      _DISC_SORT = b.dataset.discSort;
      document.querySelectorAll("[data-disc-sort]").forEach(x =>
        x.classList.toggle("active", x === b));
      renderDiscover();
    };
  });
}

// ── Shared portfolio (read-only via URL) ───────────────────────────────
let _IS_SHARED_VIEW = false;

async function openSharedPortfolio(tickers) {
  hideAllViews();
  document.querySelector(".hero")?.classList.add("hidden");
  $("portfolioPage").classList.remove("hidden");
  $("pfSharedBanner").classList.remove("hidden");
  $("pfPageTitle").textContent = "Shared Portfolio";
  $("pfPageTagline").textContent = `${tickers.length} stock${tickers.length === 1 ? "" : "s"} · live VALUS verdict on each`;

  // Hide refresh + share buttons (this view is read-only)
  $("pfRefreshBtn").style.display = "none";
  $("pfShareBtn").style.display   = "none";

  _IS_SHARED_VIEW = true;
  $("portfolioList").innerHTML = `<div class="text-muted" style="padding:24px; text-align:center;">Loading ${tickers.length} stocks…</div>`;

  // Fetch each in parallel via the cached endpoint (snappy)
  const data = await Promise.all(tickers.map(async t => {
    try {
      const res = await fetch(`/api/analyze?ticker=${encodeURIComponent(t)}`);
      const d = await res.json();
      if (d.error) return null;
      return {
        ticker: d.ticker || t,
        name:   d.company_name || t,
        sector: d.sector || "—",
        price:  d.current_price,
        iv:     d.intrinsic_value,
        mos:    d.margin_of_safety,
        tier:   d.priced_for?.label || "",
      };
    } catch { return null; }
  }));
  const items = data.filter(Boolean);

  // Render the shared list (no remove buttons)
  renderSharedList(items);
}

function renderSharedList(items) {
  const list = $("portfolioList");
  const empty = $("portfolioEmpty");
  if (!items.length) {
    empty.classList.remove("hidden");
    list.innerHTML = "";
    return;
  }
  empty.classList.add("hidden");

  list.innerHTML = items.map(it => {
    const mosClass = it.mos == null ? "neutral" : (it.mos > 5 ? "positive" : (it.mos < -5 ? "negative" : "neutral"));
    return `
      <div class="pf-item" data-pf-ticker="${escHtml(it.ticker)}">
        <span class="pf-item__ticker">${escHtml(it.ticker)}</span>
        <span class="pf-item__name">${escHtml(it.name || "")}</span>
        <span class="pf-item__price">${it.price != null ? fmtPrice(it.price) : "—"}</span>
        <span class="pf-item__mos ${mosClass}">${it.mos != null ? fmtPct(it.mos) : "—"}</span>
      </div>
    `;
  }).join("");
  list.querySelectorAll(".pf-item").forEach(row => {
    row.onclick = () => {
      const t = row.dataset.pfTicker;
      // Leave shared view; analyze the picked stock
      _IS_SHARED_VIEW = false;
      $("pfSharedBanner").classList.add("hidden");
      $("portfolioPage").classList.add("hidden");
      document.querySelector(".hero")?.classList.remove("hidden");
      $("tickerInput").value = t;
      analyze(t);
    };
  });

  // Summary: count + avg MOS + undervalued + best
  const mosVals = items.filter(it => it.mos != null).map(it => it.mos);
  const avg = mosVals.length ? mosVals.reduce((a, b) => a + b, 0) / mosVals.length : null;
  const under = items.filter(it => it.mos != null && it.mos > 5).length;
  const best = items.filter(it => it.mos != null).sort((a, b) => b.mos - a.mos)[0];
  $("pfCount").textContent = items.length;
  $("pfAvgMos").textContent = avg != null ? fmtPct(avg) : "—";
  $("pfUnderCount").textContent = `${under} / ${items.length}`;
  $("pfBest").textContent = best ? `${best.ticker} ${fmtPct(best.mos)}` : "—";
  $("pfAllocationCard").style.display = "none";   // simpler shared view
}

function setupSharePortfolio() {
  const shareBtn = $("pfShareBtn");
  if (!shareBtn) return;
  shareBtn.onclick = () => {
    const items = pfRead();
    if (items.length === 0) {
      shareBtn.textContent = "Add stocks first ★";
      setTimeout(() => { shareBtn.textContent = "↗ Share"; }, 1500);
      return;
    }
    const tickers = items.map(it => it.ticker).join(",");
    const url = `${window.location.origin}/?p=${encodeURIComponent(tickers)}`;
    navigator.clipboard.writeText(url).then(() => {
      shareBtn.classList.add("starred");
      shareBtn.textContent = "✓ Link copied";
      setTimeout(() => {
        shareBtn.classList.remove("starred");
        shareBtn.textContent = "↗ Share";
      }, 1800);
    });
  };
}

// Helper to hide every top-level view at once.  Each top-level <main>
// must be listed here or the app will end up showing two pages stacked.
function hideAllViews() {
  $("results")?.classList.add("hidden");
  $("btcHero")?.classList.add("hidden");
  $("etfHero")?.classList.add("hidden");
  $("portfolioPage")?.classList.add("hidden");
  $("discoverPage")?.classList.add("hidden");
  $("leaderboardPage")?.classList.add("hidden");   // was missing — caused
                                                   // leaderboard + discover
                                                   // to stack visually
  $("loading")?.classList.add("hidden");
  // Stop any live polling tied to the analyze hero — it's no longer visible.
  if (typeof _ANALYZE_TICK_TIMER !== "undefined" && _ANALYZE_TICK_TIMER) {
    clearInterval(_ANALYZE_TICK_TIMER);
    _ANALYZE_TICK_TIMER = null;
  }
  $("error")?.classList.add("hidden");
  document.querySelector(".hero")?.classList.add("hidden");
}

// ── URL deep-linking ──────────────────────────────────────────────────
// Supports:
//   /?t=AAPL          → auto-analyze that ticker on load
//   /?p=AAPL,MSFT     → open shared (read-only) portfolio view
function readURLParams() {
  const params = new URLSearchParams(window.location.search);
  // Hash drives view routing (#discover, #portfolio, #leaderboard) so it
  // survives browser refresh without conflicting with the existing ?t=
  // and ?p= ticker/shared-portfolio query params.
  const hash = (window.location.hash || "").replace(/^#/, "").toLowerCase();
  return {
    ticker:    (params.get("t") || params.get("ticker") || "").trim().toUpperCase(),
    portfolio: (params.get("p") || params.get("portfolio") || "").trim().toUpperCase(),
    view:      ["discover", "portfolio", "leaderboard"].includes(hash) ? hash : "",
  };
}

async function bootFromURL() {
  const { ticker, portfolio, view } = readURLParams();
  if (portfolio) {
    const tickers = portfolio.split(",").map(t => t.trim()).filter(Boolean);
    if (tickers.length > 0) {
      openSharedPortfolio(tickers);
      return;
    }
  }
  if (ticker) {
    $("tickerInput").value = ticker;
    analyze(ticker);
    return;
  }
  // No ticker / no shared portfolio — restore the hash-encoded view so
  // browser refresh on Discover/Portfolio/Leaderboard stays put instead of
  // bouncing back to the search hero.
  if (view === "discover")        { openDiscoverPage();    return; }
  if (view === "portfolio")       { openPortfolioPage();   return; }
  if (view === "leaderboard")     { openLeaderboardPage(); return; }

  // No active view, no ticker — pre-warm the Discover cache in the
  // background so when the user clicks Discover, data is already there.
  // Non-blocking; failures swallowed (the explicit click will retry).
  setTimeout(() => { try { loadDiscover(); } catch (e) {} }, 800);
}

// Hash-driven view routing.  Set when a non-search view opens, cleared on
// close.  Doesn't touch query params (?t= / ?p= continue to coexist).
function setViewHash(name) {
  const url = new URL(window.location.href);
  url.hash = name || "";
  window.history.replaceState({}, "", url);
}

// Update the URL bar when analyzing (without page reload)
function pushTickerToURL(ticker) {
  if (!ticker) return;
  const url = new URL(window.location.href);
  url.searchParams.set("t", ticker);
  url.searchParams.delete("p");
  url.hash = "";   // analyzing a ticker exits any view
  window.history.replaceState({}, "", url);
}

document.addEventListener("DOMContentLoaded", () => {
  setupSearch();
  setupAdvancedToggle();
  setupCopyButton();
  setupAddPortfolioButton();
  setupPortfolioPage();
  setupTierGlossary();
  setupModalDismiss();
  setupCustomDCFSliders();
  setupSharePortfolio();
  setupDiscover();
  setupLowConfExplainer();
  setupSubmitToLeaderboard();
  setupLeaderboard();
  setupAuthControl();
  pfUpdateBadge();
  // Fetch identity in parallel with bootFromURL — non-blocking
  refreshMe();
  bootFromURL();
  // Browser back/forward should navigate between views.  Only react to
  // hash changes that didn't come from our own setViewHash() (those use
  // replaceState, which doesn't fire popstate).
  window.addEventListener("popstate", () => {
    const { ticker, portfolio, view } = readURLParams();
    if (ticker || portfolio) { bootFromURL(); return; }
    // Hash-only navigation — close everything and re-route to the view.
    closeDiscoverPage();
    closePortfolioPage();
    closeLeaderboardPage();
    if (view === "discover")    openDiscoverPage();
    else if (view === "portfolio")  openPortfolioPage();
    else if (view === "leaderboard") openLeaderboardPage();
  });
});
