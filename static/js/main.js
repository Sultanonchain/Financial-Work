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
  deep_discount: "tier-positive",
  discount:      "tier-positive",
  fair_value:    "tier-info",
  growth:        "tier-warning",
  excellence:    "tier-warning",
  miracle:       "tier-negative",
  decline:       "tier-negative",
  distress:      "tier-positive",  // distressed = market overly pessimistic = opportunity
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

async function analyze(ticker, params = {}) {
  showLoading();
  hideError();
  $("results").classList.add("hidden");

  const url = new URL("/api/analyze", window.location.origin);
  url.searchParams.set("ticker", ticker);
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
    renderResults(data);
  } catch (err) {
    hideLoading();
    showError(err.message || "Failed to load analysis");
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

function renderResults(d) {
  _LAST_DATA = d;
  $("results").classList.remove("hidden");

  renderHeroVerdict(d);
  renderMethodology(d);
  renderScenarios(d);
  renderMiniStats(d);
  renderDrawerContent(d);

  attachCardGlow();
  // Smooth-scroll into view
  $("results").scrollIntoView({ behavior: "smooth", block: "start" });
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
    pctEl.textContent = fmtPct(mos);
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

  // Reasons
  const reasonsHtml = (vs.reasons || []).map((r, i) =>
    `<div class="reason"><span class="reason__num">${i+1}</span><span class="reason__txt">${escHtml(r)}</span></div>`
  ).join("");
  $("vReasons").innerHTML = reasonsHtml;

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

function activateScenario(which) {
  if (!_LAST_DATA) return;
  const sc = _LAST_DATA.scenarios || {};
  const slot = sc[which];
  if (!slot || slot.value == null) return;

  const ivEl = $("vIV");
  const cur  = parseFloat(ivEl.textContent.replace(/[^0-9.-]/g,"")) || 0;
  animateNumber(ivEl, cur, slot.value, 350, v => fmtPrice(v));

  // Update MOS to match
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
  const price = d.current_price;

  $("scWeightNote").textContent = sc.weight_basis ? `Weights: ${sc.weight_basis}` : "";

  const cards = [
    { key: "bear", cls: "bear", label: "Bear case" },
    { key: "base", cls: "base", label: "Base case" },
    { key: "bull", cls: "bull", label: "Bull case" },
  ].map(({ key, cls, label }) => {
    const slot = sc[key] || {};
    const v = slot.value;
    const w = slot.weight ?? 33;
    const upside = slot.upside;

    // Probability bar width
    const barW = Math.min(Math.max(w, 5), 100);

    // Assumptions row (s1, wacc)
    const s1 = slot.s1 != null ? `<span><strong>g₁</strong> ${fmt(slot.s1, 1)}%</span>` : "";
    const wacc = slot.wacc != null ? `<span><strong>WACC</strong> ${fmt(slot.wacc, 1)}%</span>` : "";

    return `
      <div class="sc-card ${cls}">
        <div class="sc-card__head">
          <span class="sc-card__label">${label}</span>
          <span class="sc-card__weight">${w}% weight</span>
        </div>
        <div class="sc-card__value">${v != null ? fmtPrice(v) : "—"}</div>
        <div class="sc-card__delta">${upside != null ? fmtPct(upside) + " vs current" : "—"}</div>
        <div class="sc-card__bar"><div class="sc-card__bar-fill" style="--bar-width: ${barW}%; width: ${barW}%;"></div></div>
        <div class="sc-card__assumptions">${s1}${wacc}</div>
      </div>
    `;
  });

  grid.innerHTML = cards.join("");

  // Weighted bar
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
  try { renderFinancialsTabs(d); }  catch (e) { console.error("[financials]", e); }
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

function renderDcfChart(fcfData) {
  const canvas = $("dcfChart");
  if (!canvas || !fcfData) return;
  const ctx = canvas.getContext("2d");
  if (dcfChartInstance) dcfChartInstance.destroy();

  // Backend returns either:
  //   { projected: { labels, values, pvs } }   (current shape)
  // or { years, projected, discounted }        (legacy)
  let labels, projected, discounted;
  if (fcfData.projected && typeof fcfData.projected === "object" && !Array.isArray(fcfData.projected)) {
    labels     = fcfData.projected.labels || [];
    projected  = (fcfData.projected.values || []).map(v => v / 1e9);
    discounted = (fcfData.projected.pvs    || []).map(v => v / 1e9);
  } else {
    labels     = fcfData.years || [];
    projected  = (fcfData.projected || []).map(v => v / 1e9);
    discounted = (fcfData.discounted || []).map(v => v / 1e9);
  }
  if (labels.length === 0 || projected.length === 0) return;

  dcfChartInstance = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        { label: "Projected FCF", data: projected, backgroundColor: "rgba(94,234,212,0.20)", borderColor: "#5eead4", borderWidth: 1, borderRadius: 4 },
        { label: "Discounted FCF (PV)", data: discounted, backgroundColor: "rgba(96,165,250,0.20)", borderColor: "#60a5fa", borderWidth: 1, borderRadius: 4 },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: "#b6bdcb", font: { size: 11 } } },
        tooltip: {
          backgroundColor: "#11151d", borderColor: "rgba(255,255,255,0.08)", borderWidth: 1,
          callbacks: { label: c => `${c.dataset.label}: $${fmt(c.parsed.y, 2)}B` }
        }
      },
      scales: {
        x: { ticks: { color: "#6b7382" }, grid: { display: false } },
        y: { ticks: { color: "#6b7382", callback: v => `$${fmt(v, 0)}B` }, grid: { color: "rgba(255,255,255,0.04)" } }
      }
    }
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

function renderFinancialsTabs(d) {
  const body = $("finBody");
  function pickTab(key) {
    const data = d[`${key}_statement`] || null;
    if (!data || !data.rows) {
      body.innerHTML = `<tr><td class="text-muted">No ${key} statement data available.</td></tr>`;
      return;
    }
    const headers = data.periods || [];
    const rows = data.rows || [];
    const headerRow = `<tr><th>Item</th>${headers.map(h => `<th>${escHtml(h)}</th>`).join("")}</tr>`;
    const bodyRows = rows.map(r =>
      `<tr><td>${escHtml(r.label)}</td>${r.values.map(v => `<td>${v == null ? "—" : fmtBig(v)}</td>`).join("")}</tr>`
    ).join("");
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
    const params = {};
    const yrs = $("advYrs").value;
    const s1  = $("advS1").value;
    const s2  = $("advS2").value;
    const tg  = $("advTg").value;
    if (yrs) params.years = yrs;
    if (s1)  params.s1 = s1;
    if (s2)  params.s2 = s2;
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
   Boot
   ════════════════════════════════════════════════════════════════════════ */

document.addEventListener("DOMContentLoaded", () => {
  setupSearch();
  setupAdvancedToggle();
  setupCopyButton();
});
