/* ── Helpers ──────────────────────────────────────────────── */
const $ = id => document.getElementById(id);

function fmt(n, d = 2) {
  if (n == null || isNaN(n)) return '—';
  return Number(n).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}
function fmtBig(n) {
  if (n == null || isNaN(n)) return '—';
  const a = Math.abs(n), s = n < 0 ? '-' : '';
  if (a >= 1e12) return s + '$' + fmt(a / 1e12) + 'T';
  if (a >= 1e9)  return s + '$' + fmt(a / 1e9)  + 'B';
  if (a >= 1e6)  return s + '$' + fmt(a / 1e6)  + 'M';
  return s + '$' + fmt(a);
}
function fmtPct(n)   { return n == null || isNaN(n) ? '—' : fmt(n, 1) + '%'; }
function fmtX(n)     { return n == null || isNaN(n) ? '—' : fmt(n, 1) + 'x'; }
function fmtPrice(n) { return n == null || isNaN(n) ? '—' : '$' + fmt(n, 2); }
function fmtVol(n)   {
  if (!n) return '—';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(0) + 'K';
  return n;
}

function cc(n, good = true) {
  if (n == null || isNaN(n)) return 'muted';
  return (n > 0) === good ? 'green' : 'red';
}

function row(label, value, cls = '') {
  return `<div class="metric-row">
    <span class="label">${label}</span>
    <span class="value ${cls}">${value}</span>
  </div>`;
}

/* ── State ────────────────────────────────────────────────── */
let priceChart = null;
let dcfChart   = null;
let currentTicker = '';
let currentData   = null;
let searchTimer   = null;

/* ── Modal ────────────────────────────────────────────────── */
function openDcfGuide()  { $('dcfModal').classList.remove('hidden'); document.body.style.overflow = 'hidden'; }
function closeDcfGuide() { $('dcfModal').classList.add('hidden');    document.body.style.overflow = ''; }
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDcfGuide(); });

/* ── Advanced panel ───────────────────────────────────────── */
function toggleAdvanced() {
  const p = $('advancedPanel'), open = !p.classList.contains('hidden');
  p.classList.toggle('hidden', open);
  $('advToggleLabel').textContent = open ? '⚙ Customize DCF Assumptions' : '▲ Hide Assumptions';
}

/* ── Search autocomplete ──────────────────────────────────── */
const input    = $('tickerInput');
const dropdown = $('searchDropdown');

input.addEventListener('input', () => {
  clearTimeout(searchTimer);
  const q = input.value.trim();
  if (!q || q.length < 2) { dropdown.classList.add('hidden'); return; }
  searchTimer = setTimeout(() => fetchSuggestions(q), 260);
});

input.addEventListener('keydown', e => {
  if (e.key === 'Enter') { dropdown.classList.add('hidden'); runAnalysis(); }
  if (e.key === 'Escape') dropdown.classList.add('hidden');
});

document.addEventListener('click', e => {
  if (!e.target.closest('.search-wrap')) dropdown.classList.add('hidden');
});

async function fetchSuggestions(q) {
  try {
    const res  = await fetch('/api/search?q=' + encodeURIComponent(q));
    const data = await res.json();
    if (!data.length) { dropdown.classList.add('hidden'); return; }
    dropdown.innerHTML = data.map(d => `
      <div class="dropdown-item" onclick="selectTicker('${d.symbol}','${escHtml(d.name)}')">
        <span class="di-symbol">${d.symbol}</span>
        <span class="di-name">${escHtml(d.name)}</span>
        <span class="di-exch">${d.exchange}</span>
      </div>`).join('');
    dropdown.classList.remove('hidden');
  } catch { dropdown.classList.add('hidden'); }
}

function escHtml(s) { return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

function selectTicker(symbol, name) {
  input.value = symbol;
  dropdown.classList.add('hidden');
  runAnalysis();
}

/* ── Period buttons ───────────────────────────────────────── */
document.querySelectorAll('.period-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    if (!currentTicker) return;
    document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    loadPeriodChart(currentTicker, btn.dataset.period);
  });
});

async function loadPeriodChart(ticker, period) {
  const loading = $('chartLoading');
  loading.classList.remove('hidden');
  try {
    const res  = await fetch(`/api/history?ticker=${ticker}&period=${period}`);
    const data = await res.json();
    if (data.prices) renderPriceChart(data.prices, currentData);
  } catch {}
  loading.classList.add('hidden');
}

/* ── Main analysis ────────────────────────────────────────── */
async function runAnalysis() {
  const ticker = input.value.trim().toUpperCase();
  if (!ticker) { input.focus(); return; }

  const params = new URLSearchParams({ ticker });
  const g1 = $('growth1').value, g2 = $('growth2').value;
  const tg = $('terminalGrowth').value, yrs = $('projYears').value;
  if (g1)  params.set('growth1',  parseFloat(g1)  / 100);
  if (g2)  params.set('growth2',  parseFloat(g2)  / 100);
  if (tg)  params.set('terminal', parseFloat(tg)  / 100);
  if (yrs) params.set('years',    parseInt(yrs));

  $('loadingTicker').textContent = ticker;
  show('loadingState'); hide('results'); hide('errorState');
  $('analyzeBtn').disabled = true;

  try {
    const res  = await fetch('/api/analyze?' + params);
    const data = await res.json();
    if (!res.ok || data.error) { showError(data.error || 'Something went wrong.'); return; }
    currentTicker = ticker;
    currentData   = data;

    // Reset period btn to 1Y
    document.querySelectorAll('.period-btn').forEach(b => b.classList.toggle('active', b.dataset.period === '1y'));

    renderResults(data);
    show('results');
    $('results').scrollIntoView({ behavior: 'smooth', block: 'start' });

    // Load statements async
    loadStatements(ticker);
  } catch { showError('Network error — make sure the server is running.'); }
  finally { hide('loadingState'); $('analyzeBtn').disabled = false; }
}

function show(id) { $(id).classList.remove('hidden'); }
function hide(id) { $(id).classList.add('hidden');    }
function showError(msg) { $('errorMsg').textContent = msg; show('errorState'); hide('loadingState'); }

/* ── Render all ───────────────────────────────────────────── */
function renderResults(d) {
  // Company header
  $('companyName').textContent   = d.company_name;
  $('tickerBadge').textContent   = d.ticker;
  $('sectorBadge').textContent   = d.sector   || 'N/A';
  $('industryBadge').textContent = d.industry || 'N/A';
  $('currentPrice').textContent  = fmtPrice(d.current_price);
  const parts = [];
  if (d['52w_low'] && d['52w_high']) parts.push(`52W  ${fmtPrice(d['52w_low'])} – ${fmtPrice(d['52w_high'])}`);
  if (d.currency && d.currency !== 'USD') parts.push(d.currency);
  $('priceMeta').textContent = parts.join('   ·   ');

  // Stats strip
  $('statsStrip').innerHTML = [
    statItem('P/E (TTM)',     fmtX(d.pe_ratio),      peColor(d.pe_ratio)),
    statItem('Forward P/E',  fmtX(d.forward_pe),    peColor(d.forward_pe, true)),
    statItem('EPS',          fmtPrice(d.eps)),
    statItem('Market Cap',   fmtBig(d.market_cap)),
    statItem('Volume',       fmtVol(d.volume)),
    statItem('Avg Volume',   fmtVol(d.avg_volume)),
    statItem('Div. Yield',   fmtPct(d.dividend_yield)),
    statItem('Target',       fmtPrice(d.target_price), 'cyan'),
  ].join('');

  // Verdict — use probability-weighted fair value
  const iv  = d.intrinsic_value;   // already set to weighted by backend
  const mos = d.margin_of_safety;
  const sc  = d.scenarios;
  const card = $('verdictCard');
  card.className = 'verdict-card';

  if (!d.dcf_available || iv == null) {
    // Show multiples-based fallback if available
    if (d.multiples_val) {
      $('intrinsicValue').textContent = fmtPrice(d.multiples_val);
      const mmos = d.multiples_mos;
      let mtxt = '';
      if (mmos > 15)       { card.classList.add('undervalued'); mtxt = `${fmtPct(mmos)} upside to multiples fair value`; }
      else if (mmos < -15) { card.classList.add('overvalued');  mtxt = `${fmtPct(Math.abs(mmos))} above multiples fair value`; }
      else                 { card.classList.add('fair');        mtxt = 'Trading near multiples-based fair value'; }
      $('verdictSub').textContent = mtxt;
      const mcolor = mmos > 15 ? 'var(--green)' : mmos < -15 ? 'var(--red)' : 'var(--gold)';
      const mosEl = $('mosValue');
      mosEl.textContent  = (mmos > 0 ? '+' : '') + fmtPct(mmos);
      mosEl.style.color  = mcolor;
      $('mosFill').style.width      = Math.min(Math.abs(mmos), 100) + '%';
      $('mosFill').style.background = mcolor;
      $('mosHint').textContent      = mmos > 0 ? 'Trading below multiples fair value' : 'Trading above multiples fair value';
    } else {
      $('intrinsicValue').textContent = 'N/A';
      $('verdictSub').textContent     = d.dcf_warning || 'DCF not available';
      $('mosValue').textContent       = '—';
      $('mosFill').style.width        = '0';
      $('mosHint').textContent        = '';
      card.classList.add('fair');
    }
  } else {
    $('intrinsicValue').textContent = fmtPrice(iv);
    $('verdictSub').textContent     = '50/25/25 probability-weighted across 3 scenarios';
    let txt = '';
    if (mos > 15)       { card.classList.add('undervalued'); txt = `${fmtPct(mos)} upside to fair value — potentially undervalued`; }
    else if (mos < -15) { card.classList.add('overvalued');  txt = `${fmtPct(Math.abs(mos))} above fair value — potentially overvalued`; }
    else                { card.classList.add('fair');         txt = 'Trading near probability-weighted fair value'; }
    $('verdictSub').textContent = txt;

    const color = mos > 15 ? 'var(--green)' : mos < -15 ? 'var(--red)' : 'var(--gold)';
    const mosEl = $('mosValue');
    mosEl.textContent  = (mos > 0 ? '+' : '') + fmtPct(mos);
    mosEl.style.color  = color;
    $('mosFill').style.width      = Math.min(Math.abs(mos), 100) + '%';
    $('mosFill').style.background = color;
    $('mosHint').textContent      = mos > 0 ? 'Trading below weighted fair value' : 'Trading above weighted fair value';
  }

  // Scenario analysis
  renderScenarios(sc, d.current_price);

  // DCF warning banner + contextual notes
  const warnEl = $('dcfWarning');
  const notes  = d.dcf_notes || [];
  let warnHtml = '';
  // Multiples fallback badge — shown prominently when DCF is replaced by multiples
  if (d.multiples_val && (!d.dcf_available || iv == null)) {
    warnHtml += `<div class="multiples-badge">
      <span class="multiples-badge-icon">◈</span>
      <span class="multiples-badge-text"><strong>Secondary Valuation Applied:</strong> Industry Multiples Method used due to ${d.multiples_reason || 'DCF unavailability'} — ${d.multiples_method}.</span>
    </div>`;
  }
  if (d.dcf_warning) warnHtml += `<div class="dcf-note dcf-note--warn">⚠ ${d.dcf_warning}</div>`;
  notes.forEach(n => {
    const cls = n.type === 'warn' ? 'dcf-note--warn' : 'dcf-note--info';
    const icon = n.type === 'warn' ? '⚠' : 'ℹ';
    warnHtml += `<div class="dcf-note ${cls}">${icon} ${n.text}</div>`;
  });
  if (warnHtml) { warnEl.innerHTML = warnHtml; warnEl.classList.remove('hidden'); }
  else          { warnEl.classList.add('hidden'); }

  // Valuation multiples
  $('valuationMetrics').innerHTML = [
    row('Market Cap',     fmtBig(d.market_cap)),
    row('P/E (TTM)',      fmtX(d.pe_ratio),      peColor(d.pe_ratio)),
    row('Forward P/E',   fmtX(d.forward_pe),    peColor(d.forward_pe, true)),
    row('PEG Ratio',     d.peg_ratio != null ? fmt(d.peg_ratio) + 'x' : '—',
                         d.peg_ratio != null ? (d.peg_ratio < 1 ? 'green' : d.peg_ratio > 2 ? 'red' : '') : ''),
    row('P/S Ratio',     fmtX(d.ps_ratio)),
    row('P/B Ratio',     fmtX(d.pb_ratio)),
    row('EV/EBITDA',     fmtX(d.ev_ebitda)),
    row('EV/Revenue',    fmtX(d.ev_revenue)),
    row('FCF Yield',     d.fcf_yield != null ? fmtPct(d.fcf_yield) : '—',
                         d.fcf_yield != null ? (d.fcf_yield > 4 ? 'green' : '') : ''),
    row('Analyst Target',fmtPrice(d.target_price), 'cyan'),
    row('Analyst Rating',ratingBadge(d.analyst_rating, d.analyst_count)),
  ].join('');

  // DCF assumptions
  $('dcfAssumptions').innerHTML = d.dcf_available ? [
    row('WACC',            fmtPct(d.wacc),           'cyan'),
    row('Cost of Equity',  fmtPct(d.cost_of_equity)),
    row('Cost of Debt',    fmtPct(d.cost_of_debt)),
    row('Tax Rate',        fmtPct(d.tax_rate)),
    row('Beta',            fmt(d.beta)),
    row('Stage 1 Growth',  fmtPct(d.stage1_growth),  'green'),
    row('Stage 2 Growth',  fmtPct(d.stage2_growth),  'green'),
    row('Terminal Growth', fmtPct(d.terminal_growth)),
    row('Base FCF',        fmtBig(d.base_fcf)),
    row('FCF Yield',       d.fcf_yield != null ? fmtPct(d.fcf_yield) : '—'),
    row('PV of FCFs',      fmtBig(d.total_pv_fcf)),
    row('PV Terminal Val', fmtBig(d.pv_terminal)),
    row('Terminal % of EV',fmtPct(d.terminal_value_pct)),
    d.growth_source ? row('Growth Source', `<span style="color:var(--muted2);font-size:11px">${d.growth_source}</span>`) : '',
    d.fcf_source    ? row('FCF Source',    `<span style="color:var(--muted2);font-size:11px">${d.fcf_source}</span>`)    : '',
    (d.fx_rate && d.financial_currency && d.trading_currency)
      ? row('FX Applied', `<span style="color:var(--muted2);font-size:11px">1 ${d.financial_currency} = ${d.fx_rate} ${d.trading_currency}</span>`) : '',
  ].join('') : '<div style="color:var(--muted);font-size:12px;padding:8px 0">DCF not available for this security.</div>';

  // Financials
  $('financialMetrics').innerHTML = [
    row('Revenue',          fmtBig(d.revenue)),
    row('EBITDA',           fmtBig(d.ebitda)),
    row('FCF (TTM)',        fmtBig(d.base_fcf)),
    row('FCF Margin',       d.fcf_margin != null ? fmtPct(d.fcf_margin) : '—', cc(d.fcf_margin)),
    row('Gross Margin',     fmtPct(d.gross_margin),     d.gross_margin > 40 ? 'green' : ''),
    row('Operating Margin', fmtPct(d.operating_margin), cc(d.operating_margin)),
    row('Net Margin',       fmtPct(d.profit_margin),    cc(d.profit_margin)),
    row('Rev. Growth (YoY)',fmtPct(d.revenue_growth),   cc(d.revenue_growth)),
    row('EPS Growth (YoY)', fmtPct(d.earnings_growth),  cc(d.earnings_growth)),
    row('ROE',              fmtPct(d.roe),               cc(d.roe)),
    row('ROA',              fmtPct(d.roa),               cc(d.roa)),
    row('Div. Yield',       d.dividend_yield != null ? fmtPct(d.dividend_yield) : '—'),
  ].join('');

  // Health
  $('healthMetrics').innerHTML = [
    row('Total Cash',    fmtBig(d.total_cash),    'green'),
    row('Total Debt',    fmtBig(d.total_debt),    d.total_debt > d.total_cash ? 'red' : ''),
    row('Net Debt',      fmtBig(d.net_debt),      cc(d.net_debt, false)),
    row('Debt/Equity',   d.debt_to_equity != null ? fmt(d.debt_to_equity/100,2)+'x' : '—',
                         d.debt_to_equity > 200 ? 'red' : d.debt_to_equity != null && d.debt_to_equity < 50 ? 'green' : ''),
    row('Current Ratio', fmtX(d.current_ratio),  d.current_ratio > 2 ? 'green' : d.current_ratio != null && d.current_ratio < 1 ? 'red' : ''),
    row('Quick Ratio',   fmtX(d.quick_ratio),    d.quick_ratio  > 1 ? 'green' : d.quick_ratio  != null && d.quick_ratio  < 0.5 ? 'red' : ''),
    row('Shares Out.',   fmtBig(d.shares_outstanding)),
    row('Payout Ratio',  fmtPct(d.payout_ratio)),
  ].join('');

  // Price chart
  renderPriceChart(d.price_history || [], d);

  // DCF chart + table
  if (d.dcf_available && d.fcf_chart && d.scenarios) {
    show('scenarioCard');
  } else {
    hide('scenarioCard');
  }

  if (d.dcf_available && d.fcf_chart) {
    show('dcfChartCard'); show('dcfTableCard');
    renderDcfChart(d);
    renderProjectionTable(d);
  } else {
    hide('dcfChartCard'); hide('dcfTableCard');
  }
}

/* ── Scenario Analysis ────────────────────────────────────── */
function renderScenarios(sc, price) {
  const el = $('scenarioCard');
  if (!sc || !el) return;

  function scCard(label, data, cls) {
    if (!data || data.value == null) return '';
    const up = data.upside;
    const upCls = up > 0 ? 'green' : 'red';
    const upSign = up > 0 ? '+' : '';
    // Scenario integrity flags
    let flags = '';
    if (data.distressed)   flags += `<span class="sc-flag sc-flag--distressed">Distressed</span>`;
    if (data.floored)      flags += `<span class="sc-flag sc-flag--floored">Floor Applied</span>`;
    if (data.recalculated) flags += `<span class="sc-flag sc-flag--recalc">Re-calculated</span>`;
    return `
      <div class="sc-case sc-${cls}">
        <div class="sc-label">${label}</div>
        <div class="sc-weight">${data.weight}% weight</div>
        <div class="sc-value">${data.value === 0 ? '<span style="color:var(--red)">$0.00</span>' : fmtPrice(data.value)}</div>
        <div class="sc-upside ${upCls}">${upSign}${fmtPct(up)} vs current</div>
        <div class="sc-assumptions">
          <span>g₁ ${fmtPct(data.s1)}</span>
          <span>WACC ${fmtPct(data.wacc)}</span>
        </div>
        ${flags ? `<div class="sc-flags">${flags}</div>` : ''}
      </div>`;
  }

  const weightedUpCls = sc.weighted_upside > 0 ? 'green' : 'red';
  const weightedSign  = sc.weighted_upside > 0 ? '+' : '';

  el.innerHTML = `
    <div class="sc-header">
      <h3 class="card-title">Scenario Analysis</h3>
      <span class="card-sub">50% Base · 25% Bull · 25% Bear</span>
    </div>
    <div class="sc-grid">
      ${scCard('Base Case',    sc.base, 'base')}
      ${scCard('Bull Case',    sc.bull, 'bull')}
      ${scCard('Bear Case',    sc.bear, 'bear')}
    </div>
    <div class="sc-weighted">
      <span class="sc-wlabel">Probability-Weighted Fair Value</span>
      <span class="sc-wvalue">${fmtPrice(sc.weighted)}</span>
      <span class="sc-wupside ${weightedUpCls}">${weightedSign}${fmtPct(sc.weighted_upside)} potential upside</span>
    </div>`;
}

/* ── Stats strip item ─────────────────────────────────────── */
function statItem(label, value, cls = '') {
  return `<div class="stat-item">
    <div class="stat-label">${label}</div>
    <div class="stat-value ${cls}">${value}</div>
  </div>`;
}

/* ── Price Chart ──────────────────────────────────────────── */
function renderPriceChart(prices, d) {
  if (priceChart) { priceChart.destroy(); priceChart = null; }
  if (!prices.length) return;

  const labels     = prices.map(p => p.date);
  const closes     = prices.map(p => p.close);
  const iv         = d && d.intrinsic_value;
  const last       = closes[closes.length - 1];
  const lineColor  = (iv && last <= iv) ? '#00e676' : '#ff3d5a';

  const datasets = [{
    label: 'Price',
    data: closes,
    borderColor: lineColor,
    backgroundColor: lineColor + '15',
    fill: true,
    borderWidth: 2,
    pointRadius: 0,
    tension: 0.1,
  }];

  if (iv && d.dcf_available) {
    datasets.push({
      label: `IV ${fmtPrice(iv)}`,
      data: closes.map(() => iv),
      borderColor: '#ffb300',
      borderWidth: 1.5,
      borderDash: [5, 5],
      pointRadius: 0,
      fill: false,
      tension: 0,
    });
  }

  priceChart = new Chart($('priceChart').getContext('2d'), {
    type: 'line',
    data: { labels, datasets },
    options: baseChartOpts(v => '$' + parseFloat(v).toFixed(0),
                           ctx => `${ctx.dataset.label}: $${ctx.parsed.y.toFixed(2)}`)
  });
}

/* ── DCF Chart ────────────────────────────────────────────── */
function renderDcfChart(d) {
  if (dcfChart) { dcfChart.destroy(); dcfChart = null; }
  const fc = d.fcf_chart;
  if (!fc) return;

  const labels     = [...fc.projected.labels, 'Terminal'];
  const projected  = [...fc.projected.values, null];
  const discounted = [...fc.projected.pvs, +(d.pv_terminal / 1e9).toFixed(2)];

  dcfChart = new Chart($('dcfChart').getContext('2d'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'Projected FCF', data: projected,  backgroundColor: 'rgba(0,212,255,0.22)', borderColor: '#00d4ff', borderWidth: 1, borderRadius: 3 },
        { label: 'Present Value', data: discounted, backgroundColor: 'rgba(0,230,118,0.22)', borderColor: '#00e676', borderWidth: 1, borderRadius: 3 },
      ]
    },
    options: baseChartOpts(v => '$' + v + 'B',
                           ctx => ctx.parsed.y != null ? `${ctx.dataset.label}: $${ctx.parsed.y.toFixed(2)}B` : null)
  });
}

/* ── Projection table ─────────────────────────────────────── */
function renderProjectionTable(d) {
  const half = Math.floor(d.projection_years / 2);
  $('projectionBody').innerHTML = (d.projected_fcf || []).map(r => `
    <tr class="${r.year > half ? 'stage2' : ''}">
      <td>Year ${r.year}</td><td>${r.year <= half ? 'Stage 1' : 'Stage 2'}</td>
      <td>${fmtPct(r.growth * 100)}</td><td>${fmtBig(r.fcf)}</td><td>${fmtBig(r.pv)}</td>
    </tr>`).join('');

  $('projectionFoot').innerHTML = `
    <tr><td colspan="3">PV Terminal Value</td><td></td><td>${fmtBig(d.pv_terminal)}</td></tr>
    <tr><td colspan="3"><strong>Enterprise Value</strong></td><td></td><td><strong>${fmtBig(d.enterprise_value)}</strong></td></tr>
    <tr><td colspan="3">Less: Net Debt</td><td></td><td>${fmtBig(d.net_debt)}</td></tr>
    <tr><td colspan="3"><strong>Equity Value</strong></td><td></td><td><strong>${fmtBig(d.equity_value)}</strong></td></tr>
    <tr><td colspan="3"><strong>Intrinsic Value / Share</strong></td><td></td><td><strong style="color:var(--cyan)">${fmtPrice(d.intrinsic_value)}</strong></td></tr>`;
}

/* ── Financial Statements ─────────────────────────────────── */
let statementsData = null;
let activeTab      = 'income';

async function loadStatements(ticker) {
  $('statementsLoading').classList.remove('hidden');
  $('statementsContent').classList.add('hidden');
  try {
    const res = await fetch('/api/statements?ticker=' + ticker);
    statementsData = await res.json();
    renderStatements('income');
  } catch { $('statementsLoading').textContent = 'Could not load statements.'; }
}

document.querySelectorAll('.stmt-tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.stmt-tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    activeTab = tab.dataset.tab;
    if (statementsData) renderStatements(activeTab);
  });
});

function renderStatements(tab) {
  $('statementsLoading').classList.add('hidden');
  const content = $('statementsContent');
  const data    = statementsData && statementsData[tab];
  const finCcy  = statementsData && statementsData.financialCurrency;
  const trdCcy  = statementsData && statementsData.tradingCurrency;
  const hasFxNote = finCcy && trdCcy && finCcy !== trdCcy;

  if (!data || !data.rows || !data.rows.length) {
    content.innerHTML = '<p style="color:var(--muted);font-size:13px;padding:12px 0">No data available.</p>';
    content.classList.remove('hidden');
    return;
  }

  const cols = data.columns || [];
  const bodyRows = data.rows.map(r => {
    if (r.section) {
      return `<tr class="stmt-section-header"><td colspan="${cols.length + 1}">${r.label}</td></tr>`;
    }
    return `<tr>
      <td class="stmt-label">${r.label}</td>
      ${r.values.map(v => `<td>${fmtStmt(v)}</td>`).join('')}
    </tr>`;
  }).join('');

  const ccyNote = hasFxNote
    ? `<div class="stmt-ccy-note">Values in <strong>${finCcy}</strong> (company reporting currency) · Stock price quoted in <strong>${trdCcy}</strong></div>`
    : '';

  content.innerHTML = `
    ${ccyNote}
    <table class="stmt-table">
      <thead>
        <tr>
          <th>Item (${finCcy || 'USD'})</th>
          ${cols.map(c => `<th>${c.slice(0,7)}</th>`).join('')}
        </tr>
      </thead>
      <tbody>${bodyRows}</tbody>
    </table>`;
  content.classList.remove('hidden');
}

function fmtStmtLabel(s) {
  // Convert camelCase/PascalCase yfinance labels to readable form
  return s.replace(/([a-z])([A-Z])/g, '$1 $2')
          .replace(/ And /g, ' & ')
          .replace(/ Of /g, ' of ')
          .replace(/ In /g, ' in ')
          .replace(/ From /g, ' from ');
}

function fmtStmt(v) {
  if (v == null) return '<span style="color:var(--muted)">—</span>';
  const n = parseFloat(v);
  if (isNaN(n)) return v;
  const abs = Math.abs(n);
  const neg = n < 0;
  const s   = neg ? '<span style="color:var(--red)">(' : '<span>';
  const e   = neg ? ')</span>' : '</span>';
  if (abs >= 1e9)  return s + '$' + (abs/1e9).toFixed(2)  + 'B' + e;
  if (abs >= 1e6)  return s + '$' + (abs/1e6).toFixed(2)  + 'M' + e;
  if (abs >= 1)    return s + (neg ? '' : '') + abs.toFixed(2) + e;
  return s + abs.toFixed(2) + e;
}

/* ── Shared chart options ─────────────────────────────────── */
function baseChartOpts(yFmt, tooltipFmt) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: {
        position: 'top', align: 'end',
        labels: { color: '#4e5a7a', font: { size: 11 }, boxWidth: 12, padding: 14 }
      },
      tooltip: {
        backgroundColor: '#0d0f1c', borderColor: '#252b42', borderWidth: 1,
        titleColor: '#4e5a7a', bodyColor: '#eef0f8', padding: 10,
        callbacks: { label: ctx => tooltipFmt(ctx) }
      }
    },
    scales: {
      x: {
        ticks: {
          color: '#4e5a7a', font: { size: 10 }, maxTicksLimit: 8,
          callback: function(value) {
            const lbl = this.getLabelForValue(value);
            // For daily data, show MM-DD only
            if (typeof lbl === 'string' && lbl.match(/^\d{4}-\d{2}-\d{2}$/))
              return lbl.slice(5);
            return lbl;
          }
        },
        grid: { color: '#1e2238' }
      },
      y: {
        ticks: { color: '#4e5a7a', font: { size: 11 }, callback: yFmt },
        grid:  { color: '#1e2238' }
      }
    }
  };
}

/* ── Utility ──────────────────────────────────────────────── */
function peColor(pe, fwd = false) {
  if (pe == null || isNaN(pe) || pe < 0) return 'muted';
  const hi = fwd ? 28 : 35, lo = fwd ? 12 : 15;
  return pe > hi ? 'red' : pe < lo ? 'green' : '';
}

function ratingBadge(r, count) {
  if (!r || r === 'N/A' || r === 'none') return '—';
  const key   = r.toLowerCase().replace(/_/g, '');
  const map   = { strongbuy:'Strong Buy', buy:'Buy', hold:'Hold', underperform:'Underperform', sell:'Sell' };
  const label = map[key] || r;
  const cls   = ['buy','strongbuy'].includes(key) ? 'green' : ['sell','underperform'].includes(key) ? 'red' : 'gold';
  return `<span class="value ${cls}">${label}${count ? ` (${count})` : ''}</span>`;
}
