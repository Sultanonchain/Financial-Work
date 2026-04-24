/* ── Helpers ──────────────────────────────────────────────────── */
const $ = id => document.getElementById(id);

function fmt(n, digits = 2) {
  if (n == null || n === '' || isNaN(n)) return '—';
  return Number(n).toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function fmtBig(n) {
  if (n == null || isNaN(n)) return '—';
  const abs = Math.abs(n);
  const sign = n < 0 ? '-' : '';
  if (abs >= 1e12) return sign + '$' + fmt(abs / 1e12) + 'T';
  if (abs >= 1e9)  return sign + '$' + fmt(abs / 1e9)  + 'B';
  if (abs >= 1e6)  return sign + '$' + fmt(abs / 1e6)  + 'M';
  return sign + '$' + fmt(abs);
}

function fmtPct(n)  { return n == null ? '—' : fmt(n, 1) + '%'; }
function fmtX(n)    { return n == null ? '—' : fmt(n, 1) + 'x'; }
function fmtPrice(n){ return n == null ? '—' : '$' + fmt(n, 2); }

function colorClass(n, positiveIsGood = true) {
  if (n == null || isNaN(n)) return 'muted';
  return (n > 0) === positiveIsGood ? 'green' : 'red';
}

function metricRow(label, value, cls = '') {
  return `<div class="metric-row">
    <span class="label">${label}</span>
    <span class="value ${cls}">${value}</span>
  </div>`;
}

/* ── Advanced panel toggle ────────────────────────────────────── */
function toggleAdvanced() {
  const panel = $('advancedPanel');
  const label = $('advToggleLabel');
  const open  = panel.classList.toggle('hidden');
  label.textContent = open ? '⚙ Customize assumptions' : '▲ Hide assumptions';
}

/* ── Enter key support ────────────────────────────────────────── */
$('tickerInput').addEventListener('keydown', e => { if (e.key === 'Enter') runAnalysis(); });

/* ── Main analysis ────────────────────────────────────────────── */
async function runAnalysis() {
  const ticker = $('tickerInput').value.trim().toUpperCase();
  if (!ticker) { $('tickerInput').focus(); return; }

  // Build query params
  const params = new URLSearchParams({ ticker });
  const g1 = $('growth1').value;
  const g2 = $('growth2').value;
  const tg = $('terminalGrowth').value;
  const yrs = $('projYears').value;
  if (g1) params.set('growth1', parseFloat(g1) / 100);
  if (g2) params.set('growth2', parseFloat(g2) / 100);
  if (tg) params.set('terminal', parseFloat(tg) / 100);
  if (yrs) params.set('years', parseInt(yrs));

  // UI state
  $('loadingTicker').textContent = ticker;
  show('loadingState');
  hide('results');
  hide('errorState');
  $('analyzeBtn').disabled = true;

  try {
    const resp = await fetch('/api/analyze?' + params.toString());
    const data = await resp.json();

    if (!resp.ok || data.error) {
      showError(data.error || 'Something went wrong. Please try again.');
      return;
    }

    renderResults(data);
    show('results');
    $('results').scrollIntoView({ behavior: 'smooth', block: 'start' });

  } catch (err) {
    showError('Network error. Make sure the server is running.');
  } finally {
    hide('loadingState');
    $('analyzeBtn').disabled = false;
  }
}

function show(id) { $(id).classList.remove('hidden'); }
function hide(id) { $(id).classList.add('hidden'); }

function showError(msg) {
  $('errorMsg').textContent = msg;
  show('errorState');
  hide('loadingState');
}

/* ── Render ───────────────────────────────────────────────────── */
function renderResults(d) {
  const cur = d.current_price;

  // ── Company header ─────────────────────────────────────────────
  $('companyName').textContent = d.company_name;
  $('tickerBadge').textContent  = d.ticker;
  $('sectorBadge').textContent  = d.sector || 'N/A';
  $('exchangeBadge').textContent = d.exchange || '';
  $('currentPrice').textContent  = fmtPrice(cur);

  if (d['52w_high'] && d['52w_low']) {
    $('priceRange').textContent = `52W: ${fmtPrice(d['52w_low'])} – ${fmtPrice(d['52w_high'])}`;
  }

  // ── Verdict card ───────────────────────────────────────────────
  const iv  = d.intrinsic_value;
  const mos = d.margin_of_safety;
  const card = $('verdictCard');
  card.className = 'verdict-card';

  $('intrinsicValue').textContent = fmtPrice(iv);

  let verdictText = '';
  if (mos == null) {
    verdictText = 'Could not determine vs. current price';
  } else if (mos > 15) {
    card.classList.add('undervalued');
    verdictText = `Trading ${fmtPct(mos)} below intrinsic value — potentially undervalued`;
  } else if (mos < -15) {
    card.classList.add('overvalued');
    verdictText = `Trading ${fmtPct(Math.abs(mos))} above intrinsic value — potentially overvalued`;
  } else {
    card.classList.add('fair');
    verdictText = 'Trading near estimated intrinsic value — roughly fairly priced';
  }
  $('verdictSub').textContent = verdictText;

  if (mos != null) {
    const mosEl = $('mosValue');
    mosEl.textContent = (mos > 0 ? '+' : '') + fmtPct(mos);
    mosEl.style.color = mos > 0 ? 'var(--green)' : mos < -15 ? 'var(--red)' : 'var(--yellow)';

    const barPct = Math.min(Math.abs(mos), 100);
    const bar = $('mosBar');
    bar.style.width = barPct + '%';
    bar.style.background = mos > 0 ? 'var(--green)' : 'var(--red)';
  } else {
    $('mosValue').textContent = '—';
  }

  // ── Valuation multiples ────────────────────────────────────────
  $('valuationMetrics').innerHTML = [
    metricRow('Market Cap',     fmtBig(d.market_cap)),
    metricRow('P/E (TTM)',      fmtX(d.pe_ratio),     d.pe_ratio > 35 ? 'red' : d.pe_ratio < 15 ? 'green' : ''),
    metricRow('Forward P/E',   fmtX(d.forward_pe),   d.forward_pe > 30 ? 'red' : d.forward_pe < 12 ? 'green' : ''),
    metricRow('P/S Ratio',     fmtX(d.ps_ratio)),
    metricRow('P/B Ratio',     fmtX(d.pb_ratio)),
    metricRow('EV/EBITDA',     fmtX(d.ev_ebitda)),
    metricRow('EV/Revenue',    fmtX(d.ev_revenue)),
    metricRow('Analyst Target', fmtPrice(d.target_price)),
    metricRow('Analyst Rating', fmtRating(d.analyst_rating)),
  ].join('');

  // ── DCF assumptions ────────────────────────────────────────────
  $('dcfAssumptions').innerHTML = [
    metricRow('WACC',            fmtPct(d.wacc)),
    metricRow('Cost of Equity',  fmtPct(d.cost_of_equity)),
    metricRow('Cost of Debt',    fmtPct(d.cost_of_debt)),
    metricRow('Tax Rate',        fmtPct(d.tax_rate)),
    metricRow('Beta',            fmt(d.beta)),
    metricRow('Stage 1 Growth',  fmtPct(d.stage1_growth), 'green'),
    metricRow('Stage 2 Growth',  fmtPct(d.stage2_growth), 'green'),
    metricRow('Terminal Growth', fmtPct(d.terminal_growth)),
    metricRow('PV of FCFs',      fmtBig(d.total_pv_fcf)),
    metricRow('PV Terminal Val', fmtBig(d.pv_terminal)),
    metricRow('Terminal Val %',  fmtPct(d.terminal_value_pct)),
  ].join('');

  // ── Financials ─────────────────────────────────────────────────
  $('financialMetrics').innerHTML = [
    metricRow('Revenue',         fmtBig(d.revenue)),
    metricRow('EBITDA',          fmtBig(d.ebitda)),
    metricRow('Gross Margin',    fmtPct(d.gross_margin),    d.gross_margin > 40 ? 'green' : ''),
    metricRow('Operating Margin',fmtPct(d.operating_margin),colorClass(d.operating_margin)),
    metricRow('Profit Margin',   fmtPct(d.profit_margin),   colorClass(d.profit_margin)),
    metricRow('Revenue Growth',  fmtPct(d.revenue_growth),  colorClass(d.revenue_growth)),
    metricRow('Earnings Growth', fmtPct(d.earnings_growth), colorClass(d.earnings_growth)),
    metricRow('ROE',             fmtPct(d.roe),             colorClass(d.roe)),
    metricRow('ROA',             fmtPct(d.roa),             colorClass(d.roa)),
    metricRow('Dividend Yield',  fmtPct(d.dividend_yield)),
  ].join('');

  // ── Health ─────────────────────────────────────────────────────
  $('healthMetrics').innerHTML = [
    metricRow('Total Cash',     fmtBig(d.total_cash)),
    metricRow('Total Debt',     fmtBig(d.total_debt)),
    metricRow('Net Debt',       fmtBig(d.net_debt), d.net_debt < 0 ? 'green' : d.net_debt > 0 ? 'red' : ''),
    metricRow('Debt/Equity',    d.debt_to_equity != null ? fmt(d.debt_to_equity / 100, 2) + 'x' : '—',
              d.debt_to_equity > 200 ? 'red' : d.debt_to_equity < 50 ? 'green' : ''),
    metricRow('Current Ratio',  fmtX(d.current_ratio),  d.current_ratio > 2 ? 'green' : d.current_ratio < 1 ? 'red' : ''),
    metricRow('Quick Ratio',    fmtX(d.quick_ratio),    d.quick_ratio > 1 ? 'green' : d.quick_ratio < 0.5 ? 'red' : ''),
    metricRow('Shares Out.',    fmtBig(d.shares_outstanding ? d.shares_outstanding : null)),
    metricRow('Payout Ratio',   fmtPct(d.payout_ratio)),
  ].join('');

  // ── Projection table ───────────────────────────────────────────
  const tbody = $('projectionBody');
  tbody.innerHTML = '';
  const half = Math.floor(d.projection_years / 2);

  (d.projected_fcf || []).forEach(row => {
    const tr = document.createElement('tr');
    if (row.year > half) tr.classList.add('stage2');
    tr.innerHTML = `
      <td>Year ${row.year}</td>
      <td>${row.year <= half ? 'Stage 1' : 'Stage 2'}</td>
      <td>${fmtPct(row.growth * 100)}</td>
      <td>${fmtBig(row.fcf)}</td>
      <td>${fmtBig(row.pv)}</td>
    `;
    tbody.appendChild(tr);
  });

  $('projectionFoot').innerHTML = `
    <tr>
      <td colspan="3">Terminal Value (PV)</td>
      <td>${fmtBig(d.pv_terminal / (1 + d.terminal_growth / 100))}</td>
      <td>${fmtBig(d.pv_terminal)}</td>
    </tr>
    <tr>
      <td colspan="3"><strong>Enterprise Value</strong></td>
      <td></td>
      <td><strong>${fmtBig(d.enterprise_value)}</strong></td>
    </tr>
    <tr>
      <td colspan="3">Less: Net Debt</td>
      <td></td>
      <td>${fmtBig(d.net_debt)}</td>
    </tr>
    <tr>
      <td colspan="3"><strong>Equity Value</strong></td>
      <td></td>
      <td><strong>${fmtBig(d.equity_value)}</strong></td>
    </tr>
    <tr>
      <td colspan="3"><strong>Intrinsic Value / Share</strong></td>
      <td></td>
      <td><strong>${fmtPrice(d.intrinsic_value)}</strong></td>
    </tr>
  `;

  // ── Bridge chart ───────────────────────────────────────────────
  renderBridge(d);

  // ── Historical FCF chart ───────────────────────────────────────
  renderHistFCF(d);
}

function renderBridge(d) {
  const items = [
    { label: 'PV of FCFs',    val: d.total_pv_fcf,   color: 'var(--accent)' },
    { label: 'PV Terminal',   val: d.pv_terminal,     color: '#6366f1' },
    { label: 'Enterprise Val',val: d.enterprise_value,color: 'var(--muted)' },
    { label: '– Net Debt',    val: -d.net_debt,       color: d.net_debt > 0 ? 'var(--red)' : 'var(--green)' },
    { label: 'Equity Value',  val: d.equity_value,    color: 'var(--yellow)' },
  ];

  const maxVal = Math.max(...items.map(i => Math.abs(i.val)));
  const MAX_H = 160;

  $('bridgeChart').innerHTML = items.map(item => {
    const h = Math.max(Math.round((Math.abs(item.val) / maxVal) * MAX_H), 8);
    return `<div class="bridge-item">
      <div class="bridge-val">${fmtBig(item.val)}</div>
      <div class="bridge-bar" style="height:${h}px; background:${item.color};"></div>
      <div class="bridge-label">${item.label}</div>
    </div>`;
  }).join('');
}

function renderHistFCF(d) {
  const vals = d.historical_fcf || [];
  if (!vals.length) { $('histChart').innerHTML = '<p style="color:var(--muted);font-size:13px">No historical FCF data available.</p>'; return; }

  const maxAbs = Math.max(...vals.map(Math.abs));
  const MAX_H = 120;
  const labels = ['TTM', 'FY-1', 'FY-2', 'FY-3', 'FY-4'];

  $('histChart').innerHTML = vals.map((v, i) => {
    const h = Math.max(Math.round((Math.abs(v) / maxAbs) * MAX_H), 4);
    const color = v >= 0 ? 'var(--green)' : 'var(--red)';
    return `<div class="hist-bar-wrap">
      <div class="hist-val">${fmtBig(v)}</div>
      <div class="hist-bar" style="height:${h}px; background:${color};"></div>
      <div class="hist-label">${labels[i] || 'FY-' + i}</div>
    </div>`;
  }).join('');
}

function fmtRating(r) {
  if (!r || r === 'N/A') return '—';
  const map = {
    strongbuy: 'Strong Buy', buy: 'Buy', hold: 'Hold',
    underperform: 'Underperform', sell: 'Sell'
  };
  const label = map[r.toLowerCase()] || r;
  const cls = ['buy','strongbuy'].includes(r.toLowerCase()) ? 'green'
             : ['sell','underperform'].includes(r.toLowerCase()) ? 'red' : '';
  return `<span class="${cls}">${label}</span>`;
}
