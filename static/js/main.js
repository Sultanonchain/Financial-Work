/* ── Helpers ──────────────────────────────────────────────────── */
const $ = id => document.getElementById(id);

function fmt(n, digits = 2) {
  if (n == null || n === '' || isNaN(n)) return '—';
  return Number(n).toLocaleString('en-US', {
    minimumFractionDigits: digits, maximumFractionDigits: digits
  });
}
function fmtBig(n) {
  if (n == null || isNaN(n)) return '—';
  const abs = Math.abs(n), sign = n < 0 ? '-' : '';
  if (abs >= 1e12) return sign + '$' + fmt(abs / 1e12) + 'T';
  if (abs >= 1e9)  return sign + '$' + fmt(abs / 1e9)  + 'B';
  if (abs >= 1e6)  return sign + '$' + fmt(abs / 1e6)  + 'M';
  return sign + '$' + fmt(abs);
}
function fmtPct(n, signed = false) {
  if (n == null || isNaN(n)) return '—';
  const s = signed && n > 0 ? '+' : '';
  return s + fmt(n, 1) + '%';
}
function fmtX(n)     { return n == null || isNaN(n) ? '—' : fmt(n, 1) + 'x'; }
function fmtPrice(n) { return n == null || isNaN(n) ? '—' : '$' + fmt(n, 2); }

function colorClass(n, positiveGood = true) {
  if (n == null || isNaN(n)) return 'muted';
  return (n > 0) === positiveGood ? 'green' : 'red';
}

function row(label, value, cls = '') {
  return `<div class="metric-row">
    <span class="label">${label}</span>
    <span class="value ${cls}">${value}</span>
  </div>`;
}

/* ── State ────────────────────────────────────────────────────── */
let priceChartInst = null;
let dcfChartInst   = null;

/* ── Advanced panel ───────────────────────────────────────────── */
function toggleAdvanced() {
  const panel = $('advancedPanel');
  const open  = !panel.classList.contains('hidden');
  panel.classList.toggle('hidden', open);
  $('advToggleLabel').textContent = open ? '⚙ Customize assumptions' : '▲ Hide assumptions';
}

/* ── Enter key ────────────────────────────────────────────────── */
$('tickerInput').addEventListener('keydown', e => { if (e.key === 'Enter') runAnalysis(); });

/* ── Main ─────────────────────────────────────────────────────── */
async function runAnalysis() {
  const ticker = $('tickerInput').value.trim().toUpperCase();
  if (!ticker) { $('tickerInput').focus(); return; }

  const params = new URLSearchParams({ ticker });
  const g1 = $('growth1').value;
  const g2 = $('growth2').value;
  const tg = $('terminalGrowth').value;
  const yrs = $('projYears').value;
  if (g1)  params.set('growth1',  parseFloat(g1)  / 100);
  if (g2)  params.set('growth2',  parseFloat(g2)  / 100);
  if (tg)  params.set('terminal', parseFloat(tg)  / 100);
  if (yrs) params.set('years',    parseInt(yrs));

  $('loadingTicker').textContent = ticker;
  show('loadingState'); hide('results'); hide('errorState');
  $('analyzeBtn').disabled = true;

  try {
    const resp = await fetch('/api/analyze?' + params);
    const data = await resp.json();
    if (!resp.ok || data.error) { showError(data.error || 'Something went wrong.'); return; }
    renderResults(data);
    show('results');
    $('results').scrollIntoView({ behavior: 'smooth', block: 'start' });
  } catch (e) {
    showError('Network error — make sure the server is running.');
  } finally {
    hide('loadingState');
    $('analyzeBtn').disabled = false;
  }
}

function show(id) { $(id).classList.remove('hidden'); }
function hide(id) { $(id).classList.add('hidden');    }
function showError(msg) {
  $('errorMsg').textContent = msg;
  show('errorState'); hide('loadingState');
}

/* ── Render ───────────────────────────────────────────────────── */
function renderResults(d) {
  /* Company header */
  $('companyName').textContent   = d.company_name;
  $('tickerBadge').textContent   = d.ticker;
  $('sectorBadge').textContent   = d.sector   || 'N/A';
  $('industryBadge').textContent = d.industry || 'N/A';
  $('currentPrice').textContent  = fmtPrice(d.current_price);

  const metaParts = [];
  if (d['52w_low'] && d['52w_high'])
    metaParts.push(`52W: ${fmtPrice(d['52w_low'])} – ${fmtPrice(d['52w_high'])}`);
  if (d.currency && d.currency !== 'USD') metaParts.push(d.currency);
  $('priceMeta').textContent = metaParts.join('  ·  ');

  /* Verdict */
  const iv  = d.intrinsic_value;
  const mos = d.margin_of_safety;
  const card = $('verdictCard');
  card.className = 'verdict-card';

  $('intrinsicValue').textContent = fmtPrice(iv);

  let verdictText = '';
  if (mos == null) {
    verdictText = 'Could not compare to current price';
  } else if (mos > 15) {
    card.classList.add('undervalued');
    verdictText = `${fmtPct(mos)} below intrinsic value — potentially undervalued`;
  } else if (mos < -15) {
    card.classList.add('overvalued');
    verdictText = `${fmtPct(Math.abs(mos))} above intrinsic value — potentially overvalued`;
  } else {
    card.classList.add('fair');
    verdictText = 'Trading close to estimated intrinsic value';
  }
  $('verdictSub').textContent = verdictText;

  if (mos != null) {
    const el = $('mosValue');
    el.textContent = (mos > 0 ? '+' : '') + fmtPct(mos);
    const color = mos > 15 ? 'var(--green)' : mos < -15 ? 'var(--red)' : 'var(--yellow)';
    el.style.color = color;
    const fill = $('mosFill');
    fill.style.width   = Math.min(Math.abs(mos), 100) + '%';
    fill.style.background = color;
    $('mosHint').textContent =
      mos > 0 ? 'Stock trades below DCF value' : 'Stock trades above DCF value';
  } else {
    $('mosValue').textContent = '—';
  }

  /* Valuation multiples */
  $('valuationMetrics').innerHTML = [
    row('Market Cap',      fmtBig(d.market_cap)),
    row('P/E (TTM)',       fmtX(d.pe_ratio),      d.pe_ratio > 35 ? 'red' : d.pe_ratio != null && d.pe_ratio < 15 ? 'green' : ''),
    row('Forward P/E',    fmtX(d.forward_pe),    d.forward_pe > 30 ? 'red' : d.forward_pe != null && d.forward_pe < 12 ? 'green' : ''),
    row('P/S Ratio',      fmtX(d.ps_ratio)),
    row('P/B Ratio',      fmtX(d.pb_ratio)),
    row('EV/EBITDA',      fmtX(d.ev_ebitda)),
    row('EV/Revenue',     fmtX(d.ev_revenue)),
    row('Analyst Target', fmtPrice(d.target_price)),
    row('Analyst Rating', ratingBadge(d.analyst_rating, d.analyst_count)),
  ].join('');

  /* DCF Assumptions */
  $('dcfAssumptions').innerHTML = [
    row('WACC',             fmtPct(d.wacc)),
    row('Cost of Equity',   fmtPct(d.cost_of_equity)),
    row('Cost of Debt',     fmtPct(d.cost_of_debt)),
    row('Tax Rate',         fmtPct(d.tax_rate)),
    row('Beta',             fmt(d.beta)),
    row('Stage 1 Growth',   fmtPct(d.stage1_growth), 'green'),
    row('Stage 2 Growth',   fmtPct(d.stage2_growth), 'green'),
    row('Terminal Growth',  fmtPct(d.terminal_growth)),
    row('PV of FCFs',       fmtBig(d.total_pv_fcf)),
    row('PV Terminal Val',  fmtBig(d.pv_terminal)),
    row('Terminal % of EV', fmtPct(d.terminal_value_pct)),
  ].join('');

  /* Financials */
  $('financialMetrics').innerHTML = [
    row('Revenue',          fmtBig(d.revenue)),
    row('EBITDA',           fmtBig(d.ebitda)),
    row('Gross Margin',     fmtPct(d.gross_margin),     d.gross_margin > 40 ? 'green' : ''),
    row('Operating Margin', fmtPct(d.operating_margin), colorClass(d.operating_margin)),
    row('Net Margin',       fmtPct(d.profit_margin),    colorClass(d.profit_margin)),
    row('Revenue Growth',   fmtPct(d.revenue_growth),   colorClass(d.revenue_growth)),
    row('Earnings Growth',  fmtPct(d.earnings_growth),  colorClass(d.earnings_growth)),
    row('ROE',              fmtPct(d.roe),               colorClass(d.roe)),
    row('ROA',              fmtPct(d.roa),               colorClass(d.roa)),
    row('Dividend Yield',   fmtPct(d.dividend_yield)),
  ].join('');

  /* Health */
  const deRatio = d.debt_to_equity != null ? fmt(d.debt_to_equity / 100, 2) + 'x' : '—';
  $('healthMetrics').innerHTML = [
    row('Total Cash',   fmtBig(d.total_cash)),
    row('Total Debt',   fmtBig(d.total_debt)),
    row('Net Debt',     fmtBig(d.net_debt),         colorClass(d.net_debt, false)),
    row('Debt/Equity',  deRatio,                     d.debt_to_equity > 200 ? 'red' : d.debt_to_equity != null && d.debt_to_equity < 50 ? 'green' : ''),
    row('Current Ratio',fmtX(d.current_ratio),       d.current_ratio > 2 ? 'green' : d.current_ratio != null && d.current_ratio < 1 ? 'red' : ''),
    row('Quick Ratio',  fmtX(d.quick_ratio),         d.quick_ratio  > 1 ? 'green' : d.quick_ratio  != null && d.quick_ratio  < 0.5 ? 'red' : ''),
    row('Shares Out.',  fmtBig(d.shares_outstanding)),
    row('Payout Ratio', fmtPct(d.payout_ratio)),
  ].join('');

  /* Charts */
  renderPriceChart(d);
  renderDcfChart(d);

  /* Projection table */
  const half = Math.floor(d.projection_years / 2);
  $('projectionBody').innerHTML = (d.projected_fcf || []).map(r => `
    <tr class="${r.year > half ? 'stage2' : ''}">
      <td>Year ${r.year}</td>
      <td>${r.year <= half ? 'Stage 1' : 'Stage 2'}</td>
      <td>${fmtPct(r.growth * 100)}</td>
      <td>${fmtBig(r.fcf)}</td>
      <td>${fmtBig(r.pv)}</td>
    </tr>`).join('');

  $('projectionFoot').innerHTML = `
    <tr><td colspan="3">Terminal Value (PV)</td><td></td><td>${fmtBig(d.pv_terminal)}</td></tr>
    <tr><td colspan="3"><strong>Enterprise Value</strong></td><td></td><td><strong>${fmtBig(d.enterprise_value)}</strong></td></tr>
    <tr><td colspan="3">Less: Net Debt</td><td></td><td>${fmtBig(d.net_debt)}</td></tr>
    <tr><td colspan="3"><strong>Equity Value</strong></td><td></td><td><strong>${fmtBig(d.equity_value)}</strong></td></tr>
    <tr><td colspan="3"><strong>Intrinsic Value / Share</strong></td><td></td><td><strong>${fmtPrice(d.intrinsic_value)}</strong></td></tr>`;
}

/* ── Price Chart ──────────────────────────────────────────────── */
function renderPriceChart(d) {
  if (priceChartInst) { priceChartInst.destroy(); priceChartInst = null; }

  const history = d.price_history || [];
  if (!history.length) { $('priceChartSub').textContent = 'No price history available'; return; }

  $('priceChartSub').textContent = `${d.ticker} · ${history.length} trading days`;

  const labels = history.map(p => p.date);
  const prices = history.map(p => p.close);
  const iv     = d.intrinsic_value;

  // colour the line: green if last price < IV, red otherwise
  const lastPrice = prices[prices.length - 1];
  const lineColor = lastPrice <= iv ? '#10b981' : '#ef4444';

  const ivLine = prices.map(() => iv);

  priceChartInst = new Chart($('priceChart').getContext('2d'), {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Price',
          data: prices,
          borderColor: lineColor,
          backgroundColor: lineColor + '14',
          fill: true,
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
        },
        {
          label: 'Intrinsic Value',
          data: ivLine,
          borderColor: '#f59e0b',
          borderWidth: 1.5,
          borderDash: [6, 4],
          pointRadius: 0,
          fill: false,
          tension: 0,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          position: 'top',
          align: 'end',
          labels: { color: '#64748b', font: { size: 11 }, boxWidth: 16, padding: 16 }
        },
        tooltip: {
          backgroundColor: '#12151e',
          borderColor: '#222636',
          borderWidth: 1,
          titleColor: '#94a3b8',
          bodyColor: '#e2e8f0',
          callbacks: {
            label: ctx => `${ctx.dataset.label}: $${ctx.parsed.y.toFixed(2)}`
          }
        }
      },
      scales: {
        x: {
          ticks: {
            color: '#64748b', font: { size: 10 }, maxTicksLimit: 8,
            callback: (_, i) => labels[i] ? labels[i].slice(5) : ''
          },
          grid: { color: '#222636' }
        },
        y: {
          ticks: { color: '#64748b', font: { size: 11 }, callback: v => '$' + v.toFixed(0) },
          grid: { color: '#222636' }
        }
      }
    }
  });
}

/* ── DCF Chart ────────────────────────────────────────────────── */
function renderDcfChart(d) {
  if (dcfChartInst) { dcfChartInst.destroy(); dcfChartInst = null; }

  const fc = d.fcf_chart;
  if (!fc) return;

  const labels = [...fc.projected.labels, 'Terminal'];
  const projected = [...fc.projected.values, null];
  const discounted = [...fc.projected.pvs, +(d.pv_terminal / 1e9).toFixed(2)];

  dcfChartInst = new Chart($('dcfChart').getContext('2d'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'Projected FCF',
          data: projected,
          backgroundColor: 'rgba(59,130,246,0.5)',
          borderColor: '#3b82f6',
          borderWidth: 1,
          borderRadius: 4,
        },
        {
          label: 'Present Value',
          data: discounted,
          backgroundColor: 'rgba(16,185,129,0.5)',
          borderColor: '#10b981',
          borderWidth: 1,
          borderRadius: 4,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          position: 'top', align: 'end',
          labels: { color: '#64748b', font: { size: 11 }, boxWidth: 16, padding: 16 }
        },
        tooltip: {
          backgroundColor: '#12151e',
          borderColor: '#222636', borderWidth: 1,
          titleColor: '#94a3b8', bodyColor: '#e2e8f0',
          callbacks: {
            label: ctx => ctx.parsed.y != null ? `${ctx.dataset.label}: $${ctx.parsed.y.toFixed(2)}B` : null
          }
        }
      },
      scales: {
        x: { ticks: { color: '#64748b', font: { size: 11 } }, grid: { color: '#222636' } },
        y: {
          ticks: { color: '#64748b', font: { size: 11 }, callback: v => '$' + v + 'B' },
          grid: { color: '#222636' }
        }
      }
    }
  });
}

/* ── Rating badge ─────────────────────────────────────────────── */
function ratingBadge(r, count) {
  if (!r || r === 'N/A' || r === 'none') return '—';
  // normalize: "strong_buy" → "strongbuy"
  const key = r.toLowerCase().replace(/_/g, '');
  const map = {
    strongbuy: 'Strong Buy', buy: 'Buy', hold: 'Hold',
    underperform: 'Underperform', sell: 'Sell'
  };
  const label = map[key] || r;
  const cls = ['buy','strongbuy'].includes(key) ? 'green'
            : ['sell','underperform'].includes(key) ? 'red'
            : 'yellow';
  const cnt = count ? ` (${count})` : '';
  return `<span class="value ${cls}">${label}${cnt}</span>`;
}
