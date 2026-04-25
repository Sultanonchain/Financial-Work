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
let priceChart    = null;
let dcfChart      = null;
let currentTicker = '';
let currentData   = null;
let searchTimer   = null;
let _activeData   = null;   // full result stored for toggle interactions
let _ivAnimRaf    = null;   // rAF handle for IV counter animation

/* ── IV counter animation ─────────────────────────────────── */
function animateIV(targetVal) {
  const el = $('intrinsicValue');
  if (!el) return;
  if (_ivAnimRaf) { cancelAnimationFrame(_ivAnimRaf); _ivAnimRaf = null; }
  const raw = el.textContent.replace(/[^0-9.]/g, '');
  const fromVal = parseFloat(raw) || 0;
  const diff    = targetVal - fromVal;
  if (Math.abs(diff) < 0.005) { el.textContent = fmtPrice(targetVal); return; }
  const duration = 480;
  const t0 = performance.now();
  function tick(now) {
    const t = Math.min((now - t0) / duration, 1);
    const ease = t >= 1 ? 1 : 1 - Math.pow(2, -10 * t); // easeOutExpo
    el.textContent = fmtPrice(fromVal + diff * ease);
    if (t < 1) { _ivAnimRaf = requestAnimationFrame(tick); }
    else        { el.textContent = fmtPrice(targetVal); _ivAnimRaf = null; }
  }
  _ivAnimRaf = requestAnimationFrame(tick);
}

/* ── Scenario toggle ──────────────────────────────────────── */
function activateScenario(key) {
  if (!_activeData) return;
  const d  = _activeData;
  const sc = d.scenarios;
  if (!sc) return;

  const map = {
    // Base now reads sc.base.value which is synced to d.intrinsic_value on the
    // backend when the Consensus Anchor fires, ensuring toggle ≡ card.
    base: { iv: sc.base?.value, mos: sc.base?.upside, label: 'Base Case' },
    bull: { iv: sc.bull?.value, mos: sc.bull?.upside, label: 'Bull Case' },
    bear: { iv: sc.bear?.value, mos: sc.bear?.upside, label: 'Bear Case' },
  };
  const data = map[key];
  if (!data || data.iv == null) return;

  // Toggle button states
  document.querySelectorAll('.sc-tog-btn').forEach(b =>
    b.classList.toggle('sc-tog-active', b.dataset.scenario === key));

  // Animate IV counter
  animateIV(data.iv);

  // MoS + signal badge + card colour
  const mosVal = data.mos;
  const card   = $('verdictCard');
  const sigEl  = $('signalBadge');
  const mosEl  = $('mosValue');
  const color  = mosVal > 15 ? 'var(--green)' : mosVal < -15 ? 'var(--red)' : 'var(--gold)';
  card.className = 'verdict-card';
  if      (mosVal > 15)  card.classList.add('undervalued');
  else if (mosVal < -15) card.classList.add('overvalued');
  else                   card.classList.add('fair');
  if (sigEl) {
    if (mosVal == null)   { sigEl.classList.add('hidden'); }
    else if (mosVal > 20) { sigEl.className = 'signal-badge signal-strong'; sigEl.textContent = '▲ Strong Value'; sigEl.classList.remove('hidden'); }
    else if (mosVal >= 0) { sigEl.className = 'signal-badge signal-fair';   sigEl.textContent = '◆ Fair Value';   sigEl.classList.remove('hidden'); }
    else                  { sigEl.className = 'signal-badge signal-over';   sigEl.textContent = '▼ Overvalued';   sigEl.classList.remove('hidden'); }
  }
  if (mosVal != null) {
    mosEl.textContent             = (mosVal > 0 ? '+' : '') + fmtPct(mosVal);
    mosEl.style.color             = color;
    $('mosFill').style.width      = Math.min(Math.abs(mosVal), 100) + '%';
    $('mosFill').style.background = color;
  } else {
    mosEl.textContent        = '—';
    $('mosFill').style.width = '0';
  }

  // Scenario card glow
  document.querySelectorAll('.sc-case').forEach(el => {
    el.classList.toggle('sc-case--active', el.classList.contains('sc-' + key));
  });
}

/* ── Copy Analysis Summary ────────────────────────────────── */
const VALUS_URL = 'https://valus.vercel.app'; // ← update if domain changes

function _buildMethodString(d) {
  const parts = [];
  // Base method
  if      (d.valuation_method === 'banking')    parts.push('Banking Specialist (P/B + ROE)');
  else if (d.valuation_method === 'biotech')    parts.push('Biotech Specialist (EV/Rev)');
  else if (d.valuation_method === 'dcf_energy') parts.push('Energy DCF');
  else if (!d.dcf_available && d.multiples_val) parts.push('Industry Multiples');
  else if (d.structural_transformer)            parts.push('Structural Transformer DCF');
  else if (d.moat_detected)                     parts.push('Adaptive Moat DCF');
  else                                           parts.push('DCF Model');
  // Modifiers
  if (d.backbone_stage1_extended) parts.push('Backbone 10-yr');
  if (d.analyst_adjusted)         parts.push('Consensus Anchor');
  if (d.momentum_premium_pct)     parts.push(`+${d.momentum_premium_pct}% Catalyst`);
  return parts.join(' + ');
}

function _buildSignalString(mos) {
  if (mos == null) return '';
  if (mos > 20)  return '▲ Strong Value';
  if (mos >= 0)  return '◆ Fair Value';
  return '▼ Overvalued';
}

/* ── 'Why' Hook — one-sentence contextual insight ─────────── */
function _buildWhyHook(d) {
  const mos    = d.margin_of_safety;
  const sector = (d.sector   || '').toLowerCase();
  const ind    = (d.industry || '').toLowerCase();
  const moat   = d.moat_path || '';
  const isST   = d.structural_transformer;
  const isAnch = d.analyst_adjusted;
  const method = d.valuation_method || '';

  if (mos == null) return '';

  // Specialist methods
  if (method === 'banking')
    return mos < -10
      ? 'Trading above P/B-implied fair value; net interest margin compression is priced in.'
      : mos > 15
        ? 'P/B discount relative to ROE suggests potential margin of safety.'
        : 'Valuation aligned with book value and normalised earnings power.';
  if (method === 'biotech')
    return 'EV/Revenue blended with analyst consensus; pipeline risk drives a wide range of outcomes.';

  const isOver  = mos < -10;
  const isUnder = mos > 15;

  // ── Overvalued hooks ────────────────────────────────────────
  if (isOver) {
    if (isST)
      return 'Market pricing in platform optionality; DCF reflects near-term execution risk.';
    if (sector.includes('energy') || ind.includes('oil') || ind.includes('gas') || ind.includes('petroleum'))
      return 'Market pricing in short-term commodity premium; DCF reflects long-term production averages.';
    if (ind.includes('semiconductor') || ind.includes('chips'))
      return 'Market pricing in AI-cycle demand peak; DCF reflects normalised fab utilisation.';
    if (sector.includes('tech') || ind.includes('software') || ind.includes('internet'))
      return 'Market pricing in elevated AI/growth premium; DCF reflects normalised FCF margins.';
    if (ind.includes('internet retail') || ind.includes('broadline'))
      return 'Market pricing in ecosystem and logistics network premium; DCF reflects reported FCF margins.';
    if (sector.includes('consumer') && !ind.includes('tech'))
      return 'Market pricing in brand momentum; DCF reflects long-run structural margin mean-reversion.';
    if (sector.includes('health') || ind.includes('pharma') || ind.includes('biotech'))
      return 'Market pricing in pipeline optionality; DCF reflects only commercialised revenue streams.';
    if (ind.includes('auto') || ind.includes('vehicle'))
      return 'Market pricing in EV transition optionality; DCF reflects current unit economics.';
    if (isAnch)
      return 'Consensus Anchor applied; final IV blends DCF model with near-term analyst targets.';
    return 'Current price embeds a growth premium not yet supported by DCF fundamentals.';
  }

  // ── Undervalued hooks ───────────────────────────────────────
  if (isUnder) {
    if (moat === 'Platform Scale Economy')
      return 'Platform and high-margin segment economics suggest market underprices hidden earning power.';
    if (moat === 'High-Growth Backbone')
      return 'Durable revenue growth and margins indicate potential unrecognised intrinsic value.';
    if (moat === 'Mature Cash Machine')
      return 'Strong cash generation and earnings quality suggest a structural discount to fair value.';
    if (moat === 'Capital-Light Compounder')
      return 'Capital-efficient compounding model trades at a discount to FCF-implied intrinsic value.';
    if (isST)
      return 'Near-term CapEx depresses reported FCF; long-run platform value is materially higher.';
    if (sector.includes('energy'))
      return 'Market applying cycle-trough discount; DCF reflects long-term normalised production cash flows.';
    if (sector.includes('financial') || ind.includes('bank'))
      return 'Defensive balance sheet with a P/B discount versus ROE-implied intrinsic value.';
    return 'Trading below modelled intrinsic value — potential margin of safety for long-term investors.';
  }

  // ── Fair-value hooks ────────────────────────────────────────
  if (moat)
    return `Quality characteristics (${moat.toLowerCase()}) at a fair price — limited near-term asymmetry.`;
  return 'Price closely aligned with DCF intrinsic value — limited near-term asymmetry.';
}

function copyAnalysis() {
  if (!_activeData) return;
  const d   = _activeData;
  const iv  = d.intrinsic_value ?? d.multiples_val;
  const mos = d.margin_of_safety ?? d.multiples_mos;
  if (!iv) return;

  const signal  = _buildSignalString(mos);
  const method  = _buildMethodString(d);
  const mosLine = mos != null
    ? `Margin of Safety: ${mos > 0 ? '+' : ''}${mos.toFixed(1)}%`
    : '';

  const text = [
    `VALUS Analysis: $${d.ticker}`,
    `Intrinsic Value: $${iv.toFixed(2)}${signal ? ` (${signal.replace(/[▲◆▼] /, '')})` : ''}`,
    `Method: ${method}`,
    mosLine,
    `Analyze yours at: ${VALUS_URL}`,
  ].filter(Boolean).join('\n');

  navigator.clipboard.writeText(text).then(() => {
    const btn = $('copyBtn');
    if (!btn) return;
    const lbl = btn.querySelector('.copy-btn-label');
    btn.classList.add('copied');
    if (lbl) lbl.textContent = 'Copied!';
    setTimeout(() => {
      btn.classList.remove('copied');
      if (lbl) lbl.textContent = 'Copy Summary';
    }, 2000);
  }).catch(() => {
    // Fallback for browsers that block clipboard without user gesture
    const ta = document.createElement('textarea');
    ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
    document.body.appendChild(ta); ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    const btn = $('copyBtn');
    if (btn) { btn.classList.add('copied'); setTimeout(() => btn.classList.remove('copied'), 2000); }
  });
}

/* ── Detail drawer ────────────────────────────────────────── */
function toggleDetailDrawer() {
  const drawer = $('detailDrawer');
  const arrow  = $('viewAnalysisArrow');
  const label  = $('viewAnalysisBtnText');
  const btn    = $('viewAnalysisBtn');
  if (!drawer) return;
  const isOpen = drawer.classList.toggle('open');
  if (arrow) arrow.classList.toggle('rotated', isOpen);
  if (btn)   btn.classList.toggle('open', isOpen);
  if (label) label.textContent = isOpen
    ? 'Hide Detailed Analysis'
    : 'View Detailed Analysis \u0026 Assumptions';
  if (isOpen) {
    // Scroll trigger into view
    setTimeout(() => {
      const trigger = $('viewAnalysisTrigger');
      if (trigger) trigger.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 80);
    // Chart.js renders at zero size when the container is collapsed.
    // Resize after the CSS grid animation completes (~700ms).
    setTimeout(() => {
      if (priceChart) priceChart.resize();
      if (dcfChart)   dcfChart.resize();
    }, 750);
  }
}

function _resetDrawer() {
  const drawer = $('detailDrawer');
  const arrow  = $('viewAnalysisArrow');
  const btn    = $('viewAnalysisBtn');
  const label  = $('viewAnalysisBtnText');
  if (drawer) drawer.classList.remove('open');
  if (arrow)  arrow.classList.remove('rotated');
  if (btn)    btn.classList.remove('open');
  if (label)  label.textContent = 'View Detailed Analysis \u0026 Assumptions';
  hide('viewAnalysisTrigger');
}

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
  // Cancel any in-flight IV animation and reset toggle + drawer state
  if (_ivAnimRaf) { cancelAnimationFrame(_ivAnimRaf); _ivAnimRaf = null; }
  _activeData = null;
  _resetDrawer();
  const _prevTog = $('scenarioToggle');
  if (_prevTog) _prevTog.classList.add('hidden');

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
  // ── Company header (hero — Name, Price, Ticker only) ──────
  $('companyName').textContent  = d.company_name;
  $('tickerBadge').textContent  = d.ticker;
  $('currentPrice').textContent = fmtPrice(d.current_price);
  const parts = [];
  if (d['52w_low'] && d['52w_high']) parts.push(`52W  ${fmtPrice(d['52w_low'])} – ${fmtPrice(d['52w_high'])}`);
  if (d.currency && d.currency !== 'USD') parts.push(d.currency);
  $('priceMeta').textContent = parts.join('   ·   ');

  // ── Mini Financial Highlights (hero card) ────────────────
  const mhMarketCap = $('mhMarketCap');
  const mhPE        = $('mhPE');
  const mhDivYield  = $('mhDivYield');
  const mhTarget    = $('mhTarget');
  if (mhMarketCap) mhMarketCap.textContent = fmtBig(d.market_cap);
  if (mhPE)        mhPE.textContent        = fmtX(d.pe_ratio);
  if (mhDivYield) {
    if (d.dividend_yield != null && d.dividend_yield > 0) {
      mhDivYield.textContent = fmtPct(d.dividend_yield);
      mhDivYield.className   = 'mh-value mh-green';
    } else {
      mhDivYield.textContent = '—';
      mhDivYield.className   = 'mh-value mh-muted';
    }
  }
  if (mhTarget) mhTarget.textContent = fmtPrice(d.target_price);

  // ── Verdict — unified rendering for DCF / Specialist / Multiples ──────────
  const iv  = d.intrinsic_value;
  const mos = d.margin_of_safety;
  const sc  = d.scenarios;
  const card = $('verdictCard');
  card.className = 'verdict-card';

  // Classify what kind of value we're showing
  const isDCF        = d.dcf_available && iv != null;
  const isSpecialist = !d.dcf_available && iv != null;
  const isMultiples  = !isDCF && !isSpecialist && d.multiples_val != null;
  const hasValue     = isDCF || isSpecialist || isMultiples;

  const displayVal = isMultiples ? d.multiples_val : iv;
  const displayMos = isMultiples ? d.multiples_mos : mos;

  // ── Verdict label ──────────────────────────────────────────────────────────
  const labelEl = $('verdictLabel');
  if (labelEl) {
    if      (isDCF)        labelEl.textContent = 'DCF Intrinsic Value';
    else if (isSpecialist) labelEl.textContent = 'Specialist Valuation';
    else if (isMultiples)  labelEl.textContent = 'Multiples-Based Value';
    else                   labelEl.textContent = 'Intrinsic Value';
  }

  // ── Why hook ──────────────────────────────────────────────────────────────
  const hookEl = $('whyHook');

  if (!hasValue) {
    $('intrinsicValue').textContent = 'N/A';
    $('mosValue').textContent       = '—';
    $('mosFill').style.width        = '0';
    card.classList.add('fair');
    hide('signalBadge');
    if (hookEl) {
      hookEl.textContent = d.dcf_warning || 'Valuation not available for this security.';
      hookEl.classList.remove('hidden');
    }
  } else {
    $('intrinsicValue').textContent = fmtPrice(displayVal);

    // ── Signal badge ──────────────────────────────────────────────────────
    const sigEl = $('signalBadge');
    if (sigEl) {
      if (displayMos == null) {
        sigEl.classList.add('hidden');
      } else if (displayMos > 20) {
        sigEl.className   = 'signal-badge signal-strong';
        sigEl.textContent = '▲ Strong Value';
        sigEl.classList.remove('hidden');
      } else if (displayMos >= 0) {
        sigEl.className   = 'signal-badge signal-fair';
        sigEl.textContent = '◆ Fair Value';
        sigEl.classList.remove('hidden');
      } else {
        sigEl.className   = 'signal-badge signal-over';
        sigEl.textContent = '▼ Overvalued';
        sigEl.classList.remove('hidden');
      }
    }

    // ── Verdict card colouring ────────────────────────────────────────────
    if (displayMos > 15)       card.classList.add('undervalued');
    else if (displayMos < -15) card.classList.add('overvalued');
    else                       card.classList.add('fair');

    // ── Why hook — contextual insight ─────────────────────────────────────
    if (hookEl) {
      const hook = _buildWhyHook(d);
      if (hook) {
        hookEl.textContent = hook;
        hookEl.classList.remove('hidden');
      } else {
        hookEl.classList.add('hidden');
      }
    }

    // ── MoS track ─────────────────────────────────────────────────────────
    const color = displayMos > 15 ? 'var(--green)' : displayMos < -15 ? 'var(--red)' : 'var(--gold)';
    const mosEl = $('mosValue');
    if (displayMos != null) {
      mosEl.textContent             = (displayMos > 0 ? '+' : '') + fmtPct(displayMos);
      mosEl.style.color             = color;
      $('mosFill').style.width      = Math.min(Math.abs(displayMos), 100) + '%';
      $('mosFill').style.background = color;
    } else {
      mosEl.textContent        = '—';
      $('mosFill').style.width = '0';
    }
  }

  // Store data for toggle interactions
  _activeData = d;

  // Scenario analysis
  renderScenarios(sc, d.current_price);

  // Set up scenario toggle — visible only for full DCF with all three scenarios
  {
    const togEl = $('scenarioToggle');
    if (togEl) {
      const hasAll = isDCF && sc &&
        sc.base?.value != null && sc.bull?.value != null && sc.bear?.value != null;
      if (hasAll) {
        togEl.classList.remove('hidden');
        // Set 'base' as initial active button (no animation on first load)
        document.querySelectorAll('.sc-tog-btn').forEach(b =>
          b.classList.toggle('sc-tog-active', b.dataset.scenario === 'base'));
        // Light up the base scenario card
        const baseCard = document.querySelector('.sc-base');
        if (baseCard) baseCard.classList.add('sc-case--active');
      } else {
        togEl.classList.add('hidden');
      }
    }
  }

  // ── Quick Insights — model flags above drawer trigger ────────────────────
  renderQuickInsights(d);

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
    d.moat_detected ? row('Moat Premium', `<span style="color:var(--gold);font-size:11px">◆ ${d.moat_path} — WACC −${(d.moat_wacc_delta||0).toFixed(1)}pp</span>`) : '',
    d.backbone_stage1_extended ? row('Stage 1 Duration', `<span style="color:var(--cyan);font-size:11px">◆ Backbone: all ${d.projection_years} yrs at Stage 1 rate</span>`) : '',
    d.cash_rich_wacc_applied   ? row('Cash-Rich WACC',   `<span style="color:var(--cyan);font-size:11px">◆ $50B+ cash · WACC capped 9%</span>`)                         : '',
    d.wacc_risk_applied        ? row('Risk WACC',        `<span style="color:var(--red); font-size:11px">⚠ Material risk · WACC +1%</span>`)                            : '',
    d.momentum_premium_pct != null ? row('Catalyst Premium', `<span style="color:var(--green);font-size:11px">▲ +${d.momentum_premium_pct}% momentum premium</span>`) : '',
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

  // Show the "View Detailed Analysis" trigger now that everything is rendered
  show('viewAnalysisTrigger');
}

/* ── Live Analyst Notes (Discovery Layer) ─────────────────── */
function renderCatalystBox(d) {
  const box = $('liveCatalystBox');
  if (!box) return;

  const insights = d.catalyst_insights || [];
  const hasCat   = d.has_positive_catalyst;
  const hasRisk  = d.has_material_risk;
  const momPct   = d.momentum_premium_pct;   // e.g. 8.0 for 8%
  const waccRisk = d.wacc_risk_applied;
  const backbone = d.backbone_stage1_extended;
  const cashRich = d.cash_rich_wacc_applied;

  // Hide box if no useful data
  if (!insights.length && !hasCat && !hasRisk && !backbone && !cashRich) {
    box.classList.add('hidden');
    return;
  }

  let html = `
    <div class="catalyst-header">
      <span class="catalyst-icon">⚡</span>
      <span class="catalyst-title">Live Analyst Notes</span>
      <span class="catalyst-sub">Catalyst research · SEC 8-K · Market intelligence</span>
    </div>`;

  // Insight bullets
  if (insights.length) {
    html += `<ul class="catalyst-bullets">`;
    insights.forEach(line => {
      html += `<li class="catalyst-bullet">${escHtml(line)}</li>`;
    });
    html += `</ul>`;
  }

  // Intelligence flags strip
  let flags = '';

  if (momPct != null && momPct > 0) {
    flags += `<span class="cat-flag cat-flag--catalyst">
      ▲ Momentum Premium +${momPct}% applied to IV
    </span>`;
  }
  if (hasCat && (momPct == null || momPct === 0)) {
    flags += `<span class="cat-flag cat-flag--catalyst">◈ Positive catalyst detected</span>`;
  }
  if (waccRisk) {
    flags += `<span class="cat-flag cat-flag--risk">⚠ Material risk · WACC +1%</span>`;
  }
  if (backbone) {
    flags += `<span class="cat-flag cat-flag--moat">◆ Backbone: 10-yr Stage 1 growth</span>`;
  }
  if (cashRich) {
    flags += `<span class="cat-flag cat-flag--moat">◆ Cash-rich: WACC capped 9%</span>`;
  }

  if (flags) html += `<div class="cat-flags">${flags}</div>`;

  box.innerHTML = html;
  box.classList.remove('hidden');
}

/* ── Quick Insights — model flags shown above drawer trigger ── */
function renderQuickInsights(d) {
  const el     = $('quickInsights');
  const listEl = $('qiList');
  if (!el || !listEl) return;

  const iv    = d.intrinsic_value;
  const notes = d.dcf_notes || [];
  const items = [];

  function qi(icon, html, cls = 'qi-info') {
    items.push(`<div class="qi-item ${cls}"><span class="qi-icon">${icon}</span><span class="qi-text">${html}</span></div>`);
  }

  // ── Moat premium ──────────────────────────────────────────────────────────
  if (d.moat_detected && d.moat_path) {
    const waccLine = d.moat_wacc_delta  ? `WACC −${d.moat_wacc_delta.toFixed(1)}pp`        : '';
    const multLine = d.moat_mult_premium ? `exit multiples +${d.moat_mult_premium}%`        : '';
    const extras   = [waccLine, multLine].filter(Boolean).join(' · ');
    qi('◆', `<strong>${escHtml(d.moat_path)}</strong> moat detected${extras ? ' — ' + extras : ''}`, 'qi-cyan');
  }

  // ── Sector specialist methodology ─────────────────────────────────────────
  if (d.sector_val_label) {
    const icon = d.valuation_method === 'banking'    ? '⬢'
               : d.valuation_method === 'biotech'    ? '⬡'
               : d.valuation_method === 'dcf_energy' ? '◉' : '◈';
    qi(icon, `<strong>Specialist method</strong> — ${escHtml(d.sector_val_label)}`, 'qi-cyan');
  }

  // ── Structural Transformer (full — TSLA-style) ────────────────────────────
  if (d.structural_transformer) {
    const rate   = d.st_capex_addback_rate_pct != null ? `${d.st_capex_addback_rate_pct}%` : '50%';
    const ab     = d.st_capex_addback_bn  != null ? ` · $${d.st_capex_addback_bn.toFixed(1)}B add-back` : '';
    const robo   = d.st_robotaxi_s2_applied ? ' · +15% Robotaxi/FSD Stage 2 premium' : '';
    qi('⚡', `<strong>Structural Transformer</strong> — ${rate} CapEx treated as growth investment${ab} · 35× EV/EBITDA · WACC ≤9%${robo}`, 'qi-cyan');
  }
  // ── Platform Logistics normalisation (partial — AMZN-style) ──────────────
  else if (d.st_capex_addback_bn != null) {
    const rate = d.st_capex_addback_rate_pct != null ? `${d.st_capex_addback_rate_pct}%` : '20%';
    const ab   = `$${d.st_capex_addback_bn.toFixed(1)}B add-back`;
    qi('📦', `<strong>Platform Logistics Normalisation</strong> — ${rate} of logistics CapEx as growth investment (${ab})`, 'qi-info');
  }

  // ── Backbone (full Stage 1 for all projection years) ──────────────────────
  if (d.backbone_stage1_extended) {
    qi('◆', `<strong>Backbone Platform</strong> — Stage 1 growth extended for all ${d.projection_years || 10} projection years`, 'qi-cyan');
  }

  // ── Cash-rich WACC cap ────────────────────────────────────────────────────
  if (d.cash_rich_wacc_applied) {
    qi('◆', `<strong>Cash-Rich</strong> — $50B+ net cash position · WACC capped at 9%`, 'qi-cyan');
  }

  // ── Momentum / catalyst premium ───────────────────────────────────────────
  if (d.momentum_premium_pct != null && d.momentum_premium_pct > 0) {
    qi('▲', `<strong>Catalyst Premium</strong> — +${d.momentum_premium_pct}% applied to intrinsic value`, 'qi-good');
  }

  // ── Risk WACC uplift ──────────────────────────────────────────────────────
  if (d.wacc_risk_applied) {
    qi('⚠', `<strong>Risk Adjustment</strong> — Material risk factor detected · WACC +1%`, 'qi-risk');
  }

  // ── Consensus anchor ──────────────────────────────────────────────────────
  if (d.analyst_adjusted) {
    const preIv = d.consensus_anchor_pre_iv != null
      ? ` · Model: $${d.consensus_anchor_pre_iv.toFixed(2)} → Blended: $${iv != null ? iv.toFixed(2) : '—'}` : '';
    const atStr = d.analyst_target != null
      ? ` · Analyst consensus: $${parseFloat(d.analyst_target).toFixed(2)}` : '';
    qi('⚖', `<strong>Consensus Anchor</strong> — 70% model · 30% analyst${atStr}${preIv}`, 'qi-warn');
  }

  // ── Multiples fallback ────────────────────────────────────────────────────
  if (d.multiples_val && (!d.dcf_available || iv == null)) {
    qi('◈', `<strong>Industry Multiples</strong> — DCF unavailable; secondary valuation applied (${escHtml(d.multiples_method || '')})`, 'qi-warn');
  }

  // ── DCF warning text ──────────────────────────────────────────────────────
  if (d.dcf_warning) {
    qi('⚠', escHtml(d.dcf_warning), 'qi-warn');
  }

  // ── DCF notes ─────────────────────────────────────────────────────────────
  notes.forEach(n => {
    const cls  = n.type === 'warn' ? 'qi-warn' : 'qi-info';
    const icon = n.type === 'warn' ? '⚠' : 'ℹ';
    qi(icon, escHtml(n.text), cls);
  });

  // ── Catalyst insights ─────────────────────────────────────────────────────
  if (d.has_positive_catalyst && !(d.momentum_premium_pct > 0)) {
    qi('◈', '<strong>Positive catalyst</strong> detected in recent filings', 'qi-good');
  }
  if (d.has_material_risk && !d.wacc_risk_applied) {
    qi('⚠', '<strong>Material risk</strong> noted in recent filings', 'qi-risk');
  }
  (d.catalyst_insights || []).forEach(line => {
    qi('▸', escHtml(line), 'qi-info');
  });

  if (items.length > 0) {
    listEl.innerHTML = items.join('');
    el.classList.remove('hidden');
  } else {
    listEl.innerHTML = '';
    el.classList.add('hidden');
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

  // Transparency note — shown when Consensus Anchor was applied to scenarios
  const anchorNote = sc.consensus_anchored
    ? `<div class="sc-anchor-note">
        <span class="sc-anchor-icon">⚖</span>
        Values adjusted by Consensus Anchor for market alignment. (70% model · 30% analyst consensus)
       </div>`
    : '';

  el.innerHTML = `
    <div class="sc-header">
      <h3 class="card-title">Scenario Analysis</h3>
      <span class="card-sub">50% Base · 25% Bull · 25% Bear</span>
    </div>
    ${anchorNote}
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
