/**
 * Rendering and small numeric helpers shared by the agents' user turns.
 *
 * Everything here is pure and deterministic (no clocks, no locale surprises)
 * so the same context always renders the same prompt, which keeps fixtures
 * stable and makes a prompt diff mean something.
 */

import type {
  CompanyProfile,
  FinancialStatements,
  NewsItem,
  PriceHistory,
  StatementPeriod,
  ValuationSnapshot,
  VendorPayload,
} from './types.ts';

export const NA = 'n/a';

/* ────────────────────────────────────────────────────────────────────────── */
/* Numbers                                                                    */
/* ────────────────────────────────────────────────────────────────────────── */

export function isNum(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

export function round(value: number, digits = 2): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

export function safeDiv(numerator: number | null | undefined, denominator: number | null | undefined): number | null {
  if (!isNum(numerator) || !isNum(denominator) || denominator === 0) return null;
  return numerator / denominator;
}

/** Compound annual growth in percent. Null unless both ends are positive. */
export function cagrPct(
  latest: number | null | undefined,
  earliest: number | null | undefined,
  years: number,
): number | null {
  if (!isNum(latest) || !isNum(earliest) || latest <= 0 || earliest <= 0 || years <= 0) {
    return null;
  }
  return round((Math.pow(latest / earliest, 1 / years) - 1) * 100, 2);
}

/**
 * Reported free cash flow, or operating cash flow less capex when the vendor
 * left FCF blank. Capex sign conventions differ across vendors, so its
 * magnitude is used.
 */
export function freeCashFlowOf(period: StatementPeriod | undefined): number | null {
  if (!period) return null;
  if (isNum(period.freeCashFlow)) return period.freeCashFlow;
  if (isNum(period.operatingCashFlow) && isNum(period.capex)) {
    return period.operatingCashFlow - Math.abs(period.capex);
  }
  return null;
}

/** ISO date from an ISO string, epoch seconds or epoch milliseconds. */
export function isoDate(value: unknown): string | null {
  if (isNum(value)) {
    const ms = value > 1e12 ? value : value * 1000;
    const date = new Date(ms);
    return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
  }
  if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value)) {
    return value.slice(0, 10);
  }
  return null;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Clock                                                                      */
/* ────────────────────────────────────────────────────────────────────────── */

let clock: () => number = Date.now;

/**
 * The one clock that date-dependent validation reads ("is this catalyst in the
 * past?"). Tests and fixture replays pin it; production uses the real time.
 */
export function setClock(next: (() => number) | null): void {
  clock = next ?? Date.now;
}

/** Today's date in UTC, YYYY-MM-DD, from the pinned or real clock. */
export function utcToday(): string {
  return new Date(clock()).toISOString().slice(0, 10);
}

/** Shift a YYYY-MM-DD date by whole days, in UTC. */
export function addDays(isoDay: string, days: number): string {
  const date = new Date(`${isoDay}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Formatting                                                                 */
/* ────────────────────────────────────────────────────────────────────────── */

function compact(abs: number): string {
  if (abs >= 1e12) return `${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e4) return `${(abs / 1e3).toFixed(1)}K`;
  return abs.toFixed(2);
}

export function money(value: number | null | undefined, currency: string | null = 'USD'): string {
  if (!isNum(value)) return NA;
  const prefix = !currency || currency === 'USD' ? '$' : `${currency} `;
  return `${value < 0 ? '-' : ''}${prefix}${compact(Math.abs(value))}`;
}

export function quantity(value: number | null | undefined): string {
  if (!isNum(value)) return NA;
  return `${value < 0 ? '-' : ''}${compact(Math.abs(value))}`;
}

export function pct(value: number | null | undefined, digits = 1, signed = false): string {
  if (!isNum(value)) return NA;
  return `${signed && value > 0 ? '+' : ''}${value.toFixed(digits)}%`;
}

export function ratio(value: number | null | undefined, digits = 2): string {
  if (!isNum(value)) return NA;
  return `${value.toFixed(digits)}x`;
}

/**
 * House style for model prose: no em or en dashes (the site copy never uses
 * them), no doubled spaces. Applied after validation, before wrapping.
 */
export function tidy(text: string): string {
  return text
    .replace(/\s*[—–]\s*/g, ', ')
    .replace(/,\s*,/g, ',')
    .replace(/[ \t]{2,}/g, ' ')
    .trim();
}

export function section(title: string, body: string): string {
  const content = body.trim();
  return `## ${title}\n${content.length ? content : '(not available)'}`;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Context blocks                                                             */
/* ────────────────────────────────────────────────────────────────────────── */

export function renderProfile(profile: CompanyProfile): string {
  const lines = [
    `Ticker: ${profile.ticker}`,
    `Name: ${profile.name}`,
    `Sector / industry: ${profile.sector ?? NA} / ${profile.industry ?? NA}`,
    `Country: ${profile.country ?? NA}`,
    `Market cap: ${money(profile.marketCap, profile.currency)}`,
    `Employees: ${isNum(profile.employees) ? String(profile.employees) : NA}`,
  ];
  const strategic = profile.strategic;
  if (strategic?.isStrategic) {
    const detail = [strategic.label, strategic.reason].filter(Boolean).join('. ');
    lines.push(`Sovereign backstop: yes${detail ? `, ${detail}` : ''}`);
  } else {
    lines.push('Sovereign backstop: none on file');
  }
  if (profile.description) {
    lines.push(`Business: ${profile.description}`);
  }
  return lines.join('\n');
}

export type NumericStatementKey = Exclude<keyof StatementPeriod, 'periodEnd'>;

const STATEMENT_COLUMNS: ReadonlyArray<[NumericStatementKey, string, 'money' | 'shares']> = [
  ['revenue', 'revenue', 'money'],
  ['grossProfit', 'gross profit', 'money'],
  ['operatingIncome', 'operating income', 'money'],
  ['netIncome', 'net income', 'money'],
  ['operatingCashFlow', 'operating cash flow', 'money'],
  ['capex', 'capex', 'money'],
  ['freeCashFlow', 'free cash flow', 'money'],
  ['stockCompensation', 'stock comp', 'money'],
  ['totalDebt', 'total debt', 'money'],
  ['cashAndEquivalents', 'cash', 'money'],
  ['shareholdersEquity', 'equity', 'money'],
  ['dilutedShares', 'diluted shares', 'shares'],
];

export function renderStatements(
  statements: FinancialStatements,
  options: { annual?: number; quarterly?: number; columns?: readonly NumericStatementKey[] } = {},
): string {
  const currency = statements.currency;
  const columns = options.columns
    ? STATEMENT_COLUMNS.filter(([key]) => options.columns?.includes(key))
    : STATEMENT_COLUMNS;
  const table = (label: string, periods: StatementPeriod[]): string => {
    if (!periods.length) return `${label}: not available`;
    const header = ['period', ...columns.map(([, name]) => name)].join(' | ');
    const rows = periods.map((period) =>
      [
        period.periodEnd,
        ...columns.map(([key, , kind]) =>
          kind === 'shares' ? quantity(period[key]) : money(period[key], currency),
        ),
      ].join(' | '),
    );
    return [`${label}, newest first (${currency ?? 'currency n/a'})`, header, ...rows].join('\n');
  };

  const parts = [table('Annual', statements.annual.slice(0, options.annual ?? 5))];
  const quarters = options.quarterly ?? 4;
  if (quarters > 0) {
    parts.push(table('Quarterly', statements.quarterly.slice(0, quarters)));
  }
  return parts.join('\n\n');
}

export function renderPrices(prices: PriceHistory): string {
  const currency = prices.currency;
  const r = prices.returnsPct;
  const lines = [
    `Last price: ${money(prices.last, currency)}`,
    `52-week range: ${money(prices.fiftyTwoWeekLow, currency)} to ${money(prices.fiftyTwoWeekHigh, currency)}`,
    `Returns: 1m ${pct(r.m1, 1, true)}, 3m ${pct(r.m3, 1, true)}, 6m ${pct(r.m6, 1, true)}, YTD ${pct(r.ytd, 1, true)}`,
    `Tape regime (computed): ${prices.regime ?? NA}`,
  ];

  // Month-end closes give the shape of the last year without dumping ~250
  // daily points into the prompt. Points arrive oldest first, so the last
  // write for each month is its final close.
  const byMonth = new Map<string, number>();
  for (const point of prices.points) {
    if (isNum(point.close)) byMonth.set(point.date.slice(0, 7), point.close);
  }
  const monthly = [...byMonth].slice(-13);
  if (monthly.length) {
    lines.push(
      `Month-end closes: ${monthly.map(([month, close]) => `${month} ${money(close, currency)}`).join(', ')}`,
    );
  }
  return lines.join('\n');
}

export function renderValuation(valuation: ValuationSnapshot | null, currency: string | null): string {
  if (!valuation) return 'Valuation engine output: not available';
  const v = valuation;
  const lines = [
    `Price: ${money(v.price, currency)}`,
    `Intrinsic value: ${money(v.intrinsicValue, currency)}`,
    `Intrinsic value range: ${money(v.ivLow, currency)} to ${money(v.ivHigh, currency)}`,
    `Margin of safety: ${pct(v.marginOfSafetyPct, 1, true)}`,
    `Discount rate (WACC): ${pct(v.waccPct, 2)}`,
    `Terminal growth: ${pct(v.terminalGrowthPct, 2)}`,
    `Stage-1 growth: ${pct(v.stage1GrowthPct, 2)}`,
    `Market-implied growth: ${pct(v.impliedGrowthPct, 2)}`,
    `Free cash flow base: ${money(v.fcfBase, currency)}`,
    `Net debt: ${money(v.netDebt, currency)}`,
    `Shares outstanding: ${quantity(v.sharesOut)}`,
    `Engine confidence: ${v.confidence ?? NA}`,
  ];
  if (v.confidenceWeaknesses.length) {
    lines.push(`Engine confidence weaknesses: ${v.confidenceWeaknesses.join('; ')}`);
  }
  if (v.qualityMetrics.length) {
    lines.push('Quality scorecard:');
    for (const m of v.qualityMetrics) {
      const unit = m.unit === 'pct' ? '%' : m.unit === 'ratio' ? 'x' : '';
      lines.push(`  ${m.label}: ${m.value ?? NA}${m.value === null ? '' : unit}${m.tier ? ` (${m.tier})` : ''}`);
    }
  }
  return lines.join('\n');
}

/** Newest first, capped. Undated items sort last. */
export function selectNews(items: NewsItem[], limit = 25): NewsItem[] {
  return [...items]
    .sort((a, b) => (b.publishedAt ?? '').localeCompare(a.publishedAt ?? ''))
    .slice(0, limit);
}

export function renderNews(items: NewsItem[]): string {
  return items
    .map((item) => {
      const head =
        `[${item.id}] ${item.publishedAt?.slice(0, 10) ?? 'undated'} | ` +
        `${item.source ?? 'unknown source'} | ${item.title}`;
      return item.summary ? `${head}\n    ${item.summary}` : head;
    })
    .join('\n');
}

/**
 * Pull named scalar keys out of a loose vendor payload. Missing, empty and
 * non-scalar values are skipped rather than rendered as noise.
 */
export function renderVendorFields(payload: VendorPayload | null, keys: readonly string[]): string {
  if (!payload) return '';
  const lines: string[] = [];
  for (const key of keys) {
    const value = payload[key];
    if (value === null || value === undefined || value === '') continue;
    if (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') {
      lines.push(`${key}: ${value}`);
    }
  }
  return lines.join('\n');
}
