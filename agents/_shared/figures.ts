/**
 * Key figures: one value per field, derived once from the context's statements,
 * so every agent that cites net debt, cash, margins, runway or recent results
 * cites the same number.
 *
 * The rules, applied the same way for every agent:
 *   balance figures   the most recent balance sheet: latest quarter, else latest fiscal year
 *   total debt        as reported; zero when that same balance sheet reports cash and equity
 *                     but no debt line (filers with no borrowings leave it out)
 *   cash              cash, equivalents and short-term investments when the filer reports
 *                     them, else cash and equivalents
 *   net debt          total debt minus that cash
 *   flow figures      trailing twelve months, the sum of the last four quarters when all four
 *                     are present and contiguous, else the latest fiscal year (flowBasis says which)
 *   cash runway       cash against the larger free cash flow burn of the trailing twelve months
 *                     and the last fiscal year, so one strong quarter cannot hide a year of burn
 *
 * Vendor summary fields that define the same things differently (yfinance
 * operatingMargins is the latest quarter; debtToEquity uses its own debt) and
 * the valuation engine's own net debt are deliberately not used here.
 */

import { freeCashFlowOf, isNum, money, pct, quantity, ratio, round } from './format.ts';
import type { FinancialStatements, StatementPeriod } from './types.ts';

export const KEY_FIGURES_TITLE = 'Key figures (every reviewer sees these same figures; cite them)';

type Basis = 'ttm' | 'annual';

export interface KeyFigures {
  /** Period end of the balance sheet the balance figures come from. */
  balanceSheetDate: string | null;
  totalDebt: number | null;
  /** True when that balance sheet reports cash and equity but no debt line, so debt is taken as zero. */
  debtAssumedZero: boolean;
  cash: number | null;
  cashBasis: 'with_short_term_investments' | 'cash_and_equivalents' | null;
  netDebt: number | null;
  shareholdersEquity: number | null;
  debtToEquity: number | null;
  flowBasis: Basis | null;
  /** Period end of the latest quarter (ttm) or fiscal year (annual). */
  flowPeriodEnd: string | null;
  revenue: number | null;
  operatingIncome: number | null;
  operatingMarginPct: number | null;
  netIncome: number | null;
  operatingCashFlow: number | null;
  freeCashFlow: number | null;
  stockCompensation: number | null;
  dilutedShares: number | null;
  /** burning: runway computed; not_burning: free cash flow is not negative on either basis. */
  cashRunwayStatus: 'burning' | 'not_burning' | 'not_computable';
  /** Yearly free cash flow burn behind the runway, as a positive number: the larger of ttm and the last fiscal year. */
  cashBurn: number | null;
  cashBurnBasis: Basis | null;
  cashBurnPeriodEnd: string | null;
  cashRunwayYears: number | null;
}

const DAY_MS = 86_400_000;

export function keyFigures(statements: FinancialStatements): KeyFigures {
  const annual = statements.annual ?? [];
  const quarterly = statements.quarterly ?? [];
  const latestAnnual = annual[0];
  const latestQuarter = quarterly[0];

  // Balance sheet: the most recent period that reports any balance figure.
  const balance = [latestQuarter, latestAnnual]
    .filter((p): p is StatementPeriod =>
      p !== undefined && (isNum(p.totalDebt) || isNum(cashOf(p).value) || isNum(p.shareholdersEquity)),
    )
    .sort((a, b) => b.periodEnd.localeCompare(a.periodEnd))[0];
  const cash = balance ? cashOf(balance) : { value: null, basis: null };
  const equity = balance?.shareholdersEquity ?? null;
  // A balance sheet complete enough to show cash and equity but no debt line has no borrowings.
  const debtAssumedZero = balance !== undefined && !isNum(balance.totalDebt) && isNum(cash.value) && isNum(equity);
  const totalDebt = isNum(balance?.totalDebt) ? balance.totalDebt : debtAssumedZero ? 0 : null;

  // Flows: trailing twelve months when four contiguous quarters report revenue.
  const lastFour = quarterly.slice(0, 4);
  const ttm = contiguousQuarters(lastFour) && lastFour.every((p) => isNum(p.revenue));
  const flowPeriods = ttm ? lastFour : latestAnnual ? [latestAnnual] : [];
  const sum = (pick: (p: StatementPeriod) => number | null | undefined) => sumOf(flowPeriods, pick);

  const revenue = sum((p) => p.revenue);
  const operatingIncome = sum((p) => p.operatingIncome);
  const freeCashFlow = sum(freeCashFlowOf);

  // Runway: the larger burn of the trailing twelve months and the last fiscal year.
  const fiscalYearFcf = freeCashFlowOf(latestAnnual);
  const candidates: Array<{ fcf: number | null; basis: Basis; periodEnd: string | null }> = [
    { fcf: ttm ? freeCashFlow : null, basis: 'ttm', periodEnd: lastFour[0]?.periodEnd ?? null },
    { fcf: fiscalYearFcf, basis: 'annual', periodEnd: latestAnnual?.periodEnd ?? null },
  ];
  const burn = candidates
    .filter((c): c is { fcf: number; basis: Basis; periodEnd: string | null } => isNum(c.fcf) && c.fcf < 0)
    .sort((a, b) => a.fcf - b.fcf)[0];
  const cashRunwayStatus = !isNum(cash.value)
    ? 'not_computable'
    : burn
      ? 'burning'
      : candidates.some((c) => isNum(c.fcf))
        ? 'not_burning'
        : 'not_computable';

  return {
    balanceSheetDate: balance?.periodEnd ?? null,
    totalDebt,
    debtAssumedZero,
    cash: cash.value,
    cashBasis: cash.basis,
    netDebt: isNum(totalDebt) && isNum(cash.value) ? totalDebt - cash.value : null,
    shareholdersEquity: equity,
    debtToEquity: isNum(totalDebt) && isNum(equity) && equity > 0 ? round(totalDebt / equity, 2) : null,
    flowBasis: flowPeriods.length === 0 ? null : ttm ? 'ttm' : 'annual',
    flowPeriodEnd: flowPeriods[0]?.periodEnd ?? null,
    revenue,
    operatingIncome,
    operatingMarginPct:
      isNum(revenue) && revenue > 0 && isNum(operatingIncome) ? round((operatingIncome / revenue) * 100, 1) : null,
    netIncome: sum((p) => p.netIncome),
    operatingCashFlow: sum((p) => p.operatingCashFlow),
    freeCashFlow,
    stockCompensation: sum((p) => p.stockCompensation),
    dilutedShares: flowPeriods[0]?.dilutedShares ?? null,
    cashRunwayStatus,
    cashBurn: burn ? -burn.fcf : null,
    cashBurnBasis: burn?.basis ?? null,
    cashBurnPeriodEnd: burn?.periodEnd ?? null,
    cashRunwayYears: burn && isNum(cash.value) ? round(cash.value / -burn.fcf, 1) : null,
  };
}

export function renderKeyFigures(f: KeyFigures, currency: string | null): string {
  const flow =
    f.flowBasis === 'ttm'
      ? `trailing twelve months to ${f.flowPeriodEnd}`
      : f.flowBasis === 'annual'
        ? `fiscal year to ${f.flowPeriodEnd}`
        : 'not available';
  const cashLabel = f.cashBasis === 'cash_and_equivalents' ? 'cash and equivalents' : 'cash and short-term investments';
  const debt = f.debtAssumedZero ? 'total debt none reported (taken as zero)' : `total debt ${money(f.totalDebt, currency)}`;
  return [
    `Results, ${flow}: revenue ${money(f.revenue, currency)}, operating income ${money(f.operatingIncome, currency)} ` +
      `(operating margin ${pct(f.operatingMarginPct)}), net income ${money(f.netIncome, currency)}, ` +
      `operating cash flow ${money(f.operatingCashFlow, currency)}, free cash flow ${money(f.freeCashFlow, currency)}, ` +
      `stock-based compensation ${money(f.stockCompensation, currency)}`,
    `Balance sheet at ${f.balanceSheetDate ?? 'n/a'}: ${debt}, ` +
      `${cashLabel} ${money(f.cash, currency)}, net debt ${money(f.netDebt, currency)}, ` +
      `shareholders' equity ${money(f.shareholdersEquity, currency)}, debt to equity ${ratio(f.debtToEquity)}`,
    `Cash runway: ${renderRunway(f, currency)}`,
    `Diluted shares, latest period: ${quantity(f.dilutedShares)}`,
  ].join('\n');
}

function renderRunway(f: KeyFigures, currency: string | null): string {
  const periods = f.flowBasis === 'ttm' ? 'the trailing twelve months or the last fiscal year' : 'the last fiscal year';
  switch (f.cashRunwayStatus) {
    case 'burning': {
      const basis =
        f.cashBurnBasis === 'ttm'
          ? `trailing twelve months to ${f.cashBurnPeriodEnd}`
          : `fiscal year to ${f.cashBurnPeriodEnd}`;
      return (
        `${f.cashRunwayYears} years, cash against free cash flow burn of ${money(f.cashBurn, currency)} a year ` +
        `(${basis}, the larger burn of the trailing twelve months and the last fiscal year)`
      );
    }
    case 'not_burning':
      return `not burning cash (free cash flow is not negative over ${periods})`;
    case 'not_computable':
      return 'not computable';
  }
}

function cashOf(period: StatementPeriod): { value: number | null; basis: KeyFigures['cashBasis'] } {
  if (isNum(period.cashAndShortTermInvestments)) {
    return { value: period.cashAndShortTermInvestments, basis: 'with_short_term_investments' };
  }
  if (isNum(period.cashAndEquivalents)) {
    return { value: period.cashAndEquivalents, basis: 'cash_and_equivalents' };
  }
  return { value: null, basis: null };
}

/** Four quarters, newest first, each 75 to 105 days after the next. */
function contiguousQuarters(quarters: StatementPeriod[]): boolean {
  if (quarters.length < 4) return false;
  for (let i = 0; i < 3; i++) {
    const gap = (Date.parse(quarters[i]?.periodEnd ?? '') - Date.parse(quarters[i + 1]?.periodEnd ?? '')) / DAY_MS;
    if (!(gap >= 75 && gap <= 105)) return false;
  }
  return true;
}

/** Null unless every period reports the value: one basis per figure, never a mix. */
function sumOf(periods: StatementPeriod[], pick: (p: StatementPeriod) => number | null | undefined): number | null {
  if (periods.length === 0) return null;
  let total = 0;
  for (const period of periods) {
    const value = pick(period);
    if (!isNum(value)) return null;
    total += value;
  }
  return total;
}
