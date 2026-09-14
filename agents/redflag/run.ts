import { generateValidated, MODELS } from '../_shared/client.ts';
import {
  cagrPct,
  freeCashFlowOf,
  isNum,
  money,
  pct,
  ratio,
  renderProfile,
  renderStatements,
  renderVendorFields,
  round,
  section,
  tidy,
} from '../_shared/format.ts';
import {
  field,
  finalizeOutput,
  unavailable,
  type AgentContext,
  type AgentResult,
  type FinancialStatements,
} from '../_shared/types.ts';

import { SYSTEM_PROMPT } from './prompt.ts';
import {
  RedflagModelSchema,
  RedflagOutputSchema,
  SEVERITIES,
  type RedflagMetrics,
  type RedflagModel,
  type RedflagOutput,
} from './schema.ts';

const YFINANCE_KEYS = [
  'shortPercentOfFloat',
  'heldPercentInsiders',
  'heldPercentInstitutions',
  'debtToEquity',
  'currentRatio',
  'quickRatio',
  'auditRisk',
  'boardRisk',
] as const;
const FINVIZ_KEYS = ['Short Float', 'Short Ratio', 'Insider Own', 'Insider Trans', 'Inst Trans', 'Debt/Eq', 'Current Ratio'] as const;

/** This agent's model. VALUS_AGENT_MODEL overrides it (resolveModel in client.ts). */
export const model: string = MODELS.sonnet;

export async function run(ctx: AgentContext): Promise<AgentResult<RedflagOutput>> {
  const startedAt = Date.now();
  const base = { slug: 'redflag' as const, ticker: ctx.ticker, startedAt };

  if (ctx.statements.annual.length === 0) {
    return unavailable(
      { kind: 'insufficient_data', message: 'no annual statements on the context' },
      base,
    );
  }

  const metrics = computeMetrics(ctx.statements);

  const gen = await generateValidated({
    slug: 'redflag',
    ticker: ctx.ticker,
    system: SYSTEM_PROMPT,
    user: buildUserTurn(ctx, metrics),
    schema: RedflagModelSchema,
    effort: 'low',
    model,
    signal: ctx.signal,
  });

  const init = { ...base, model: gen.model, attempts: gen.attempts, usage: gen.usage };
  if (!gen.data) {
    return unavailable(gen.error ?? { kind: 'unknown', message: 'no data returned' }, init);
  }

  return finalizeOutput(RedflagOutputSchema, toOutput(gen.data, metrics), init);
}

function buildUserTurn(ctx: AgentContext, m: RedflagMetrics): string {
  const currency = ctx.statements.currency;
  const runway =
    m.cashRunwayYears === null ? 'not burning cash, or not computable' : `${m.cashRunwayYears} years`;
  const negativeEquity = m.negativeEquity === null ? 'n/a' : m.negativeEquity ? 'yes' : 'no';

  const vendor = [
    renderVendorFields(ctx.yfinance, YFINANCE_KEYS),
    renderVendorFields(ctx.finviz, FINVIZ_KEYS),
  ]
    .filter(Boolean)
    .join('\n');

  return [
    `Data as of ${ctx.asOf}.`,
    section('Company', renderProfile(ctx.profile)),
    section(
      `Pre-computed metrics (latest annual period vs ${m.spanYears} year(s) earlier)`,
      [
        `Annual periods available: ${m.annualPeriods}`,
        `Free cash flow / net income, latest year: ${ratio(m.fcfToNetIncome)}`,
        `Stock-based compensation as a share of free cash flow: ${pct(m.sbcPctOfFcf)}`,
        `Diluted share count change over the span: ${pct(m.shareCountChangePct, 1, true)}`,
        `Net debt: ${money(m.netDebt, currency)}`,
        `Net debt / operating cash flow: ${ratio(m.netDebtToOperatingCashFlow)}`,
        `Cash runway at the latest free cash flow burn: ${runway}`,
        `Operating margin, latest year: ${pct(m.operatingMarginPct)}`,
        `Operating margin change over the span: ${m.operatingMarginChangePp === null ? 'n/a' : `${m.operatingMarginChangePp > 0 ? '+' : ''}${m.operatingMarginChangePp} points`}`,
        `Revenue growth per year over the span: ${pct(m.revenueCagrPct, 2)}`,
        `Negative shareholder equity: ${negativeEquity}`,
      ].join('\n'),
    ),
    section('Statements', renderStatements(ctx.statements, { annual: 5, quarterly: 4 })),
    section('Vendor fields (raw; yfinance ratios are fractions)', vendor),
  ].join('\n\n');
}

function computeMetrics(statements: FinancialStatements): RedflagMetrics {
  const annual = statements.annual;
  const latest = annual[0];
  const spanIndex = Math.min(3, annual.length - 1);
  const earlier = spanIndex > 0 ? annual[spanIndex] : undefined;

  const fcf = freeCashFlowOf(latest);
  const netIncome = latest?.netIncome;
  const ocf = latest?.operatingCashFlow;
  const netDebt =
    isNum(latest?.totalDebt) && isNum(latest?.cashAndEquivalents)
      ? latest.totalDebt - latest.cashAndEquivalents
      : null;

  const marginOf = (revenue: number | null | undefined, opIncome: number | null | undefined) =>
    isNum(revenue) && revenue > 0 && isNum(opIncome) ? (opIncome / revenue) * 100 : null;
  const marginNow = marginOf(latest?.revenue, latest?.operatingIncome);
  const marginThen = marginOf(earlier?.revenue, earlier?.operatingIncome);

  const sharesNow = latest?.dilutedShares;
  const sharesThen = earlier?.dilutedShares;

  return {
    annualPeriods: annual.length,
    spanYears: spanIndex,
    fcfToNetIncome: isNum(fcf) && isNum(netIncome) && netIncome > 0 ? round(fcf / netIncome, 2) : null,
    sbcPctOfFcf:
      isNum(fcf) && fcf > 0 && isNum(latest?.stockCompensation)
        ? round((latest.stockCompensation / fcf) * 100, 1)
        : null,
    shareCountChangePct:
      isNum(sharesNow) && isNum(sharesThen) && sharesThen > 0
        ? round((sharesNow / sharesThen - 1) * 100, 1)
        : null,
    netDebt,
    netDebtToOperatingCashFlow: netDebt !== null && isNum(ocf) && ocf > 0 ? round(netDebt / ocf, 2) : null,
    cashRunwayYears:
      isNum(fcf) && fcf < 0 && isNum(latest?.cashAndEquivalents)
        ? round(latest.cashAndEquivalents / -fcf, 1)
        : null,
    operatingMarginPct: marginNow === null ? null : round(marginNow, 1),
    operatingMarginChangePp:
      marginNow !== null && marginThen !== null ? round(marginNow - marginThen, 1) : null,
    revenueCagrPct: cagrPct(latest?.revenue, earlier?.revenue, spanIndex),
    negativeEquity: isNum(latest?.shareholdersEquity) ? latest.shareholdersEquity < 0 : null,
  };
}

function toOutput(model: RedflagModel, metrics: RedflagMetrics): RedflagOutput {
  const flags = model.flags
    .map((f) => ({
      code: f.code,
      title: tidy(f.title),
      severity: f.severity,
      evidence: tidy(f.evidence),
      whatWouldClearIt: tidy(f.whatWouldClearIt),
    }))
    // high → medium → low; stable, so the model's order breaks ties.
    .sort((a, b) => SEVERITIES.indexOf(a.severity) - SEVERITIES.indexOf(b.severity));

  const top = flags[0];
  const countOf = (severity: (typeof SEVERITIES)[number]) =>
    flags.filter((f) => f.severity === severity).length;

  return {
    headline: field(tidy(model.headline), 'headline', 'model'),
    overall: field(model.overall, 'headline', 'model'),

    plainEnglish: field(tidy(model.plainEnglish), 'summary', 'model'),
    topFlag: field(
      top ? { code: top.code, title: top.title, severity: top.severity } : null,
      'summary',
      'model',
    ),
    flagCount: field(
      { high: countOf('high'), medium: countOf('medium'), low: countOf('low') },
      'summary',
      'computed',
    ),

    flags: field(flags, 'detail', 'model'),
    metrics: field(metrics, 'detail', 'computed'),
  };
}
