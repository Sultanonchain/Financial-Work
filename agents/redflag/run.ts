import { generateValidated, MODELS } from '../_shared/client.ts';
import { keyFigures, KEY_FIGURES_TITLE, renderKeyFigures, type KeyFigures } from '../_shared/figures.ts';
import {
  cagrPct,
  isNum,
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

/** This agent's model. VALUS_AGENT_MODEL overrides it (resolveModel in client.ts). */
export const model: string = MODELS.sonnet;

/** Hard max_tokens cap: thinking plus up to eight flags. */
export const maxTokens = 1_500;

// Debt to equity is not taken from vendor fields: key figures own it.
const YFINANCE_KEYS = [
  'shortPercentOfFloat',
  'heldPercentInsiders',
  'heldPercentInstitutions',
  'currentRatio',
  'quickRatio',
  'auditRisk',
  'boardRisk',
] as const;
const FINVIZ_KEYS = ['Short Float', 'Short Ratio', 'Insider Own', 'Insider Trans', 'Inst Trans', 'Current Ratio'] as const;

export async function run(ctx: AgentContext): Promise<AgentResult<RedflagOutput>> {
  const startedAt = Date.now();
  const base = { slug: 'redflag' as const, ticker: ctx.ticker, startedAt };

  if (ctx.statements.annual.length === 0) {
    return unavailable(
      { kind: 'insufficient_data', message: 'no annual statements on the context' },
      base,
    );
  }

  const figures = keyFigures(ctx.statements);
  const metrics = computeMetrics(ctx.statements, figures);

  const gen = await generateValidated({
    slug: 'redflag',
    ticker: ctx.ticker,
    system: SYSTEM_PROMPT,
    user: buildUserTurn(ctx, metrics, figures),
    schema: RedflagModelSchema,
    effort: 'low',
    model,
    maxTokens,
    signal: ctx.signal,
  });

  const init = { ...base, model: gen.model, attempts: gen.attempts, usage: gen.usage };
  if (!gen.data) {
    return unavailable(gen.error ?? { kind: 'unknown', message: 'no data returned' }, init);
  }

  return finalizeOutput(RedflagOutputSchema, toOutput(gen.data, metrics), init);
}

function buildUserTurn(ctx: AgentContext, m: RedflagMetrics, figures: KeyFigures): string {
  const currency = ctx.statements.currency;
  const flow =
    m.flowBasis === 'ttm'
      ? `trailing twelve months to ${m.flowPeriodEnd}`
      : m.flowBasis === 'annual'
        ? `fiscal year to ${m.flowPeriodEnd}`
        : 'latest period';
  const negativeEquity = m.negativeEquity === null ? 'n/a' : m.negativeEquity ? 'yes' : 'no';
  const span = `${m.spanYears} fiscal year(s)`;

  const vendor = [
    renderVendorFields(ctx.yfinance, YFINANCE_KEYS),
    renderVendorFields(ctx.finviz, FINVIZ_KEYS),
  ]
    .filter(Boolean)
    .join('\n');

  return [
    `Data as of ${ctx.asOf}.`,
    section('Company', renderProfile(ctx.profile)),
    section(KEY_FIGURES_TITLE, renderKeyFigures(figures, currency)),
    section(
      'Pre-computed ratios (from the key figures, except multi-year changes, which compare fiscal years)',
      [
        `Free cash flow / net income, ${flow}: ${ratio(m.fcfToNetIncome)}`,
        `Stock-based compensation as a share of free cash flow, ${flow}: ${pct(m.sbcPctOfFcf)}`,
        `Net debt / operating cash flow, ${flow}: ${ratio(m.netDebtToOperatingCashFlow)}`,
        `Negative shareholder equity: ${negativeEquity}`,
        `Annual periods available: ${m.annualPeriods}`,
        `Diluted share count change over ${span}: ${pct(m.shareCountChangePct, 1, true)}`,
        `Operating margin change over ${span}: ${m.operatingMarginChangePp === null ? 'n/a' : `${m.operatingMarginChangePp > 0 ? '+' : ''}${m.operatingMarginChangePp} points`}`,
        `Revenue growth per year over ${span}: ${pct(m.revenueCagrPct, 2)}`,
      ].join('\n'),
    ),
    section('Statements', renderStatements(ctx.statements, { annual: 5, quarterly: 4 })),
    section('Vendor fields (raw; yfinance ratios are fractions)', vendor),
  ].join('\n\n');
}

/**
 * Point-in-time and trailing figures come from key figures, so they match what
 * dcf and verdict see. Multi-year changes still compare fiscal years.
 */
function computeMetrics(statements: FinancialStatements, f: KeyFigures): RedflagMetrics {
  const annual = statements.annual;
  const latest = annual[0];
  const spanIndex = Math.min(3, annual.length - 1);
  const earlier = spanIndex > 0 ? annual[spanIndex] : undefined;

  const marginOf = (revenue: number | null | undefined, opIncome: number | null | undefined) =>
    isNum(revenue) && revenue > 0 && isNum(opIncome) ? (opIncome / revenue) * 100 : null;
  const marginNow = marginOf(latest?.revenue, latest?.operatingIncome);
  const marginThen = marginOf(earlier?.revenue, earlier?.operatingIncome);

  const sharesNow = latest?.dilutedShares;
  const sharesThen = earlier?.dilutedShares;

  return {
    annualPeriods: annual.length,
    spanYears: spanIndex,
    flowBasis: f.flowBasis,
    flowPeriodEnd: f.flowPeriodEnd,
    balanceSheetDate: f.balanceSheetDate,
    fcfToNetIncome:
      isNum(f.freeCashFlow) && isNum(f.netIncome) && f.netIncome > 0 ? round(f.freeCashFlow / f.netIncome, 2) : null,
    sbcPctOfFcf:
      isNum(f.freeCashFlow) && f.freeCashFlow > 0 && isNum(f.stockCompensation)
        ? round((f.stockCompensation / f.freeCashFlow) * 100, 1)
        : null,
    shareCountChangePct:
      isNum(sharesNow) && isNum(sharesThen) && sharesThen > 0
        ? round((sharesNow / sharesThen - 1) * 100, 1)
        : null,
    netDebt: f.netDebt,
    netDebtToOperatingCashFlow:
      isNum(f.netDebt) && isNum(f.operatingCashFlow) && f.operatingCashFlow > 0
        ? round(f.netDebt / f.operatingCashFlow, 2)
        : null,
    cashRunwayYears: f.cashRunwayYears,
    cashBurnBasis: f.cashBurnBasis,
    operatingMarginPct: f.operatingMarginPct,
    operatingMarginChangePp:
      marginNow !== null && marginThen !== null ? round(marginNow - marginThen, 1) : null,
    revenueCagrPct: cagrPct(latest?.revenue, earlier?.revenue, spanIndex),
    negativeEquity: isNum(f.shareholdersEquity) ? f.shareholdersEquity < 0 : null,
  };
}

function toOutput(reply: RedflagModel, metrics: RedflagMetrics): RedflagOutput {
  const flags = reply.flags
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
    headline: field(tidy(reply.headline), 'headline', 'model'),
    overall: field(reply.overall, 'headline', 'model'),

    plainEnglish: field(tidy(reply.plainEnglish), 'summary', 'model'),
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
