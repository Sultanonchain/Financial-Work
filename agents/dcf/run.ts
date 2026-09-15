import { generateValidated, MODELS } from '../_shared/client.ts';
import { keyFigures, KEY_FIGURES_TITLE, renderKeyFigures, type KeyFigures } from '../_shared/figures.ts';
import {
  cagrPct,
  freeCashFlowOf,
  isNum,
  pct,
  renderProfile,
  renderStatements,
  renderValuation,
  round,
  safeDiv,
  section,
  tidy,
  type NumericStatementKey,
} from '../_shared/format.ts';
import {
  field,
  finalizeOutput,
  unavailable,
  type AgentContext,
  type AgentResult,
  type FinancialStatements,
  type ValuationSnapshot,
} from '../_shared/types.ts';

import { SYSTEM_PROMPT } from './prompt.ts';
import {
  DCF_ASSUMPTION_KEYS,
  DcfModelSchema,
  DcfOutputSchema,
  type Assessment,
  type DcfAssumptionKey,
  type EngineComparison,
  type DcfHistory,
  type DcfModel,
  type DcfOutput,
} from './schema.ts';

/** This agent's model. VALUS_AGENT_MODEL overrides it (resolveModel in client.ts). */
export const model: string = MODELS.sonnet;

/** Hard max_tokens cap: thinking plus the assumption review. */
export const maxTokens = 2_000;

/** The statement lines the assumption review uses; the rest only lengthen the prompt. */
const STATEMENT_COLUMNS: readonly NumericStatementKey[] = [
  'revenue',
  'operatingIncome',
  'netIncome',
  'operatingCashFlow',
  'capex',
  'freeCashFlow',
  'stockCompensation',
  'totalDebt',
  'cashAndEquivalents',
  'cashAndShortTermInvestments',
  'dilutedShares',
];

const LABELS: Record<DcfAssumptionKey, string> = {
  stage1_growth: 'Near-term growth',
  terminal_growth: 'Long-run growth',
  wacc: 'Discount rate',
  fcf_base: 'Starting free cash flow',
  share_count: 'Share count',
  net_debt: 'Net debt',
};

const UNITS: Record<DcfAssumptionKey, 'pct' | 'money' | 'shares'> = {
  stage1_growth: 'pct',
  terminal_growth: 'pct',
  wacc: 'pct',
  fcf_base: 'money',
  share_count: 'shares',
  net_debt: 'money',
};

/** Whether a figure above the evidence raises the value: growth and cash flow do; the discount rate, shares and debt lower it. */
const HIGHER_RAISES_VALUE: Record<DcfAssumptionKey, boolean> = {
  stage1_growth: true,
  terminal_growth: true,
  fcf_base: true,
  wacc: false,
  share_count: false,
  net_debt: false,
};

/** The public label, from the model's higher or lower call and the input's known effect on value. */
function assessmentOf(key: DcfAssumptionKey, comparison: EngineComparison): Assessment {
  switch (comparison) {
    case 'in_line':
      return 'supported';
    case 'unclear':
      return 'unclear';
    case 'higher':
      return HIGHER_RAISES_VALUE[key] ? 'aggressive' : 'conservative';
    case 'lower':
      return HIGHER_RAISES_VALUE[key] ? 'conservative' : 'aggressive';
  }
}

/** The inputs the value rests on. Terminal growth is judged against a fixed range instead. */
const CORE_INPUTS: readonly DcfAssumptionKey[] = ['stage1_growth', 'wacc', 'fcf_base'];

/**
 * How many core inputs the review could not assess (rated unclear, or missing
 * from the engine) before the valuation is marked unreliable. One is common and
 * not enough on its own: in the 2026-09-14 smoke run every ticker had one.
 */
const UNASSESSABLE_CORE_LIMIT = 2;

/**
 * Below this the pure DCF value and the displayed value are the same number for
 * the reader. At or above it the review must say which one it is reviewing.
 */
const MATERIAL_DIFFERENCE = 0.1;

interface ValuationBasis {
  baseIv: number | null;
  displayIv: number | null;
  differsMaterially: boolean;
}

/** What the review is of (the engine's pure DCF) against what the reader is shown. */
function valuationBasisOf(v: ValuationSnapshot): ValuationBasis {
  const baseIv = isNum(v.baseIv) ? v.baseIv : null;
  const displayIv = isNum(v.intrinsicValue) ? v.intrinsicValue : null;
  return {
    baseIv,
    displayIv,
    differsMaterially:
      baseIv !== null &&
      displayIv !== null &&
      baseIv > 0 &&
      displayIv > 0 &&
      Math.abs(displayIv / baseIv - 1) >= MATERIAL_DIFFERENCE,
  };
}

export async function run(ctx: AgentContext): Promise<AgentResult<DcfOutput>> {
  const startedAt = Date.now();
  const base = { slug: 'dcf' as const, ticker: ctx.ticker, startedAt };

  const valuation = ctx.valuation;
  if (!valuation || !isNum(valuation.intrinsicValue)) {
    return unavailable(
      { kind: 'insufficient_data', message: 'no valuation engine output on the context' },
      base,
    );
  }

  const figures = keyFigures(ctx.statements);
  const history = computeHistory(ctx.statements, figures);
  const basis = valuationBasisOf(valuation);

  // The reader is shown the post-adjustment value while this review is of the
  // inputs to the pure one. When they are different numbers, say both.
  const schema = basis.differsMaterially
    ? DcfModelSchema.superRefine((out, issues) => {
        const written = out.inputRationale.replace(/,/g, '');
        const missing = [basis.baseIv, basis.displayIv].filter(
          (value): value is number => value !== null && !written.includes(value.toFixed(2)),
        );
        if (missing.length) {
          issues.addIssue({
            code: 'custom',
            path: ['inputRationale'],
            message:
              'inputRationale must name both values as figures: this review is of the pure discounted cash flow ' +
              `value of ${basis.baseIv?.toFixed(2)}, while the page shows ${basis.displayIv?.toFixed(2)}`,
          });
        }
      })
    : DcfModelSchema;

  const gen = await generateValidated({
    slug: 'dcf',
    ticker: ctx.ticker,
    system: SYSTEM_PROMPT,
    user: buildUserTurn(ctx, valuation, history, figures),
    schema,
    effort: 'low',
    model,
    maxTokens,
    signal: ctx.signal,
  });

  const init = { ...base, model: gen.model, attempts: gen.attempts, usage: gen.usage };
  if (!gen.data) {
    return unavailable(gen.error ?? { kind: 'unknown', message: 'no data returned' }, init);
  }

  return finalizeOutput(DcfOutputSchema, toOutput(gen.data, ctx, valuation, history, basis), init);
}

function buildUserTurn(
  ctx: AgentContext,
  valuation: ValuationSnapshot,
  history: DcfHistory,
  figures: KeyFigures,
): string {
  const currency = ctx.profile.currency ?? ctx.statements.currency;
  const trailing = history.trailing;
  const trailingLine =
    trailing?.basis === 'ttm' && trailing.revenueVsLastFiscalYearPct !== null
      ? `Revenue for the trailing twelve months to ${trailing.periodEnd} against the last fiscal year: ` +
        pct(trailing.revenueVsLastFiscalYearPct, 1, true)
      : null;

  return [
    `Data as of ${ctx.asOf}.`,
    section('Company', renderProfile(ctx.profile)),
    section('Valuation engine output', renderValuation(valuation, currency, { engineInputs: true })),
    // Statement currency, not trading currency: the figures come from the statements.
    section(KEY_FIGURES_TITLE, renderKeyFigures(figures, ctx.statements.currency)),
    section(
      'Reported history (computed from the statements)',
      [
        `Annual periods available: ${history.annualPeriods}`,
        `Revenue growth per year across those periods: ${pct(history.revenueCagrPct, 2)}`,
        `Free cash flow growth per year across those periods: ${pct(history.fcfCagrPct, 2)}`,
        `Average free cash flow margin: ${pct(history.avgFcfMarginPct, 1)}`,
        `Stock-based compensation as a share of latest free cash flow: ${pct(history.latestSbcPctOfFcf, 1)}`,
        ...(trailingLine ? [trailingLine] : []),
      ].join('\n'),
    ),
    // Annual statements, trimmed to the lines the review uses. Recent quarters
    // reach the model through key figures; price is in the valuation block.
    section(
      'Annual statements',
      renderStatements(ctx.statements, { annual: 5, quarterly: 0, columns: STATEMENT_COLUMNS }),
    ),
  ].join('\n\n');
}

function computeHistory(statements: FinancialStatements, figures: KeyFigures): DcfHistory {
  const annual = statements.annual.filter((p) => isNum(p.revenue));
  const latest = annual[0];
  const oldest = annual[annual.length - 1];
  const span = annual.length - 1;

  const margins = annual
    .map((p) => safeDiv(freeCashFlowOf(p), p.revenue))
    .filter((m): m is number => m !== null);

  const latestFcf = freeCashFlowOf(latest);
  const sbcShare =
    latest && isNum(latest.stockCompensation) && isNum(latestFcf) && latestFcf > 0
      ? round((latest.stockCompensation / latestFcf) * 100, 1)
      : null;

  const trailing =
    figures.flowBasis && figures.flowPeriodEnd
      ? {
          basis: figures.flowBasis,
          periodEnd: figures.flowPeriodEnd,
          revenue: figures.revenue,
          freeCashFlow: figures.freeCashFlow,
          operatingMarginPct: figures.operatingMarginPct,
          revenueVsLastFiscalYearPct:
            figures.flowBasis === 'ttm' &&
            latest &&
            figures.flowPeriodEnd > latest.periodEnd &&
            isNum(figures.revenue) &&
            isNum(latest.revenue) &&
            latest.revenue > 0
              ? round((figures.revenue / latest.revenue - 1) * 100, 1)
              : null,
        }
      : null;

  return {
    annualPeriods: annual.length,
    revenueCagrPct: span > 0 ? cagrPct(latest?.revenue, oldest?.revenue, span) : null,
    fcfCagrPct: span > 0 ? cagrPct(latestFcf, freeCashFlowOf(oldest), span) : null,
    avgFcfMarginPct: margins.length
      ? round((margins.reduce((sum, m) => sum + m, 0) / margins.length) * 100, 1)
      : null,
    latestSbcPctOfFcf: sbcShare,
    trailing,
  };
}

function engineValue(v: ValuationSnapshot, key: DcfAssumptionKey): number | null {
  switch (key) {
    case 'stage1_growth':
      return v.stage1GrowthPct;
    case 'terminal_growth':
      return v.terminalGrowthPct;
    case 'wacc':
      return v.waccPct;
    case 'fcf_base':
      return v.fcfBase;
    case 'share_count':
      return v.sharesOut;
    case 'net_debt':
      return v.netDebt;
  }
}

/**
 * Whether the engine's value can be trusted, decided in code from the review
 * so the engine's own confidence cannot override it: unreliable when the growth
 * path breaks from history, or when enough core inputs could not be assessed.
 */
function reliabilityOf(reply: DcfModel, v: ValuationSnapshot): { reliable: boolean; reasons: string[] } {
  const reasons: string[] = [];
  if (reply.historicalFit === 'break') {
    reasons.push("the growth path breaks from the company's own history");
  }
  const comparison = new Map(reply.assumptions.map((row) => [row.key, row.engineVsEvidence]));
  const unassessable = CORE_INPUTS.filter(
    (key) => comparison.get(key) === 'unclear' || engineValue(v, key) === null,
  );
  if (unassessable.length >= UNASSESSABLE_CORE_LIMIT) {
    reasons.push(`${joinList(unassessable.map((key) => LABELS[key].toLowerCase()))} could not be assessed`);
  }
  return { reliable: reasons.length === 0, reasons };
}

function joinList(items: string[]): string {
  return items.length <= 1 ? items.join('') : `${items.slice(0, -1).join(', ')} and ${items[items.length - 1]}`;
}

function toOutput(
  reply: DcfModel,
  ctx: AgentContext,
  v: ValuationSnapshot,
  history: DcfHistory,
  basis: ValuationBasis,
): DcfOutput {
  const byKey = new Map(reply.assumptions.map((row) => [row.key, row]));

  // Fixed display order, independent of the order the model wrote them in.
  const assumptions = DCF_ASSUMPTION_KEYS.flatMap((key) => {
    const row = byKey.get(key);
    if (!row) return [];
    return [
      {
        key,
        label: LABELS[key],
        engineValue: engineValue(v, key),
        unit: UNITS[key],
        assessment: assessmentOf(key, row.engineVsEvidence),
        reasoning: tidy(row.reasoning),
        evidence: tidy(row.evidence),
      },
    ];
  });

  const range = isNum(v.ivLow) && isNum(v.ivHigh) ? { low: v.ivLow, high: v.ivHigh } : null;
  const reliability = reliabilityOf(reply, v);
  const unreliable = reliability.reliable ? undefined : `Marked unreliable by the dcf review: ${reliability.reasons.join('; ')}`;

  return {
    headline: field(tidy(reply.headline), 'headline', 'model'),
    valuationReliability: field(
      reliability,
      'headline',
      'computed',
      'Unreliable when the growth path breaks from history, or when two or more of near-term growth, ' +
        'discount rate and starting free cash flow could not be assessed (rated unclear, or missing from the engine)',
    ),
    intrinsicValue: field(v.intrinsicValue, 'headline', 'computed', unreliable),
    valueRange: field(range, 'headline', 'computed', unreliable),

    price: field(v.price ?? ctx.prices.last, 'summary', 'api'),
    marginOfSafetyPct: field(v.marginOfSafetyPct, 'summary', 'computed', unreliable),
    plainEnglish: field(tidy(reply.plainEnglish), 'summary', 'model'),
    inputRationale: field(
      tidy(reply.inputRationale),
      'summary',
      'model',
      basis.differsMaterially
        ? 'The engine replaces its own discounted cash flow value before display, so the review is of the pure value'
        : undefined,
    ),
    historicalFit: field(reply.historicalFit, 'summary', 'model'),
    confidence: reliability.reliable
      ? field(reply.confidence, 'summary', 'model')
      : field<DcfModel['confidence']>('low', 'summary', 'computed', `Forced to low: ${reliability.reasons.join('; ')}`),

    assumptions: field(
      assumptions,
      'detail',
      'model',
      'engineValue is copied from the valuation engine; reasoning and evidence are model-written; assessment is ' +
        "set in code from the model's call on whether the engine figure is higher or lower than the evidence supports",
    ),
    dominantSensitivity: field(
      {
        key: reply.dominantSensitivity.key,
        label: LABELS[reply.dominantSensitivity.key],
        explanation: tidy(reply.dominantSensitivity.explanation),
      },
      'detail',
      'model',
    ),
    confidenceReasons: field(reply.confidenceReasons.map(tidy), 'detail', 'model'),
    valuationBasis: field(
      basis,
      'detail',
      'computed',
      'baseIv is the engine\'s pure discounted cash flow value; displayIv is what the reader is shown',
    ),
    history: field(history, 'detail', 'computed'),
  };
}
