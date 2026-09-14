import { generateValidated, MODELS } from '../_shared/client.ts';
import {
  cagrPct,
  freeCashFlowOf,
  isNum,
  pct,
  renderPrices,
  renderProfile,
  renderStatements,
  renderValuation,
  round,
  safeDiv,
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
  type ValuationSnapshot,
} from '../_shared/types.ts';

import { SYSTEM_PROMPT } from './prompt.ts';
import {
  DCF_ASSUMPTION_KEYS,
  DcfModelSchema,
  DcfOutputSchema,
  type DcfAssumptionKey,
  type DcfHistory,
  type DcfModel,
  type DcfOutput,
} from './schema.ts';

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

/** This agent's model. VALUS_AGENT_MODEL overrides it (resolveModel in client.ts). */
export const model: string = MODELS.sonnet;

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

  const history = computeHistory(ctx.statements);

  const gen = await generateValidated({
    slug: 'dcf',
    ticker: ctx.ticker,
    system: SYSTEM_PROMPT,
    user: buildUserTurn(ctx, valuation, history),
    schema: DcfModelSchema,
    effort: 'medium',
    model,
    signal: ctx.signal,
  });

  const init = { ...base, model: gen.model, attempts: gen.attempts, usage: gen.usage };
  if (!gen.data) {
    return unavailable(gen.error ?? { kind: 'unknown', message: 'no data returned' }, init);
  }

  return finalizeOutput(DcfOutputSchema, toOutput(gen.data, ctx, valuation, history), init);
}

function buildUserTurn(ctx: AgentContext, valuation: ValuationSnapshot, history: DcfHistory): string {
  const currency = ctx.profile.currency ?? ctx.statements.currency;
  return [
    `Data as of ${ctx.asOf}.`,
    section('Company', renderProfile(ctx.profile)),
    section('Valuation engine output', renderValuation(valuation, currency)),
    section(
      'Reported history (computed from the statements below)',
      [
        `Annual periods available: ${history.annualPeriods}`,
        `Revenue growth per year across those periods: ${pct(history.revenueCagrPct, 2)}`,
        `Free cash flow growth per year across those periods: ${pct(history.fcfCagrPct, 2)}`,
        `Average free cash flow margin: ${pct(history.avgFcfMarginPct, 1)}`,
        `Stock-based compensation as a share of latest free cash flow: ${pct(history.latestSbcPctOfFcf, 1)}`,
      ].join('\n'),
    ),
    section('Statements', renderStatements(ctx.statements, { annual: 5, quarterly: 4 })),
    section('Price', renderPrices(ctx.prices)),
  ].join('\n\n');
}

function computeHistory(statements: FinancialStatements): DcfHistory {
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

  return {
    annualPeriods: annual.length,
    revenueCagrPct: span > 0 ? cagrPct(latest?.revenue, oldest?.revenue, span) : null,
    fcfCagrPct: span > 0 ? cagrPct(latestFcf, freeCashFlowOf(oldest), span) : null,
    avgFcfMarginPct: margins.length
      ? round((margins.reduce((sum, m) => sum + m, 0) / margins.length) * 100, 1)
      : null,
    latestSbcPctOfFcf: sbcShare,
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

function toOutput(
  model: DcfModel,
  ctx: AgentContext,
  v: ValuationSnapshot,
  history: DcfHistory,
): DcfOutput {
  const byKey = new Map(model.assumptions.map((row) => [row.key, row]));

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
        assessment: row.assessment,
        reasoning: tidy(row.reasoning),
        evidence: tidy(row.evidence),
      },
    ];
  });

  const range = isNum(v.ivLow) && isNum(v.ivHigh) ? { low: v.ivLow, high: v.ivHigh } : null;

  return {
    headline: field(tidy(model.headline), 'headline', 'model'),
    intrinsicValue: field(v.intrinsicValue, 'headline', 'computed'),
    valueRange: field(range, 'headline', 'computed'),

    price: field(v.price ?? ctx.prices.last, 'summary', 'api'),
    marginOfSafetyPct: field(v.marginOfSafetyPct, 'summary', 'computed'),
    plainEnglish: field(tidy(model.plainEnglish), 'summary', 'model'),
    historicalFit: field(model.historicalFit, 'summary', 'model'),
    confidence: field(model.confidence, 'summary', 'model'),

    assumptions: field(
      assumptions,
      'detail',
      'model',
      'engineValue is copied from the valuation engine; assessment, reasoning and evidence are model-written',
    ),
    dominantSensitivity: field(
      {
        key: model.dominantSensitivity.key,
        label: LABELS[model.dominantSensitivity.key],
        explanation: tidy(model.dominantSensitivity.explanation),
      },
      'detail',
      'model',
    ),
    confidenceReasons: field(model.confidenceReasons.map(tidy), 'detail', 'model'),
    history: field(history, 'detail', 'computed'),
  };
}
