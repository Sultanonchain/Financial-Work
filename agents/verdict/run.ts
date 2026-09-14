/**
 * Stage 2. Reads the stage-1 results the runner placed on ctx.upstream.
 *
 * Only *types* cross from the sibling agent folders into this file. The verdict
 * never imports another agent's run.ts and never triggers another agent; if a
 * stage-1 result is missing, it works without it.
 */

import { generateValidated, MODELS } from '../_shared/client.ts';
import {
  isNum,
  money,
  pct,
  quantity,
  renderPrices,
  renderProfile,
  renderValuation,
  renderVendorFields,
  section,
  tidy,
} from '../_shared/format.ts';
import {
  field,
  finalizeOutput,
  unavailable,
  type AgentContext,
  type AgentResult,
} from '../_shared/types.ts';

import type { CatalystOutput } from '../catalyst/schema.ts';
import type { DcfOutput } from '../dcf/schema.ts';
import type { NewsOutput } from '../news/schema.ts';
import type { RedflagOutput } from '../redflag/schema.ts';

import { SYSTEM_PROMPT } from './prompt.ts';
import {
  NEUTRAL_BAND_INDEX,
  STAGE_ONE_SLUGS,
  VALUATION_BANDS,
  VerdictModelSchema,
  VerdictOutputSchema,
  type ValuationBand,
  type VerdictModel,
  type VerdictOutput,
  type VerdictTier,
} from './schema.ts';

const MAX_STEPS_FROM_ENGINE = 2;

const DISCLAIMER =
  'Educational analysis of price against estimated value. Not investment advice.';

const TIER_BY_BAND: Record<ValuationBand, VerdictTier> = {
  Undervalued: 'positive',
  'Modestly Undervalued': 'positive',
  'Fairly Valued': 'info',
  'Slightly Overvalued': 'warning',
  Overvalued: 'negative',
};

const YFINANCE_VALUATION_KEYS = [
  'pegRatio',
  'trailingPE',
  'forwardPE',
  'revenueGrowth',
  'earningsGrowth',
  'dividendYield',
  'payoutRatio',
  'debtToEquity',
] as const;
const YFINANCE_TAPE_KEYS = ['shortPercentOfFloat', 'heldPercentInsiders', 'heldPercentInstitutions'] as const;
const FINVIZ_TAPE_KEYS = ['Short Float', 'Short Ratio', 'Rel Volume'] as const;

type StageOneSlug = (typeof STAGE_ONE_SLUGS)[number];

interface Guardrails {
  engineBand: ValuationBand | null;
  regimeCap: boolean;
  backstopFloor: boolean;
  allowedBands: ValuationBand[];
}

/** This agent's model. VALUS_AGENT_MODEL overrides it (resolveModel in client.ts). */
export const model: string = MODELS.sonnet;

/** Hard max_tokens cap: medium-effort thinking plus the verdict. */
export const maxTokens = 2_500;

export async function run(ctx: AgentContext): Promise<AgentResult<VerdictOutput>> {
  const startedAt = Date.now();
  const base = { slug: 'verdict' as const, ticker: ctx.ticker, startedAt };

  const v = ctx.valuation;
  const price = v?.price ?? ctx.prices.last;
  if (!v || !isNum(v.intrinsicValue) || !isNum(price)) {
    return unavailable(
      {
        kind: 'insufficient_data',
        message: 'a valuation verdict needs both a price and an intrinsic value',
      },
      base,
    );
  }

  const guardrails = computeGuardrails(ctx);

  // The allowed set is also stated in the user turn, so a reply outside it is
  // a model slip and the repair turn carries the exact list back.
  const schema = VerdictModelSchema.superRefine((out, issues) => {
    if (!guardrails.allowedBands.includes(out.band)) {
      issues.addIssue({
        code: 'custom',
        path: ['band'],
        message:
          `"${out.band}" is not allowed for this ticker; choose one of: ` +
          guardrails.allowedBands.map((b) => `"${b}"`).join(', '),
      });
    }
  });

  const gen = await generateValidated({
    slug: 'verdict',
    ticker: ctx.ticker,
    system: SYSTEM_PROMPT,
    user: buildUserTurn(ctx, guardrails),
    schema,
    effort: 'medium',
    model,
    maxTokens,
    signal: ctx.signal,
  });

  const init = { ...base, model: gen.model, attempts: gen.attempts, usage: gen.usage };
  if (!gen.data) {
    return unavailable(gen.error ?? { kind: 'unknown', message: 'no data returned' }, init);
  }

  return finalizeOutput(VerdictOutputSchema, toOutput(gen.data, ctx, guardrails), init);
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Guardrails                                                                 */
/* ────────────────────────────────────────────────────────────────────────── */

/** Cut-points match the Python engine's MOS bands so the two cannot disagree. */
function bandFromMarginOfSafety(mos: number | null): ValuationBand | null {
  if (!isNum(mos)) return null;
  if (mos >= 40) return 'Undervalued';
  if (mos >= 15) return 'Modestly Undervalued';
  if (mos >= -10) return 'Fairly Valued';
  if (mos >= -25) return 'Slightly Overvalued';
  return 'Overvalued';
}

function computeGuardrails(ctx: AgentContext): Guardrails {
  const engineBand = bandFromMarginOfSafety(ctx.valuation?.marginOfSafetyPct ?? null);
  const engineIndex = engineBand === null ? null : VALUATION_BANDS.indexOf(engineBand);
  const regime = ctx.prices.regime;

  // A melt-up or a squeeze is not the moment to call a stock cheap.
  const regimeCap = regime === 'momentum_runup' || regime === 'squeeze_risk';
  // Sovereign capital takes failure off the table: never call it expensive.
  const backstopFloor = ctx.profile.strategic?.isStrategic === true;

  const allowedBands = VALUATION_BANDS.filter((_, index) => {
    if (regimeCap && index < NEUTRAL_BAND_INDEX) return false;
    if (backstopFloor && index > NEUTRAL_BAND_INDEX) return false;
    if (engineIndex !== null && Math.abs(index - engineIndex) > MAX_STEPS_FROM_ENGINE) return false;
    return true;
  });

  return { engineBand, regimeCap, backstopFloor, allowedBands };
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Prompt                                                                     */
/* ────────────────────────────────────────────────────────────────────────── */

function buildUserTurn(ctx: AgentContext, g: Guardrails): string {
  const currency = ctx.profile.currency ?? ctx.prices.currency;
  const rules = [
    `Engine band (from margin of safety): ${g.engineBand ?? 'not available'}`,
    `Tape regime: ${ctx.prices.regime ?? 'not available'}${g.regimeCap ? ' (verdict capped at Fairly Valued)' : ''}`,
    `Sovereign backstop: ${g.backstopFloor ? 'yes (verdict floored at Fairly Valued)' : 'none on file'}`,
    `Allowed bands for this ticker: ${g.allowedBands.join(', ')}`,
  ].join('\n');

  const tape = [
    renderPrices(ctx.prices),
    renderVendorFields(ctx.yfinance, YFINANCE_TAPE_KEYS),
    renderVendorFields(ctx.finviz, FINVIZ_TAPE_KEYS),
  ]
    .filter(Boolean)
    .join('\n');

  return [
    `Data as of ${ctx.asOf}.`,
    section('Verdict rules for this ticker', rules),
    section('Company', renderProfile(ctx.profile)),
    section('Valuation engine output', renderValuation(ctx.valuation, currency)),
    section(
      'Valuation and growth fields (yfinance; growth, yield and payout as fractions)',
      renderVendorFields(ctx.yfinance, YFINANCE_VALUATION_KEYS),
    ),
    section('Tape', tape),
    section('Reviewer: dcf', renderDcf(ctx, currency)),
    section('Reviewer: catalyst', renderCatalyst(ctx)),
    section('Reviewer: news', renderNews(ctx)),
    section('Reviewer: redflag', renderRedflag(ctx, currency)),
  ].join('\n\n');
}

function upstreamData<T>(ctx: AgentContext, slug: StageOneSlug): T | null {
  const result = ctx.upstream?.[slug];
  return result && result.status === 'ok' && result.data !== null ? (result.data as T) : null;
}

function notAvailable(ctx: AgentContext, slug: StageOneSlug): string {
  const kind = ctx.upstream?.[slug]?.error?.kind;
  return kind ? `Not available (${kind}).` : 'Not available (did not run).';
}

function renderDcf(ctx: AgentContext, currency: string | null): string {
  const d = upstreamData<DcfOutput>(ctx, 'dcf');
  if (!d) return notAvailable(ctx, 'dcf');

  const engineValue = (value: number | null, unit: 'pct' | 'money' | 'shares') =>
    unit === 'pct' ? pct(value, 2) : unit === 'money' ? money(value, currency) : quantity(value);

  return [
    `Headline: ${d.headline.value}`,
    `Plain English: ${d.plainEnglish.value}`,
    `Historical fit of the growth path: ${d.historicalFit.value}`,
    `Reviewer confidence: ${d.confidence.value}`,
    'Assumptions:',
    ...d.assumptions.value.map(
      (a) =>
        `  ${a.label} (engine ${engineValue(a.engineValue, a.unit)}): ${a.assessment}. ${a.reasoning}`,
    ),
    `Dominant sensitivity: ${d.dominantSensitivity.value.label}. ${d.dominantSensitivity.value.explanation}`,
  ].join('\n');
}

function renderCatalyst(ctx: AgentContext): string {
  const d = upstreamData<CatalystOutput>(ctx, 'catalyst');
  if (!d) return notAvailable(ctx, 'catalyst');

  const upcoming = d.upcomingCatalysts.value.map(
    (c) =>
      `  ${c.title} (${c.status}, ${c.direction}, ${c.horizon}, likelihood ${c.likelihood}, via ${c.valueLever}` +
      `${c.expectedDate ? `, expected ${c.expectedDate}` : ''}): ${c.whyItMatters}`,
  );
  const past = d.historicalAnalogs.value.map(
    (c) => `  ${c.title} (${c.status} ${c.eventDate}, ${c.direction}, via ${c.valueLever}): ${c.whyItMatters}`,
  );
  return [
    `Headline: ${d.headline.value}`,
    `Net tilt of upcoming catalysts: ${d.netTilt.value}`,
    `Next earnings date: ${d.nextEarningsDate.value ?? 'not available'}`,
    upcoming.length ? 'Upcoming:' : 'Upcoming: none specific in view',
    ...upcoming,
    past.length ? 'Recently announced or shipped:' : 'Recently announced or shipped: none',
    ...past,
  ].join('\n');
}

function renderNews(ctx: AgentContext): string {
  const d = upstreamData<NewsOutput>(ctx, 'news');
  if (!d) return notAvailable(ctx, 'news');

  const rows = d.stories.value.map(
    (s) => `  [${s.materiality}, ${s.direction}] ${s.title}: ${s.takeaway}`,
  );
  const move = d.priceMove.value;
  return [
    `Headline: ${d.headline.value}`,
    `Tone: ${d.tone.value}`,
    rows.length ? 'Material stories:' : 'Material stories: none',
    ...rows,
    `One-month move ${pct(move.returnPct1m, 1, true)} explained by news: ${move.explained}${move.note ? `. ${move.note}` : ''}`,
  ].join('\n');
}

function renderRedflag(ctx: AgentContext, currency: string | null): string {
  const d = upstreamData<RedflagOutput>(ctx, 'redflag');
  if (!d) return notAvailable(ctx, 'redflag');

  const m = d.metrics.value;
  const rows = d.flags.value.map((f) => `  [${f.severity}] ${f.title}: ${f.evidence}`);
  return [
    `Headline: ${d.headline.value}`,
    `Overall: ${d.overall.value}`,
    rows.length ? 'Flags:' : 'Flags: none',
    ...rows,
    `Key metrics: FCF/net income ${m.fcfToNetIncome ?? 'n/a'}, stock comp ${pct(m.sbcPctOfFcf)} of FCF, ` +
      `share count ${pct(m.shareCountChangePct, 1, true)} over ${m.spanYears}y, net debt ${money(m.netDebt, currency)}, ` +
      `operating margin ${pct(m.operatingMarginPct)}`,
  ].join('\n');
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Output                                                                     */
/* ────────────────────────────────────────────────────────────────────────── */

function toOutput(model: VerdictModel, ctx: AgentContext, g: Guardrails): VerdictOutput {
  const available: StageOneSlug[] = [];
  const missing: StageOneSlug[] = [];
  for (const slug of STAGE_ONE_SLUGS) {
    (ctx.upstream?.[slug]?.status === 'ok' ? available : missing).push(slug);
  }

  return {
    band: field(model.band, 'headline', 'model', 'Constrained to guardrails.allowedBands'),
    tier: field(TIER_BY_BAND[model.band], 'headline', 'computed'),
    headline: field(tidy(model.headline), 'headline', 'model'),

    plainEnglish: field(tidy(model.plainEnglish), 'summary', 'model'),
    category: field(model.category, 'summary', 'model'),
    confidence: field(model.confidence, 'summary', 'model'),
    bullPoints: field(model.bullPoints.map(tidy), 'summary', 'model'),
    bearPoints: field(model.bearPoints.map(tidy), 'summary', 'model'),
    disclaimer: field(DISCLAIMER, 'summary', 'computed'),

    thesis: field(tidy(model.thesis), 'detail', 'model'),
    bandRationale: field(tidy(model.bandRationale), 'detail', 'model'),
    tapeVsFundamentals: field(model.tapeVsFundamentals, 'detail', 'model'),
    engineBand: field(
      g.engineBand,
      'detail',
      'computed',
      'Margin-of-safety cut-points: >=40, >=15, >=-10, >=-25',
    ),
    regime: field(ctx.prices.regime, 'detail', 'computed'),
    guardrails: field(
      { regimeCap: g.regimeCap, backstopFloor: g.backstopFloor, allowedBands: g.allowedBands },
      'detail',
      'computed',
    ),
    inputs: field({ available, unavailable: missing }, 'detail', 'computed'),
  };
}
