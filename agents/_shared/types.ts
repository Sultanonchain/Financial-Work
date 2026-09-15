/**
 * The contract every VALUS agent is written against.
 *
 * Two rules drive the shapes in this file:
 *   1. Agents never fetch. Everything an agent is allowed to look at arrives on
 *      AgentContext, assembled by the caller (today: the Flask analyze route).
 *   2. Agents never throw into the UI. Every failure comes back as an
 *      AgentResult with status "unavailable", so one dead agent leaves a hole
 *      in the page instead of a 500.
 */

import { z } from 'zod';

/* ────────────────────────────────────────────────────────────────────────── */
/* Visibility + fields                                                        */
/* ────────────────────────────────────────────────────────────────────────── */

/**
 * Where a value is allowed to surface.
 *   headline — the one line above the fold (verdict band, IV, top flag)
 *   summary  — the card body a free user reads
 *   detail   — the drill-down: evidence, per-assumption reasoning, raw lists
 */
export const VISIBILITY_TIERS = ['headline', 'summary', 'detail'] as const;
export type VisibilityTier = (typeof VISIBILITY_TIERS)[number];

/** Provenance. `model` means Claude wrote it and it carries model risk. */
export const FIELD_SOURCES = ['model', 'computed', 'api'] as const;
export type FieldSource = (typeof FIELD_SOURCES)[number];

export interface Field<T> {
  value: T;
  visibility: VisibilityTier;
  source: FieldSource;
  note?: string;
}

/** Build a Field. Agents use this instead of writing the literal by hand. */
export function field<T>(
  value: T,
  visibility: VisibilityTier,
  source: FieldSource,
  note?: string,
): Field<T> {
  return note === undefined
    ? { value, visibility, source }
    : { value, visibility, source, note };
}

/** Zod counterpart of Field<T>: wraps an inner schema in the field envelope. */
export function zField<S extends z.ZodType>(inner: S) {
  return z.object({
    value: inner,
    visibility: z.enum(VISIBILITY_TIERS),
    source: z.enum(FIELD_SOURCES),
    note: z.string().optional(),
  });
}

/**
 * VALUS is educational analysis, not a registered investment adviser, and an
 * explicit call on a named security reads as a personalised recommendation.
 * The verdict enums make action tokens impossible in structured fields; this
 * pattern catches them in model-written prose. A match fails validation, which
 * sends the reply back through the repair turn.
 *
 * It targets advice phrasing, not the words themselves: "buyback",
 * "sell-side", "margins hold up" and "short interest" all pass.
 */
export const ADVICE_PATTERN = new RegExp(
  [
    String.raw`\b(?:you|investors|readers|shareholders)\s+(?:should|could|may want to)\s+(?:consider\s+)?(?:buy|sell|hold|avoid|add|trim|accumulate|exit|load up)\b`,
    String.raw`\b(?:strong\s+)?(?:buy|sell|hold|avoid|accumulate)\s+(?:rating|recommendation|signal)\b`,
    String.raw`\bstrong\s+(?:buy|sell)\b`,
    String.raw`\b(?:i|we)\s+(?:recommend|advise)\b`,
    String.raw`\btime\s+to\s+(?:buy|sell)\b`,
    String.raw`\b(?:is|looks like|remains|as)\s+an?\s+(?:buy|sell|hold)\b`,
  ].join('|'),
  'i',
);

/**
 * Words that accuse the valuation engine or the data of dishonesty. An engine
 * figure can disagree with the reported figures; "fabricated" claims intent the
 * site cannot know. Checked on dcf and verdict prose (zProse neutral option).
 */
export const ACCUSATORY_PATTERN = /\b(?:fabricat\w*|made[- ]up|bogus)\b/i;

/**
 * Model-written prose: bounded length, no trading advice. `neutral` also rules
 * out accusatory wording, for agents that describe the engine's figures.
 */
export function zProse(maxChars: number, options: { neutral?: boolean } = {}) {
  const prose = z
    .string()
    .min(1)
    .max(maxChars)
    .refine((text) => !ADVICE_PATTERN.test(text), {
      message:
        'reads as trading advice; describe price against value and never tell ' +
        'the reader to buy, sell, hold or avoid',
    });
  return options.neutral
    ? prose.refine((text) => !ACCUSATORY_PATTERN.test(text), {
        message: 'calls a figure fabricated or made up; say it does not match the reported figures instead',
      })
    : prose;
}

/** Free users see headline + summary. Premium sees everything. */
export const TIER_VISIBILITY: Record<Tier, readonly VisibilityTier[]> = {
  free: ['headline', 'summary'],
  premium: ['headline', 'summary', 'detail'],
};

/* ────────────────────────────────────────────────────────────────────────── */
/* Context: everything an agent is allowed to see                             */
/* ────────────────────────────────────────────────────────────────────────── */

export type Tier = 'free' | 'premium';

export type AgentSlug = 'dcf' | 'catalyst' | 'news' | 'redflag' | 'verdict';

export interface CompanyProfile {
  ticker: string;
  name: string;
  sector: string | null;
  industry: string | null;
  country: string | null;
  currency: string | null;
  exchange: string | null;
  marketCap: number | null;
  employees: number | null;
  description: string | null;
  /**
   * Sovereign-backstop narrative (CHIPS Act grants, DPA Title III, energy PPAs,
   * direct government equity). The Python side already classifies this; agents
   * must not re-derive it from the description.
   */
  strategic: StrategicProfile | null;
  /**
   * Facts for the company section (_shared/company.ts), copied from the data
   * layer and never inferred. Optional so contexts assembled before these
   * existed stay valid; a missing value just omits its row.
   */
  /** yfinance forwardPE. */
  forwardPE?: number | null;
  /** Founded or incorporated year. yfinance has none, so this needs another source. */
  foundedYear?: number | null;
  /** yfinance city, state, country. */
  headquarters?: CompanyHeadquarters | null;
  /** yfinance companyOfficers, name and title only. */
  officers?: CompanyOfficer[];
}

export interface StrategicProfile {
  isStrategic: boolean;
  label: string | null;
  reason: string | null;
}

export interface CompanyHeadquarters {
  city: string | null;
  region: string | null;
  country: string | null;
}

export interface CompanyOfficer {
  name: string;
  title: string | null;
}

/** One fiscal period, already normalised to reporting currency units. */
export interface StatementPeriod {
  periodEnd: string; // ISO date
  revenue: number | null;
  grossProfit: number | null;
  operatingIncome: number | null;
  netIncome: number | null;
  operatingCashFlow: number | null;
  capex: number | null;
  freeCashFlow: number | null;
  stockCompensation: number | null;
  totalDebt: number | null;
  cashAndEquivalents: number | null;
  /** Cash, equivalents and short-term investments, when the filer reports it. Optional for older contexts. */
  cashAndShortTermInvestments?: number | null;
  shareholdersEquity: number | null;
  dilutedShares: number | null;
}

export interface FinancialStatements {
  currency: string | null;
  annual: StatementPeriod[]; // newest first
  quarterly: StatementPeriod[]; // newest first
}

export interface PricePoint {
  date: string; // ISO date
  close: number;
  volume: number | null;
}

export interface PriceHistory {
  currency: string | null;
  last: number | null;
  points: PricePoint[]; // oldest first
  fiftyTwoWeekHigh: number | null;
  fiftyTwoWeekLow: number | null;
  returnsPct: {
    m1: number | null;
    m3: number | null;
    m6: number | null;
    ytd: number | null;
  };
  /** Computed by the caller, never by an agent. */
  regime: MarketRegime | null;
}

export const MARKET_REGIMES = [
  'stable',
  'momentum_runup',
  'squeeze_risk',
  'post_runup_pullback',
  'broken',
] as const;
export type MarketRegime = (typeof MARKET_REGIMES)[number];

/**
 * Raw vendor payloads. Deliberately loose: yfinance and the Finviz scrape both
 * change shape without warning, and an agent that hard-codes a key is a bug
 * waiting for the next upstream rename. Agents read these defensively or not
 * at all — anything load-bearing should be lifted into a typed field above.
 */
export interface VendorPayload {
  [key: string]: unknown;
}

export interface NewsItem {
  id: string;
  title: string;
  summary: string | null;
  url: string | null;
  source: string | null;
  publishedAt: string | null; // ISO timestamp
}

/**
 * Output of the deterministic Python valuation engine. Agents interpret these
 * numbers; they never recompute them. `intrinsicValue` is whatever the engine
 * finally settled on after its own chain of adjustments.
 */
export interface ValuationSnapshot {
  price: number | null;
  intrinsicValue: number | null;
  ivLow: number | null;
  ivHigh: number | null;
  marginOfSafetyPct: number | null;
  waccPct: number | null;
  terminalGrowthPct: number | null;
  stage1GrowthPct: number | null;
  impliedGrowthPct: number | null;
  fcfBase: number | null;
  netDebt: number | null;
  sharesOut: number | null;
  confidence: 'low' | 'medium' | 'high' | null;
  confidenceWeaknesses: string[];
  qualityMetrics: QualityMetric[];

  // How the displayed intrinsic value was produced. The engine replaces its own
  // DCF result before display (an FCFE model, a sector method, a blend with the
  // analyst target), so the inputs above may not be the inputs to
  // intrinsicValue. Shown to dcf only. Optional: older contexts omit them.

  /** The engine's pure DCF value from the inputs above, before those adjustments. */
  baseIv?: number | null;
  /** Which path produced intrinsicValue. */
  ivSourceLabel?: string | null;
  /** True when an FCFE model replaced the DCF result. */
  fin415Used?: boolean | null;
  /** The sector method applied, such as biotech or banking. */
  sectorValLabel?: string | null;
  /** The value before the blend with the analyst target. */
  consensusAnchorPreIv?: number | null;
}

export interface QualityMetric {
  key: string;
  label: string;
  value: number | string | null;
  unit: 'pct' | 'ratio' | 'raw' | null;
  tier: string | null;
}

/**
 * Stage-2 input. The runner writes it; stage-1 agents never see it, and no
 * agent ever reaches for another agent's module to get at this.
 */
export type UpstreamResults = Partial<Record<AgentSlug, AgentResult<unknown>>>;

export interface AgentContext {
  ticker: string;
  tier: Tier;
  /** When the caller assembled this data. Agents treat it as "now". */
  asOf: string;
  profile: CompanyProfile;
  statements: FinancialStatements;
  prices: PriceHistory;
  yfinance: VendorPayload;
  finviz: VendorPayload | null;
  news: NewsItem[];
  valuation: ValuationSnapshot | null;
  /** Populated by runner.ts for stage-2 agents only. */
  upstream?: UpstreamResults;
  signal?: AbortSignal;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Results                                                                    */
/* ────────────────────────────────────────────────────────────────────────── */

export type AgentStatus = 'ok' | 'unavailable';

export type AgentErrorKind =
  | 'validation'
  | 'api'
  | 'timeout'
  | 'cooldown'
  | 'no_api_key'
  | 'insufficient_data'
  | 'unknown';

export interface AgentError {
  kind: AgentErrorKind;
  message: string;
  /** Trimmed model reply or API detail. Never rendered to end users. */
  detail?: string;
}

export interface TokenUsage {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
}

export interface AgentMeta {
  slug: AgentSlug;
  ticker: string;
  model: string | null;
  /** 1 on a clean first pass, 2 when the schema-repair retry was needed. */
  attempts: number;
  cached: boolean;
  latencyMs: number;
  generatedAt: string;
  usage: TokenUsage | null;
}

export interface AgentResult<T> {
  slug: AgentSlug;
  status: AgentStatus;
  data: T | null;
  meta: AgentMeta;
  error: AgentError | null;
}

export interface AgentModule<T> {
  slug: AgentSlug;
  title: string;
  /** 1 = runs in parallel on raw data. 2 = runs after stage 1, reads upstream. */
  stage: 1 | 2;
  ttlSeconds: number;
  /** The agent's own model ID, exported from its run.ts. VALUS_AGENT_MODEL overrides it. */
  model: string;
  /** Hard max_tokens cap (thinking plus reply), exported from its run.ts. */
  maxTokens: number;
  run(ctx: AgentContext): Promise<AgentResult<T>>;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Result constructors                                                        */
/* ────────────────────────────────────────────────────────────────────────── */

export interface ResultInit {
  slug: AgentSlug;
  ticker: string;
  startedAt: number;
  model?: string | null;
  attempts?: number;
  usage?: TokenUsage | null;
  cached?: boolean;
}

export function ok<T>(data: T, init: ResultInit): AgentResult<T> {
  return {
    slug: init.slug,
    status: 'ok',
    data,
    meta: buildMeta(init),
    error: null,
  };
}

/**
 * The degraded result. Every failure path in every agent ends here — a missing
 * API key, a timeout, a reply that would not validate twice in a row. The UI
 * gets a well-formed object with `data: null` and renders the gap.
 */
export function unavailable(error: AgentError, init: ResultInit): AgentResult<never> {
  return {
    slug: init.slug,
    status: 'unavailable',
    data: null,
    meta: buildMeta(init),
    error,
  };
}

/**
 * Last step of every run.ts: validate the Field-wrapped output against the
 * agent's public schema. The model reply was already validated (and repaired)
 * upstream, so a failure here is a bug in the agent's own wrapping code. It
 * still degrades instead of throwing, and it is never retried, because the
 * same code would fail the same way.
 */
export function finalizeOutput<S extends z.ZodType>(
  schema: S,
  output: z.infer<S>,
  init: ResultInit,
): AgentResult<z.infer<S>> {
  const parsed = schema.safeParse(output);
  if (!parsed.success) {
    return unavailable(
      {
        kind: 'validation',
        message: 'agent output failed its public schema',
        detail: parsed.error.issues
          .slice(0, 6)
          .map((issue) => `${issue.path.join('.') || '(root)'}: ${issue.message}`)
          .join('; '),
      },
      init,
    );
  }
  return ok(parsed.data, init);
}

function buildMeta(init: ResultInit): AgentMeta {
  return {
    slug: init.slug,
    ticker: init.ticker,
    model: init.model ?? null,
    attempts: init.attempts ?? 0,
    cached: init.cached ?? false,
    latencyMs: Math.max(0, Math.round(Date.now() - init.startedAt)),
    generatedAt: new Date().toISOString(),
    usage: init.usage ?? null,
  };
}

/** Narrowing helper for callers: `if (isOk(res)) res.data.band...`. */
export function isOk<T>(result: AgentResult<T>): result is AgentResult<T> & { data: T } {
  return result.status === 'ok' && result.data !== null;
}
