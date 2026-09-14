/**
 * The orchestrator, and the only module that composes agents.
 *
 *   stage 1 ─┬─ dcf ───────┐
 *            ├─ catalyst ──┤
 *            ├─ news ──────┼──▶ stage 2 ── verdict
 *            └─ redflag ───┘
 *
 * Stage 1 runs in parallel. Stage 2 runs once all of stage 1 has settled,
 * reading the stage-1 results off ctx.upstream. Each agent is wrapped in a
 * guard (cache read-through, deadline, catch-all) that always resolves to an
 * AgentResult, so a failed or slow agent becomes one "unavailable" entry and
 * never blocks its siblings or the verdict.
 */

import { withCache } from './cache.ts';
import { buildCompanySection, type CompanySection } from './company.ts';
import { registry, STAGE_ONE, STAGE_TWO, type AgentOutputs } from './registry.ts';
import {
  FIELD_SOURCES,
  TIER_VISIBILITY,
  VISIBILITY_TIERS,
  unavailable,
  type AgentContext,
  type AgentErrorKind,
  type AgentMeta,
  type AgentModule,
  type AgentResult,
  type AgentSlug,
  type AgentStatus,
  type Field,
  type FieldSource,
  type Tier,
  type UpstreamResults,
  type VisibilityTier,
} from './types.ts';

export const DEFAULT_AGENT_TIMEOUT_MS = 60_000;

export interface RunOptions {
  /** Hard deadline per agent. The in-flight request is aborted when it passes. */
  agentTimeoutMs?: number;
  /** Ignore cached results (fresh results are still written). */
  refresh?: boolean;
  /** Clock override for the cache key. Tests only. */
  now?: Date;
  /** Fires as each agent settles, so the UI can fill cards progressively. */
  onResult?: (result: AgentResult<unknown>) => void;
}

export type AgentResults = { [K in AgentSlug]: AgentResult<AgentOutputs[K]> };

export interface AgentReport {
  ticker: string;
  tier: Tier;
  asOf: string;
  startedAt: string;
  durationMs: number;
  /** Stats and facts from the data layer. No agent, no model call; see company.ts. */
  company: CompanySection;
  results: AgentResults;
  unavailable: AgentSlug[];
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Orchestration                                                              */
/* ────────────────────────────────────────────────────────────────────────── */

export async function runAgents(ctx: AgentContext, options: RunOptions = {}): Promise<AgentReport> {
  const started = Date.now();
  // Built from the context alone, before any agent runs, so it cannot fail with them.
  const company = buildCompanySection(ctx);

  // runGuarded never rejects, so Promise.all cannot short-circuit here.
  const stageOne = await Promise.all(
    STAGE_ONE.map(async (slug) => [slug, await runGuarded(slug, ctx, options)] as const),
  );

  const upstream: UpstreamResults = Object.fromEntries(stageOne);
  const stageTwo = await Promise.all(
    STAGE_TWO.map(
      async (slug) => [slug, await runGuarded(slug, { ...ctx, upstream }, options)] as const,
    ),
  );

  const results = Object.fromEntries([...stageOne, ...stageTwo]) as AgentResults;

  return {
    ticker: ctx.ticker,
    tier: ctx.tier,
    asOf: ctx.asOf,
    startedAt: new Date(started).toISOString(),
    durationMs: Date.now() - started,
    company,
    results,
    unavailable: (Object.keys(results) as AgentSlug[]).filter(
      (slug) => results[slug].status === 'unavailable',
    ),
  };
}

/**
 * Run a single agent through the same guard. For targeted refreshes (news on
 * its 4h cycle). Calling it for a stage-2 agent without ctx.upstream is
 * allowed; the verdict then works from the valuation snapshot alone.
 */
export function runAgent<K extends AgentSlug>(
  slug: K,
  ctx: AgentContext,
  options: RunOptions = {},
): Promise<AgentResult<AgentOutputs[K]>> {
  return runGuarded(slug, ctx, options);
}

async function runGuarded<K extends AgentSlug>(
  slug: K,
  ctx: AgentContext,
  options: RunOptions,
): Promise<AgentResult<AgentOutputs[K]>> {
  const agent = registry[slug] as AgentModule<AgentOutputs[K]>;
  const startedAt = Date.now();
  const init = { slug, ticker: ctx.ticker, startedAt };
  const timeoutMs = options.agentTimeoutMs ?? DEFAULT_AGENT_TIMEOUT_MS;

  // Our own controller, chained to the caller's signal, so the deadline can
  // abort the in-flight HTTP request instead of leaving it running.
  const controller = new AbortController();
  const forwardAbort = () => controller.abort();
  if (ctx.signal?.aborted) controller.abort();
  ctx.signal?.addEventListener('abort', forwardAbort, { once: true });

  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<AgentResult<never>>((resolve) => {
    timer = setTimeout(() => {
      controller.abort();
      resolve(
        unavailable(
          { kind: 'timeout', message: `${slug} did not finish within ${timeoutMs}ms` },
          init,
        ),
      );
    }, timeoutMs);
  });

  // A stage-2 result built while some upstream agent was unavailable is fine
  // to show, but must not be served for the next four hours after the
  // upstream recovers.
  const upstreamComplete =
    agent.stage === 1 ||
    STAGE_ONE.every((s) => ctx.upstream?.[s]?.status === 'ok');

  const attempt = withCache<AgentOutputs[K]>(
    {
      slug,
      ticker: ctx.ticker,
      ttlSeconds: agent.ttlSeconds,
      now: options.now,
      bypassRead: options.refresh,
      shouldStore: () => upstreamComplete,
    },
    () => agent.run({ ...ctx, signal: controller.signal }),
  ).catch((err: unknown) =>
    // Converting the rejection here (rather than in a try around the race)
    // also covers the case where the deadline already won and the agent
    // throws later, which would otherwise be an unhandled rejection.
    unavailable(
      {
        kind: 'unknown',
        message: err instanceof Error ? err.message.slice(0, 300) : `${slug} threw a non-error`,
      },
      init,
    ),
  );

  const result = await Promise.race([attempt, deadline]);
  clearTimeout(timer);
  ctx.signal?.removeEventListener('abort', forwardAbort);

  try {
    options.onResult?.(result);
  } catch {
    // A throwing UI callback must not take the run down.
  }
  return result;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Tier redaction at the serialization boundary                               */
/* ────────────────────────────────────────────────────────────────────────── */

/** What a Field above the viewer's tier becomes: its label, never its value. */
export interface LockedField {
  visibility: VisibilityTier;
  source: FieldSource;
  locked: true;
}

export type Redacted<T> =
  T extends Field<infer V>
    ? Field<V> | LockedField
    : T extends ReadonlyArray<infer U>
      ? Redacted<U>[]
      : T extends object
        ? { [K in keyof T]: Redacted<T[K]> }
        : T;

export interface ClientAgentResult<T> {
  slug: AgentSlug;
  status: AgentStatus;
  data: Redacted<T> | null;
  meta: AgentMeta;
  /** `detail` is stripped: it can hold model output and API internals. */
  error: { kind: AgentErrorKind; message: string } | null;
}

export interface ClientPayload {
  ticker: string;
  tier: Tier;
  asOf: string;
  company: Redacted<CompanySection>;
  results: { [K in AgentSlug]: ClientAgentResult<AgentOutputs[K]> };
  unavailable: AgentSlug[];
}

/**
 * The only shape that should cross the wire to a browser. Agents always
 * produce the full result (and that is what gets cached); the viewer's tier is
 * applied here, so an upgrade never needs a re-run.
 */
export function toClientPayload(report: AgentReport): ClientPayload {
  const results = Object.fromEntries(
    (Object.keys(report.results) as AgentSlug[]).map((slug) => {
      const r = report.results[slug];
      const client: ClientAgentResult<unknown> = {
        slug: r.slug,
        status: r.status,
        data: r.data === null ? null : redactForTier(r.data, report.tier),
        meta: r.meta,
        error: r.error ? { kind: r.error.kind, message: r.error.message } : null,
      };
      return [slug, client];
    }),
  ) as ClientPayload['results'];

  return {
    ticker: report.ticker,
    tier: report.tier,
    asOf: report.asOf,
    company: redactForTier(report.company, report.tier),
    results,
    unavailable: report.unavailable,
  };
}

/** Replace every Field the tier may not see with a LockedField, at any depth. */
export function redactForTier<T>(value: T, tier: Tier): Redacted<T> {
  const allowed = new Set<VisibilityTier>(TIER_VISIBILITY[tier]);

  const walk = (node: unknown): unknown => {
    if (Array.isArray(node)) return node.map(walk);
    if (node === null || typeof node !== 'object') return node;
    if (isField(node)) {
      if (allowed.has(node.visibility)) return node;
      const locked: LockedField = { visibility: node.visibility, source: node.source, locked: true };
      return locked;
    }
    return Object.fromEntries(Object.entries(node).map(([key, child]) => [key, walk(child)]));
  };

  return walk(value) as Redacted<T>;
}

function isField(node: object): node is Field<unknown> {
  const candidate = node as Partial<Record<'value' | 'visibility' | 'source', unknown>>;
  return (
    'value' in node &&
    VISIBILITY_TIERS.includes(candidate.visibility as VisibilityTier) &&
    FIELD_SOURCES.includes(candidate.source as FieldSource)
  );
}
