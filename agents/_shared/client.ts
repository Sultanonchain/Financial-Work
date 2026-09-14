/**
 * The only place in the agent layer that talks to Anthropic.
 *
 * Responsibilities:
 *   • one lazily-built client with a timeout and SDK-level retries
 *   • structured output + the schema-repair retry the contract requires
 *     (one retry with the validation error appended, then give up cleanly)
 *   • token logging, including cache hit/miss so a broken cached prefix is
 *     visible instead of just expensive
 *   • a rate-limit cooldown: after a 429/529 we stop calling for a few minutes
 *     rather than adding latency to every page view
 *   • a test seam (setModelTransport) so fixtures can drive agents offline
 *
 * Nothing here knows what a stock is. Agents pass a system prompt, a user turn
 * and a zod schema; they get back either validated data or a typed error.
 * This module never throws.
 */

import Anthropic from '@anthropic-ai/sdk';
import { zodOutputFormat } from '@anthropic-ai/sdk/helpers/zod';
import type { z } from 'zod';

import type { AgentError, AgentSlug, TokenUsage } from './types.ts';

/* ────────────────────────────────────────────────────────────────────────── */
/* Configuration                                                              */
/* ────────────────────────────────────────────────────────────────────────── */

export interface ClientConfig {
  apiKey: string | undefined;
  /** Default model for every agent. Override per call via GenerateOptions. */
  model: string;
  /** Per-request timeout in MILLISECONDS (the TS SDK takes ms, not seconds). */
  timeoutMs: number;
  /** SDK-level retries for 408/409/429/5xx and connection errors. */
  maxRetries: number;
  /** How long to stop calling after a 429/529 survives the SDK retries. */
  cooldownMs: number;
  /**
   * Server-side refusal fallback. When a safety classifier declines a request,
   * the API re-runs it on a fallback model inside the same call instead of
   * returning an empty refusal. First-party API only; turn off on Bedrock,
   * Vertex or Foundry.
   */
  refusalFallbacks: boolean;
}

function defaultConfig(): ClientConfig {
  return {
    apiKey: process.env['ANTHROPIC_API_KEY'],
    model: process.env['VALUS_AGENT_MODEL'] ?? 'claude-opus-5',
    timeoutMs: 45_000,
    maxRetries: 2,
    cooldownMs: 5 * 60_000,
    refusalFallbacks: true,
  };
}

let config: ClientConfig = defaultConfig();
let client: Anthropic | null = null;

export function configureClient(patch: Partial<ClientConfig>): void {
  config = { ...config, ...patch };
  client = null; // rebuilt on next use so the patch takes effect
}

export function getClientConfig(): Readonly<ClientConfig> {
  return config;
}

/** Back to environment defaults, no transport, no cooldown. Used between tests. */
export function resetClient(): void {
  config = defaultConfig();
  client = null;
  transport = null;
  replyRecorder = null;
  cooldownUntil = 0;
}

function getClient(): Anthropic {
  if (!client) {
    client = new Anthropic({
      apiKey: config.apiKey,
      timeout: config.timeoutMs,
      maxRetries: config.maxRetries,
    });
  }
  return client;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Token logging                                                              */
/* ────────────────────────────────────────────────────────────────────────── */

export interface TokenLogRecord {
  slug: AgentSlug;
  ticker: string;
  model: string;
  attempt: number;
  latencyMs: number;
  /** This attempt only, not the running total. */
  usage: TokenUsage;
  /** false on a real call means the cached system prefix missed. */
  cacheHit: boolean;
  outcome: 'ok' | 'invalid' | 'refused' | 'error';
  stopReason: string | null;
}

export type TokenLogger = (record: TokenLogRecord) => void;

const consoleLogger: TokenLogger = (r) => {
  const cache = r.cacheHit ? `cache_read=${r.usage.cacheReadTokens}` : 'cache_miss';
  console.log(
    `[valus.agents] ${r.slug} ${r.ticker} ${r.model} attempt=${r.attempt} ${r.outcome} ` +
      `stop=${r.stopReason ?? '-'} in=${r.usage.inputTokens} out=${r.usage.outputTokens} ` +
      `${cache} ${r.latencyMs}ms`,
  );
};

let tokenLogger: TokenLogger = consoleLogger;

export function setTokenLogger(logger: TokenLogger | null): void {
  tokenLogger = logger ?? consoleLogger;
}

function log(record: TokenLogRecord): void {
  try {
    tokenLogger(record);
  } catch {
    // A broken logger must never take an agent down with it.
  }
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Test seam                                                                  */
/* ────────────────────────────────────────────────────────────────────────── */

export interface ModelTransportRequest {
  slug: AgentSlug;
  ticker: string;
  system: string;
  user: string;
  /** 1 on the first pass, 2 on the schema-repair retry. */
  attempt: number;
  /** The validation error being repaired, present only on attempt 2. */
  repairHint: string | null;
}

/**
 * Stand-in for the API. When set, no HTTP happens: whatever the transport
 * returns (an object, or a raw JSON string) is fed through the same
 * validation and repair loop as a real reply, which is what makes the
 * recorded fixtures an actual test of each agent.
 */
export type ModelTransport = (req: ModelTransportRequest) => unknown;

let transport: ModelTransport | null = null;

export function setModelTransport(next: ModelTransport | null): void {
  transport = next;
}

/**
 * Observes every reply that passed validation, exactly as the agent received
 * it. tests/record-fixtures.ts uses it to capture live replies as fixtures.
 */
export type ReplyRecorder = (slug: AgentSlug, ticker: string, reply: unknown) => void;

let replyRecorder: ReplyRecorder | null = null;

export function setReplyRecorder(next: ReplyRecorder | null): void {
  replyRecorder = next;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Cooldown                                                                   */
/* ────────────────────────────────────────────────────────────────────────── */

let cooldownUntil = 0;

export function isCoolingDown(): boolean {
  return Date.now() < cooldownUntil;
}

function enterCooldown(): void {
  cooldownUntil = Date.now() + config.cooldownMs;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Generate + validate                                                        */
/* ────────────────────────────────────────────────────────────────────────── */

export type Effort = 'low' | 'medium' | 'high' | 'xhigh' | 'max';

export interface GenerateOptions<S extends z.ZodType> {
  slug: AgentSlug;
  ticker: string;
  /** Byte-stable across tickers. Anything per-ticker in here kills the cache. */
  system: string;
  /** Per-ticker context. Sits after the cache breakpoint. */
  user: string;
  /** The model-facing schema. Also sent as the structured-output format. */
  schema: S;
  maxTokens?: number;
  effort?: Effort;
  model?: string;
  signal?: AbortSignal | undefined;
}

export interface GenerateResult<T> {
  data: T | null;
  error: AgentError | null;
  attempts: number;
  model: string;
  usage: TokenUsage | null;
}

const MAX_ATTEMPTS = 2;
const DEFAULT_MAX_TOKENS = 8_000;
const MAX_TOKENS_CEILING = 16_000;
const FALLBACK_BETA = 'server-side-fallback-2026-07-01';

interface Reply {
  /** Already-structured value (transport only). */
  value: unknown;
  text: string;
  stopReason: string | null;
  usage: TokenUsage;
  refusal: string | null;
}

interface Repair {
  hint: string;
  previousReply: string;
}

/**
 * One model call, validated against `schema`.
 *
 * Attempt 1 asks for structured output. If the reply will not validate,
 * attempt 2 replays the turn with the model's reply and the zod error appended
 * and asks for a correction. If that also fails the caller gets a validation
 * error and degrades. Transport and API failures are not retried here: the SDK
 * has already retried the retryable statuses, and a second identical request
 * would only double the latency.
 */
export async function generateValidated<S extends z.ZodType>(
  opts: GenerateOptions<S>,
): Promise<GenerateResult<z.infer<S>>> {
  const model = opts.model ?? config.model;
  const total = emptyUsage();

  if (!transport && !config.apiKey) {
    return failure(model, 0, null, {
      kind: 'no_api_key',
      message: 'ANTHROPIC_API_KEY is not set in this runtime',
    });
  }
  if (!transport && isCoolingDown()) {
    return failure(model, 0, null, {
      kind: 'cooldown',
      message: 'Anthropic rate-limit cooldown is active; skipped the call',
    });
  }

  let maxTokens = opts.maxTokens ?? DEFAULT_MAX_TOKENS;
  let repair: Repair | null = null;
  let lastError: AgentError | null = null;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const startedAt = Date.now();
    let reply: Reply;

    try {
      reply = transport
        ? await callTransport(transport, opts, attempt, repair)
        : await callApi(opts, model, maxTokens, repair);
    } catch (err) {
      const error = toAgentError(err);
      log({
        slug: opts.slug,
        ticker: opts.ticker,
        model,
        attempt,
        latencyMs: Date.now() - startedAt,
        usage: emptyUsage(),
        cacheHit: false,
        outcome: 'error',
        stopReason: null,
      });
      return failure(model, attempt, total, error);
    }

    addUsage(total, reply.usage);
    const logBase = {
      slug: opts.slug,
      ticker: opts.ticker,
      model,
      attempt,
      latencyMs: Date.now() - startedAt,
      usage: reply.usage,
      cacheHit: reply.usage.cacheReadTokens > 0,
      stopReason: reply.stopReason,
    };

    if (reply.stopReason === 'refusal') {
      // The fallback chain (when enabled) already had its chance inside the
      // call. Asking the same thing again would be declined the same way.
      log({ ...logBase, outcome: 'refused' });
      return failure(model, attempt, total, {
        kind: 'api',
        message: 'the model declined this request',
        ...(reply.refusal ? { detail: reply.refusal } : {}),
      });
    }

    const check = validate(opts.schema, reply);
    log({ ...logBase, outcome: check.success ? 'ok' : 'invalid' });

    if (check.success) {
      try {
        replyRecorder?.(opts.slug, opts.ticker, check.data);
      } catch {
        // Recording is diagnostic; it never affects the result.
      }
      return { data: check.data, error: null, attempts: attempt, model, usage: total };
    }

    lastError = {
      kind: 'validation',
      message: `model reply failed schema validation on attempt ${attempt}`,
      detail: check.hint,
    };
    repair = { hint: check.hint, previousReply: reply.text };
    if (reply.stopReason === 'max_tokens') {
      // max_tokens caps adaptive thinking and the JSON together; a truncated
      // reply gets more room on the repair pass rather than the same wall.
      maxTokens = Math.min(maxTokens * 2, MAX_TOKENS_CEILING);
    }
  }

  return failure(model, MAX_ATTEMPTS, total, lastError ?? {
    kind: 'validation',
    message: 'model reply failed schema validation',
  });
}

async function callApi<S extends z.ZodType>(
  opts: GenerateOptions<S>,
  model: string,
  maxTokens: number,
  repair: Repair | null,
): Promise<Reply> {
  // zodOutputFormat() gives the strict JSON schema the API wants. It is sent
  // through create(), not parse(): parse() throws on a reply that fails zod,
  // which would skip the repair turn the contract requires.
  const { schema } = zodOutputFormat(opts.schema);

  const params: Anthropic.Beta.Messages.MessageCreateParamsNonStreaming = {
    model,
    max_tokens: maxTokens,
    // Byte-identical across tickers and marked cacheable. The per-ticker
    // context goes in the user turn, after the breakpoint.
    system: [{ type: 'text', text: opts.system, cache_control: { type: 'ephemeral' } }],
    messages: buildMessages(opts.user, repair),
    // Thinking is left at the model default (adaptive on Opus 5) and bounded by
    // effort, which is cheaper and better behaved than disabling it.
    output_config: {
      effort: opts.effort ?? 'low',
      format: { type: 'json_schema', schema },
    },
  };
  if (config.refusalFallbacks) {
    params.betas = [FALLBACK_BETA];
    params.fallbacks = 'default';
  }

  const response = await getClient().beta.messages.create(
    params,
    opts.signal ? { signal: opts.signal } : undefined,
  );

  const text = response.content
    .map((block) => (block.type === 'text' ? block.text : ''))
    .join('')
    .trim();

  return {
    value: undefined,
    text,
    stopReason: response.stop_reason,
    usage: {
      inputTokens: response.usage.input_tokens ?? 0,
      outputTokens: response.usage.output_tokens ?? 0,
      cacheReadTokens: response.usage.cache_read_input_tokens ?? 0,
      cacheWriteTokens: response.usage.cache_creation_input_tokens ?? 0,
    },
    refusal:
      response.stop_reason === 'refusal' && response.stop_details
        ? JSON.stringify(response.stop_details).slice(0, 300)
        : null,
  };
}

async function callTransport<S extends z.ZodType>(
  fn: ModelTransport,
  opts: GenerateOptions<S>,
  attempt: number,
  repair: Repair | null,
): Promise<Reply> {
  const out = await fn({
    slug: opts.slug,
    ticker: opts.ticker,
    system: opts.system,
    user: opts.user,
    attempt,
    repairHint: repair?.hint ?? null,
  });
  const isText = typeof out === 'string';
  return {
    value: isText ? undefined : out,
    text: isText ? out : (JSON.stringify(out) ?? ''),
    stopReason: 'end_turn',
    usage: emptyUsage(),
    refusal: null,
  };
}

function buildMessages(
  user: string,
  repair: Repair | null,
): Anthropic.Beta.Messages.BetaMessageParam[] {
  if (!repair) {
    return [{ role: 'user', content: user }];
  }
  return [
    { role: 'user', content: user },
    { role: 'assistant', content: repair.previousReply || '(empty reply)' },
    {
      role: 'user',
      content:
        'That reply did not satisfy the required output schema:\n' +
        `${repair.hint}\n\n` +
        'Return the corrected object for the same company. Fix only what these ' +
        'errors require and keep every other value as it was.',
    },
  ];
}

type Validation<T> = { success: true; data: T } | { success: false; hint: string };

function validate<S extends z.ZodType>(schema: S, reply: Reply): Validation<z.infer<S>> {
  let candidate = reply.value;

  if (candidate === undefined) {
    const truncated = reply.stopReason === 'max_tokens';
    if (!reply.text) {
      return {
        success: false,
        hint: truncated
          ? '  • (root): the reply was cut off before any JSON was written; keep it shorter'
          : '  • (root): the reply was empty',
      };
    }
    try {
      candidate = JSON.parse(reply.text);
    } catch (err) {
      const reason = err instanceof Error ? err.message : 'unparseable';
      return {
        success: false,
        hint:
          `  • (root): the reply was not valid JSON (${reason})` +
          (truncated ? '; it was cut off at the token limit, so keep it shorter' : ''),
      };
    }
  }

  const parsed = schema.safeParse(candidate);
  return parsed.success
    ? { success: true, data: parsed.data }
    : { success: false, hint: formatZodError(parsed.error) };
}

function formatZodError(error: z.ZodError): string {
  const lines = error.issues.slice(0, 12).map((issue) => {
    const path = issue.path.length ? issue.path.join('.') : '(root)';
    return `  • ${path}: ${issue.message}`;
  });
  const extra = error.issues.length - lines.length;
  return extra > 0 ? `${lines.join('\n')}\n  • …and ${extra} more` : lines.join('\n');
}

function toAgentError(err: unknown): AgentError {
  if (err instanceof Anthropic.APIUserAbortError) {
    return { kind: 'timeout', message: 'request aborted before the model replied' };
  }
  if (err instanceof Anthropic.APIConnectionTimeoutError) {
    return { kind: 'timeout', message: 'Anthropic request timed out' };
  }
  if (err instanceof Anthropic.RateLimitError) {
    enterCooldown();
    return { kind: 'api', message: 'rate limited by Anthropic; cooling down' };
  }
  if (err instanceof Anthropic.APIError) {
    if (err.status === 529) {
      enterCooldown(); // overloaded: same treatment as a 429
    }
    return {
      kind: 'api',
      message: `Anthropic API error ${err.status ?? 'without status'}`,
      detail: err.message.slice(0, 300),
    };
  }
  return {
    kind: 'unknown',
    message: err instanceof Error ? err.message.slice(0, 300) : 'unknown model failure',
  };
}

function failure<T>(
  model: string,
  attempts: number,
  usage: TokenUsage | null,
  error: AgentError,
): GenerateResult<T> {
  return { data: null, error, attempts, model, usage };
}

function emptyUsage(): TokenUsage {
  return { inputTokens: 0, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 };
}

function addUsage(into: TokenUsage, add: TokenUsage): void {
  into.inputTokens += add.inputTokens;
  into.outputTokens += add.outputTokens;
  into.cacheReadTokens += add.cacheReadTokens;
  into.cacheWriteTokens += add.cacheWriteTokens;
}
