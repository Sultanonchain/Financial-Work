/**
 * The only place in the agent layer that talks to Anthropic.
 *
 * Responsibilities:
 *   • the model table: which model IDs agents use, which request features each
 *     model accepts, and how an agent's model is resolved
 *   • one lazily-built client with a timeout and SDK-level retries
 *   • structured output + the schema-repair retry the contract requires
 *     (one retry with the validation error appended, then give up cleanly)
 *   • token logging per attempt, naming the model that served it, so cost is
 *     attributable per agent
 *   • a rate-limit cooldown: after a 429/529 we stop calling for a few minutes
 *     rather than adding latency to every page view
 *   • a test seam (setModelTransport) so fixtures can drive agents offline
 *
 * Nothing here knows what a stock is. Agents pass a system prompt, a user turn,
 * a zod schema and their model; they get back either validated data or a typed
 * error. This module never throws.
 */

import Anthropic from '@anthropic-ai/sdk';
import { zodOutputFormat } from '@anthropic-ai/sdk/helpers/zod';
import type { z } from 'zod';

import type { AgentError, AgentSlug, TokenUsage } from './types.ts';

/* ────────────────────────────────────────────────────────────────────────── */
/* Models: the single place to bump versions                                  */
/* ────────────────────────────────────────────────────────────────────────── */

/**
 * Full model IDs, not aliases. An alias moves to a new snapshot on its own,
 * which would change agent output and cost with no diff in this repo. Sonnet 5
 * is published without a dated snapshot, so its full ID is the bare name.
 */
export const MODELS = {
  sonnet: 'claude-sonnet-5',
  haiku: 'claude-haiku-4-5-20251001',
} as const;

/** Used only when an agent names no model and no override is set. */
export const DEFAULT_MODEL: string = MODELS.sonnet;

/** Global override, for testing: when set, every agent uses this model. */
export const MODEL_OVERRIDE_ENV = 'VALUS_AGENT_MODEL';

export interface ModelFeatures {
  /** `output_config.effort`. Haiku 4.5 rejects it with a 400. */
  effort: boolean;
  /** Server-side `fallbacks: "default"`, built for the Opus 5 / Fable 5 refusal classifiers. */
  refusalFallbacks: boolean;
}

/**
 * Request features that differ by model. Structured outputs are not listed
 * because every model here supports them. A model missing from this table,
 * typically a VALUS_AGENT_MODEL override, gets the conservative set.
 */
const MODEL_FEATURES: Readonly<Record<string, ModelFeatures>> = {
  [MODELS.sonnet]: { effort: true, refusalFallbacks: false },
  [MODELS.haiku]: { effort: false, refusalFallbacks: false },
  // Not used by any agent; listed so an override onto it keeps its features.
  'claude-opus-5': { effort: true, refusalFallbacks: true },
};

const CONSERVATIVE_FEATURES: ModelFeatures = { effort: false, refusalFallbacks: false };

export function modelFeatures(model: string): ModelFeatures {
  return MODEL_FEATURES[model] ?? CONSERVATIVE_FEATURES;
}

export type ModelSource = 'env' | 'agent' | 'default';

export interface ResolvedModel {
  id: string;
  source: ModelSource;
}

/**
 * VALUS_AGENT_MODEL, then the agent's own model, then DEFAULT_MODEL. The env
 * var is read on every call rather than at import, so setting it takes effect
 * without reloading modules. `env` is injectable so tooling (agents:status) can
 * report the resolution for a given environment.
 */
export function resolveModel(
  agentModel: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): ResolvedModel {
  const override = env[MODEL_OVERRIDE_ENV]?.trim();
  if (override) return { id: override, source: 'env' };
  if (agentModel) return { id: agentModel, source: 'agent' };
  return { id: DEFAULT_MODEL, source: 'default' };
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Configuration                                                              */
/* ────────────────────────────────────────────────────────────────────────── */

export interface ClientConfig {
  apiKey: string | undefined;
  /** Per-request timeout in MILLISECONDS (the TS SDK takes ms, not seconds). */
  timeoutMs: number;
  /** SDK-level retries for 408/409/429/5xx and connection errors. */
  maxRetries: number;
  /** How long to stop calling after a 429/529 survives the SDK retries. */
  cooldownMs: number;
  /**
   * Server-side refusal fallback, sent only to models whose MODEL_FEATURES
   * allow it. First-party API only; turn off on Bedrock, Vertex or Foundry.
   */
  refusalFallbacks: boolean;
}

function defaultConfig(): ClientConfig {
  return {
    apiKey: process.env['ANTHROPIC_API_KEY'],
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
  /** The model that served this attempt, as reported by the API. Bill against this. */
  model: string;
  /** The model that was asked for. Differs from `model` only when a refusal fallback served the reply. */
  requestedModel: string;
  /** Why `requestedModel` was chosen: the env override, the agent's own model, or the default. */
  modelSource: ModelSource;
  attempt: number;
  latencyMs: number;
  /** This attempt only, not the running total. */
  usage: TokenUsage;
  /**
   * Whether the system prompt was served from cache. A miss is expected when
   * the prompt is below the model's cache minimum (1,024 tokens on Sonnet 5,
   * 4,096 on Haiku 4.5).
   */
  cacheHit: boolean;
  outcome: 'ok' | 'invalid' | 'refused' | 'error';
  stopReason: string | null;
  /** For an invalid attempt: the validation issues sent back in the repair turn, trimmed. */
  issues?: string;
}

export type TokenLogger = (record: TokenLogRecord) => void;

const consoleLogger: TokenLogger = (r) => {
  const cache = r.cacheHit ? `cache_read=${r.usage.cacheReadTokens}` : 'cache_miss';
  const requested = r.requestedModel === r.model ? '' : ` requested=${r.requestedModel}`;
  console.log(
    `[valus.agents] ${r.slug} ${r.ticker} model=${r.model}${requested} model_source=${r.modelSource} ` +
      `attempt=${r.attempt} ${r.outcome} stop=${r.stopReason ?? '-'} ` +
      `in=${r.usage.inputTokens} out=${r.usage.outputTokens} ${cache} ${r.latencyMs}ms` +
      (r.issues ? ` issues="${r.issues}"` : ''),
  );
};

/** One line, bounded: a repair costs a full extra call, so the log should say why. */
function summarizeIssues(hint: string): string {
  const line = hint
    .split('\n')
    .map((part) => part.replace(/^\s*•\s*/, '').trim())
    .filter(Boolean)
    .join(' | ');
  return line.length > 300 ? `${line.slice(0, 299)}…` : line;
}

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
  /** The resolved model ID, after the env override. */
  model: string;
  /** The agent's max_tokens cap. */
  maxTokens: number;
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
  /** The agent's own model. VALUS_AGENT_MODEL still wins; see resolveModel. */
  model?: string;
  /** Hard max_tokens cap (thinking plus reply), for both attempts. */
  maxTokens?: number;
  /** Dropped for models that do not accept effort (see MODEL_FEATURES). */
  effort?: Effort;
  signal?: AbortSignal | undefined;
}

export interface GenerateResult<T> {
  data: T | null;
  error: AgentError | null;
  attempts: number;
  /** The model that served the last attempt (the requested one if none was served). */
  model: string;
  usage: TokenUsage | null;
}

export interface Repair {
  hint: string;
  previousReply: string;
}

const MAX_ATTEMPTS = 2;
/** Only for callers that set no cap; every registered agent sets its own. */
const DEFAULT_MAX_TOKENS = 4_000;
const FALLBACK_BETA = 'server-side-fallback-2026-07-01';

interface Reply {
  /** Already-structured value (transport only). */
  value: unknown;
  text: string;
  /** The model that actually produced the reply. */
  model: string;
  stopReason: string | null;
  usage: TokenUsage;
  refusal: string | null;
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
  const { id: requestedModel, source: modelSource } = resolveModel(opts.model);
  let servedModel = requestedModel;
  const total = emptyUsage();

  if (!transport && !config.apiKey) {
    return failure(requestedModel, 0, null, {
      kind: 'no_api_key',
      message: 'ANTHROPIC_API_KEY is not set in this runtime',
    });
  }
  if (!transport && isCoolingDown()) {
    return failure(requestedModel, 0, null, {
      kind: 'cooldown',
      message: 'Anthropic rate-limit cooldown is active; skipped the call',
    });
  }

  const maxTokens = opts.maxTokens ?? DEFAULT_MAX_TOKENS;
  let repair: Repair | null = null;
  let lastError: AgentError | null = null;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const startedAt = Date.now();
    const identity = {
      slug: opts.slug,
      ticker: opts.ticker,
      requestedModel,
      modelSource,
      attempt,
    };
    let reply: Reply;

    try {
      reply = transport
        ? await callTransport(transport, opts, requestedModel, maxTokens, attempt, repair)
        : await callApi(opts, requestedModel, maxTokens, repair);
    } catch (err) {
      const error = toAgentError(err);
      log({
        ...identity,
        model: requestedModel,
        latencyMs: Date.now() - startedAt,
        usage: emptyUsage(),
        cacheHit: false,
        outcome: 'error',
        stopReason: null,
      });
      return failure(servedModel, attempt, total, error);
    }

    servedModel = reply.model;
    addUsage(total, reply.usage);
    const logBase = {
      ...identity,
      model: reply.model,
      latencyMs: Date.now() - startedAt,
      usage: reply.usage,
      cacheHit: reply.usage.cacheReadTokens > 0,
      stopReason: reply.stopReason,
    };

    if (reply.stopReason === 'refusal') {
      // Any fallback chain already had its chance inside the call. Asking the
      // same thing again would be declined the same way.
      log({ ...logBase, outcome: 'refused' });
      return failure(servedModel, attempt, total, {
        kind: 'api',
        message: 'the model declined this request',
        ...(reply.refusal ? { detail: reply.refusal } : {}),
      });
    }

    const check = validate(opts.schema, reply);
    log(
      check.success
        ? { ...logBase, outcome: 'ok' }
        : { ...logBase, outcome: 'invalid', issues: summarizeIssues(check.hint) },
    );

    if (check.success) {
      try {
        replyRecorder?.(opts.slug, opts.ticker, check.data);
      } catch {
        // Recording is diagnostic; it never affects the result.
      }
      return { data: check.data, error: null, attempts: attempt, model: servedModel, usage: total };
    }

    lastError = {
      kind: 'validation',
      message: `model reply failed schema validation on attempt ${attempt}`,
      detail: check.hint,
    };
    // A reply cut off at max_tokens is repaired under the same cap: the cap is a
    // latency and cost bound, and the repair hint asks for a shorter reply.
    repair = { hint: check.hint, previousReply: reply.text };
  }

  return failure(servedModel, MAX_ATTEMPTS, total, lastError ?? {
    kind: 'validation',
    message: 'model reply failed schema validation',
  });
}

/**
 * The exact request body for one attempt, shaped for `model`. Exported so the
 * per-model request shape can be tested without HTTP.
 */
export function buildRequest<S extends z.ZodType>(
  opts: GenerateOptions<S>,
  model: string,
  maxTokens: number,
  repair: Repair | null,
): Anthropic.Beta.Messages.MessageCreateParamsNonStreaming {
  const features = modelFeatures(model);
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
    // Thinking is left at each model's default (adaptive on Sonnet 5, off on
    // Haiku 4.5). Where the model accepts effort, effort bounds it.
    output_config: {
      format: { type: 'json_schema', schema },
      ...(features.effort ? { effort: opts.effort ?? 'low' } : {}),
    },
  };
  if (config.refusalFallbacks && features.refusalFallbacks) {
    params.betas = [FALLBACK_BETA];
    params.fallbacks = 'default';
  }
  return params;
}

async function callApi<S extends z.ZodType>(
  opts: GenerateOptions<S>,
  model: string,
  maxTokens: number,
  repair: Repair | null,
): Promise<Reply> {
  const response = await getClient().beta.messages.create(
    buildRequest(opts, model, maxTokens, repair),
    opts.signal ? { signal: opts.signal } : undefined,
  );

  const text = response.content
    .map((block) => (block.type === 'text' ? block.text : ''))
    .join('')
    .trim();

  return {
    value: undefined,
    text,
    model: response.model,
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
  model: string,
  maxTokens: number,
  attempt: number,
  repair: Repair | null,
): Promise<Reply> {
  const out = await fn({
    slug: opts.slug,
    ticker: opts.ticker,
    model,
    maxTokens,
    system: opts.system,
    user: opts.user,
    attempt,
    repairHint: repair?.hint ?? null,
  });
  const isText = typeof out === 'string';
  return {
    value: isText ? undefined : out,
    text: isText ? out : (JSON.stringify(out) ?? ''),
    model,
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
