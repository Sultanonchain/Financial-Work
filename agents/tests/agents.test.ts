import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { beforeEach, describe, it } from 'node:test';

import { zodOutputFormat } from '@anthropic-ai/sdk/helpers/zod';

import { cacheKey, MemoryStore, setCacheStore } from '../_shared/cache.ts';
import {
  buildRequest,
  configureClient,
  DEFAULT_MODEL,
  MODEL_OVERRIDE_ENV,
  MODELS,
  resetClient,
  resolveModel,
  setModelTransport,
  setTokenLogger,
  type ModelTransportRequest,
  type TokenLogRecord,
} from '../_shared/client.ts';
import { AGENT_SLUGS, registry } from '../_shared/registry.ts';
import { runAgents, toClientPayload } from '../_shared/runner.ts';
import {
  ADVICE_PATTERN,
  FIELD_SOURCES,
  VISIBILITY_TIERS,
  type AgentContext,
  type AgentSlug,
} from '../_shared/types.ts';

import { CatalystModelSchema } from '../catalyst/schema.ts';
import { DcfModelSchema } from '../dcf/schema.ts';
import { NewsModelSchema } from '../news/schema.ts';
import { RedflagModelSchema } from '../redflag/schema.ts';
import { VerdictModelSchema, type VerdictOutput } from '../verdict/schema.ts';

import { AGENTS_ROOT, loadFixture, toJson, type Fixture } from './helpers.ts';

/* ────────────────────────────────────────────────────────────────────────── */
/* Harness                                                                    */
/* ────────────────────────────────────────────────────────────────────────── */

const fixtures = Object.fromEntries(AGENT_SLUGS.map((slug) => [slug, loadFixture(slug)])) as Record<
  AgentSlug,
  Fixture
>;

/** The shared stage-1 context, without any upstream results. */
function baseContext(): AgentContext {
  const { upstream: _ignored, ...ctx } = structuredClone(fixtures.dcf.input);
  return ctx;
}

/** The verdict fixture's context, including its recorded upstream results. */
function verdictContext(): AgentContext {
  return structuredClone(fixtures.verdict.input);
}

function reply(slug: AgentSlug): Record<string, unknown> {
  return structuredClone(fixtures[slug].modelOutput) as Record<string, unknown>;
}

type Override = (req: ModelTransportRequest) => unknown;

/** Replays each agent's recorded reply unless overridden, and logs every call. */
function useTransport(overrides: Partial<Record<AgentSlug, Override>> = {}): ModelTransportRequest[] {
  const calls: ModelTransportRequest[] = [];
  setModelTransport((req) => {
    calls.push(req);
    const override = overrides[req.slug];
    return override ? override(req) : reply(req.slug);
  });
  return calls;
}

const LOCKED = (source: string) => ({ visibility: 'detail', source, locked: true });

/** Runs `body` with VALUS_AGENT_MODEL set to `value` (or unset), then restores it. */
async function withModelOverride(value: string | undefined, body: () => Promise<void> | void) {
  const saved = process.env[MODEL_OVERRIDE_ENV];
  const apply = (next: string | undefined) => {
    if (next === undefined) delete process.env[MODEL_OVERRIDE_ENV];
    else process.env[MODEL_OVERRIDE_ENV] = next;
  };
  apply(value);
  try {
    await body();
  } finally {
    apply(saved);
  }
}

beforeEach(() => {
  resetClient();
  setTokenLogger(() => {});
  setCacheStore(new MemoryStore());
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Fixtures                                                                   */
/* ────────────────────────────────────────────────────────────────────────── */

describe('recorded fixtures', () => {
  for (const slug of AGENT_SLUGS) {
    it(`${slug} reproduces its recorded output`, async () => {
      const fixture = fixtures[slug];
      assert.ok(fixture.expected, `${slug} has no expected output yet; run npm run fixtures:replay`);

      const calls = useTransport();
      const result = await registry[slug].run(structuredClone(fixture.input));
      const { SYSTEM_PROMPT } = (await import(`../${slug}/prompt.ts`)) as { SYSTEM_PROMPT: string };

      assert.equal(result.status, fixture.expected.status, JSON.stringify(result.error));
      assert.deepEqual(toJson(result.data), fixture.expected.data);
      assert.equal(result.meta.attempts, 1);
      assert.equal(calls.length, 1);
      assert.equal(calls[0]?.system, SYSTEM_PROMPT);
      assert.match(calls[0]?.user ?? '', /Ticker: AAPL/);
    });
  }
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Contract                                                                   */
/* ────────────────────────────────────────────────────────────────────────── */

describe('contract', () => {
  it('every agent folder holds exactly prompt.ts, schema.ts, run.ts and fixtures/', () => {
    for (const slug of AGENT_SLUGS) {
      const dir = path.join(AGENTS_ROOT, slug);
      const entries = readdirSync(dir).filter((name) => !name.startsWith('.')).sort();
      assert.deepEqual(entries, ['fixtures', 'prompt.ts', 'run.ts', 'schema.ts'], slug);
      const recorded = readdirSync(path.join(dir, 'fixtures')).filter((name) => name.endsWith('.json'));
      assert.ok(recorded.length >= 1, `${slug} has no fixtures`);
    }
  });

  it('prompt.ts exports one system prompt string and nothing else', async () => {
    for (const slug of AGENT_SLUGS) {
      const mod = (await import(`../${slug}/prompt.ts`)) as Record<string, unknown>;
      assert.deepEqual(Object.keys(mod), ['SYSTEM_PROMPT'], slug);
      assert.equal(typeof mod['SYSTEM_PROMPT'], 'string', slug);
    }
  });

  it('agents never import another agent at runtime and never fetch', () => {
    const importPattern = /import\s+(type\s+)?[^;]*?from\s+'([^']+)'/g;
    for (const slug of AGENT_SLUGS) {
      for (const file of ['prompt.ts', 'schema.ts', 'run.ts']) {
        const source = readFileSync(path.join(AGENTS_ROOT, slug, file), 'utf8');
        for (const [, typeOnly, specifier = ''] of source.matchAll(importPattern)) {
          assert.notEqual(file, 'prompt.ts', `${slug}/prompt.ts must not import anything (${specifier})`);
          const sibling = AGENT_SLUGS.find((other) => other !== slug && specifier.startsWith(`../${other}/`));
          if (sibling) {
            assert.ok(typeOnly, `${slug}/${file} imports ${specifier} at runtime`);
          }
        }
        assert.doesNotMatch(
          source,
          /\bfetch\s*\(|from\s+'node:https?'|axios|XMLHttpRequest/,
          `${slug}/${file} looks like it fetches data`,
        );
      }
    }
  });

  it('every model schema converts to a strict structured-output schema', () => {
    const schemas = { DcfModelSchema, CatalystModelSchema, NewsModelSchema, RedflagModelSchema, VerdictModelSchema };
    for (const [name, schema] of Object.entries(schemas)) {
      const json = zodOutputFormat(schema).schema as Record<string, unknown>;
      assert.equal(json['type'], 'object', name);
      assert.equal(json['additionalProperties'], false, name);
    }
  });

  it('every top-level output field is a Field<T>', () => {
    for (const slug of AGENT_SLUGS) {
      const data = fixtures[slug].expected?.data as Record<string, Record<string, unknown>>;
      assert.ok(data, slug);
      for (const [key, value] of Object.entries(data)) {
        assert.ok('value' in value, `${slug}.${key} has no value`);
        assert.ok(VISIBILITY_TIERS.includes(value['visibility'] as never), `${slug}.${key} visibility`);
        assert.ok(FIELD_SOURCES.includes(value['source'] as never), `${slug}.${key} source`);
      }
    }
  });

  it('the advice filter catches trading instructions but not ordinary finance words', () => {
    for (const text of [
      'Investors should buy before the launch.',
      'We recommend the shares at this level.',
      'This is a strong buy.',
      'At this price it looks like a hold.',
      'Now is the time to sell.',
    ]) {
      assert.match(text, ADVICE_PATTERN);
    }
    for (const text of [
      'The buyback cut the share count 8%.',
      'Sell-side analysts disagree on margins.',
      'Margins should hold up next year.',
      'Short interest is 0.7% of the float.',
      'Customers avoided the price increase.',
    ]) {
      assert.doesNotMatch(text, ADVICE_PATTERN);
    }
  });
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Validation and repair                                                      */
/* ────────────────────────────────────────────────────────────────────────── */

describe('validation and repair', () => {
  it('repairs an invalid reply with one retry', async () => {
    const calls = useTransport({
      dcf: (req) => (req.attempt === 1 ? { ...reply('dcf'), confidence: 'certain' } : reply('dcf')),
    });
    const result = await registry.dcf.run(baseContext());

    assert.equal(result.status, 'ok');
    assert.equal(result.meta.attempts, 2);
    assert.equal(calls.length, 2);
    assert.equal(calls[0]?.repairHint, null);
    assert.match(calls[1]?.repairHint ?? '', /confidence/);
  });

  it('returns unavailable after a second invalid reply, without throwing', async () => {
    const calls = useTransport({ dcf: () => ({ nonsense: true }) });
    const result = await registry.dcf.run(baseContext());

    assert.equal(result.status, 'unavailable');
    assert.equal(result.data, null);
    assert.equal(result.error?.kind, 'validation');
    assert.equal(result.meta.attempts, 2);
    assert.equal(calls.length, 2);
  });

  it('sends trading advice in prose back for repair', async () => {
    const calls = useTransport({
      verdict: (req) =>
        req.attempt === 1
          ? { ...reply('verdict'), headline: 'Investors should buy Apple before the launch.' }
          : reply('verdict'),
    });
    const result = await registry.verdict.run(verdictContext());

    assert.equal(result.status, 'ok');
    assert.equal(result.meta.attempts, 2);
    assert.match(calls[1]?.repairHint ?? '', /trading advice/);
  });

  it('parses a raw JSON string reply', async () => {
    useTransport({ redflag: () => JSON.stringify(reply('redflag')) });
    const result = await registry.redflag.run(baseContext());
    assert.equal(result.status, 'ok');
  });

  it('treats prose instead of JSON as a validation failure', async () => {
    const calls = useTransport({ redflag: () => 'Sure, here is my analysis of Apple.' });
    const result = await registry.redflag.run(baseContext());

    assert.equal(result.status, 'unavailable');
    assert.match(result.error?.detail ?? '', /not valid JSON/);
    assert.equal(calls.length, 2);
  });

  it('does not retry a transport failure', async () => {
    const calls = useTransport({
      catalyst: () => {
        throw new Error('socket hang up');
      },
    });
    const result = await registry.catalyst.run(baseContext());

    assert.equal(result.status, 'unavailable');
    assert.equal(result.error?.kind, 'unknown');
    assert.equal(calls.length, 1);
  });

  it('rejects evidence ids that were not in the context', async () => {
    const calls = useTransport({
      news: (req) => {
        const out = reply('news') as { items: Array<{ newsId: string }> };
        if (req.attempt === 1 && out.items[0]) out.items[0].newsId = 'n-invented';
        return out;
      },
    });
    const result = await registry.news.run(baseContext());

    assert.equal(result.status, 'ok');
    assert.equal(result.meta.attempts, 2);
    assert.match(calls[1]?.repairHint ?? '', /n-invented/);
  });

  it('degrades without a network call when no API key is configured', async () => {
    setModelTransport(null);
    configureClient({ apiKey: undefined });
    const result = await registry.dcf.run(baseContext());

    assert.equal(result.status, 'unavailable');
    assert.equal(result.error?.kind, 'no_api_key');
  });
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Missing inputs                                                             */
/* ────────────────────────────────────────────────────────────────────────── */

describe('missing inputs', () => {
  it('dcf without a valuation snapshot is unavailable and makes no call', async () => {
    const calls = useTransport();
    const result = await registry.dcf.run({ ...baseContext(), valuation: null });

    assert.equal(result.status, 'unavailable');
    assert.equal(result.error?.kind, 'insufficient_data');
    assert.equal(calls.length, 0);
  });

  it('news with no items answers from computed data and makes no call', async () => {
    const calls = useTransport();
    const result = await registry.news.run({ ...baseContext(), news: [] });

    assert.equal(result.status, 'ok');
    assert.ok(result.data);
    assert.equal(result.data.tone.value, 'quiet');
    assert.equal(result.data.tone.source, 'computed');
    assert.equal(calls.length, 0);
  });

  it('verdict without an intrinsic value is unavailable and makes no call', async () => {
    const calls = useTransport();
    const result = await registry.verdict.run({ ...verdictContext(), valuation: null });

    assert.equal(result.error?.kind, 'insufficient_data');
    assert.equal(calls.length, 0);
  });
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Verdict guardrails                                                         */
/* ────────────────────────────────────────────────────────────────────────── */

describe('verdict guardrails', () => {
  it('allows bands within two steps of the engine band', async () => {
    const calls = useTransport();
    await registry.verdict.run(verdictContext());
    const data = fixtures.verdict.expected?.data as VerdictOutput;

    assert.equal(data.engineBand.value, 'Slightly Overvalued');
    assert.deepEqual(data.guardrails.value.allowedBands, [
      'Modestly Undervalued',
      'Fairly Valued',
      'Slightly Overvalued',
      'Overvalued',
    ]);
    assert.match(
      calls[0]?.user ?? '',
      /Allowed bands for this ticker: Modestly Undervalued, Fairly Valued, Slightly Overvalued, Overvalued/,
    );
  });

  it('a momentum run-up caps the verdict at Fairly Valued', async () => {
    const ctx = verdictContext();
    ctx.prices.regime = 'momentum_runup';
    assert.ok(ctx.valuation);
    ctx.valuation.marginOfSafetyPct = 45; // engine band: Undervalued

    const calls = useTransport({
      verdict: (req) => ({ ...reply('verdict'), band: req.attempt === 1 ? 'Undervalued' : 'Fairly Valued' }),
    });
    const result = await registry.verdict.run(ctx);

    assert.equal(result.status, 'ok');
    assert.equal(result.meta.attempts, 2);
    assert.match(calls[1]?.repairHint ?? '', /not allowed/);
    assert.equal(result.data?.band.value, 'Fairly Valued');
    assert.equal(result.data?.tier.value, 'info');
    assert.deepEqual(result.data?.guardrails.value.allowedBands, ['Fairly Valued']);
  });

  it('a sovereign backstop floors the verdict at Fairly Valued', async () => {
    const ctx = verdictContext();
    ctx.profile.strategic = { isStrategic: true, label: 'Test backstop', reason: 'Set by the test' };
    assert.ok(ctx.valuation);
    ctx.valuation.marginOfSafetyPct = -40; // engine band: Overvalued

    useTransport({
      verdict: (req) => ({ ...reply('verdict'), band: req.attempt === 1 ? 'Overvalued' : 'Fairly Valued' }),
    });
    const result = await registry.verdict.run(ctx);

    assert.equal(result.status, 'ok');
    assert.equal(result.data?.band.value, 'Fairly Valued');
    assert.equal(result.data?.guardrails.value.backstopFloor, true);
  });
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Runner                                                                     */
/* ────────────────────────────────────────────────────────────────────────── */

describe('runner', () => {
  it('runs stage 1, then the verdict on the stage-1 results', async () => {
    const calls = useTransport();
    const report = await runAgents(baseContext());

    assert.deepEqual(report.unavailable, []);
    assert.equal(calls.length, 5);
    assert.equal(calls.at(-1)?.slug, 'verdict');
    const redflag = report.results.redflag.data;
    assert.ok(redflag);
    assert.ok(calls.at(-1)?.user.includes(redflag.headline.value), 'verdict prompt carries the redflag report');
  });

  it('starts every stage-1 agent before any of them finishes', async () => {
    const started = new Set<AgentSlug>();
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    // Each stage-1 reply waits until all four have started. Run sequentially,
    // the first would wait forever and hit its deadline.
    const gated: Override = (req) => {
      started.add(req.slug);
      if (started.size === 4) release();
      return gate.then(() => reply(req.slug));
    };
    useTransport({ dcf: gated, catalyst: gated, news: gated, redflag: gated });

    const report = await runAgents(baseContext(), { agentTimeoutMs: 2_000 });
    assert.deepEqual(report.unavailable, []);
  });

  it('one failing agent does not block the others or the verdict', async () => {
    const calls = useTransport({
      redflag: () => {
        throw new Error('boom');
      },
    });
    const report = await runAgents(baseContext());

    assert.deepEqual(report.unavailable, ['redflag']);
    assert.equal(report.results.verdict.status, 'ok');
    assert.deepEqual(report.results.verdict.data?.inputs.value.unavailable, ['redflag']);
    assert.match(calls.at(-1)?.user ?? '', /Reviewer: redflag\nNot available \(unknown\)\./);
  });

  it('an agent that throws outside the model call is contained', async () => {
    useTransport();
    const original = registry.news.run;
    registry.news.run = async () => {
      throw new Error('bug in news/run.ts');
    };
    try {
      const report = await runAgents(baseContext());
      assert.equal(report.results.news.status, 'unavailable');
      assert.match(report.results.news.error?.message ?? '', /bug in news/);
      assert.equal(report.results.verdict.status, 'ok');
    } finally {
      registry.news.run = original;
    }
  });

  it('a hung agent times out without holding up the rest', async () => {
    useTransport({ catalyst: () => new Promise(() => {}) });
    const report = await runAgents(baseContext(), { agentTimeoutMs: 100 });

    assert.equal(report.results.catalyst.status, 'unavailable');
    assert.equal(report.results.catalyst.error?.kind, 'timeout');
    assert.equal(report.results.dcf.status, 'ok');
    assert.equal(report.results.verdict.status, 'ok');
  });

  it('serves repeat runs from cache and re-runs news and the verdict after four hours', async () => {
    let clock = Date.parse('2026-09-12T02:00:00Z');
    setCacheStore(new MemoryStore({ now: () => clock }));
    const calls = useTransport();

    await runAgents(baseContext(), { now: new Date(clock) });
    assert.equal(calls.length, 5);

    const second = await runAgents(baseContext(), { now: new Date(clock) });
    assert.equal(calls.length, 5);
    for (const slug of AGENT_SLUGS) {
      assert.equal(second.results[slug].meta.cached, true, slug);
    }

    clock += 4 * 3600 * 1000 + 1000;
    await runAgents(baseContext(), { now: new Date(clock) });
    assert.deepEqual(
      calls.slice(5).map((call) => call.slug),
      ['news', 'verdict'],
    );
  });

  it('never caches unavailable results, or a verdict built on partial inputs', async () => {
    const calls = useTransport({ dcf: () => ({ nonsense: true }) });
    await runAgents(baseContext());
    await runAgents(baseContext());
    const count = (slug: AgentSlug) => calls.filter((call) => call.slug === slug).length;

    assert.equal(count('dcf'), 4, 'two attempts per run, and nothing cached');
    assert.equal(count('verdict'), 2, 'the first verdict saw dcf unavailable, so it was not cached');
    assert.equal(count('news'), 1, 'healthy agents were cached');
  });

  it('keys the cache by agent, ticker and UTC date', () => {
    assert.equal(
      cacheKey('news', ' aapl ', new Date('2026-09-12T23:59:59-04:00')),
      'valus:agent:v1:news:AAPL:2026-09-13',
    );
  });
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Models                                                                     */
/* ────────────────────────────────────────────────────────────────────────── */

describe('models', () => {
  it('each agent calls its own model, and the token log attributes it', async () => {
    await withModelOverride(undefined, async () => {
      const records: TokenLogRecord[] = [];
      setTokenLogger((record) => records.push(record));
      const calls = useTransport();
      const report = await runAgents(baseContext());

      const expected: Record<AgentSlug, string> = {
        dcf: MODELS.sonnet,
        catalyst: MODELS.sonnet,
        news: MODELS.haiku,
        redflag: MODELS.sonnet,
        verdict: MODELS.sonnet,
      };
      for (const slug of AGENT_SLUGS) {
        assert.equal(registry[slug].model, expected[slug], `${slug} module`);
        assert.equal(calls.find((call) => call.slug === slug)?.model, expected[slug], `${slug} request`);
        assert.equal(report.results[slug].meta.model, expected[slug], `${slug} meta`);
        const record = records.find((r) => r.slug === slug);
        assert.equal(record?.model, expected[slug], `${slug} token log`);
        assert.equal(record?.modelSource, 'agent', `${slug} token log source`);
      }
    });
  });

  it(`resolves ${MODEL_OVERRIDE_ENV}, then the agent's model, then the default`, async () => {
    await withModelOverride(undefined, () => {
      assert.deepEqual(resolveModel(MODELS.haiku), { id: MODELS.haiku, source: 'agent' });
      assert.deepEqual(resolveModel(undefined), { id: DEFAULT_MODEL, source: 'default' });
    });

    await withModelOverride('claude-override-test', async () => {
      assert.deepEqual(resolveModel(MODELS.haiku), { id: 'claude-override-test', source: 'env' });

      const records: TokenLogRecord[] = [];
      setTokenLogger((record) => records.push(record));
      const calls = useTransport();
      await runAgents(baseContext());

      assert.equal(calls.length, 5);
      assert.ok(calls.every((call) => call.model === 'claude-override-test'));
      assert.ok(records.every((r) => r.model === 'claude-override-test' && r.modelSource === 'env'));
    });
  });

  it('sends effort and refusal fallbacks only to models that accept them', () => {
    const opts = {
      slug: 'news' as const,
      ticker: 'AAPL',
      system: 'system',
      user: 'user',
      schema: NewsModelSchema,
      effort: 'medium' as const,
    };

    const haiku = buildRequest(opts, MODELS.haiku, 1_000, null);
    assert.equal(haiku.model, MODELS.haiku);
    assert.equal(haiku.output_config?.effort, undefined, 'Haiku 4.5 rejects effort');
    assert.equal(haiku.output_config?.format?.type, 'json_schema');
    assert.equal(haiku.fallbacks, undefined);

    const sonnet = buildRequest(opts, MODELS.sonnet, 1_000, null);
    assert.equal(sonnet.output_config?.effort, 'medium');
    assert.equal(sonnet.fallbacks, undefined);

    const opus = buildRequest(opts, 'claude-opus-5', 1_000, null);
    assert.equal(opus.fallbacks, 'default');
    assert.deepEqual(opus.betas, ['server-side-fallback-2026-07-01']);

    const unknown = buildRequest(opts, 'claude-unlisted-model', 1_000, null);
    assert.equal(unknown.output_config?.effort, undefined, 'unlisted models get the conservative set');
  });
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Tier redaction                                                             */
/* ────────────────────────────────────────────────────────────────────────── */

describe('tier redaction', () => {
  it('free tier locks detail fields and keeps headline and summary', async () => {
    useTransport();
    const report = await runAgents({ ...baseContext(), tier: 'free' });
    const payload = toClientPayload(report);

    assert.deepEqual(payload.results.dcf.data?.assumptions, LOCKED('model'));
    assert.deepEqual(payload.results.dcf.data?.history, LOCKED('computed'));
    assert.deepEqual(payload.results.verdict.data?.thesis, LOCKED('model'));
    assert.deepEqual(payload.results.dcf.data?.headline, report.results.dcf.data?.headline);
    assert.deepEqual(payload.results.verdict.data?.band, report.results.verdict.data?.band);
    assert.deepEqual(payload.results.redflag.data?.plainEnglish, report.results.redflag.data?.plainEnglish);
  });

  it('premium tier sees every field', async () => {
    useTransport();
    const payload = toClientPayload(await runAgents({ ...baseContext(), tier: 'premium' }));
    assert.doesNotMatch(JSON.stringify(payload), /"locked":true/);
  });

  it('strips internal error detail from the client payload', async () => {
    useTransport({ dcf: () => ({ nonsense: true }) });
    const payload = toClientPayload(await runAgents(baseContext()));

    assert.equal(payload.results.dcf.status, 'unavailable');
    assert.ok(payload.results.dcf.error);
    assert.equal('detail' in payload.results.dcf.error, false);
  });
});
