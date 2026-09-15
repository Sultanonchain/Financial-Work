import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { zodOutputFormat } from '@anthropic-ai/sdk/helpers/zod';

import { cacheKey, MemoryStore, setCacheStore } from '../_shared/cache.ts';
import { keyFigures, renderKeyFigures } from '../_shared/figures.ts';
import { valuationPathOf } from '../_shared/valuation.ts';
import { setClock } from '../_shared/format.ts';
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
import { buildCompanySection, CompanySectionSchema, selectCeo } from '../_shared/company.ts';
import {
  ADVICE_PATTERN,
  FIELD_SOURCES,
  VISIBILITY_TIERS,
  zProse,
  type AgentContext,
  type AgentSlug,
  type NewsItem,
} from '../_shared/types.ts';

import {
  CatalystModelSchema,
  catalystOutputSchemaFor,
  SHIPPED_WINDOW_DAYS,
  type CatalystOutput,
} from '../catalyst/schema.ts';
import { DcfModelSchema, type DcfModel, type DcfOutput } from '../dcf/schema.ts';
import { NewsModelSchema } from '../news/schema.ts';
import { RedflagModelSchema } from '../redflag/schema.ts';
import { VALUATION_BANDS, VerdictModelSchema, type VerdictOutput } from '../verdict/schema.ts';

import { collectStatus, renderStatus } from '../scripts/agents-status.ts';

import { AGENTS_ROOT, loadFixture, provenanceIssues, toJson, type Fixture } from './helpers.ts';

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

/**
 * npm test loads agents/.env, so a real API key can be present. No test may
 * reach the API: the default transport refuses and records the attempt, and
 * afterEach fails the test that made it.
 */
let unstubbedModelCalls: string[] = [];

/**
 * "Today" for date-dependent validation, pinned so tests do not rot as the
 * calendar moves: the catalyst fixture's own reference date.
 */
const TEST_NOW = fixtures.catalyst.recordedAt ?? fixtures.catalyst.input.asOf;

beforeEach(() => {
  resetClient();
  setClock(() => Date.parse(TEST_NOW));
  unstubbedModelCalls = [];
  setModelTransport((req) => {
    unstubbedModelCalls.push(req.slug);
    throw new Error('model call without a test transport');
  });
  setTokenLogger(() => {});
  setCacheStore(new MemoryStore());
});

afterEach(() => {
  assert.deepEqual(unstubbedModelCalls, [], 'a test called the model without installing a transport');
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Fixtures                                                                   */
/* ────────────────────────────────────────────────────────────────────────── */

describe('recorded fixtures', () => {
  for (const slug of AGENT_SLUGS) {
    it(`${slug} reproduces its recorded output`, async () => {
      const fixture = fixtures[slug];
      assert.ok(fixture.expected, `${slug} has no expected output yet; run npm run fixtures:replay`);

      setClock(() => Date.parse(fixture.recordedAt ?? fixture.input.asOf));
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

  it('every fixture declares consistent provenance', () => {
    for (const slug of AGENT_SLUGS) {
      assert.deepEqual(provenanceIssues(fixtures[slug]), [], slug);
    }
    assert.deepEqual(provenanceIssues({ source: 'synthetic', recordedAt: null, recordedModel: null }), []);
    assert.equal(provenanceIssues({ recordedAt: null }).length, 1, 'a missing source is never assumed');
    assert.equal(
      provenanceIssues({ source: 'recorded', recordedAt: null, recordedModel: null, modelOutput: {} }).length,
      2,
      'recorded requires a timestamp and a model',
    );
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
    const records: TokenLogRecord[] = [];
    setTokenLogger((record) => records.push(record));
    const result = await registry.dcf.run(baseContext());

    assert.equal(result.status, 'ok');
    assert.equal(result.meta.attempts, 2);
    assert.equal(calls.length, 2);
    assert.equal(calls[0]?.repairHint, null);
    assert.match(calls[1]?.repairHint ?? '', /confidence/);
    assert.equal(records[0]?.outcome, 'invalid');
    assert.match(records[0]?.issues ?? '', /confidence/, 'the token log says why a repair was needed');
    assert.equal(records[1]?.issues, undefined);
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
/* Catalyst lifecycle                                                         */
/* ────────────────────────────────────────────────────────────────────────── */

describe('catalyst lifecycle', () => {
  const TODAY = '2026-09-14';
  const SHIPPED_SINCE = '2026-06-16'; // TODAY minus SHIPPED_WINDOW_DAYS
  type Item = Record<string, unknown>;

  // News in the base context used below: n-0731-h "Apple Newsroom" (company
  // statement, 2026-07-31); n-0912-a Reuters (major outlet, 2026-09-11);
  // n-0908-c Motley Fool (neither); n-0910-b Bloomberg on the EU review (no day);
  // n-0828-f Reuters, trial "beginning in March 2027" (a month, no day). The
  // calendar's next earnings date is 2026-10-29.
  const upcoming = (overrides: Item = {}): Item => ({
    title: 'Q4 earnings report',
    status: 'reported',
    kind: 'earnings',
    direction: 'two_sided',
    horizon: 'under_3m',
    likelihood: 'high',
    valueLever: 'revenue_growth',
    whyItMatters: 'A test item for the lifecycle rules.',
    evidenceNewsIds: [],
    expectedDate: '2026-10-29',
    ...overrides,
  });
  const past = (overrides: Item = {}): Item => ({
    title: 'Q3 results released',
    status: 'announced',
    kind: 'earnings',
    direction: 'positive',
    valueLever: 'revenue_growth',
    whyItMatters: 'A test item for the lifecycle rules.',
    evidenceNewsIds: ['n-0731-h'],
    announcedDate: '2026-07-31',
    ...overrides,
  });
  const newsItem = (id: string, date: string, source: string, title: string): NewsItem => ({
    id,
    title,
    summary: null,
    url: null,
    source,
    publishedAt: `${date}T13:00:00Z`,
  });

  type Reply = { upcoming?: Item[]; past?: Item[] };

  /** Runs the catalyst agent on TODAY; attempt N gets replies[N-1] (or the last one). */
  const runCatalyst = async (replies: Reply[], extraNews: NewsItem[] = []) => {
    setClock(() => Date.parse(`${TODAY}T12:00:00Z`));
    const calls = useTransport({
      catalyst: (req) => {
        const reply = replies[Math.min(req.attempt, replies.length) - 1] ?? {};
        return {
          headline: 'A test headline.',
          plainEnglish: 'A test summary for the reader.',
          upcoming: reply.upcoming ?? [],
          past: reply.past ?? [],
        };
      },
    });
    const ctx = baseContext();
    ctx.news = [...ctx.news, ...extraNews];
    return { result: await registry.catalyst.run(ctx), calls };
  };

  it('keeps a dated upcoming item and computes netTilt from what is kept', async () => {
    const { result, calls } = await runCatalyst([{ upcoming: [upcoming({ direction: 'negative' })] }]);

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.equal(calls.length, 1);
    assert.deepEqual(
      result.data?.upcomingCatalysts.value.map((c) => [c.title, c.kind, c.expectedDate]),
      [['Q4 earnings report', 'earnings', '2026-10-29']],
    );
    assert.equal(result.data?.netTilt.value, 'negative');
    assert.equal(result.data?.netTilt.source, 'computed');
    assert.equal(result.data?.topCatalyst.value?.title, 'Q4 earnings report');
  });

  it('drops undated upcoming items in the same attempt, litigation and regulatory included', async () => {
    const { result, calls } = await runCatalyst([
      {
        upcoming: [
          upcoming(),
          upcoming({ title: 'EU decision on App Store fees', kind: 'regulatory', expectedDate: '', evidenceNewsIds: ['n-0910-b'] }),
          upcoming({ title: 'DOJ antitrust trial', kind: 'legal', horizon: 'over_12m', expectedDate: '2027-03-01', evidenceNewsIds: ['n-0828-f'] }),
        ],
      },
    ]);

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.equal(calls.length, 1, 'no repair turn');
    assert.deepEqual(result.data?.upcomingCatalysts.value.map((c) => c.title), ['Q4 earnings report']);
    const note = result.data?.upcomingCatalysts.note ?? '';
    assert.match(note, /EU decision on App Store fees \(no exact day\)/);
    assert.match(note, /DOJ antitrust trial \(2027-03-01 is not a day the context states\)/);
  });

  it('drops past-dated upcoming items instead of rendering them', async () => {
    const event = newsItem('n-event', '2026-09-01', 'Reuters', 'Apple will hold its event on September 9, 2026');
    const { result, calls } = await runCatalyst(
      [{ upcoming: [upcoming({ title: 'Apple event', kind: 'launch_event', expectedDate: '2026-09-09', evidenceNewsIds: ['n-event'] })] }],
      [event],
    );

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.equal(calls.length, 1);
    assert.deepEqual(result.data?.upcomingCatalysts.value, []);
    assert.equal(result.data?.netTilt.value, 'none');
    assert.match(result.data?.upcomingCatalysts.note ?? '', /Apple event \(dated 2026-09-09, before today\)/);
  });

  it('keeps upcoming items dated today', async () => {
    const event = newsItem('n-today', '2026-09-10', 'Reuters', 'Apple to hold an investor event on September 14, 2026');
    const { result } = await runCatalyst(
      [{ upcoming: [upcoming({ title: 'Investor event', kind: 'launch_event', expectedDate: TODAY, evidenceNewsIds: ['n-today'] })] }],
      [event],
    );

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.equal(result.data?.upcomingCatalysts.value[0]?.expectedDate, TODAY);
  });

  it('the calendar date only dates the earnings report', async () => {
    const borrowed = upcoming({ title: 'EU decision', kind: 'regulatory', evidenceNewsIds: ['n-0910-b'] });
    const { result } = await runCatalyst([{ upcoming: [borrowed] }]);

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.deepEqual(result.data?.upcomingCatalysts.value, []);
  });

  it('a future ship date, price or availability window needs a company statement', async () => {
    const shipReport = newsItem('n-ship-r', '2026-09-12', 'Reuters', 'iPhone 18 Pro to ship on September 25, 2026, sources say');
    const shipStatement = newsItem('n-ship-c', '2026-09-12', 'Apple Newsroom', 'iPhone 18 Pro available September 25, 2026');
    const ships = (evidence: string) =>
      upcoming({ title: 'iPhone 18 Pro ships', kind: 'ship_date', expectedDate: '2026-09-25', evidenceNewsIds: [evidence] });

    const reported = await runCatalyst([{ upcoming: [ships('n-ship-r')] }], [shipReport, shipStatement]);
    assert.equal(reported.result.status, 'unavailable');
    assert.match(reported.calls[1]?.repairHint ?? '', /is a future ship date, which needs a company statement among its evidence/);

    const fromCompany = await runCatalyst([{ upcoming: [ships('n-ship-c')] }], [shipReport, shipStatement]);
    assert.equal(fromCompany.result.status, 'ok', JSON.stringify(fromCompany.result.error));
    assert.equal(fromCompany.calls.length, 1);
    assert.equal(fromCompany.result.data?.upcomingCatalysts.value[0]?.expectedDate, '2026-09-25');
  });

  it('a past official event reported by a major outlet counts as announced', async () => {
    const unveiled = past({ title: 'iPhone 18 unveiled', kind: 'launch_event', evidenceNewsIds: ['n-0912-a'], announcedDate: '2026-09-11' });
    const { result, calls } = await runCatalyst([{ past: [unveiled] }]);

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.equal(calls.length, 1, 'no repair turn');
    assert.deepEqual(
      result.data?.historicalAnalogs.value.map((c) => [c.title, c.status, c.kind, c.eventDate, c.announcedDate]),
      [['iPhone 18 unveiled', 'announced', 'launch_event', '2026-09-11', '2026-09-11']],
    );
  });

  it('evidence with unofficial sourcing cannot support announced, whichever outlet published it', async () => {
    // n-0822-g is Nikkei, a major outlet: "according to people familiar with the schedule".
    const production = past({
      title: 'TSMC begins A20 production',
      kind: 'other',
      evidenceNewsIds: ['n-0822-g'],
      announcedDate: '2026-08-22',
    });
    const { result, calls } = await runCatalyst([{ past: [past(), production] }]);

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.equal(calls.length, 1, 'a downgrade is not a repair turn');
    assert.deepEqual(result.data?.historicalAnalogs.value.map((c) => c.title), ['Q3 results released']);
    assert.match(
      result.data?.historicalAnalogs.note ?? '',
      /Downgraded to reported.*TSMC begins A20 production \(unofficial sourcing in Nikkei\)/,
    );
    assert.deepEqual(result.data?.upcomingCatalysts.value, [], 'a downgraded past event has no day still ahead');
  });

  it('each hedging phrase downgrades, and unhedged official evidence alongside still supports', async () => {
    const phrases = ['people familiar with the plans', 'sources said', 'according to people briefed', 'is said to have', 'reportedly'];
    for (const [n, phrase] of phrases.entries()) {
      const hedged = newsItem(`n-hedge-${n}`, '2026-09-01', 'Bloomberg', `Apple ${phrase} signed a supply deal`);
      const deal = past({ title: 'Supply deal', kind: 'corporate', evidenceNewsIds: [hedged.id], announcedDate: '2026-09-01' });
      const { result, calls } = await runCatalyst([{ past: [deal] }], [hedged]);
      assert.equal(result.status, 'ok', `"${phrase}": ${JSON.stringify(result.error)}`);
      assert.equal(calls.length, 1, `"${phrase}"`);
      assert.deepEqual(result.data?.historicalAnalogs.value, [], `"${phrase}" should downgrade`);
    }

    const hedged = newsItem('n-hedge', '2026-09-01', 'Bloomberg', 'Apple reportedly signed a supply deal');
    const statement = newsItem('n-deal', '2026-09-01', 'Apple Newsroom', 'Apple signs a supply deal');
    const deal = past({ title: 'Supply deal', kind: 'corporate', evidenceNewsIds: ['n-hedge', 'n-deal'], announcedDate: '2026-09-01' });
    const { result } = await runCatalyst([{ past: [deal] }], [hedged, statement]);
    assert.deepEqual(result.data?.historicalAnalogs.value.map((c) => [c.title, c.status]), [['Supply deal', 'announced']]);
  });

  it('legal and regulatory events that already happened stay in historicalAnalogs', async () => {
    const trialDateSet = past({
      title: 'Judge sets DOJ trial date',
      kind: 'legal',
      direction: 'negative',
      evidenceNewsIds: ['n-0828-f'],
      announcedDate: '2026-08-28',
    });
    const reviewOpened = past({
      title: 'EU opens App Store fee review',
      kind: 'regulatory',
      direction: 'negative',
      evidenceNewsIds: ['n-0910-b'],
      announcedDate: '2026-09-10',
    });
    const { result } = await runCatalyst([{ past: [trialDateSet, reviewOpened] }]);

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.deepEqual(
      result.data?.historicalAnalogs.value.map((c) => [c.kind, c.eventDate]),
      [
        ['regulatory', '2026-09-10'],
        ['legal', '2026-08-28'],
      ],
    );
  });

  it('announced still needs a company statement or a major outlet', async () => {
    const viaOpinion = past({ evidenceNewsIds: ['n-0908-c'], announcedDate: '2026-09-08' });
    const { result, calls } = await runCatalyst([{ past: [viaOpinion] }]);

    assert.equal(result.status, 'unavailable');
    assert.match(calls[1]?.repairHint ?? '', /neither a company statement nor a major outlet/);
  });

  it('announcedDate is required once status is announced or shipped', async () => {
    const undated = past({ status: 'shipped', announcedDate: undefined });
    const { result, calls } = await runCatalyst([{ past: [undated] }]);

    assert.equal(result.status, 'unavailable');
    assert.match(calls[1]?.repairHint ?? '', /past\.0\.announcedDate/);
  });

  it('announcedDate must match the cited evidence', async () => {
    const { result, calls } = await runCatalyst([{ past: [past({ announcedDate: '2026-07-15' })] }]);

    assert.equal(result.status, 'unavailable');
    assert.match(calls[1]?.repairHint ?? '', /announcedDate 2026-07-15 does not match the cited evidence \(published 2026-07-31\)/);
  });

  it('announced items land in historicalAnalogs, dated by announcedDate', async () => {
    const { result } = await runCatalyst([{ upcoming: [upcoming()], past: [past()] }]);

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.deepEqual(result.data?.upcomingCatalysts.value.map((c) => c.title), ['Q4 earnings report']);
    assert.deepEqual(
      result.data?.historicalAnalogs.value.map((c) => [c.title, c.status, c.eventDate]),
      [['Q3 results released', 'announced', '2026-07-31']],
    );
  });

  it(`shipped items drop off ${SHIPPED_WINDOW_DAYS} days after announcedDate`, async () => {
    const oldStatement = newsItem('n-0501-s', '2026-05-01', 'Apple Newsroom', 'Apple introduces a new product');
    const recent = past({ title: 'Shipped recently', status: 'shipped' });
    const old = past({ title: 'Shipped long ago', status: 'shipped', announcedDate: '2026-05-01', evidenceNewsIds: ['n-0501-s'] });
    const { result } = await runCatalyst([{ past: [recent, old] }], [oldStatement]);

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.deepEqual(result.data?.historicalAnalogs.value.map((c) => c.title), ['Shipped recently']);
  });

  it('the returned object is checked against today as well', () => {
    const clean = structuredClone(fixtures.catalyst.expected?.data) as CatalystOutput;
    assert.equal(catalystOutputSchemaFor(TODAY, SHIPPED_SINCE).safeParse(clean).success, true);

    const stale = structuredClone(clean);
    stale.upcomingCatalysts.value.push({
      title: 'Stale',
      status: 'reported',
      kind: 'other',
      direction: 'positive',
      horizon: 'under_3m',
      likelihood: 'high',
      valueLever: 'margins',
      whyItMatters: 'Dated before today.',
      expectedDate: '2026-09-01',
      evidence: [],
    });
    const parsed = catalystOutputSchemaFor(TODAY, SHIPPED_SINCE).safeParse(stale);
    assert.equal(parsed.success, false);
    assert.match(parsed.error?.issues[0]?.message ?? '', /past-dated item in upcomingCatalysts/);
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
      'valus:agent:v4:news:AAPL:2026-09-13',
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
        assert.equal(
          calls.find((call) => call.slug === slug)?.maxTokens,
          registry[slug].maxTokens,
          `${slug} max_tokens cap`,
        );
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
/* agents:status                                                              */
/* ────────────────────────────────────────────────────────────────────────── */

describe('agents:status', () => {
  it('lists every registered agent with its resolved model, files and fixture provenance', async () => {
    const report = await collectStatus({});

    assert.equal(report.apiKeySet, false);
    assert.equal(report.modelOverride, null);
    assert.equal(report.registryError, null);
    assert.deepEqual(
      report.agents.map((agent) => agent.slug),
      AGENT_SLUGS,
    );
    for (const agent of report.agents) {
      const slug = agent.slug as AgentSlug;
      const fixture = fixtures[slug];
      assert.equal(agent.ready, true, `${slug}: ${agent.problems.join('; ')}`);
      assert.deepEqual(agent.missing, [], slug);
      assert.equal(agent.model, registry[slug].model, slug);
      assert.equal(agent.modelSource, 'agent', slug);
      assert.equal(agent.provenance, fixture.source === 'recorded' ? 'recorded' : 'hand-written', slug);
      assert.equal(agent.lastRecordedAt, fixture.recordedAt, slug);
    }
  });

  it('reports the API key as set without printing it, and shows the model override', async () => {
    const secret = 'sk-ant-test-0000-must-not-appear';
    const report = await collectStatus({
      ANTHROPIC_API_KEY: secret,
      [MODEL_OVERRIDE_ENV]: 'claude-override-test',
    });
    const output = renderStatus(report);

    assert.equal(report.apiKeySet, true);
    assert.equal(output.includes(secret), false, 'the key value must never be printed');
    assert.match(output, /ANTHROPIC_API_KEY\s+set\n/);
    assert.ok(report.agents.every((agent) => agent.model === 'claude-override-test' && agent.modelSource === 'env'));
    for (const slug of AGENT_SLUGS) {
      assert.match(output, new RegExp(`^  ${slug}\\s+yes\\s+claude-override-test\\s+env\\s+ok`, 'm'));
    }
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

/* ────────────────────────────────────────────────────────────────────────── */
/* Company section                                                            */
/* ────────────────────────────────────────────────────────────────────────── */

describe('company section', () => {
  /** The base context plus the data-layer facts the company section reads. */
  const withFacts = (profile: Partial<AgentContext['profile']> = {}): AgentContext => {
    const ctx = baseContext();
    ctx.profile = {
      ...ctx.profile,
      forwardPE: 28.6,
      headquarters: { city: 'Cupertino', region: 'CA', country: 'United States' },
      officers: [
        { name: 'Ms. Jane  Doe', title: 'Chief Financial Officer' },
        { name: 'Mr. John  Ternus', title: 'CEO & Director' },
      ],
      ...profile,
    };
    return ctx;
  };

  it('builds the stats row and the facts panel in display order, every value source api', () => {
    const company = buildCompanySection(withFacts());

    assert.equal(CompanySectionSchema.safeParse(company).success, true);
    assert.deepEqual(Object.keys(company), ['stats', 'facts', 'businessSummary'], 'facts come before businessSummary');
    assert.deepEqual(Object.keys(company.stats), ['price', 'marketCap', 'forwardPE']);
    assert.deepEqual(company.stats.forwardPE, { value: 28.6, visibility: 'summary', source: 'api' });
    assert.deepEqual(Object.keys(company.facts), ['headquarters', 'employees', 'sector', 'industry', 'ceo']);
    assert.equal(company.facts.headquarters?.value, 'Cupertino, CA, United States');
    assert.equal(company.facts.employees?.value, 166000);
    assert.equal(company.facts.ceo?.value, 'John Ternus');

    const values = [...Object.values(company.stats), ...Object.values(company.facts), company.businessSummary];
    assert.ok(values.every((value) => value?.source === 'api' && value.visibility === 'summary'));
  });

  it('omits any row the data layer does not have, and never has an IPO date', () => {
    const sparse = buildCompanySection(
      withFacts({ forwardPE: null, headquarters: null, officers: [], employees: null, industry: null, description: null }),
    );
    assert.deepEqual(Object.keys(sparse.stats), ['price', 'marketCap']);
    assert.deepEqual(Object.keys(sparse.facts), ['sector']);
    assert.equal('businessSummary' in sparse, false);
    assert.equal(/ipo/i.test(JSON.stringify(sparse)), false);

    // A context assembled before these fields existed stays valid.
    const legacy = buildCompanySection(baseContext());
    assert.deepEqual(Object.keys(legacy.stats), ['price', 'marketCap']);
    assert.deepEqual(Object.keys(legacy.facts), ['employees', 'sector', 'industry']);
  });

  it('shows founded only when the data layer supplies a year, and drops a negative forward P/E', () => {
    const founded = buildCompanySection(withFacts({ foundedYear: 1976 }));
    assert.deepEqual(Object.keys(founded.facts)[0], 'founded');
    assert.equal(founded.facts.founded?.value, 1976);

    assert.equal('forwardPE' in buildCompanySection(withFacts({ forwardPE: -8.2 })).stats, false);
  });

  it('picks the company CEO, not a division CEO, and cleans the name', () => {
    const jpm = [
      { name: 'Mr. James  Dimon', title: 'Chairman & CEO' },
      { name: 'Mr. Douglas B. Petno', title: 'CEO of the Commercial & Investment Bank and Co-President' },
      { name: 'Ms. Mary Callahan Erdoes', title: 'Chief Executive Officer of Asset & Wealth Management and Executive VP' },
    ];
    assert.equal(selectCeo(jpm), 'James Dimon');
    assert.equal(selectCeo([{ name: 'Dr. Brian  Lian Ph.D.', title: 'President, CEO & Director' }]), 'Brian Lian');
    // Micron's officer list from yfinance.
    assert.equal(
      selectCeo([
        { name: 'Mr. Sanjay  Mehrotra', title: 'CEO & Chairman' },
        { name: 'Mr. Sumit  Sadana', title: 'Senior Advisor to the CEO' },
      ]),
      'Sanjay Mehrotra',
      'a role that serves the CEO is not a second CEO',
    );
    assert.equal(selectCeo([{ name: 'A One', title: 'Co-CEO' }, { name: 'B Two', title: 'Co-CEO & Director' }]), 'A One and B Two');
    assert.equal(
      selectCeo([{ name: 'A One', title: 'CEO' }, { name: 'B Two', title: 'Chief Executive Officer' }]),
      null,
      'two company-level CEOs is ambiguous, so the row is omitted',
    );
    assert.equal(selectCeo([{ name: 'A One', title: 'Former CEO' }, { name: 'B Two', title: 'CFO' }]), null);
  });

  it('needs no model call and survives every agent failing', async () => {
    setModelTransport(null);
    configureClient({ apiKey: undefined });
    const report = await runAgents(withFacts());

    assert.deepEqual([...report.unavailable].sort(), [...AGENT_SLUGS].sort());
    assert.equal(report.company.facts.ceo?.value, 'John Ternus');
    assert.ok(Object.keys(report).indexOf('company') < Object.keys(report).indexOf('results'));

    const free = toClientPayload({ ...report, tier: 'free' });
    assert.deepEqual(free.company.stats.forwardPE, { value: 28.6, visibility: 'summary', source: 'api' });
    assert.deepEqual(free.company.facts.ceo, report.company.facts.ceo, 'summary rows stay visible on the free tier');
  });
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Key figures                                                                */
/* ────────────────────────────────────────────────────────────────────────── */

describe('key figures', () => {
  // Fixture quarters end 2026-06-27, 2026-03-28, 2025-12-27 and 2025-09-27.
  it('sums the last four contiguous quarters and reads the latest balance sheet', () => {
    const f = keyFigures(baseContext().statements);

    assert.equal(f.flowBasis, 'ttm');
    assert.equal(f.flowPeriodEnd, '2026-06-27');
    assert.equal(f.revenue, 440.2e9);
    assert.equal(f.operatingIncome, 140.6e9);
    assert.equal(f.operatingMarginPct, 31.9);
    assert.equal(f.balanceSheetDate, '2026-06-27');
    assert.equal(f.cashBasis, 'cash_and_equivalents');
    assert.equal(f.netDebt, 64.9e9);
  });

  it('falls back to the fiscal year without four contiguous quarters, and counts short-term investments as cash', () => {
    const missing = baseContext().statements;
    missing.quarterly.splice(2, 1);
    assert.equal(keyFigures(missing).flowBasis, 'annual');

    const outOfSequence = baseContext().statements;
    const oldest = outOfSequence.quarterly[3];
    assert.ok(oldest);
    oldest.periodEnd = '2025-06-28';
    const annual = keyFigures(outOfSequence);
    assert.equal(annual.flowBasis, 'annual');
    assert.equal(annual.flowPeriodEnd, '2025-09-27');
    assert.equal(annual.revenue, 416.2e9);

    const withInvestments = baseContext().statements;
    const latest = withInvestments.quarterly[0];
    assert.ok(latest);
    latest.cashAndShortTermInvestments = 60e9;
    const cash = keyFigures(withInvestments);
    assert.equal(cash.cashBasis, 'with_short_term_investments');
    assert.equal(cash.cash, 60e9);
    assert.equal(cash.netDebt, 36.1e9);
  });

  it('leaves a flow figure empty rather than mixing bases', () => {
    const statements = baseContext().statements;
    const quarter = statements.quarterly[1];
    assert.ok(quarter);
    quarter.stockCompensation = null;
    const f = keyFigures(statements);

    assert.equal(f.flowBasis, 'ttm');
    assert.equal(f.stockCompensation, null);
    assert.equal(f.revenue, 440.2e9);
  });

  it('dcf, redflag and verdict render the same figures; only dcf sees the engine inputs', async () => {
    const calls = useTransport();
    const ctx = baseContext();
    await registry.dcf.run(structuredClone(ctx));
    await registry.redflag.run(structuredClone(ctx));
    await registry.verdict.run(verdictContext());

    const block = renderKeyFigures(keyFigures(ctx.statements), ctx.statements.currency);
    assert.match(block, /revenue \$440\.20B, operating income \$140\.60B \(operating margin 31\.9%\)/);
    assert.match(block, /net debt \$64\.90B/);

    const users = Object.fromEntries(calls.map((call) => [call.slug, call.user]));
    for (const slug of ['dcf', 'redflag', 'verdict'] as const) {
      assert.ok(users[slug]?.includes(block), `${slug} renders the shared key figures`);
      assert.doesNotMatch(users[slug] ?? '', /Free cash flow margin: /, `${slug} gets no second margin from the scorecard`);
    }
    assert.match(users.dcf ?? '', /Net debt used by the engine: /);
    for (const slug of ['redflag', 'verdict'] as const) {
      // Line-anchored: reviewer prose quoted from dcf may say "used by the engine".
      assert.doesNotMatch(users[slug] ?? '', /^(Free cash flow base|Net debt|Shares outstanding) used by the engine: /m);
      assert.doesNotMatch(users[slug] ?? '', /^debtToEquity:/m, `${slug} gets no vendor debt to equity`);
    }
  });

  it('gives dcf the trailing twelve months beside the fiscal years', async () => {
    const calls = useTransport();
    const result = await registry.dcf.run(baseContext());
    const user = calls[0]?.user ?? '';

    assert.match(user, /Results, trailing twelve months to 2026-06-27: revenue \$440\.20B/);
    assert.match(user, /Revenue for the trailing twelve months to 2026-06-27 against the last fiscal year: \+5\.8%/);
    const trailing = result.data?.history.value.trailing;
    assert.equal(trailing?.basis, 'ttm');
    assert.equal(trailing?.revenue, 440.2e9);
    assert.equal(trailing?.revenueVsLastFiscalYearPct, 5.8);
  });

  it('takes the larger burn of the trailing twelve months and the last fiscal year for the runway', () => {
    // AMC on 2026-09-14: one strong quarter left trailing burn at $22.4M against $365.9M for the fiscal year.
    const statements = baseContext().statements;
    const quarterlyFcf = [190.1e6, -174.7e6, 43.3e6, -81.1e6];
    statements.quarterly.forEach((quarter, i) => {
      quarter.freeCashFlow = quarterlyFcf[i] ?? null;
    });
    const latestQuarter = statements.quarterly[0];
    const fiscalYear = statements.annual[0];
    assert.ok(latestQuarter && fiscalYear);
    latestQuarter.cashAndShortTermInvestments = 778.4e6;
    fiscalYear.freeCashFlow = -365.9e6;

    const f = keyFigures(statements);
    assert.deepEqual(
      [f.cashRunwayStatus, f.cashBurn, f.cashBurnBasis, f.cashBurnPeriodEnd, f.cashRunwayYears],
      ['burning', 365.9e6, 'annual', '2025-09-27', 2.1],
    );
    assert.match(
      renderKeyFigures(f, 'USD'),
      /Cash runway: 2\.1 years, cash against free cash flow burn of \$365\.9M a year \(fiscal year to 2025-09-27, the larger burn/,
    );

    fiscalYear.freeCashFlow = 10e9;
    assert.equal(keyFigures(statements).cashBurnBasis, 'ttm', 'a trailing burn counts when the fiscal year was positive');

    statements.quarterly.forEach((quarter) => {
      quarter.freeCashFlow = 1e9;
    });
    const positive = keyFigures(statements);
    assert.deepEqual([positive.cashRunwayStatus, positive.cashRunwayYears], ['not_burning', null]);
  });

  it('takes missing debt as zero only when the same balance sheet reports cash and equity', () => {
    const statements = baseContext().statements;
    const latestQuarter = statements.quarterly[0];
    assert.ok(latestQuarter);
    latestQuarter.totalDebt = null;

    const f = keyFigures(statements);
    assert.deepEqual([f.totalDebt, f.debtAssumedZero, f.netDebt, f.debtToEquity], [0, true, -31.2e9, 0]);
    assert.match(
      renderKeyFigures(f, 'USD'),
      /total debt none reported \(taken as zero\), cash and equivalents \$31\.20B, net debt -\$31\.20B/,
    );

    latestQuarter.shareholdersEquity = null;
    const partial = keyFigures(statements);
    assert.deepEqual([partial.totalDebt, partial.debtAssumedZero, partial.netDebt], [null, false, null]);
  });
});

/* ────────────────────────────────────────────────────────────────────────── */
/* Valuation reliability                                                      */
/* ────────────────────────────────────────────────────────────────────────── */

describe('valuation reliability', () => {
  /** The recorded dcf reply on a consistent history, with the named inputs rated unclear. */
  const dcfReply =
    (patch: Partial<DcfModel> = {}, unclear: string[] = []) =>
    (): DcfModel => {
      const recorded = reply('dcf') as unknown as DcfModel;
      return {
        ...recorded,
        historicalFit: 'consistent',
        assumptions: recorded.assumptions.map((row) => ({
          ...row,
          engineVsEvidence: unclear.includes(row.key)
            ? ('unclear' as const)
            : row.engineVsEvidence === 'unclear'
              ? ('in_line' as const)
              : row.engineVsEvidence,
        })),
        ...patch,
      };
    };

  it('a growth path that breaks from history overrides a confident engine', async () => {
    useTransport({ dcf: dcfReply({ historicalFit: 'break', confidence: 'high' }) });
    const ctx = baseContext();
    assert.ok(ctx.valuation);
    ctx.valuation.confidence = 'high';
    ctx.valuation.marginOfSafetyPct = 191;
    const result = await registry.dcf.run(ctx);
    const d = result.data;

    assert.ok(d, JSON.stringify(result.error));
    assert.deepEqual(d.valuationReliability.value, {
      reliable: false,
      reasons: ["the growth path breaks from the company's own history"],
    });
    assert.equal(d.valuationReliability.visibility, 'headline');
    assert.deepEqual([d.confidence.value, d.confidence.source], ['low', 'computed']);
    assert.match(d.confidence.note ?? '', /^Forced to low: the growth path breaks/);
    assert.equal(d.intrinsicValue.value, ctx.valuation.intrinsicValue, 'the engine value stays visible, with a warning');
    for (const shown of [d.intrinsicValue, d.valueRange, d.marginOfSafetyPct]) {
      assert.match(shown.note ?? '', /Marked unreliable by the dcf review/);
    }
  });

  it('two core inputs rated unclear mark the value unreliable; one does not', async () => {
    useTransport({ dcf: dcfReply({ confidence: 'medium' }, ['stage1_growth', 'wacc']) });
    const two = await registry.dcf.run(baseContext());
    assert.deepEqual(two.data?.valuationReliability.value, {
      reliable: false,
      reasons: ['near-term growth and discount rate could not be assessed'],
    });
    assert.equal(two.data?.confidence.value, 'low');

    useTransport({ dcf: dcfReply({ confidence: 'medium' }, ['wacc']) });
    const one = await registry.dcf.run(baseContext());
    assert.deepEqual(one.data?.valuationReliability.value, { reliable: true, reasons: [] });
    assert.deepEqual([one.data?.confidence.value, one.data?.confidence.source], ['medium', 'model']);
    assert.equal(one.data?.intrinsicValue.note, undefined);
  });

  it('inputs the engine left empty count as not assessable', async () => {
    useTransport({ dcf: dcfReply() });
    const ctx = baseContext();
    assert.ok(ctx.valuation);
    ctx.valuation.waccPct = null;
    ctx.valuation.fcfBase = null;
    const result = await registry.dcf.run(ctx);

    assert.deepEqual(result.data?.valuationReliability.value, {
      reliable: false,
      reasons: ['discount rate and starting free cash flow could not be assessed'],
    });
  });

  it('an unreliable dcf lifts the engine anchor from the verdict and forces its confidence low', async () => {
    const ctx = verdictContext();
    const dcf = ctx.upstream?.dcf?.data as DcfOutput | undefined;
    assert.ok(dcf && ctx.valuation);
    dcf.valuationReliability = {
      value: { reliable: false, reasons: ['test reason'] },
      visibility: 'headline',
      source: 'computed',
    };
    ctx.valuation.marginOfSafetyPct = 191; // engine band: Undervalued

    const calls = useTransport({ verdict: () => ({ ...reply('verdict'), band: 'Overvalued', confidence: 'high' }) });
    const result = await registry.verdict.run(ctx);

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.equal(result.meta.attempts, 1, 'four steps from the engine band is allowed once the anchor is lifted');
    assert.equal(result.data?.engineBand.value, 'Undervalued');
    assert.match(result.data?.engineBand.note ?? '', /Not used as an anchor/);
    assert.deepEqual(result.data?.guardrails.value, {
      regimeCap: false,
      backstopFloor: false,
      valuationReliable: false,
      allowedBands: [...VALUATION_BANDS],
    });
    assert.deepEqual([result.data?.confidence.value, result.data?.confidence.source], ['low', 'computed']);

    const user = calls[0]?.user ?? '';
    assert.match(user, /Valuation reliability: not a reliable anchor for the verdict \(test reason\)/);
    assert.match(user, /## Valuation engine output \(marked unreliable; not evidence\)/);
    assert.match(user, /Valuation reliability: unreliable \(test reason\)/);
  });

  it('a dcf result from before the field keeps the engine anchor', async () => {
    useTransport();
    const ctx = verdictContext();
    const dcf = ctx.upstream?.dcf?.data as Partial<DcfOutput> | undefined;
    assert.ok(dcf);
    delete dcf.valuationReliability;
    const result = await registry.verdict.run(ctx);

    assert.equal(result.data?.guardrails.value.valuationReliable, true);
    assert.equal(result.data?.guardrails.value.allowedBands.length, 4);
    assert.equal(result.data?.confidence.source, 'model');
  });
});

/* ────────────────────────────────────────────────────────────────────────── */
/* dcf assessments and wording                                                */
/* ────────────────────────────────────────────────────────────────────────── */

describe('dcf assessments', () => {
  type Row = DcfModel['assumptions'][number];
  const row = (key: Row['key'], engineVsEvidence: Row['engineVsEvidence']): Row => ({
    key,
    evidence: 'Figures from the context.',
    reasoning: 'Compared with the reported figures.',
    engineVsEvidence,
  });

  it('sets aggressive or conservative in code from the higher or lower call', async () => {
    // MU on 2026-09-14: a $1.67B base against $26.17B trailing free cash flow came back "aggressive".
    useTransport({
      dcf: () => ({
        ...reply('dcf'),
        historicalFit: 'consistent',
        assumptions: [
          row('stage1_growth', 'higher'),
          row('terminal_growth', 'in_line'),
          row('wacc', 'lower'),
          row('fcf_base', 'lower'),
          row('share_count', 'higher'),
          row('net_debt', 'unclear'),
        ],
      }),
    });
    const result = await registry.dcf.run(baseContext());

    assert.deepEqual(
      Object.fromEntries((result.data?.assumptions.value ?? []).map((a) => [a.key, a.assessment])),
      {
        stage1_growth: 'aggressive',
        terminal_growth: 'supported',
        wacc: 'aggressive',
        fcf_base: 'conservative',
        share_count: 'conservative',
        net_debt: 'unclear',
      },
    );
  });

  it('asks for the call after the reasoning, so the label follows it', () => {
    assert.deepEqual(Object.keys(DcfModelSchema.shape.assumptions.element.shape), [
      'key',
      'evidence',
      'reasoning',
      'engineVsEvidence',
    ]);
  });
});

describe('valuation basis', () => {
  /** The base context with the engine's pure DCF value and the path that replaced it. */
  const withBasis = (baseIv: number | null): AgentContext => {
    const ctx = baseContext();
    assert.ok(ctx.valuation);
    ctx.valuation.baseIv = baseIv;
    ctx.valuation.ivSourceLabel = 'DCF';
    ctx.valuation.fin415Used = true;
    ctx.valuation.consensusAnchorPreIv = 355.96;
    return ctx;
  };

  it('shows dcf how the displayed value was produced, and nobody else', async () => {
    const calls = useTransport();
    await registry.dcf.run(withBasis(34.72));
    await registry.redflag.run(withBasis(34.72));
    const users = Object.fromEntries(calls.map((call) => [call.slug, call.user]));

    assert.match(
      users.dcf ?? '',
      /pure discounted cash flow value from these inputs, before the site's later adjustments: \$34\.72/,
    );
    assert.match(users.dcf ?? '', /an FCFE model replaced the discounted cash flow result before display/);
    assert.match(users.dcf ?? '', /value before the blend with the analyst target: \$355\.96/);
    assert.doesNotMatch(users.redflag ?? '', /How the displayed value was produced/);
  });

  it('requires both figures in inputRationale when the two values differ materially', async () => {
    // MU on 2026-09-14: a $34.72 pure DCF behind a $471.68 displayed value.
    const calls = useTransport({
      dcf: (req) =>
        req.attempt === 1
          ? { ...reply('dcf'), inputRationale: 'This review is of the engine valuation.' }
          : {
              ...reply('dcf'),
              inputRationale:
                'This review is of the pure discounted cash flow value of $34.72; the page shows $198.50 after later adjustments.',
            },
    });
    const result = await registry.dcf.run(withBasis(34.72));

    assert.equal(result.status, 'ok', JSON.stringify(result.error));
    assert.equal(result.meta.attempts, 2);
    assert.match(calls[1]?.repairHint ?? '', /must name both values as figures/);
    assert.deepEqual(result.data?.valuationBasis.value, {
      baseIv: 34.72,
      displayIv: 198.5,
      differsMaterially: true,
    });
    assert.equal(result.data?.inputRationale.visibility, 'summary');
    assert.match(result.data?.inputRationale.note ?? '', /review is of the pure value/);
  });

  it('asks for nothing extra when the displayed value is the DCF value, or when there is none', async () => {
    const sameValue = useTransport({
      dcf: () => ({ ...reply('dcf'), inputRationale: 'This review is of the value shown on the page.' }),
    });
    const same = await registry.dcf.run(withBasis(198.5));
    assert.equal(same.meta.attempts, 1);
    assert.equal(sameValue.length, 1);
    assert.equal(same.data?.valuationBasis.value.differsMaterially, false);

    useTransport({
      dcf: () => ({ ...reply('dcf'), inputRationale: 'This review is of the engine value.' }),
    });
    const missing = await registry.dcf.run(baseContext());
    assert.deepEqual(missing.data?.valuationBasis.value, {
      baseIv: null,
      displayIv: 198.5,
      differsMaterially: false,
    });
  });
});

describe('valuation method', () => {
  const pathFor = (label: string | null) => {
    const ctx = baseContext();
    assert.ok(ctx.valuation);
    ctx.valuation.ivSourceLabel = label;
    return valuationPathOf(ctx.valuation);
  };

  const labelled = (label: string | null): AgentContext => {
    const ctx = verdictContext();
    assert.ok(ctx.valuation);
    ctx.valuation.ivSourceLabel = label;
    return ctx;
  };

  it("reads the engine's own labels", () => {
    for (const label of [
      'DCF',
      'Network Effect Valuation: Standard DCF applied to asset-light payment infrastructure.',
      null,
    ]) {
      assert.equal(pathFor(label).isDcf, true, `${label} is a discounted cash flow`);
    }
    for (const label of [
      'Analyst Target (pre-revenue biotech)',
      'Banking-DCF (616.01) 70% + P/B (1.56x) + P/E (13.2x) blend + ROE 17.8% efficiency premium 30%',
      'Banking-DCF (Net Income → FCFE)',
      'DCF 50% + EV/Revenue 4.2x 50%',
      'DCF + Multiples (50/50)',
      'Multiples (EV/EBITDA)',
      'Cash-Only Distress Proxy',
      'Sultan Split',
    ]) {
      assert.equal(pathFor(label).isDcf, false, `${label} is not a discounted cash flow`);
    }
  });

  it('dcf refuses a value no discounted cash flow produced, without calling the model', async () => {
    const calls = useTransport();
    const ctx = baseContext();
    assert.ok(ctx.valuation);
    ctx.valuation.ivSourceLabel = 'Analyst Target (pre-revenue biotech)';
    const result = await registry.dcf.run(ctx);

    assert.equal(result.status, 'unavailable');
    assert.equal(result.error?.kind, 'insufficient_data');
    assert.match(result.error?.message ?? '', /it used Analyst Target \(pre-revenue biotech\)/);
    assert.equal(calls.length, 0, 'the refusal costs no model call');
  });

  it('dcf still runs on a discounted cash flow, or when the label is missing', async () => {
    const calls = useTransport();
    const ctx = baseContext();
    assert.ok(ctx.valuation);
    ctx.valuation.ivSourceLabel = 'DCF';

    assert.equal((await registry.dcf.run(ctx)).status, 'ok');
    assert.equal((await registry.dcf.run(baseContext())).status, 'ok');
    assert.equal(calls.length, 2);
  });

  it('the verdict names the method in its rules and carries it as a field', async () => {
    const calls = useTransport();
    const blended = await registry.verdict.run(
      labelled('Banking-DCF (616.01) 70% + P/B (1.56x) + P/E (13.2x) blend'),
    );

    assert.match(
      calls[0]?.user ?? '',
      /Valuation method: NOT a discounted cash flow\. The engine valued this on Banking-DCF/,
    );
    assert.equal(blended.data?.valuationMethod.value.isDcf, false);
    assert.equal(blended.data?.valuationMethod.visibility, 'summary');
    assert.match(blended.data?.valuationMethod.note ?? '', /not a discounted cash flow/);

    const plain = await registry.verdict.run(labelled('DCF'));
    assert.match(calls[1]?.user ?? '', /Valuation method: discounted cash flow \(DCF\)/);
    assert.deepEqual(plain.data?.valuationMethod.value, { label: 'DCF', isDcf: true });
    assert.equal(plain.data?.valuationMethod.note, undefined);
  });

  it('a value no discounted cash flow produced is no anchor either', async () => {
    const calls = useTransport();
    const blended = await registry.verdict.run(labelled('Analyst Target (pre-revenue biotech)'));

    assert.equal(blended.data?.guardrails.value.valuationReliable, false);
    assert.deepEqual(blended.data?.guardrails.value.allowedBands, [...VALUATION_BANDS]);
    assert.deepEqual([blended.data?.confidence.value, blended.data?.confidence.source], ['low', 'computed']);
    assert.match(
      calls[0]?.user ?? '',
      /not a reliable anchor for the verdict \(the engine valued this on Analyst Target \(pre-revenue biotech\), not a discounted cash flow\)/,
    );
    assert.match(blended.data?.engineBand.note ?? '', /Not used as an anchor/);

    const plain = await registry.verdict.run(labelled('DCF'));
    assert.equal(plain.data?.guardrails.value.valuationReliable, true);
    assert.equal(plain.data?.confidence.source, 'model');
  });
});

describe('neutral wording', () => {
  it('sends dcf and verdict prose that calls a figure fabricated back for repair', async () => {
    const calls = useTransport({
      dcf: (req) =>
        req.attempt === 1
          ? { ...reply('dcf'), headline: 'This value rests on a fabricated free cash flow figure.' }
          : reply('dcf'),
      verdict: (req) =>
        req.attempt === 1
          ? { ...reply('verdict'), bandRationale: 'The engine call uses a made up cash flow base.' }
          : reply('verdict'),
    });
    const dcf = await registry.dcf.run(baseContext());
    const verdict = await registry.verdict.run(verdictContext());

    for (const result of [dcf, verdict]) {
      assert.equal(result.status, 'ok');
      assert.equal(result.meta.attempts, 2);
    }
    for (const slug of ['dcf', 'verdict'] as const) {
      const repair = calls.find((call) => call.slug === slug && call.attempt === 2);
      assert.match(repair?.repairHint ?? '', /does not match the reported figures/);
    }
  });

  it('applies only where asked: other agents may report an allegation of fabricated figures', () => {
    const allegation = 'Regulators allege the sales figures were fabricated.';
    assert.equal(zProse(200).safeParse(allegation).success, true);
    assert.equal(zProse(200, { neutral: true }).safeParse(allegation).success, false);
    assert.equal(
      zProse(200, { neutral: true }).safeParse('The engine figure does not match the reported figures.').success,
      true,
    );
  });
});
