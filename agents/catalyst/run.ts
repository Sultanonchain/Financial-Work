import { generateValidated, MODELS } from '../_shared/client.ts';
import {
  isoDate,
  pct,
  renderNews,
  renderPrices,
  renderProfile,
  renderVendorFields,
  section,
  selectNews,
  tidy,
} from '../_shared/format.ts';
import {
  field,
  finalizeOutput,
  unavailable,
  type AgentContext,
  type AgentResult,
  type NewsItem,
  type VendorPayload,
} from '../_shared/types.ts';

import { SYSTEM_PROMPT } from './prompt.ts';
import {
  CATALYST_HORIZONS,
  CatalystModelSchema,
  CatalystOutputSchema,
  LIKELIHOODS,
  type CatalystModel,
  type CatalystOutput,
} from './schema.ts';

const NEWS_LIMIT = 25;

const YFINANCE_KEYS = ['exDividendDate', 'dividendRate', 'dividendYield', 'shortPercentOfFloat'] as const;
const FINVIZ_KEYS = ['Earnings', 'Short Float', 'Insider Trans', 'Inst Trans'] as const;

/** This agent's model. VALUS_AGENT_MODEL overrides it (resolveModel in client.ts). */
export const model: string = MODELS.sonnet;

export async function run(ctx: AgentContext): Promise<AgentResult<CatalystOutput>> {
  const startedAt = Date.now();
  const base = { slug: 'catalyst' as const, ticker: ctx.ticker, startedAt };

  const news = selectNews(ctx.news, NEWS_LIMIT);
  const newsById = new Map(news.map((item) => [item.id, item]));
  const earningsDate = nextEarningsDate(ctx.yfinance, ctx.asOf);

  // Evidence must point at news the model was actually shown. An invented id
  // fails validation and goes back through the repair turn.
  const schema = CatalystModelSchema.superRefine((out, issues) => {
    out.catalysts.forEach((catalyst, i) => {
      catalyst.evidenceNewsIds.forEach((id, j) => {
        if (!newsById.has(id)) {
          issues.addIssue({
            code: 'custom',
            path: ['catalysts', i, 'evidenceNewsIds', j],
            message: `"${id}" is not one of the news ids in the context`,
          });
        }
      });
    });
  });

  const gen = await generateValidated({
    slug: 'catalyst',
    ticker: ctx.ticker,
    system: SYSTEM_PROMPT,
    user: buildUserTurn(ctx, news, earningsDate),
    schema,
    effort: 'low',
    model,
    signal: ctx.signal,
  });

  const init = { ...base, model: gen.model, attempts: gen.attempts, usage: gen.usage };
  if (!gen.data) {
    return unavailable(gen.error ?? { kind: 'unknown', message: 'no data returned' }, init);
  }

  return finalizeOutput(CatalystOutputSchema, toOutput(gen.data, newsById, earningsDate), init);
}

function buildUserTurn(ctx: AgentContext, news: NewsItem[], earningsDate: string | null): string {
  const v = ctx.valuation;
  const calendar = [
    `Next earnings date: ${earningsDate ?? 'not in the calendar fields'}`,
    renderVendorFields(ctx.yfinance, YFINANCE_KEYS),
    renderVendorFields(ctx.finviz, FINVIZ_KEYS),
  ]
    .filter(Boolean)
    .join('\n');

  return [
    `Data as of ${ctx.asOf}.`,
    section('Company', renderProfile(ctx.profile)),
    section(
      'Growth the valuation already assumes',
      v
        ? [
            `Stage-1 growth used by the engine: ${pct(v.stage1GrowthPct, 2)}`,
            `Growth the current price implies: ${pct(v.impliedGrowthPct, 2)}`,
          ].join('\n')
        : '',
    ),
    section('Price', renderPrices(ctx.prices)),
    section('Calendar and vendor fields', calendar),
    section(`Recent news (${news.length} items, newest first)`, news.length ? renderNews(news) : ''),
  ].join('\n\n');
}

function nextEarningsDate(yfinance: VendorPayload, asOf: string): string | null {
  const today = asOf.slice(0, 10);
  const candidates: unknown[] = [];
  for (const key of ['nextEarningsDate', 'earningsDate', 'earningsTimestampStart', 'earningsTimestamp']) {
    const value = yfinance[key];
    if (Array.isArray(value)) candidates.push(...value);
    else candidates.push(value);
  }
  const upcoming = candidates
    .map(isoDate)
    .filter((date): date is string => date !== null && date >= today)
    .sort();
  return upcoming[0] ?? null;
}

function toOutput(
  model: CatalystModel,
  newsById: Map<string, NewsItem>,
  earningsDate: string | null,
): CatalystOutput {
  const catalysts = model.catalysts.map((c) => ({
    title: tidy(c.title),
    direction: c.direction,
    horizon: c.horizon,
    likelihood: c.likelihood,
    valueLever: c.valueLever,
    whyItMatters: tidy(c.whyItMatters),
    dateHint: c.dateHint,
    evidence: [...new Set(c.evidenceNewsIds)].flatMap((id) => {
      const item = newsById.get(id);
      return item
        ? [{ id, title: item.title, source: item.source, url: item.url, publishedAt: item.publishedAt }]
        : [];
    }),
  }));

  // Most likely first, then nearest. Stable, so the model's own ordering
  // breaks remaining ties.
  const top = [...catalysts].sort(
    (a, b) =>
      LIKELIHOODS.indexOf(b.likelihood) - LIKELIHOODS.indexOf(a.likelihood) ||
      CATALYST_HORIZONS.indexOf(a.horizon) - CATALYST_HORIZONS.indexOf(b.horizon),
  )[0];

  return {
    headline: field(tidy(model.headline), 'headline', 'model'),
    topCatalyst: field(
      top ? { title: top.title, direction: top.direction, horizon: top.horizon } : null,
      'headline',
      'model',
      'Selected in code: highest likelihood, then nearest horizon',
    ),

    plainEnglish: field(tidy(model.plainEnglish), 'summary', 'model'),
    netTilt: field(model.netTilt, 'summary', 'model'),
    nextEarningsDate: field(earningsDate, 'summary', 'api'),

    catalysts: field(
      catalysts,
      'detail',
      'model',
      'evidence rows are resolved from the supplied news by id',
    ),
  };
}
