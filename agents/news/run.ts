import { generateValidated } from '../_shared/client.ts';
import {
  isNum,
  renderNews,
  renderPrices,
  renderProfile,
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
} from '../_shared/types.ts';

import { SYSTEM_PROMPT } from './prompt.ts';
import {
  MATERIALITY,
  NewsModelSchema,
  NewsOutputSchema,
  type NewsModel,
  type NewsOutput,
} from './schema.ts';

const NEWS_LIMIT = 25;
/** Matches the "about 5 percent" threshold in the prompt. */
const NOTABLE_MOVE_PCT = 5;

export async function run(ctx: AgentContext): Promise<AgentResult<NewsOutput>> {
  const startedAt = Date.now();
  const base = { slug: 'news' as const, ticker: ctx.ticker, startedAt };

  const news = selectNews(ctx.news, NEWS_LIMIT);

  // Nothing to read is a real answer, not a failure, and not worth a call.
  if (news.length === 0) {
    return finalizeOutput(NewsOutputSchema, quietOutput(ctx), base);
  }

  const newsById = new Map(news.map((item) => [item.id, item]));
  const schema = NewsModelSchema.superRefine((out, issues) => {
    out.items.forEach((item, index) => {
      if (!newsById.has(item.newsId)) {
        issues.addIssue({
          code: 'custom',
          path: ['items', index, 'newsId'],
          message: `"${item.newsId}" is not one of the news ids in the context`,
        });
      }
    });
  });

  const gen = await generateValidated({
    slug: 'news',
    ticker: ctx.ticker,
    system: SYSTEM_PROMPT,
    user: buildUserTurn(ctx, news),
    schema,
    effort: 'low',
    signal: ctx.signal,
  });

  const init = { ...base, model: gen.model, attempts: gen.attempts, usage: gen.usage };
  if (!gen.data) {
    return unavailable(gen.error ?? { kind: 'unknown', message: 'no data returned' }, init);
  }

  return finalizeOutput(NewsOutputSchema, toOutput(gen.data, ctx, news, newsById), init);
}

function buildUserTurn(ctx: AgentContext, news: NewsItem[]): string {
  return [
    `Data as of ${ctx.asOf}.`,
    section('Company', renderProfile(ctx.profile)),
    section('Price', renderPrices(ctx.prices)),
    section(`News items (${news.length}, newest first)`, renderNews(news)),
  ].join('\n\n');
}

function toOutput(
  model: NewsModel,
  ctx: AgentContext,
  news: NewsItem[],
  newsById: Map<string, NewsItem>,
): NewsOutput {
  const stories = model.items
    .flatMap((item) => {
      const source = newsById.get(item.newsId);
      if (!source) return [];
      return [
        {
          id: source.id,
          title: source.title,
          source: source.source,
          url: source.url,
          publishedAt: source.publishedAt,
          materiality: item.materiality,
          direction: item.direction,
          affects: item.affects,
          takeaway: tidy(item.takeaway),
        },
      ];
    })
    // high → medium → low; stable, so the model's order breaks ties.
    .sort((a, b) => MATERIALITY.indexOf(a.materiality) - MATERIALITY.indexOf(b.materiality));

  return {
    headline: field(tidy(model.headline), 'headline', 'model'),
    tone: field(model.tone, 'headline', 'model'),

    plainEnglish: field(tidy(model.plainEnglish), 'summary', 'model'),
    materialCount: field(
      stories.filter((s) => s.materiality !== 'low').length,
      'summary',
      'computed',
      'Stories rated high or medium materiality',
    ),

    stories: field(stories, 'detail', 'model', 'title, source, url and date come from the news feed'),
    priceMove: field(
      {
        returnPct1m: ctx.prices.returnsPct.m1,
        explained: model.priceMoveExplained,
        note: model.priceMoveNote === null ? null : tidy(model.priceMoveNote),
      },
      'detail',
      'model',
    ),
    coverage: field(coverageOf(news), 'detail', 'computed'),
  };
}

function quietOutput(ctx: AgentContext): NewsOutput {
  const m1 = ctx.prices.returnsPct.m1;
  const notable = isNum(m1) && Math.abs(m1) >= NOTABLE_MOVE_PCT;
  return {
    headline: field(`No recent news items were available for ${ctx.profile.name}.`, 'headline', 'computed'),
    tone: field('quiet', 'headline', 'computed'),

    plainEnglish: field(
      'There were no recent news stories to review, so nothing in the news changes the picture.',
      'summary',
      'computed',
    ),
    materialCount: field(0, 'summary', 'computed'),

    stories: field([], 'detail', 'computed'),
    priceMove: field(
      { returnPct1m: m1, explained: notable ? 'no' : 'no_notable_move', note: null },
      'detail',
      'computed',
    ),
    coverage: field(coverageOf([]), 'detail', 'computed'),
  };
}

function coverageOf(news: NewsItem[]) {
  return {
    itemsReviewed: news.length,
    newestAt: news[0]?.publishedAt ?? null,
    oldestAt: news[news.length - 1]?.publishedAt ?? null,
  };
}
