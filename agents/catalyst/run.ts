import { generateValidated, MODELS } from '../_shared/client.ts';
import {
  addDays,
  isoDate,
  pct,
  renderPrices,
  renderProfile,
  renderVendorFields,
  section,
  selectNews,
  tidy,
  utcToday,
} from '../_shared/format.ts';
import {
  field,
  finalizeOutput,
  unavailable,
  type AgentContext,
  type AgentResult,
  type CompanyProfile,
  type NewsItem,
  type VendorPayload,
} from '../_shared/types.ts';

import { SYSTEM_PROMPT } from './prompt.ts';
import {
  CatalystModelSchema,
  catalystOutputSchemaFor,
  COMPANY_CONFIRMED_KINDS,
  LIKELIHOODS,
  SHIPPED_WINDOW_DAYS,
  type CatalystModel,
  type CatalystOutput,
} from './schema.ts';

/** This agent's model. VALUS_AGENT_MODEL overrides it (resolveModel in client.ts). */
export const model: string = MODELS.sonnet;

/** Hard max_tokens cap: thinking plus up to twelve catalyst rows. */
export const maxTokens = 2_500;

const NEWS_LIMIT = 25;

const YFINANCE_KEYS = ['exDividendDate', 'dividendRate', 'dividendYield', 'shortPercentOfFloat'] as const;
const FINVIZ_KEYS = ['Earnings', 'Short Float', 'Insider Trans', 'Inst Trans'] as const;

/** Channels that only carry a company's own words: press-release wires and filings. */
const COMPANY_CHANNELS = /^(business wire|pr newswire|globe ?newswire|accesswire|sec filing|sec edgar|form (8-k|10-q|10-k|6-k))$/i;
const LEGAL_SUFFIXES = /\b(inc|corp|corporation|co|company|ltd|plc|holdings|group|sa|ag|nv)\b\.?/gi;

/**
 * Major news organizations. Their report of an official event that has already
 * happened is enough to call it announced. Opinion sites and scoop-driven
 * newsletters are deliberately absent.
 */
const MAJOR_OUTLETS =
  /^(the\s+)?(reuters|bloomberg(\s+news)?|wall street journal|wsj|financial times|ft|associated press|ap(\s+news)?|cnbc|new york times|nyt|washington post|nikkei(\s+asia)?|bbc(\s+news)?|economist|barron'?s|dow jones(\s+newswires)?)$/i;

/**
 * Hedging that marks unofficial sourcing. A news item whose title or summary
 * contains one of these cannot support announced or shipped, whichever outlet
 * published it.
 */
const UNOFFICIAL_SOURCING =
  /\b(people|person|sources?) familiar\b|\bsources? (said|say|says)\b|\baccording to (people|a person|sources)\b|\b(is|are|was|were) said to\b|\breportedly\b/i;

/* Exact days written in text. "March 2027" has no day, so it matches nothing. */
const MONTH = String.raw`(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?`;
const MONTH_DAY_YEAR = new RegExp(String.raw`\b${MONTH}\s+(\d{1,2})(?:st|nd|rd|th)?\b(?:,?\s+(\d{4})\b)?`, 'gi');
const DAY_MONTH_YEAR = new RegExp(String.raw`\b(\d{1,2})\s+${MONTH},?\s+(\d{4})\b`, 'gi');
const ISO_DAY = /\b(\d{4})-(\d{2})-(\d{2})\b/g;
const ISO_DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/;
const MONTH_KEYS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];

type ModelUpcoming = CatalystModel['upcoming'][number];
type ModelPast = CatalystModel['past'][number];
type Upcoming = CatalystOutput['upcomingCatalysts']['value'][number];
type Historical = CatalystOutput['historicalAnalogs']['value'][number];
type NetTilt = CatalystOutput['netTilt']['value'];

export async function run(ctx: AgentContext): Promise<AgentResult<CatalystOutput>> {
  const startedAt = Date.now();
  const base = { slug: 'catalyst' as const, ticker: ctx.ticker, startedAt };

  const today = utcToday();
  const shippedSince = addDays(today, -SHIPPED_WINDOW_DAYS);
  const news = selectNews(ctx.news, NEWS_LIMIT);
  const newsById = new Map(news.map((item) => [item.id, item]));
  const companyStatements = new Set(
    news.filter((item) => isCompanyStatement(item, ctx.profile)).map((item) => item.id),
  );
  const majorOutlets = new Set(news.filter(isMajorOutlet).map((item) => item.id));
  const unofficial = new Set(news.filter(isUnofficialSourcing).map((item) => item.id));
  const earningsDate = nextEarningsDate(ctx.yfinance, today);
  const calendar = renderCalendar(ctx, earningsDate);
  const calendarDays = statedDays(calendar, yearOf(ctx.asOf));
  const daysIn = (item: NewsItem) =>
    statedDays(`${item.title}\n${item.summary ?? ''}`, yearOf(item.publishedAt ?? ctx.asOf));
  const cite = (ids: string[]) =>
    ids.flatMap((id) => {
      const found = newsById.get(id);
      return found ? [found] : [];
    });

  /**
   * Why an upcoming item cannot be shown, or null when it can. It needs a day
   * the context states, on or after today. The calendar's date only dates the
   * earnings report; any other item needs a cited news item that gives the day.
   * Items that fail are dropped from the list, without a repair turn.
   */
  const upcomingProblem = (item: ModelUpcoming): string | null => {
    if (!ISO_DATE_ONLY.test(item.expectedDate)) return 'no exact day';
    if (item.expectedDate < today) return `dated ${item.expectedDate}, before today`;
    const stated = new Set(cite(item.evidenceNewsIds).flatMap(daysIn));
    if (item.kind === 'earnings') {
      for (const day of calendarDays) stated.add(day);
    }
    if (!stated.has(item.expectedDate)) return `${item.expectedDate} is not a day the context states`;
    return null;
  };

  /**
   * Why a past item is downgraded to reported, or null when its evidence
   * supports it. A company statement or major outlet only supports announced
   * or shipped when its text does not hedge ("people familiar", "reportedly").
   * A reported item needs a stated day still ahead, which an event that has
   * already happened cannot have, so a downgraded item is left out, without a
   * repair turn. Evidence that is neither a company statement nor a major
   * outlet is a different problem, and the refine below sends that back.
   */
  const downgradeReason = (item: ModelPast): string | null => {
    const candidates = cite(item.evidenceNewsIds).filter(
      (found) => companyStatements.has(found.id) || majorOutlets.has(found.id),
    );
    if (candidates.length === 0 || candidates.some((found) => !unofficial.has(found.id))) return null;
    return `unofficial sourcing in ${candidates.map((found) => found.source ?? found.id).join(', ')}`;
  };

  // Rules that do send the reply back for repair.
  const schema = CatalystModelSchema.superRefine((out, issues) => {
    const unknownIds = (list: 'upcoming' | 'past', i: number, ids: string[]) =>
      ids.forEach((id, j) => {
        if (!newsById.has(id)) {
          issues.addIssue({
            code: 'custom',
            path: [list, i, 'evidenceNewsIds', j],
            message: `"${id}" is not one of the news ids in the context`,
          });
        }
      });

    out.upcoming.forEach((item, i) => {
      // An item that will be dropped for its date is not worth a repair turn.
      if (upcomingProblem(item) !== null) return;
      unknownIds('upcoming', i, item.evidenceNewsIds);
      // Still ahead: only the company can confirm a ship date, price or
      // availability window.
      if (
        COMPANY_CONFIRMED_KINDS.includes(item.kind) &&
        !cite(item.evidenceNewsIds).some((found) => companyStatements.has(found.id))
      ) {
        issues.addIssue({
          code: 'custom',
          path: ['upcoming', i, 'kind'],
          message:
            `"${item.title}" is a future ${item.kind.replace('_', ' ')}, which needs a company statement ` +
            'among its evidence. Cite one, or leave it out',
        });
      }
    });

    out.past.forEach((item, i) => {
      const add = (path: (string | number)[], message: string) =>
        issues.addIssue({ code: 'custom', path: ['past', i, ...path], message });

      unknownIds('past', i, item.evidenceNewsIds);
      // Already happened: a company statement or a major outlet's report will do.
      const qualifying = cite(item.evidenceNewsIds).filter(
        (found) => companyStatements.has(found.id) || majorOutlets.has(found.id),
      );
      if (qualifying.length === 0) {
        add(
          ['evidenceNewsIds'],
          `"${item.title}" is ${item.status}, but its evidence is neither a company statement nor a major outlet. ` +
            'Cite one that reports the event, or leave it out',
        );
        return;
      }
      // Hedged evidence only: toOutput downgrades it, so its dates do not matter.
      if (downgradeReason(item) !== null) return;
      const supporting = qualifying.filter((found) => !unofficial.has(found.id));

      if (item.announcedDate > today) {
        add(['announcedDate'], `announcedDate ${item.announcedDate} is after today (${today})`);
      }
      // The supporting item's own date (or the day before, for a US evening
      // story stamped the next day in UTC), or a date the item gives.
      const allowed = new Set<string>();
      for (const source of supporting) {
        const published = isoDate(source.publishedAt);
        if (published) {
          allowed.add(published);
          allowed.add(addDays(published, -1));
        }
        for (const day of daysIn(source)) allowed.add(day);
      }
      if (!allowed.has(item.announcedDate)) {
        const published = supporting.map((s) => isoDate(s.publishedAt) ?? 'undated').join(', ');
        add(
          ['announcedDate'],
          `announcedDate ${item.announcedDate} does not match the cited evidence (published ${published}). ` +
            "Use that item's date, or a date it gives",
        );
      }
    });
  });

  const gen = await generateValidated({
    slug: 'catalyst',
    ticker: ctx.ticker,
    system: SYSTEM_PROMPT,
    user: buildUserTurn(ctx, news, companyStatements, majorOutlets, calendar, today),
    schema,
    effort: 'low',
    model,
    maxTokens,
    signal: ctx.signal,
  });

  const init = { ...base, model: gen.model, attempts: gen.attempts, usage: gen.usage };
  if (!gen.data) {
    return unavailable(gen.error ?? { kind: 'unknown', message: 'no data returned' }, init);
  }

  return finalizeOutput(
    catalystOutputSchemaFor(today, shippedSince),
    toOutput(gen.data, newsById, earningsDate, shippedSince, upcomingProblem, downgradeReason),
    init,
  );
}

function renderCalendar(ctx: AgentContext, earningsDate: string | null): string {
  return [
    `Next earnings date: ${earningsDate ?? 'not in the calendar fields'}`,
    renderVendorFields(ctx.yfinance, YFINANCE_KEYS),
    renderVendorFields(ctx.finviz, FINVIZ_KEYS),
  ]
    .filter(Boolean)
    .join('\n');
}

function buildUserTurn(
  ctx: AgentContext,
  news: NewsItem[],
  companyStatements: Set<string>,
  majorOutlets: Set<string>,
  calendar: string,
  today: string,
): string {
  const v = ctx.valuation;
  const newsLines = news
    .map((item) => {
      const marker = companyStatements.has(item.id)
        ? ' | company statement'
        : majorOutlets.has(item.id)
          ? ' | major outlet'
          : '';
      const head =
        `[${item.id}] ${item.publishedAt?.slice(0, 10) ?? 'undated'} | ` +
        `${item.source ?? 'unknown source'}${marker} | ${item.title}`;
      return item.summary ? `${head}\n    ${item.summary}` : head;
    })
    .join('\n');

  return [
    `Today (UTC): ${today}. Data as of ${ctx.asOf}.`,
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
    section(
      `Recent news (${news.length} items, newest first; dates are publication dates)`,
      newsLines,
    ),
  ].join('\n\n');
}

/**
 * A company statement is an item the company issued itself: its own newsroom
 * or investor-relations channel (source named after the company), a
 * press-release wire, or a filing. A media outlet reporting what the company
 * said is not one, and neither is a site whose name merely contains the
 * company's ("AppleInsider").
 */
function isCompanyStatement(item: NewsItem, profile: CompanyProfile): boolean {
  const source = item.source?.trim();
  if (!source) return false;
  if (COMPANY_CHANNELS.test(source)) return true;

  const company = profile.name.replace(LEGAL_SUFFIXES, ' ').replace(/[,.]/g, ' ').replace(/\s+/g, ' ').trim();
  if (!company) return false;
  const own = new RegExp(
    `^${escapeRegExp(company)}(\\s+(inc|corp|corporation|ltd|plc)\\.?)?` +
      '(\\s+(newsroom|press room|press release|investor relations))?$',
    'i',
  );
  return own.test(source.replace(/,/g, ''));
}

function isMajorOutlet(item: NewsItem): boolean {
  const source = item.source?.trim();
  return source ? MAJOR_OUTLETS.test(source) : false;
}

function isUnofficialSourcing(item: NewsItem): boolean {
  return UNOFFICIAL_SOURCING.test(`${item.title}\n${item.summary ?? ''}`);
}

/** Exact days a text states, as YYYY-MM-DD. A month or a year on its own is not a day. */
function statedDays(text: string, referenceYear: number): string[] {
  const days: string[] = [];
  const add = (year: number, month: number, day: number) => {
    const date = new Date(Date.UTC(year, month, day));
    if (month >= 0 && date.getUTCFullYear() === year && date.getUTCMonth() === month && date.getUTCDate() === day) {
      days.push(date.toISOString().slice(0, 10));
    }
  };
  const monthOf = (name: string | undefined) => MONTH_KEYS.indexOf((name ?? '').slice(0, 3).toLowerCase());

  for (const [, year, month, day] of text.matchAll(ISO_DAY)) {
    add(Number(year), Number(month) - 1, Number(day));
  }
  for (const [, month, day, year] of text.matchAll(MONTH_DAY_YEAR)) {
    if (year) {
      add(Number(year), monthOf(month), Number(day));
    } else {
      // No year ("Oct 29 AMC"): this year or the next.
      add(referenceYear, monthOf(month), Number(day));
      add(referenceYear + 1, monthOf(month), Number(day));
    }
  }
  for (const [, day, month, year] of text.matchAll(DAY_MONTH_YEAR)) {
    add(Number(year), monthOf(month), Number(day));
  }
  return days;
}

function yearOf(timestamp: string): number {
  return Number(timestamp.slice(0, 4));
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function nextEarningsDate(yfinance: VendorPayload, today: string): string | null {
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

/** Positive against negative items; two-sided items count for neither. */
function netTiltOf(items: Upcoming[]): NetTilt {
  if (items.length === 0) return 'none';
  const positive = items.filter((item) => item.direction === 'positive').length;
  const negative = items.filter((item) => item.direction === 'negative').length;
  return positive > negative ? 'positive' : negative > positive ? 'negative' : 'balanced';
}

function toOutput(
  reply: CatalystModel,
  newsById: Map<string, NewsItem>,
  earningsDate: string | null,
  shippedSince: string,
  upcomingProblem: (item: ModelUpcoming) => string | null,
  downgradeReason: (item: ModelPast) => string | null,
): CatalystOutput {
  const evidenceFor = (ids: string[]) =>
    [...new Set(ids)].flatMap((id) => {
      const item = newsById.get(id);
      return item
        ? [{ id, title: item.title, source: item.source, url: item.url, publishedAt: item.publishedAt }]
        : [];
    });

  const upcoming: Upcoming[] = [];
  const leftOut: string[] = [];
  for (const item of reply.upcoming) {
    const problem = upcomingProblem(item);
    if (problem !== null) {
      leftOut.push(`${tidy(item.title)} (${problem})`);
      continue;
    }
    upcoming.push({
      title: tidy(item.title),
      status: item.status,
      kind: item.kind,
      direction: item.direction,
      horizon: item.horizon,
      likelihood: item.likelihood,
      valueLever: item.valueLever,
      whyItMatters: tidy(item.whyItMatters),
      expectedDate: item.expectedDate,
      evidence: evidenceFor(item.evidenceNewsIds),
    });
  }

  const historical: Historical[] = [];
  const downgraded: string[] = [];
  for (const item of reply.past) {
    const reason = downgradeReason(item);
    if (reason !== null) {
      downgraded.push(`${tidy(item.title)} (${reason})`);
      continue;
    }
    // Shipped items drop off once they fall outside the window.
    if (item.status === 'shipped' && item.announcedDate < shippedSince) continue;
    historical.push({
      title: tidy(item.title),
      status: item.status,
      kind: item.kind,
      direction: item.direction,
      valueLever: item.valueLever,
      whyItMatters: tidy(item.whyItMatters),
      eventDate: item.announcedDate,
      announcedDate: item.announcedDate,
      evidence: evidenceFor(item.evidenceNewsIds),
    });
  }

  upcoming.sort((a, b) => a.expectedDate.localeCompare(b.expectedDate));
  historical.sort((a, b) => b.eventDate.localeCompare(a.eventDate));

  // Most likely first, then soonest. Stable, so date order breaks ties.
  const top = [...upcoming].sort(
    (a, b) => LIKELIHOODS.indexOf(b.likelihood) - LIKELIHOODS.indexOf(a.likelihood),
  )[0];

  const upcomingNote =
    'Rumored and reported items on a day the context states, today or later, soonest first' +
    (leftOut.length ? `. Left out: ${leftOut.join('; ')}` : '');

  return {
    headline: field(tidy(reply.headline), 'headline', 'model'),
    topCatalyst: field(
      top
        ? {
            title: top.title,
            status: top.status,
            kind: top.kind,
            direction: top.direction,
            horizon: top.horizon,
            expectedDate: top.expectedDate,
          }
        : null,
      'headline',
      'model',
      'Selected in code from upcomingCatalysts: highest likelihood, then soonest expectedDate',
    ),

    plainEnglish: field(tidy(reply.plainEnglish), 'summary', 'model'),
    netTilt: field(
      netTiltOf(upcoming),
      'summary',
      'computed',
      'Positive against negative items in upcomingCatalysts; two-sided items count for neither',
    ),
    nextEarningsDate: field(earningsDate, 'summary', 'api'),

    upcomingCatalysts: field(upcoming, 'detail', 'model', upcomingNote),
    historicalAnalogs: field(
      historical,
      'detail',
      'model',
      `Announced items, and shipped items announced within ${SHIPPED_WINDOW_DAYS} days; eventDate is announcedDate` +
        (downgraded.length
          ? `. Downgraded to reported, and left out because a reported item needs a day still ahead: ${downgraded.join('; ')}`
          : ''),
    ),
  };
}
