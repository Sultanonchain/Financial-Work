/**
 * The company section: the stats row and the facts panel, straight from the
 * data layer.
 *
 * No agent and no model call touch anything here. Every value is copied from
 * AgentContext, which the caller filled from yfinance, so every Field is source
 * "api". A value the data layer does not have is left out, and nothing is
 * inferred to fill the gap. There is deliberately no IPO date: yfinance only
 * has a first-trade date, which is Yahoo's data start for older listings.
 */

import { z } from 'zod';

import { isNum, utcToday } from './format.ts';
import { zField, type AgentContext, type CompanyHeadquarters, type CompanyOfficer } from './types.ts';

const apiField = <S extends z.ZodType>(inner: S) => zField(inner).extend({ source: z.literal('api') });

export const CompanySectionSchema = z.object({
  /** Stats row, in display order. */
  stats: z.object({
    price: apiField(z.number().positive()).optional(),
    marketCap: apiField(z.number().positive()).optional(),
    forwardPE: apiField(z.number().positive()).optional(),
  }),
  /** Facts panel, in display order, shown above businessSummary. */
  facts: z.object({
    founded: apiField(z.number().int()).optional(),
    headquarters: apiField(z.string().min(1)).optional(),
    employees: apiField(z.number().int().positive()).optional(),
    sector: apiField(z.string().min(1)).optional(),
    industry: apiField(z.string().min(1)).optional(),
    ceo: apiField(z.string().min(1)).optional(),
  }),
  businessSummary: apiField(z.string().min(1)).optional(),
});

export type CompanySection = z.infer<typeof CompanySectionSchema>;

/** Stats and facts are what a free user sees first. */
const VISIBILITY = 'summary' as const;

function apiValue<T>(value: T, note?: string) {
  return note === undefined
    ? { value, visibility: VISIBILITY, source: 'api' as const }
    : { value, visibility: VISIBILITY, source: 'api' as const, note };
}

/**
 * Build the section from the context alone. Pure and synchronous, so a caller
 * can render it before any agent finishes, and it cannot fail with them.
 */
export function buildCompanySection(ctx: AgentContext): CompanySection {
  const profile = ctx.profile;

  const stats: CompanySection['stats'] = {};
  const price = positive(ctx.prices?.last) ?? positive(ctx.valuation?.price);
  if (price !== null) stats.price = apiValue(price);
  const marketCap = positive(profile.marketCap);
  if (marketCap !== null) stats.marketCap = apiValue(marketCap);
  // A negative forward P/E (losses expected) is not a usable multiple, so it is
  // left out like a missing one.
  const forwardPE = positive(profile.forwardPE);
  if (forwardPE !== null) stats.forwardPE = apiValue(forwardPE);

  const facts: CompanySection['facts'] = {};
  const founded = profile.foundedYear;
  if (isNum(founded) && Number.isInteger(founded) && founded >= 1600 && founded <= Number(utcToday().slice(0, 4))) {
    facts.founded = apiValue(founded);
  }
  const headquarters = formatHeadquarters(profile.headquarters);
  if (headquarters) facts.headquarters = apiValue(headquarters);
  const employees = positive(profile.employees);
  if (employees !== null) facts.employees = apiValue(Math.round(employees));
  const sector = text(profile.sector);
  if (sector) facts.sector = apiValue(sector);
  const industry = text(profile.industry);
  if (industry) facts.industry = apiValue(industry);
  const ceo = selectCeo(profile.officers);
  if (ceo) facts.ceo = apiValue(ceo, 'The listed officer whose title makes them chief executive of the whole company');

  const businessSummary = text(profile.description);
  return businessSummary ? { stats, facts, businessSummary: apiValue(businessSummary) } : { stats, facts };
}

/** "Cupertino, CA, United States". Null without at least a city or a country. */
export function formatHeadquarters(hq: CompanyHeadquarters | null | undefined): string | null {
  if (!hq || typeof hq !== 'object') return null;
  const city = text(hq.city);
  const country = text(hq.country);
  if (!city && !country) return null;
  return [city, text(hq.region), country].filter(Boolean).join(', ');
}

const CEO_TITLE = /\b(ceo|chief executive officer)\b/i;
/**
 * Titles that mention a CEO without making the holder the company CEO: a role
 * scoped to part of the company ("CEO of Consumer Banking"), a role that serves
 * the CEO ("Senior Advisor to the CEO", "Office of the CEO"), or one no longer held.
 */
const NOT_COMPANY_CEO =
  /\b(ceo|chief executive officer)\s+(of|for)\b|\b(to|of)\s+the\s+(ceo|chief executive officer)\b|\b(former|retired|emeritus)\b/i;
const CO_CEO = /\bco-?\s*(ceo|chief executive)/i;
const HONORIFIC = /^(mr|mrs|ms|miss|mx|dr|prof|sir|dame)\.?\s+/i;
const CREDENTIAL = /,?\s+(ph\.?\s?d|m\.?d|j\.?d|cpa|cfa|mba|esq)\.?$/i;

/**
 * The company's chief executive from the officer list, or null. A title that
 * scopes the role to a division, serves the CEO, or is in the past does not
 * count. Co-CEOs are listed together; any other case with more than one
 * company-level CEO is ambiguous, so the row is omitted rather than guessed.
 */
export function selectCeo(officers: readonly CompanyOfficer[] | null | undefined): string | null {
  if (!Array.isArray(officers)) return null;
  const ceos = officers.filter(
    (officer) =>
      typeof officer?.name === 'string' &&
      typeof officer.title === 'string' &&
      CEO_TITLE.test(officer.title) &&
      !NOT_COMPANY_CEO.test(officer.title),
  );
  const names = [...new Set(ceos.map((officer) => cleanName(officer.name)).filter(Boolean))];
  if (names.length === 1) return names[0] ?? null;
  if (names.length > 1 && ceos.every((officer) => CO_CEO.test(officer.title ?? ''))) return names.join(' and ');
  return null;
}

/** "Dr. Brian  Lian Ph.D." → "Brian Lian". */
export function cleanName(name: string): string {
  let out = name.replace(/\s+/g, ' ').trim().replace(HONORIFIC, '');
  for (let previous = ''; previous !== out; ) {
    previous = out;
    out = out.replace(CREDENTIAL, '').trim();
  }
  return out;
}

function positive(value: unknown): number | null {
  return isNum(value) && value > 0 ? value : null;
}

function text(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}
