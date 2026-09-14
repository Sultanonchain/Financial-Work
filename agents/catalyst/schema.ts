import { z } from 'zod';

import { zField, zProse } from '../_shared/types.ts';

/**
 * Lifecycle. rumored → reported → announced → shipped.
 *   rumored, reported   events still ahead: the only upcoming statuses, each on a stated day
 *   announced           an official event that has already happened; lands in historicalAnalogs
 *   shipped             delivered; stays in historicalAnalogs for SHIPPED_WINDOW_DAYS
 */
export const CATALYST_STATUSES = ['rumored', 'reported', 'announced', 'shipped'] as const;
export type CatalystStatus = (typeof CATALYST_STATUSES)[number];

export const UPCOMING_STATUSES = ['rumored', 'reported'] as const;
export const CONFIRMED_STATUSES = ['announced', 'shipped'] as const;

/** Shipped items stay in historicalAnalogs this many days after announcedDate. */
export const SHIPPED_WINDOW_DAYS = 90;

export const CATALYST_KINDS = [
  'earnings',
  'launch_event',
  'ship_date',
  'price',
  'availability',
  'legal',
  'regulatory',
  'corporate',
  'other',
] as const;
export type CatalystKind = (typeof CATALYST_KINDS)[number];

/**
 * Future claims only the company can confirm. An upcoming item of one of these
 * kinds needs a company statement among its evidence: a report or a rumor of a
 * ship date, price or availability window is not enough.
 */
export const COMPANY_CONFIRMED_KINDS: readonly CatalystKind[] = ['ship_date', 'price', 'availability'];

export const CATALYST_DIRECTIONS = ['positive', 'negative', 'two_sided'] as const;
export const CATALYST_HORIZONS = ['under_3m', '3_to_12m', 'over_12m'] as const;
export const LIKELIHOODS = ['low', 'medium', 'high'] as const;
export const VALUE_LEVERS = [
  'revenue_growth',
  'margins',
  'capital_returns',
  'balance_sheet',
  'risk_discount',
  'sentiment_only',
] as const;
export const NET_TILTS = ['positive', 'negative', 'balanced', 'none'] as const;

const isoDate = () => z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'use YYYY-MM-DD');

export function isUpcomingStatus(status: CatalystStatus): status is (typeof UPCOMING_STATUSES)[number] {
  return status === 'rumored' || status === 'reported';
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Model-facing                                                               */
/* ────────────────────────────────────────────────────────────────────────── */

const itemFields = {
  title: zProse(120),
  kind: z.enum(CATALYST_KINDS),
  direction: z.enum(CATALYST_DIRECTIONS),
  valueLever: z.enum(VALUE_LEVERS),
  whyItMatters: zProse(400),
  evidenceNewsIds: z.array(z.string()).max(5),
};

/**
 * Two lists, so the structure carries the lifecycle: an upcoming item cannot
 * leave out its date, and a past item cannot leave out announcedDate.
 *
 * run.ts adds the rules that need context. An upcoming item whose expectedDate
 * is not a day the context states, on or after today, is dropped there instead
 * of being sent back for repair, which is why expectedDate is a plain string
 * here: a model that writes "TBD" loses that one item, not the whole reply.
 */
export const CatalystModelSchema = z.object({
  headline: zProse(200),
  plainEnglish: zProse(400),
  upcoming: z
    .array(
      z.object({
        ...itemFields,
        status: z.enum(UPCOMING_STATUSES),
        horizon: z.enum(CATALYST_HORIZONS),
        likelihood: z.enum(LIKELIHOODS),
        expectedDate: z.string().describe('YYYY-MM-DD: a day the context states, today or later'),
      }),
    )
    .max(6),
  past: z
    .array(
      z.object({
        ...itemFields,
        status: z.enum(CONFIRMED_STATUSES),
        announcedDate: isoDate(),
      }),
    )
    .max(6),
});

export type CatalystModel = z.infer<typeof CatalystModelSchema>;

/* ────────────────────────────────────────────────────────────────────────── */
/* Public output                                                              */
/* ────────────────────────────────────────────────────────────────────────── */

const EvidenceSchema = z.object({
  id: z.string(),
  title: z.string(),
  source: z.string().nullable(),
  url: z.string().nullable(),
  publishedAt: z.string().nullable(),
});

/** Rumored or reported, on a stated day today or later. Never undated. */
const UpcomingCatalystSchema = z.object({
  title: z.string(),
  status: z.enum(UPCOMING_STATUSES),
  kind: z.enum(CATALYST_KINDS),
  direction: z.enum(CATALYST_DIRECTIONS),
  horizon: z.enum(CATALYST_HORIZONS),
  likelihood: z.enum(LIKELIHOODS),
  valueLever: z.enum(VALUE_LEVERS),
  whyItMatters: z.string(),
  expectedDate: isoDate(),
  evidence: z.array(EvidenceSchema),
});

/** Announced, or shipped within the window. eventDate is announcedDate. */
const HistoricalAnalogSchema = z.object({
  title: z.string(),
  status: z.enum(CONFIRMED_STATUSES),
  kind: z.enum(CATALYST_KINDS),
  direction: z.enum(CATALYST_DIRECTIONS),
  valueLever: z.enum(VALUE_LEVERS),
  whyItMatters: z.string(),
  eventDate: isoDate(),
  announcedDate: isoDate(),
  evidence: z.array(EvidenceSchema),
});

export const CatalystOutputSchema = z.object({
  headline: zField(z.string()),
  topCatalyst: zField(
    z
      .object({
        title: z.string(),
        status: z.enum(UPCOMING_STATUSES),
        kind: z.enum(CATALYST_KINDS),
        direction: z.enum(CATALYST_DIRECTIONS),
        horizon: z.enum(CATALYST_HORIZONS),
        expectedDate: isoDate(),
      })
      .nullable(),
  ),

  plainEnglish: zField(z.string()),
  netTilt: zField(z.enum(NET_TILTS)),
  nextEarningsDate: zField(isoDate().nullable()),

  upcomingCatalysts: zField(z.array(UpcomingCatalystSchema)),
  historicalAnalogs: zField(z.array(HistoricalAnalogSchema)),
});

export type CatalystOutput = z.infer<typeof CatalystOutputSchema>;

/**
 * The public schema plus the date rules, checked on the final object right
 * before it is returned. run.ts already dropped anything that breaks them, so
 * this only fails if run.ts itself is wrong, but it means a past-dated
 * upcoming item can never reach the UI.
 */
export function catalystOutputSchemaFor(today: string, shippedSince: string) {
  return CatalystOutputSchema.superRefine((out, issues) => {
    const add = (path: (string | number)[], message: string) =>
      issues.addIssue({ code: 'custom', path, message });

    out.upcomingCatalysts.value.forEach((item, i) => {
      if (item.expectedDate < today) {
        add(
          ['upcomingCatalysts', 'value', i, 'expectedDate'],
          `past-dated item in upcomingCatalysts: ${item.expectedDate} is before today (${today})`,
        );
      }
    });

    const top = out.topCatalyst.value;
    if (top !== null && top.expectedDate < today) {
      add(['topCatalyst', 'value', 'expectedDate'], `topCatalyst is dated ${top.expectedDate}, before today (${today})`);
    }

    const earnings = out.nextEarningsDate.value;
    if (earnings !== null && earnings < today) {
      add(['nextEarningsDate', 'value'], `nextEarningsDate ${earnings} is before today (${today})`);
    }

    out.historicalAnalogs.value.forEach((item, i) => {
      const at = ['historicalAnalogs', 'value', i];
      if (item.eventDate !== item.announcedDate) {
        add([...at, 'eventDate'], 'eventDate must be the announcedDate');
      }
      if (item.announcedDate > today) {
        add([...at, 'announcedDate'], `announcedDate ${item.announcedDate} is after today (${today})`);
      }
      if (item.status === 'shipped' && item.announcedDate < shippedSince) {
        add(
          [...at, 'announcedDate'],
          `shipped item announced ${item.announcedDate} is outside the ${SHIPPED_WINDOW_DAYS}-day window`,
        );
      }
    });
  });
}
