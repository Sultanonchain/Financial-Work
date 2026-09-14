import { z } from 'zod';

import { zField, zProse } from '../_shared/types.ts';

export const MATERIALITY = ['high', 'medium', 'low'] as const;
export const NEWS_DIRECTIONS = ['positive', 'negative', 'neutral'] as const;
export const NEWS_AFFECTS = [
  'demand',
  'pricing',
  'costs',
  'capital',
  'legal_regulatory',
  'management',
  'competition',
  'macro',
] as const;
export const NEWS_TONES = ['positive', 'negative', 'mixed', 'quiet'] as const;
export const PRICE_MOVE_EXPLAINED = ['yes', 'partly', 'no', 'no_notable_move'] as const;

/* ────────────────────────────────────────────────────────────────────────── */
/* Model-facing                                                               */
/* ────────────────────────────────────────────────────────────────────────── */

/** run.ts adds the context-dependent rule: every newsId must exist. */
export const NewsModelSchema = z
  .object({
    headline: zProse(200),
    plainEnglish: zProse(400),
    tone: z.enum(NEWS_TONES),
    items: z
      .array(
        z.object({
          newsId: z.string(),
          materiality: z.enum(MATERIALITY),
          direction: z.enum(NEWS_DIRECTIONS),
          affects: z.enum(NEWS_AFFECTS),
          takeaway: zProse(400),
        }),
      )
      .max(12),
    priceMoveExplained: z.enum(PRICE_MOVE_EXPLAINED),
    priceMoveNote: zProse(300).nullable(),
  })
  .superRefine((out, issues) => {
    const seen = new Set<string>();
    out.items.forEach((item, index) => {
      if (seen.has(item.newsId)) {
        issues.addIssue({
          code: 'custom',
          path: ['items', index, 'newsId'],
          message: `"${item.newsId}" is listed twice; include each story once`,
        });
      }
      seen.add(item.newsId);
    });
    if (out.items.length === 0 && out.tone !== 'quiet') {
      issues.addIssue({
        code: 'custom',
        path: ['tone'],
        message: 'tone must be "quiet" when no items are included',
      });
    }
    if (out.tone === 'quiet' && out.items.some((item) => item.materiality === 'high')) {
      issues.addIssue({
        code: 'custom',
        path: ['tone'],
        message: 'tone cannot be "quiet" when a high-materiality item is included',
      });
    }
  });

export type NewsModel = z.infer<typeof NewsModelSchema>;

/* ────────────────────────────────────────────────────────────────────────── */
/* Public output                                                              */
/* ────────────────────────────────────────────────────────────────────────── */

const StorySchema = z.object({
  id: z.string(),
  title: z.string(),
  source: z.string().nullable(),
  url: z.string().nullable(),
  publishedAt: z.string().nullable(),
  materiality: z.enum(MATERIALITY),
  direction: z.enum(NEWS_DIRECTIONS),
  affects: z.enum(NEWS_AFFECTS),
  takeaway: z.string(),
});

export const NewsOutputSchema = z.object({
  headline: zField(z.string()),
  tone: zField(z.enum(NEWS_TONES)),

  plainEnglish: zField(z.string()),
  materialCount: zField(z.number().int().nonnegative()),

  stories: zField(z.array(StorySchema)),
  priceMove: zField(
    z.object({
      returnPct1m: z.number().nullable(),
      explained: z.enum(PRICE_MOVE_EXPLAINED),
      note: z.string().nullable(),
    }),
  ),
  coverage: zField(
    z.object({
      itemsReviewed: z.number().int().nonnegative(),
      newestAt: z.string().nullable(),
      oldestAt: z.string().nullable(),
    }),
  ),
});

export type NewsOutput = z.infer<typeof NewsOutputSchema>;
