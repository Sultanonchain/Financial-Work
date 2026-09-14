import { z } from 'zod';

import { zField, zProse } from '../_shared/types.ts';

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

/* ────────────────────────────────────────────────────────────────────────── */
/* Model-facing                                                               */
/* ────────────────────────────────────────────────────────────────────────── */

/**
 * Context-free rules live here. run.ts layers the context-dependent one on top
 * (every evidence id must exist in the news it was shown).
 */
export const CatalystModelSchema = z
  .object({
    headline: zProse(200),
    plainEnglish: zProse(400),
    netTilt: z.enum(NET_TILTS),
    catalysts: z
      .array(
        z.object({
          title: zProse(120),
          direction: z.enum(CATALYST_DIRECTIONS),
          horizon: z.enum(CATALYST_HORIZONS),
          likelihood: z.enum(LIKELIHOODS),
          valueLever: z.enum(VALUE_LEVERS),
          whyItMatters: zProse(400),
          evidenceNewsIds: z.array(z.string()).max(5),
          dateHint: z
            .string()
            .regex(/^\d{4}-\d{2}-\d{2}$/, 'use YYYY-MM-DD')
            .nullable(),
        }),
      )
      .max(6),
  })
  .superRefine((out, issues) => {
    if (out.catalysts.length === 0 && out.netTilt !== 'none') {
      issues.addIssue({
        code: 'custom',
        path: ['netTilt'],
        message: 'netTilt must be "none" when the catalyst list is empty',
      });
    }
    if (out.catalysts.length > 0 && out.netTilt === 'none') {
      issues.addIssue({
        code: 'custom',
        path: ['netTilt'],
        message: 'netTilt cannot be "none" when catalysts are listed',
      });
    }
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

const CatalystRowSchema = z.object({
  title: z.string(),
  direction: z.enum(CATALYST_DIRECTIONS),
  horizon: z.enum(CATALYST_HORIZONS),
  likelihood: z.enum(LIKELIHOODS),
  valueLever: z.enum(VALUE_LEVERS),
  whyItMatters: z.string(),
  dateHint: z.string().nullable(),
  evidence: z.array(EvidenceSchema),
});

export const CatalystOutputSchema = z.object({
  headline: zField(z.string()),
  topCatalyst: zField(
    z
      .object({
        title: z.string(),
        direction: z.enum(CATALYST_DIRECTIONS),
        horizon: z.enum(CATALYST_HORIZONS),
      })
      .nullable(),
  ),

  plainEnglish: zField(z.string()),
  netTilt: zField(z.enum(NET_TILTS)),
  nextEarningsDate: zField(z.string().nullable()),

  catalysts: zField(z.array(CatalystRowSchema)),
});

export type CatalystOutput = z.infer<typeof CatalystOutputSchema>;
