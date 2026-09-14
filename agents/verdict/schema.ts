import { z } from 'zod';

import { MARKET_REGIMES, zField, zProse } from '../_shared/types.ts';

/**
 * Most undervalued → most overvalued. Order is load-bearing: the guardrails
 * in run.ts reason in indexes on this array. There are no action tokens
 * (Buy, Hold, Avoid...) anywhere in this vocabulary, by design.
 */
export const VALUATION_BANDS = [
  'Undervalued',
  'Modestly Undervalued',
  'Fairly Valued',
  'Slightly Overvalued',
  'Overvalued',
] as const;
export type ValuationBand = (typeof VALUATION_BANDS)[number];

export const NEUTRAL_BAND_INDEX = 2;

/** Neutral colour bucket the frontend themes off. */
export const VERDICT_TIERS = ['positive', 'info', 'warning', 'negative'] as const;
export type VerdictTier = (typeof VERDICT_TIERS)[number];

export const LYNCH_CATEGORIES = [
  'slowGrower',
  'stalwart',
  'fastGrower',
  'cyclical',
  'turnaround',
  'assetPlay',
] as const;

export const TAPE_VS_FUNDAMENTALS = ['agree', 'disagree', 'unclear'] as const;
export const CONFIDENCE = ['low', 'medium', 'high'] as const;

export const STAGE_ONE_SLUGS = ['dcf', 'catalyst', 'news', 'redflag'] as const;

/* ────────────────────────────────────────────────────────────────────────── */
/* Model-facing                                                               */
/* ────────────────────────────────────────────────────────────────────────── */

/** run.ts adds the per-ticker rule: band must be in the allowed set. */
export const VerdictModelSchema = z.object({
  band: z.enum(VALUATION_BANDS),
  category: z.enum(LYNCH_CATEGORIES),
  headline: zProse(200),
  thesis: zProse(900),
  plainEnglish: zProse(350),
  bandRationale: zProse(500),
  bullPoints: z.array(zProse(160)).min(1).max(3),
  bearPoints: z.array(zProse(160)).min(1).max(3),
  tapeVsFundamentals: z.enum(TAPE_VS_FUNDAMENTALS),
  confidence: z.enum(CONFIDENCE),
});

export type VerdictModel = z.infer<typeof VerdictModelSchema>;

/* ────────────────────────────────────────────────────────────────────────── */
/* Public output                                                              */
/* ────────────────────────────────────────────────────────────────────────── */

export const VerdictOutputSchema = z.object({
  band: zField(z.enum(VALUATION_BANDS)),
  tier: zField(z.enum(VERDICT_TIERS)),
  headline: zField(z.string()),

  plainEnglish: zField(z.string()),
  category: zField(z.enum(LYNCH_CATEGORIES)),
  confidence: zField(z.enum(CONFIDENCE)),
  bullPoints: zField(z.array(z.string())),
  bearPoints: zField(z.array(z.string())),
  disclaimer: zField(z.string()),

  thesis: zField(z.string()),
  bandRationale: zField(z.string()),
  tapeVsFundamentals: zField(z.enum(TAPE_VS_FUNDAMENTALS)),
  engineBand: zField(z.enum(VALUATION_BANDS).nullable()),
  regime: zField(z.enum(MARKET_REGIMES).nullable()),
  guardrails: zField(
    z.object({
      regimeCap: z.boolean(),
      backstopFloor: z.boolean(),
      allowedBands: z.array(z.enum(VALUATION_BANDS)),
    }),
  ),
  inputs: zField(
    z.object({
      available: z.array(z.enum(STAGE_ONE_SLUGS)),
      unavailable: z.array(z.enum(STAGE_ONE_SLUGS)),
    }),
  ),
});

export type VerdictOutput = z.infer<typeof VerdictOutputSchema>;
