import { z } from 'zod';

import { zField, zProse } from '../_shared/types.ts';

export const FLAG_CODES = [
  'cash_conversion',
  'dilution',
  'stock_compensation',
  'leverage',
  'liquidity_runway',
  'margin_erosion',
  'revenue_quality',
  'negative_equity',
  'short_interest',
  'insider_selling',
  'data_gap',
  'other',
] as const;
export const SEVERITIES = ['high', 'medium', 'low'] as const;
export const OVERALL_RISK = ['clean', 'watch', 'concerning', 'serious'] as const;

/* ────────────────────────────────────────────────────────────────────────── */
/* Model-facing                                                               */
/* ────────────────────────────────────────────────────────────────────────── */

export const RedflagModelSchema = z
  .object({
    headline: zProse(200),
    plainEnglish: zProse(400),
    overall: z.enum(OVERALL_RISK),
    flags: z
      .array(
        z.object({
          code: z.enum(FLAG_CODES),
          title: zProse(120),
          severity: z.enum(SEVERITIES),
          evidence: zProse(400),
          whatWouldClearIt: zProse(300),
        }),
      )
      .max(8),
  })
  .superRefine((out, issues) => {
    // The overall rating must agree with the flags it summarizes, using the
    // same definitions as the prompt.
    const hasHigh = out.flags.some((f) => f.severity === 'high');
    const hasMedium = out.flags.some((f) => f.severity === 'medium');
    const add = (message: string) => issues.addIssue({ code: 'custom', path: ['overall'], message });

    if (out.flags.length === 0 && out.overall !== 'clean') {
      add('overall must be "clean" when no flags are listed');
    }
    if (hasHigh && (out.overall === 'clean' || out.overall === 'watch')) {
      add('a high-severity flag requires overall "concerning" or "serious"');
    }
    if (hasMedium && out.overall === 'clean') {
      add('a medium-severity flag rules out overall "clean"');
    }
    if (!hasHigh && out.overall === 'serious') {
      add('overall "serious" requires at least one high-severity flag');
    }
  });

export type RedflagModel = z.infer<typeof RedflagModelSchema>;

/* ────────────────────────────────────────────────────────────────────────── */
/* Public output                                                              */
/* ────────────────────────────────────────────────────────────────────────── */

export const RedflagMetricsSchema = z.object({
  annualPeriods: z.number().int().nonnegative(),
  /** Years between the latest period and the comparison period (up to 3). */
  spanYears: z.number().int().nonnegative(),
  fcfToNetIncome: z.number().nullable(),
  sbcPctOfFcf: z.number().nullable(),
  shareCountChangePct: z.number().nullable(),
  netDebt: z.number().nullable(),
  netDebtToOperatingCashFlow: z.number().nullable(),
  cashRunwayYears: z.number().nullable(),
  operatingMarginPct: z.number().nullable(),
  operatingMarginChangePp: z.number().nullable(),
  revenueCagrPct: z.number().nullable(),
  negativeEquity: z.boolean().nullable(),
});
export type RedflagMetrics = z.infer<typeof RedflagMetricsSchema>;

const FlagRowSchema = z.object({
  code: z.enum(FLAG_CODES),
  title: z.string(),
  severity: z.enum(SEVERITIES),
  evidence: z.string(),
  whatWouldClearIt: z.string(),
});

export const RedflagOutputSchema = z.object({
  headline: zField(z.string()),
  overall: zField(z.enum(OVERALL_RISK)),

  plainEnglish: zField(z.string()),
  topFlag: zField(
    z.object({ code: z.enum(FLAG_CODES), title: z.string(), severity: z.enum(SEVERITIES) }).nullable(),
  ),
  flagCount: zField(
    z.object({
      high: z.number().int().nonnegative(),
      medium: z.number().int().nonnegative(),
      low: z.number().int().nonnegative(),
    }),
  ),

  flags: zField(z.array(FlagRowSchema)),
  metrics: zField(RedflagMetricsSchema),
});

export type RedflagOutput = z.infer<typeof RedflagOutputSchema>;
