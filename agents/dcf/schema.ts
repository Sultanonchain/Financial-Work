import { z } from 'zod';

import { zField, zProse } from '../_shared/types.ts';

export const DCF_ASSUMPTION_KEYS = [
  'stage1_growth',
  'terminal_growth',
  'wacc',
  'fcf_base',
  'share_count',
  'net_debt',
] as const;
export type DcfAssumptionKey = (typeof DCF_ASSUMPTION_KEYS)[number];

const REQUIRED_ASSUMPTIONS: readonly DcfAssumptionKey[] = ['stage1_growth', 'terminal_growth', 'wacc'];

export const ASSESSMENTS = ['supported', 'aggressive', 'conservative', 'unclear'] as const;
export type Assessment = (typeof ASSESSMENTS)[number];

/**
 * The model's call on each engine input: is the engine's figure higher or lower
 * than the evidence supports? run.ts turns it into the public assessment, so the
 * model never works out the sign (a higher discount rate is conservative). It
 * comes after reasoning in the schema, so the call is made after the reasoning.
 */
export const ENGINE_COMPARISONS = ['in_line', 'higher', 'lower', 'unclear'] as const;
export type EngineComparison = (typeof ENGINE_COMPARISONS)[number];
export const HISTORICAL_FIT = ['consistent', 'stretch', 'break', 'unclear'] as const;
export const CONFIDENCE = ['low', 'medium', 'high'] as const;
export const ASSUMPTION_UNITS = ['pct', 'money', 'shares'] as const;

/* ────────────────────────────────────────────────────────────────────────── */
/* Model-facing: what Claude returns. Plain values, no Field envelopes.       */
/* ────────────────────────────────────────────────────────────────────────── */

/** dcf prose may say an engine figure does not match the reported figures, never that it is fabricated. */
const prose = (maxChars: number) => zProse(maxChars, { neutral: true });

export const DcfModelSchema = z
  .object({
    headline: prose(200),
    plainEnglish: prose(400),
    historicalFit: z.enum(HISTORICAL_FIT),
    assumptions: z
      .array(
        z.object({
          key: z.enum(DCF_ASSUMPTION_KEYS),
          evidence: prose(300),
          reasoning: prose(500),
          engineVsEvidence: z.enum(ENGINE_COMPARISONS),
        }),
      )
      .min(3)
      .max(6),
    dominantSensitivity: z.object({
      key: z.enum(DCF_ASSUMPTION_KEYS),
      explanation: prose(400),
    }),
    confidence: z.enum(CONFIDENCE),
    confidenceReasons: z.array(prose(200)).max(4),
  })
  .superRefine((out, issues) => {
    const seen = new Set<DcfAssumptionKey>();
    out.assumptions.forEach((row, index) => {
      if (seen.has(row.key)) {
        issues.addIssue({
          code: 'custom',
          path: ['assumptions', index, 'key'],
          message: `"${row.key}" is assessed more than once; give each assumption one row`,
        });
      }
      seen.add(row.key);
    });
    for (const key of REQUIRED_ASSUMPTIONS) {
      if (!seen.has(key)) {
        issues.addIssue({
          code: 'custom',
          path: ['assumptions'],
          message: `missing a row for "${key}", which must always be assessed`,
        });
      }
    }
  });

export type DcfModel = z.infer<typeof DcfModelSchema>;

/* ────────────────────────────────────────────────────────────────────────── */
/* Public output: every field wrapped in Field<T>.                            */
/* ────────────────────────────────────────────────────────────────────────── */

export const DcfHistorySchema = z.object({
  annualPeriods: z.number().int(),
  revenueCagrPct: z.number().nullable(),
  fcfCagrPct: z.number().nullable(),
  avgFcfMarginPct: z.number().nullable(),
  latestSbcPctOfFcf: z.number().nullable(),
  /** Latest results on the key-figures basis: trailing twelve months, else the last fiscal year. */
  trailing: z
    .object({
      basis: z.enum(['ttm', 'annual']),
      periodEnd: z.string(),
      revenue: z.number().nullable(),
      freeCashFlow: z.number().nullable(),
      operatingMarginPct: z.number().nullable(),
      revenueVsLastFiscalYearPct: z.number().nullable(),
    })
    .nullable(),
});
export type DcfHistory = z.infer<typeof DcfHistorySchema>;

const AssumptionRowSchema = z.object({
  key: z.enum(DCF_ASSUMPTION_KEYS),
  label: z.string(),
  engineValue: z.number().nullable(),
  unit: z.enum(ASSUMPTION_UNITS),
  assessment: z.enum(ASSESSMENTS),
  reasoning: z.string(),
  evidence: z.string(),
});

export const DcfOutputSchema = z.object({
  headline: zField(z.string()),
  /** Set in code from the review, so the engine cannot claim confidence the review does not support. */
  valuationReliability: zField(z.object({ reliable: z.boolean(), reasons: z.array(z.string()) })),
  intrinsicValue: zField(z.number().nullable()),
  valueRange: zField(z.object({ low: z.number(), high: z.number() }).nullable()),

  price: zField(z.number().nullable()),
  marginOfSafetyPct: zField(z.number().nullable()),
  plainEnglish: zField(z.string()),
  historicalFit: zField(z.enum(HISTORICAL_FIT)),
  confidence: zField(z.enum(CONFIDENCE)),

  assumptions: zField(z.array(AssumptionRowSchema)),
  dominantSensitivity: zField(
    z.object({ key: z.enum(DCF_ASSUMPTION_KEYS), label: z.string(), explanation: z.string() }),
  ),
  confidenceReasons: zField(z.array(z.string())),
  history: zField(DcfHistorySchema),
});

export type DcfOutput = z.infer<typeof DcfOutputSchema>;
