/**
 * Which method actually produced the engine's intrinsic value.
 *
 * app.py labels the path in `iv_source_label` (preferring `sector_val_label`
 * when a sector method ran). Only some of those paths are a discounted cash
 * flow of the inputs dcf reviews; the rest are multiples, an analyst target, a
 * book-value or banking blend, or a distress proxy. On 2026-09-14 VKTX's value
 * was "Analyst Target (pre-revenue biotech)" and JPM's was a banking blend of
 * P/B and P/E, while both still carried DCF-shaped inputs.
 */

import type { ValuationSnapshot } from './types.ts';

/** Methods that are not a discounted cash flow, however the label spells them. */
const NON_DCF_METHOD =
  /analyst target|multiple|p\s*\/\s*[be]\b|price[- ]to[- ](?:book|earnings|sales|revenue)|book value|ev\s*\/\s*(?:revenue|sales|ebitda)|p\s*\/\s*revenue|cash[- ]only|distress|liquidation|comparable|banking|net asset value|sultan split/i;

/** "Banking-DCF (616.01) 70% + P/B 30%": a blend of methods, not one discounted cash flow. */
const BLEND = /\+[^%]*\d\s*%|\d\s*%[^+]*\+|\bblend/i;

export interface ValuationPath {
  /** The engine's own label, verbatim. */
  label: string | null;
  /** True when the displayed value is a discounted cash flow of the inputs dcf is shown. */
  isDcf: boolean;
}

export function valuationPathOf(valuation: ValuationSnapshot | null): ValuationPath {
  const label = valuation?.ivSourceLabel?.trim() || null;
  // No label: a context assembled before the field existed. The engine's own
  // default path is a DCF, so review it rather than refusing every old context.
  if (!label) return { label: null, isDcf: true };
  return { label, isDcf: /\bdcf\b/i.test(label) && !NON_DCF_METHOD.test(label) && !BLEND.test(label) };
}

/** The label, trimmed for a one-line message. */
export function shortMethod(label: string, maxChars = 160): string {
  return label.length > maxChars ? `${label.slice(0, maxChars - 3)}...` : label;
}
