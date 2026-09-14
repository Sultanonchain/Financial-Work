import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import type { AgentContext, AgentResult, AgentSlug, AgentStatus } from '../_shared/types.ts';

export const AGENTS_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const FIXTURE_FILE = 'aapl.json';

/**
 * Where a fixture's model reply came from.
 *   recorded   captured from a live API call by `npm run fixtures:record`
 *   synthetic  written by hand; never came from a model
 */
export const FIXTURE_SOURCES = ['recorded', 'synthetic'] as const;
export type FixtureSource = (typeof FIXTURE_SOURCES)[number];

/**
 * One recorded pair per agent: the exact context the agent received, the model
 * reply it got back, and the result it produced. Replaying `modelOutput`
 * through run() must reproduce `expected`.
 */
export interface Fixture {
  name: string;
  description: string;
  source: FixtureSource;
  /** When the live call was made. Null for a synthetic fixture. */
  recordedAt: string | null;
  /** The model that served the recorded reply. Null for a synthetic fixture. */
  recordedModel: string | null;
  input: AgentContext;
  /** Null when the agent answered from computed data without calling a model. */
  modelOutput: unknown;
  expected: { status: AgentStatus; data: unknown } | null;
}

export function fixturePath(slug: AgentSlug, file = FIXTURE_FILE): string {
  return path.join(AGENTS_ROOT, slug, 'fixtures', file);
}

export function loadFixture(slug: AgentSlug, file = FIXTURE_FILE): Fixture {
  return JSON.parse(readFileSync(fixturePath(slug, file), 'utf8')) as Fixture;
}

/**
 * Problems with a fixture's provenance fields; empty when they are consistent.
 * Provenance is only ever trusted when these rules hold, never inferred.
 */
export function provenanceIssues(fixture: Partial<Record<keyof Fixture, unknown>>): string[] {
  const { source, recordedAt, recordedModel, modelOutput } = fixture;

  if (source !== 'recorded' && source !== 'synthetic') {
    return [`source must be "recorded" or "synthetic" (found ${JSON.stringify(source) ?? 'nothing'})`];
  }

  const issues: string[] = [];
  if (source === 'recorded') {
    if (typeof recordedAt !== 'string' || Number.isNaN(Date.parse(recordedAt))) {
      issues.push('a recorded fixture needs recordedAt as an ISO timestamp');
    }
    if (typeof recordedModel !== 'string' || recordedModel.length === 0) {
      issues.push('a recorded fixture needs recordedModel');
    }
    if (modelOutput === null || modelOutput === undefined) {
      issues.push('a recorded fixture needs the recorded modelOutput');
    }
  } else {
    if (recordedAt !== null) issues.push('a synthetic fixture was never recorded, so recordedAt must be null');
    if (recordedModel !== null) issues.push('a synthetic fixture was never recorded, so recordedModel must be null');
  }
  return issues;
}

/** Plain JSON copy with clock- and run-dependent meta pinned. */
export function stableResult<T>(result: AgentResult<T>, at: string): AgentResult<T> {
  return toJson({
    ...result,
    meta: { ...result.meta, latencyMs: 0, generatedAt: at, usage: null, cached: false },
  });
}

export function toJson<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
