import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import type { AgentContext, AgentResult, AgentSlug, AgentStatus } from '../_shared/types.ts';

export const AGENTS_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const FIXTURE_FILE = 'aapl.json';

/**
 * One recorded pair per agent: the exact context the agent received, the model
 * reply it got back, and the result it produced. Replaying `modelOutput`
 * through run() must reproduce `expected`.
 */
export interface Fixture {
  name: string;
  description: string;
  recordedAt: string;
  /** Model id for a live recording; a description otherwise. */
  recordedWith: string;
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
