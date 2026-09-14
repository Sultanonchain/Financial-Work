/**
 * Regenerate the agent fixtures.
 *
 *   npm run fixtures:replay
 *     Keep each fixture's input and modelOutput; re-derive `expected` by running
 *     the agent on them. Use after an intentional change to an agent's output
 *     wrapping, then review the diff.
 *
 *   ANTHROPIC_API_KEY=... npm run fixtures:record
 *     Call Claude for every agent and overwrite modelOutput and expected with
 *     the live reply. Costs five real requests.
 *
 * Stage-1 fixtures run first. The verdict fixture's input.upstream is rebuilt
 * from their fresh results, so the five files always describe one run.
 */

import { writeFileSync } from 'node:fs';

import { getClientConfig, setModelTransport, setReplyRecorder, setTokenLogger } from '../_shared/client.ts';
import { registry, STAGE_ONE, STAGE_TWO } from '../_shared/registry.ts';
import type { AgentModule, AgentSlug, UpstreamResults } from '../_shared/types.ts';

import { fixturePath, loadFixture, stableResult, type Fixture } from './helpers.ts';

const live = process.argv.includes('--live');

if (live && !getClientConfig().apiKey) {
  console.error('fixtures:record needs ANTHROPIC_API_KEY in the environment.');
  process.exit(1);
}
if (!live) {
  setTokenLogger(() => {});
}

const replies = new Map<AgentSlug, unknown>();
setReplyRecorder((slug, _ticker, reply) => replies.set(slug, reply));

const now = new Date().toISOString();
const upstream: UpstreamResults = {};

for (const slug of [...STAGE_ONE, ...STAGE_TWO]) {
  const agent: AgentModule<unknown> = registry[slug];
  const fixture = loadFixture(slug);
  const recordedAt = live ? now : fixture.recordedAt;
  const input = agent.stage === 2 ? { ...fixture.input, upstream: { ...upstream } } : fixture.input;

  setModelTransport(live ? null : () => structuredClone(fixture.modelOutput));
  replies.delete(slug);

  const result = stableResult(await agent.run(input), recordedAt);

  if (result.status !== 'ok') {
    console.error(
      `${slug}: ${result.error?.kind} ${result.error?.message}\n${result.error?.detail ?? ''}`,
    );
    process.exitCode = 1;
  }
  if (agent.stage === 1) {
    upstream[slug] = result;
  }

  const next: Fixture = {
    ...fixture,
    recordedAt,
    recordedWith: live ? (result.meta.model ?? 'live') : fixture.recordedWith,
    input,
    modelOutput: live ? (replies.get(slug) ?? null) : fixture.modelOutput,
    expected: { status: result.status, data: result.data },
  };
  writeFileSync(fixturePath(slug), `${JSON.stringify(next, null, 2)}\n`);
  console.log(`${slug}: ${result.status} (attempts ${result.meta.attempts})`);
}
