/**
 * Regenerate the agent fixtures.
 *
 *   npm run fixtures:replay [-- <slug>...]
 *     Re-derive `expected` for each fixture by running the agent on its stored
 *     reply. Input, reply and provenance are kept. Use after an intentional
 *     change to an agent's output wrapping, then review the diff.
 *
 *   npm run fixtures:record [-- <slug>...]
 *     Call Claude for each selected agent and overwrite its input, reply,
 *     expected result and provenance (source "recorded", recordedAt,
 *     recordedModel).
 *
 * Nothing is written for an agent that fails, in either mode: a fixture is
 * never replaced by a worse result, and never claims a recording that did not
 * happen.
 *
 * Agents that are not selected still run offline from their own fixtures,
 * without writing, so a selected verdict sees current stage-1 results as
 * upstream. On replay, a recorded verdict keeps its stored input, because that
 * input is what the model was actually sent.
 *
 * Date-dependent validation reads the real clock for a live call, and the
 * fixture's recordedAt (or input.asOf, if never recorded) otherwise, so a
 * replay gives the same answer on any day.
 */

import { writeFileSync } from 'node:fs';

import { getClientConfig, setModelTransport, setReplyRecorder, setTokenLogger } from '../_shared/client.ts';
import { setClock } from '../_shared/format.ts';
import { AGENT_SLUGS, registry, STAGE_ONE, STAGE_TWO } from '../_shared/registry.ts';
import type { AgentModule, AgentResult, AgentSlug, UpstreamResults } from '../_shared/types.ts';

import { fixturePath, loadFixture, stableResult, type Fixture } from './helpers.ts';

const args = process.argv.slice(2);
const live = args.includes('--live');
const only = args.filter((arg) => !arg.startsWith('--'));
const unknown = only.filter((slug) => !(AGENT_SLUGS as string[]).includes(slug));

if (unknown.length) {
  console.error(`Unknown agent: ${unknown.join(', ')}. Registered: ${AGENT_SLUGS.join(', ')}.`);
  process.exit(1);
}
if (live && !getClientConfig().apiKey) {
  console.error('fixtures:record needs ANTHROPIC_API_KEY in the environment.');
  process.exit(1);
}

const replies = new Map<AgentSlug, unknown>();
setReplyRecorder((slug, _ticker, reply) => replies.set(slug, reply));

const now = new Date().toISOString();
const upstream: UpstreamResults = {};

function fail(slug: AgentSlug, reason: string): void {
  console.error(`${slug}: ${reason}\n${slug}: fixture left unchanged`);
  process.exitCode = 1;
}

for (const slug of [...STAGE_ONE, ...STAGE_TWO]) {
  const agent: AgentModule<unknown> = registry[slug];
  const selected = only.length === 0 || only.includes(slug);
  if (!selected && agent.stage === 2) continue;

  const fixture = loadFixture(slug);
  const callLive = live && selected;
  const reference = fixture.recordedAt ?? fixture.input.asOf;
  const rebuildUpstream = agent.stage === 2 && (callLive || fixture.source !== 'recorded');
  const input = rebuildUpstream ? { ...fixture.input, upstream: { ...upstream } } : fixture.input;

  setModelTransport(callLive ? null : () => structuredClone(fixture.modelOutput));
  setClock(callLive ? null : () => Date.parse(reference));
  setTokenLogger(callLive ? null : () => {});
  replies.delete(slug);

  let result: AgentResult<unknown>;
  try {
    result = stableResult(await agent.run(input), callLive ? now : reference);
  } catch (err) {
    const reason = `threw: ${err instanceof Error ? err.message : String(err)}`;
    if (selected) fail(slug, reason);
    else console.error(`${slug}: could not run offline for upstream (${reason})`);
    continue;
  }

  if (agent.stage === 1) upstream[slug] = result;
  if (!selected) continue;

  if (result.status !== 'ok') {
    const detail = result.error?.detail ? `\n${result.error.detail}` : '';
    fail(slug, `${result.error?.kind}: ${result.error?.message}${detail}`);
    continue;
  }

  const expected = { status: result.status, data: result.data };
  let next: Fixture;
  if (!callLive) {
    next = { ...fixture, input, expected };
  } else if (replies.has(slug)) {
    next = {
      ...fixture,
      source: 'recorded',
      recordedAt: now,
      recordedModel: result.meta.model,
      input,
      modelOutput: replies.get(slug),
      expected,
    };
  } else {
    fail(slug, 'answered without calling the model, so there is nothing to record');
    continue;
  }

  writeFileSync(fixturePath(slug), `${JSON.stringify(next, null, 2)}\n`);
  console.log(
    `${slug}: ok (attempts ${result.meta.attempts})` +
      (callLive ? `, recorded on ${result.meta.model}` : ', replayed'),
  );
}
