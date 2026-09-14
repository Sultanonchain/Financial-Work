/**
 * The single place agents register.
 *
 * Adding an agent means: create its folder (prompt.ts, schema.ts, run.ts,
 * fixtures/), add a TTL in cache.ts, and add one entry here. Nothing else in
 * the layer imports an agent's run.ts — runner.ts reaches agents only through
 * this map, which is what keeps "agents never call each other" enforceable.
 */

import { TTL_SECONDS } from './cache.ts';
import type { AgentModule, AgentSlug } from './types.ts';

import { run as runCatalyst } from '../catalyst/run.ts';
import { run as runDcf } from '../dcf/run.ts';
import { run as runNews } from '../news/run.ts';
import { run as runRedflag } from '../redflag/run.ts';
import { run as runVerdict } from '../verdict/run.ts';

import type { CatalystOutput } from '../catalyst/schema.ts';
import type { DcfOutput } from '../dcf/schema.ts';
import type { NewsOutput } from '../news/schema.ts';
import type { RedflagOutput } from '../redflag/schema.ts';
import type { VerdictOutput } from '../verdict/schema.ts';

export interface AgentOutputs {
  dcf: DcfOutput;
  catalyst: CatalystOutput;
  news: NewsOutput;
  redflag: RedflagOutput;
  verdict: VerdictOutput;
}

export type AgentRegistry = { [K in AgentSlug]: AgentModule<AgentOutputs[K]> };

export const registry: AgentRegistry = {
  dcf: {
    slug: 'dcf',
    title: 'Valuation assumptions',
    stage: 1,
    ttlSeconds: TTL_SECONDS.dcf,
    run: runDcf,
  },
  catalyst: {
    slug: 'catalyst',
    title: 'Catalysts',
    stage: 1,
    ttlSeconds: TTL_SECONDS.catalyst,
    run: runCatalyst,
  },
  news: {
    slug: 'news',
    title: 'News read',
    stage: 1,
    ttlSeconds: TTL_SECONDS.news,
    run: runNews,
  },
  redflag: {
    slug: 'redflag',
    title: 'Red flags',
    stage: 1,
    ttlSeconds: TTL_SECONDS.redflag,
    run: runRedflag,
  },
  verdict: {
    slug: 'verdict',
    title: 'Verdict',
    stage: 2,
    ttlSeconds: TTL_SECONDS.verdict,
    run: runVerdict,
  },
};

export const AGENT_SLUGS = Object.keys(registry) as AgentSlug[];

export const STAGE_ONE: AgentSlug[] = AGENT_SLUGS.filter((s) => registry[s].stage === 1);
export const STAGE_TWO: AgentSlug[] = AGENT_SLUGS.filter((s) => registry[s].stage === 2);

export function getAgent<K extends AgentSlug>(slug: K): AgentRegistry[K] {
  return registry[slug];
}
