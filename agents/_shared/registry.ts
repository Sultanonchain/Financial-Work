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

import * as catalyst from '../catalyst/run.ts';
import * as dcf from '../dcf/run.ts';
import * as news from '../news/run.ts';
import * as redflag from '../redflag/run.ts';
import * as verdict from '../verdict/run.ts';

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
    model: dcf.model,
    maxTokens: dcf.maxTokens,
    run: dcf.run,
  },
  catalyst: {
    slug: 'catalyst',
    title: 'Catalysts',
    stage: 1,
    ttlSeconds: TTL_SECONDS.catalyst,
    model: catalyst.model,
    maxTokens: catalyst.maxTokens,
    run: catalyst.run,
  },
  news: {
    slug: 'news',
    title: 'News read',
    stage: 1,
    ttlSeconds: TTL_SECONDS.news,
    model: news.model,
    maxTokens: news.maxTokens,
    run: news.run,
  },
  redflag: {
    slug: 'redflag',
    title: 'Red flags',
    stage: 1,
    ttlSeconds: TTL_SECONDS.redflag,
    model: redflag.model,
    maxTokens: redflag.maxTokens,
    run: redflag.run,
  },
  verdict: {
    slug: 'verdict',
    title: 'Verdict',
    stage: 2,
    ttlSeconds: TTL_SECONDS.verdict,
    model: verdict.model,
    maxTokens: verdict.maxTokens,
    run: verdict.run,
  },
};

export const AGENT_SLUGS = Object.keys(registry) as AgentSlug[];

export const STAGE_ONE: AgentSlug[] = AGENT_SLUGS.filter((s) => registry[s].stage === 1);
export const STAGE_TWO: AgentSlug[] = AGENT_SLUGS.filter((s) => registry[s].stage === 2);

export function getAgent<K extends AgentSlug>(slug: K): AgentRegistry[K] {
  return registry[slug];
}
