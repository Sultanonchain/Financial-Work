/**
 * npm run agents:status
 *
 * One row per registered agent: its resolved model, whether its required files
 * exist, and where its fixtures came from. ANTHROPIC_API_KEY is reported as
 * set or not set; its value is never printed.
 *
 * Exits 1 when any agent is not ready, so it can gate CI. If registry.ts itself
 * fails to load (usually a missing or broken run.ts), agents are found by
 * scanning folders instead, so the command still shows what is wrong.
 */

import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { DEFAULT_MODEL, MODEL_OVERRIDE_ENV, resolveModel, type ModelSource } from '../_shared/client.ts';
import type { AgentRegistry } from '../_shared/registry.ts';
import type { AgentSlug } from '../_shared/types.ts';

import { AGENTS_ROOT, provenanceIssues } from '../tests/helpers.ts';

const REQUIRED_FILES = ['prompt.ts', 'schema.ts', 'run.ts'] as const;
const NON_AGENT_DIRS = new Set(['_shared', 'tests', 'scripts', 'node_modules', 'node_modules.nosync']);

/** `mixed` when an agent has both recorded and hand-written fixtures; `none` when it has none. */
export type Provenance = 'recorded' | 'hand-written' | 'mixed' | 'none';

export interface AgentReadiness {
  slug: string;
  /** After VALUS_AGENT_MODEL > agent.model > default. Null when the agent's model could not be read. */
  model: string | null;
  modelSource: ModelSource | null;
  /** Any of prompt.ts, schema.ts, run.ts, fixtures/ that is absent (fixtures/ counts as absent when empty). */
  missing: string[];
  fixtureCount: number;
  provenance: Provenance;
  /** Newest recordedAt across this agent's recorded fixtures. */
  lastRecordedAt: string | null;
  problems: string[];
  ready: boolean;
}

export interface StatusReport {
  apiKeySet: boolean;
  modelOverride: string | null;
  defaultModel: string;
  /** Set when registry.ts failed to load; agents were then found by scanning folders. */
  registryError: string | null;
  agents: AgentReadiness[];
}

export async function collectStatus(env: NodeJS.ProcessEnv = process.env): Promise<StatusReport> {
  let registry: AgentRegistry | null = null;
  let registryError: string | null = null;
  try {
    ({ registry } = (await import('../_shared/registry.ts')) as { registry: AgentRegistry });
  } catch (err) {
    registryError = errorMessage(err);
  }

  const agents: AgentReadiness[] = [];
  for (const slug of registry ? Object.keys(registry) : scanAgentFolders()) {
    const loaded = registry
      ? { model: registry[slug as AgentSlug].model, error: null }
      : await modelFromRunFile(slug);
    agents.push(inspectAgent(slug, loaded.model, loaded.error, env));
  }

  return {
    apiKeySet: Boolean(env['ANTHROPIC_API_KEY']?.trim()),
    modelOverride: env[MODEL_OVERRIDE_ENV]?.trim() || null,
    defaultModel: DEFAULT_MODEL,
    registryError,
    agents,
  };
}

function inspectAgent(
  slug: string,
  agentModel: string | null,
  loadError: string | null,
  env: NodeJS.ProcessEnv,
): AgentReadiness {
  const dir = path.join(AGENTS_ROOT, slug);
  const problems: string[] = [];

  const missing: string[] = REQUIRED_FILES.filter((file) => !existsSync(path.join(dir, file)));
  const fixtureDir = path.join(dir, 'fixtures');
  const fixtureFiles = isDirectory(fixtureDir)
    ? readdirSync(fixtureDir).filter((name) => name.endsWith('.json')).sort()
    : [];
  if (fixtureFiles.length === 0) missing.push('fixtures/');

  if (loadError) {
    problems.push(`run.ts failed to load: ${loadError}`);
  } else if (!agentModel && !missing.includes('run.ts')) {
    problems.push('run.ts does not export a model');
  }

  let recorded = 0;
  let handWritten = 0;
  let lastRecordedAt: string | null = null;

  for (const file of fixtureFiles) {
    let fixture: Record<string, unknown>;
    try {
      fixture = JSON.parse(readFileSync(path.join(fixtureDir, file), 'utf8')) as Record<string, unknown>;
    } catch {
      problems.push(`fixtures/${file} is not valid JSON`);
      handWritten++;
      continue;
    }

    const issues = provenanceIssues(fixture);
    problems.push(...issues.map((issue) => `fixtures/${file}: ${issue}`));

    // Recorded only when the file says so AND its provenance is consistent;
    // anything else is treated as hand-written.
    if (issues.length === 0 && fixture['source'] === 'recorded') {
      recorded++;
      const at = fixture['recordedAt'] as string;
      if (lastRecordedAt === null || Date.parse(at) > Date.parse(lastRecordedAt)) lastRecordedAt = at;
    } else {
      handWritten++;
    }
  }

  const provenance: Provenance =
    fixtureFiles.length === 0 ? 'none' : recorded === 0 ? 'hand-written' : handWritten === 0 ? 'recorded' : 'mixed';
  const resolved = agentModel ? resolveModel(agentModel, env) : null;

  return {
    slug,
    model: resolved?.id ?? null,
    modelSource: resolved?.source ?? null,
    missing,
    fixtureCount: fixtureFiles.length,
    provenance,
    lastRecordedAt,
    problems,
    ready: missing.length === 0 && resolved !== null && problems.length === 0,
  };
}

async function modelFromRunFile(slug: string): Promise<{ model: string | null; error: string | null }> {
  const file = path.join(AGENTS_ROOT, slug, 'run.ts');
  if (!existsSync(file)) return { model: null, error: null };
  try {
    const mod = (await import(pathToFileURL(file).href)) as { model?: unknown };
    return { model: typeof mod.model === 'string' ? mod.model : null, error: null };
  } catch (err) {
    return { model: null, error: errorMessage(err) };
  }
}

function scanAgentFolders(): string[] {
  return readdirSync(AGENTS_ROOT, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith('.') && !NON_AGENT_DIRS.has(entry.name))
    .filter((entry) =>
      [...REQUIRED_FILES, 'fixtures'].some((name) => existsSync(path.join(AGENTS_ROOT, entry.name, name))),
    )
    .map((entry) => entry.name)
    .sort();
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Rendering                                                                  */
/* ────────────────────────────────────────────────────────────────────────── */

export function renderStatus(report: StatusReport): string {
  const label = (text: string) => text.padEnd(MODEL_OVERRIDE_ENV.length);
  const lines = [
    'VALUS agent status',
    '',
    `  ${label('ANTHROPIC_API_KEY')}  ${report.apiKeySet ? 'set' : 'not set (nothing can be recorded without it)'}`,
    `  ${label(MODEL_OVERRIDE_ENV)}  ${
      report.modelOverride ? `${report.modelOverride} (overrides every agent)` : 'not set (each agent uses its own model)'
    }`,
    `  ${label('default model')}  ${report.defaultModel}`,
  ];

  const header = ['slug', 'ready', 'model', 'via', 'files', 'fixtures', 'provenance', 'last recorded'];
  const rows = report.agents.map((a) => [
    a.slug,
    a.ready ? 'yes' : 'no',
    a.model ?? '-',
    a.modelSource ?? '-',
    a.missing.length ? `missing ${a.missing.join(', ')}` : 'ok',
    String(a.fixtureCount),
    a.provenance,
    a.lastRecordedAt ? a.lastRecordedAt.slice(0, 10) : '-',
  ]);
  const widths = header.map((title, i) => Math.max(title.length, ...rows.map((row) => (row[i] ?? '').length)));
  const format = (cells: string[]) =>
    `  ${cells.map((cell, i) => cell.padEnd(widths[i] ?? 0)).join('  ')}`.trimEnd();

  lines.push('', format(header), format(widths.map((w) => '-'.repeat(w))), ...rows.map(format));

  if (report.registryError) {
    lines.push('', '  registry.ts failed to load, so agents were found by scanning folders:', `    ${report.registryError}`);
  }

  const problems = report.agents.flatMap((a) => a.problems.map((p) => `    ${a.slug}: ${p}`));
  if (problems.length) {
    lines.push('', '  Problems:', ...problems);
  }

  const total = report.agents.length;
  const ready = report.agents.filter((a) => a.ready).length;
  const recorded = report.agents.filter((a) => a.provenance === 'recorded').length;
  lines.push('', `  ${ready} of ${total} agents ready. ${recorded} of ${total} have recorded fixtures.`);
  if (recorded < total) {
    lines.push(
      report.apiKeySet
        ? '  Record the rest with: npm run fixtures:record'
        : '  To record them, set ANTHROPIC_API_KEY and run: npm run fixtures:record',
    );
  }
  return lines.join('\n');
}

function isDirectory(target: string): boolean {
  try {
    return statSync(target).isDirectory();
  } catch {
    return false;
  }
}

function errorMessage(err: unknown): string {
  const message = err instanceof Error ? err.message : String(err);
  return message.split('\n')[0] ?? message;
}

const invokedDirectly =
  process.argv[1] !== undefined && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (invokedDirectly) {
  const report = await collectStatus();
  console.log(renderStatus(report));
  if (report.registryError || report.agents.some((agent) => !agent.ready)) {
    process.exitCode = 1;
  }
}
