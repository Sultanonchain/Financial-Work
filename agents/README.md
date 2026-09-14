# VALUS agent layer

Five single-purpose Claude agents behind one orchestrator. TypeScript, run
directly by Node's type stripping (Node ≥ 22.18, no build step).

```
agents/
  _shared/
    types.ts      AgentContext, AgentResult<T>, Field<T>, VisibilityTier, result constructors, advice filter
    client.ts     Anthropic wrapper: timeout, retries, structured output + one repair turn, token log, cooldown
    cache.ts      cache keyed by (agentSlug, ticker, UTC date), pluggable store, TTLs
    registry.ts   slug → agent module, the only place agents register
    runner.ts     stage 1 in parallel → stage 2 verdict; tier redaction for the client
    format.ts     deterministic prompt-rendering helpers shared by the agents
  dcf/ catalyst/ news/ redflag/ verdict/
    prompt.ts  schema.ts  run.ts  fixtures/aapl.json
  tests/
```

## Using it

```ts
import { runAgents, toClientPayload } from './agents/_shared/runner.ts';

const report = await runAgents(ctx, { onResult: (result) => pushToClient(result) });
return toClientPayload(report); // applies ctx.tier and strips internal error detail
```

The caller assembles `AgentContext`: ticker, tier, company profile, financial
statements, price history (with the computed tape regime), yfinance and Finviz
payloads, news items, and the Python engine's valuation snapshot. Agents never
fetch anything.

## The contract, and what enforces each rule

| Rule | Enforced by |
|---|---|
| Agents never call each other | `registry.ts` is the only importer of any `run.ts`. A test scans imports; a sibling-agent import must be `import type`. |
| Agents never fetch data | Everything arrives on `AgentContext`. A test scans agent files for fetch/http. |
| Output is validated before returning | The model reply is checked against `XModelSchema`. On failure, one repair turn carries the zod errors back; a second failure returns `status: "unavailable"`. The wrapped result is checked against `XOutputSchema` in `finalizeOutput`. |
| Never throw into the UI | `client.ts` never throws. The runner's guard turns throws and timeouts into `unavailable` results. |
| One failure does not block the rest | Stage-1 agents are isolated, with a per-agent deadline (60s default) that aborts the request. The verdict runs on whatever arrived and lists missing inputs. |

Each agent has two schemas. `XModelSchema` is what Claude returns: plain
values, also sent as the structured-output format. `XOutputSchema` is the
public shape, where every top-level field is a `Field<T>`. Visibility and
source are decided in code, not by the model. `source` is `model` for
Claude-written values, `computed` for engine and derived numbers, `api` for
vendor values.

## Model calls

- Default model `claude-opus-5` (override with `VALUS_AGENT_MODEL`), adaptive
  thinking. Effort is `low` for catalyst, news and redflag, `medium` for dcf
  and verdict.
- The system prompt is byte-stable and marked cacheable. Per-ticker context
  goes in the user turn. The token log shows `cache_miss` when that breaks.
- Structured output uses `zodOutputFormat` sent through `messages.create()`,
  not `parse()`. `parse()` throws on a zod failure, which would skip the
  repair turn. The SDK moves enums and length limits into schema descriptions,
  so the client-side zod check is what actually enforces them.
- The server-side refusal fallback is on (`fallbacks: "default"`, beta
  `server-side-fallback-2026-07-01`). It only exists on the first-party API:
  set `configureClient({ refusalFallbacks: false })` on Bedrock, Vertex or
  Foundry.
- The SDK retries 429/5xx twice, with a 45s timeout per request. If a 429 or
  529 survives the retries, calls are skipped for 5 minutes and agents return
  `unavailable` with kind `cooldown`.

## Guardrails carried over from app.py

- No action tokens. Verdict bands run Undervalued → Overvalued, and
  `ADVICE_PATTERN` sends any model prose that reads as trading advice back for
  repair.
- Verdict: `momentum_runup` or `squeeze_risk` caps it at Fairly Valued. A
  sovereign backstop floors it at Fairly Valued. It always stays within two
  steps of the engine's margin-of-safety band, using the same cut-points as
  the Python code. The allowed bands are stated in the prompt and enforced by
  validation.
- No em or en dashes in rendered prose (`tidy`).

## Cache

Key: `valus:agent:v1:<slug>:<TICKER>:<YYYY-MM-DD UTC>`.

- TTLs: dcf, catalyst and redflag 24h. News 4h. Verdict 4h, because it reads news.
- A TTL is also capped at the next UTC midnight.
- Only `ok` results are written. A verdict built while a stage-1 agent was
  unavailable is not written.
- Tier is not part of the key. Redaction happens in `toClientPayload`, so an
  upgrade never forces a re-run.
- Bump `CACHE_VERSION` when a prompt or schema change makes old entries wrong.

The default `MemoryStore` is per process, which means per instance on
serverless. For production, plug in a shared store:

```ts
setCacheStore({
  get: (key) => redis.get(key),
  set: async (key, value, ttl) => { await redis.set(key, value, { EX: ttl }); },
  delete: async (key) => { await redis.del(key); },
});
```

## Tests and fixtures

```
npm test              36 tests, offline (model replies come through the transport seam)
npm run typecheck
```

Each `fixtures/aapl.json` holds the agent's input, the model reply, and the
expected result. Replaying the reply through `run()` must reproduce the
expected result.

**The current model replies are hand-authored, not live recordings.** No API
key was available when they were created, and the AAPL figures are
approximate. To replace them with real recordings (five requests):

```
ANTHROPIC_API_KEY=... npm run fixtures:record
```

After an intentional change to how an agent wraps its output, run
`npm run fixtures:replay` and review the diff.

## iCloud note

`~/Desktop` is synced to iCloud Drive, which evicts files inside `node_modules`
(TypeScript lost its lib files). Dependencies live in `node_modules.nosync`,
which iCloud skips, behind a `node_modules` symlink. `npm install` replaces the
symlink with a real folder, so run `npm run icloud:nosync` after installing.
If tsc then reports missing global types, the install itself was damaged by
sync: install in a folder outside Desktop and move `node_modules` in as
`node_modules.nosync`.
