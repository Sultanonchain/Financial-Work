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

## Company section (no model)

`runAgents` also returns `report.company`, built in code from the context by
`_shared/company.ts`. No agent and no model call is involved, so it survives
every agent failing, and a caller can call `buildCompanySection(ctx)` directly
to render it before the agents finish. Every value is a `Field` with
`source: "api"` and `visibility: "summary"`.

- `stats`, in display order: `price`, `marketCap`, `forwardPE`. A missing or
  negative forward P/E is omitted.
- `facts`, in display order and above `businessSummary`: `founded`,
  `headquarters`, `employees`, `sector`, `industry`, `ceo`. A value the data
  layer does not have is omitted; nothing fills the gap.
- `businessSummary`: the data layer's business description.

The caller fills these on `profile` from yfinance: `forwardPE` (`forwardPE`),
`headquarters` (`city`, `state`, `country`), `officers` (`companyOfficers`,
name and title), `employees` (`fullTimeEmployees`), `sector`, `industry` and
`description` (`longBusinessSummary`). yfinance has no founded or incorporation
year, so `founded` only appears if another source supplies `foundedYear`.
There is no IPO date row: yfinance only has a first-trade date, which is
Yahoo's data start for older listings (JPM shows 1980-03-17).

The CEO is the officer whose title makes them chief executive of the whole
company. A "CEO of <division>", "Senior Advisor to the CEO" or "former CEO"
title does not count, co-CEOs
are listed together, any other case with more than one company-level CEO omits
the row, and honorifics and credentials are dropped from the name.

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

- Each agent names its own model in its `run.ts` (`export const model`), taken
  from the `MODELS` constants block in `_shared/client.ts`, the one place to
  bump versions. News runs on Haiku 4.5 (`claude-haiku-4-5-20251001`); dcf,
  catalyst, redflag and verdict run on Sonnet 5 (`claude-sonnet-5`). IDs are
  full model IDs, not aliases.
- Resolution order: `VALUS_AGENT_MODEL` (global override, for testing), then
  the agent's `model`, then `DEFAULT_MODEL` (Sonnet 5).
- Request features that differ by model sit next to the IDs in
  `MODEL_FEATURES`. Effort is sent only where accepted (Haiku 4.5 rejects it),
  so the news agent's `effort` only applies when it is overridden onto Sonnet.
  Thinking stays at each model's default: adaptive on Sonnet 5, off on Haiku.
  Effort is `low` for dcf, catalyst, news and redflag, and `medium` for verdict.
- Each agent sets a hard `max_tokens` cap in its `run.ts` (`export const maxTokens`),
  covering thinking and the reply: dcf 2,000, catalyst 2,500, news 1,500,
  redflag 1,500, verdict 2,500. A reply cut off at the cap is repaired under the
  same cap, never a bigger one.
- The token log names the model that served each attempt and why it was
  chosen, for cost per agent:
  `[valus.agents] news AAPL model=claude-haiku-4-5-20251001 model_source=agent attempt=1 ok ...`.
  `AgentResult.meta.model` carries the same served model.
- The system prompt is byte-stable and marked cacheable, with per-ticker
  context in the user turn. Prompts shorter than the model's cache minimum
  (1,024 tokens on Sonnet 5, 4,096 on Haiku 4.5) never cache, so `cache_miss`
  is expected for those.
- Structured output uses `zodOutputFormat` sent through `messages.create()`,
  not `parse()`. `parse()` throws on a zod failure, which would skip the
  repair turn. The SDK moves enums and length limits into schema descriptions,
  so the client-side zod check is what actually enforces them.
- The server-side refusal fallback (`fallbacks: "default"`, beta
  `server-side-fallback-2026-07-01`) is sent only to models `MODEL_FEATURES`
  marks as supporting it, which today means an override onto `claude-opus-5`.
  On Sonnet and Haiku a refusal comes back as `unavailable`.
  `configureClient({ refusalFallbacks: false })` turns it off everywhere, which
  Bedrock, Vertex and Foundry need.
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

## Catalyst lifecycle

Every catalyst has a `status`: `rumored`, `reported`, `announced` or `shipped`.

- `upcomingCatalysts` holds only rumored and reported items, each with an
  `expectedDate` that is a day the context states, on or after today. The
  calendar's date can only date the earnings report; any other item needs a
  cited news item that gives the day. An item that fails is dropped on its own,
  without a repair turn, and named in the field's `note`. Litigation and
  regulatory matters without a date never appear. `netTilt` is computed from
  the items that remain.
- Every item has a `kind`. A future ship date, price or availability window
  (`ship_date`, `price`, `availability`) needs a company statement among its
  evidence: a news item from the company itself (its newsroom or investor
  relations), a press-release wire, or a filing.
- `announced` and `shipped` are official events that have already happened.
  Their evidence can be a company statement or a major outlet (Reuters,
  Bloomberg, WSJ, FT, AP, CNBC, NYT, Washington Post, Nikkei, BBC, The
  Economist, Barron's, Dow Jones; `MAJOR_OUTLETS` in `catalyst/run.ts`). Both
  require `announcedDate`.
- A cited item whose title or summary hedges its sourcing ("people familiar",
  "sources said", "according to people", "is said to", "reportedly") cannot
  support `announced` or `shipped`, whichever outlet published it. An item
  with no other support is downgraded to `reported`. A reported event that has
  already happened has no day still ahead, so it is left out and named in
  `historicalAnalogs.note`. This is a code check (`UNOFFICIAL_SOURCING` in
  `catalyst/run.ts`), not a prompt instruction, and it never costs a repair turn.
- Legal and regulatory events that have already happened stay in
  `historicalAnalogs`.
- Announced items move to `historicalAnalogs`, with `eventDate` set to
  `announcedDate`. Shipped items stay there for 90 days after `announcedDate`,
  then drop off.
- Dates must come from the context. `expectedDate` has to be a day stated in
  the calendar fields or in a cited news item ("March 2027" is not a day), and
  `announcedDate` has to match the date of the cited evidence.
- Every date is checked against today (UTC), and the final object is checked
  again before it is returned, so a past-dated upcoming item never renders.
  Tests and fixture replays pin the clock (`setClock` in `_shared/format.ts`).

## Cache

Key: `valus:agent:v3:<slug>:<TICKER>:<YYYY-MM-DD UTC>`.

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

## Status, tests and fixtures

```
npm run agents:status   every registered agent: resolved model, required files, fixture provenance
npm test                offline tests (model replies come through the transport seam)
npm run typecheck
```

`agents:status` also reports whether `ANTHROPIC_API_KEY` is set (never its
value) and exits 1 when any agent is not ready.

`agents:status`, `fixtures:record` and `npm test` load `agents/.env` when it
exists, using Node's built-in `--env-file-if-exists` (no dotenv). Without the
file they run normally, which is what CI gets. A variable already set in the
shell or CI takes precedence over the file. The key is never used by the
tests: a test that calls the model without installing a transport fails.

Each `fixtures/*.json` holds the agent's input, the model reply, the expected
result, and its provenance. Replaying the reply through `run()` must reproduce
the expected result.

| Field | Recorded fixture | Hand-written fixture |
|---|---|---|
| `source` | `"recorded"` | `"synthetic"` |
| `recordedAt` | ISO timestamp of the live call | `null` |
| `recordedModel` | model that served the reply | `null` |

Only `npm run fixtures:record` writes `source: "recorded"`, and only for an
agent whose live call succeeded. A test rejects any fixture whose provenance
fields disagree, and `agents:status` counts a fixture as recorded only when
they agree.

**The current fixtures are synthetic.** No API key was available when they were
created, and the AAPL figures are approximate. To replace them with real
recordings (five requests):

```
ANTHROPIC_API_KEY=... npm run fixtures:record
```

After an intentional change to how an agent wraps its output, run
`npm run fixtures:replay` and review the diff. Both commands take agent slugs
(`npm run fixtures:record -- catalyst verdict`), and neither ever writes a
fixture for an agent that failed. Replay leaves a recorded fixture's input
alone, since that is what the model was sent.

## iCloud note

`~/Desktop` is synced to iCloud Drive, which evicts files inside `node_modules`
(TypeScript lost its lib files). Dependencies live in `node_modules.nosync`,
which iCloud skips, behind a `node_modules` symlink. `npm install` replaces the
symlink with a real folder, so run `npm run icloud:nosync` after installing.
If tsc then reports missing global types, the install itself was damaged by
sync: install in a folder outside Desktop and move `node_modules` in as
`node_modules.nosync`.
