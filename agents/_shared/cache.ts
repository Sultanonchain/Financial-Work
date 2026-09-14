/**
 * Result cache keyed by (agentSlug, ticker, UTC date).
 *
 * The UTC date in the key means every entry rolls over at 00:00 UTC no matter
 * how long its TTL is; the TTL then trims inside the day (news keeps 4h). The
 * two together are the freshness rule: never older than the TTL, never from a
 * previous UTC day.
 *
 * Only `status: "ok"` results are written. Caching an "unavailable" result
 * would turn a thirty-second Anthropic blip into a day-long blank card.
 *
 * Tier is deliberately NOT part of the key. Agents produce the full result
 * once; free/premium redaction happens at the serialization boundary in
 * runner.ts, so a premium upgrade never forces a re-run.
 *
 * The store is pluggable. MemoryStore is the default and is per-process, which
 * on serverless means per-instance; production should pass a Redis/KV adapter
 * (the interface is the get/set-with-TTL subset every KV client already has).
 */

import type { AgentResult, AgentSlug } from './types.ts';

/* ────────────────────────────────────────────────────────────────────────── */
/* TTLs                                                                       */
/* ────────────────────────────────────────────────────────────────────────── */

const HOUR = 3600;

export const TTL_SECONDS: Record<AgentSlug, number> = {
  dcf: 24 * HOUR,
  catalyst: 24 * HOUR,
  redflag: 24 * HOUR,
  news: 4 * HOUR,
  // The verdict reads news, so it cannot outlive the freshest input it was
  // built from. Keep this at or below the shortest stage-1 TTL.
  verdict: 4 * HOUR,
};

/**
 * Bump when a prompt or schema change makes old entries wrong. Entries under
 * the old version are simply never read again and age out on their own TTL.
 */
// v2: catalyst output split into upcomingCatalysts / historicalAnalogs
// v3: catalyst items gain kind; upcoming items are always dated
export const CACHE_VERSION = 'v4';

/* ────────────────────────────────────────────────────────────────────────── */
/* Store                                                                      */
/* ────────────────────────────────────────────────────────────────────────── */

export interface CacheStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlSeconds: number): Promise<void>;
  delete(key: string): Promise<void>;
}

interface MemoryEntry {
  value: string;
  expiresAt: number;
}

/** In-process store with lazy expiry and a hard size cap (oldest out first). */
export class MemoryStore implements CacheStore {
  readonly #entries = new Map<string, MemoryEntry>();
  readonly #maxEntries: number;
  readonly #now: () => number;

  constructor(options: { maxEntries?: number; now?: () => number } = {}) {
    this.#maxEntries = options.maxEntries ?? 5_000;
    this.#now = options.now ?? Date.now;
  }

  async get(key: string): Promise<string | null> {
    const entry = this.#entries.get(key);
    if (!entry) return null;
    if (entry.expiresAt <= this.#now()) {
      this.#entries.delete(key);
      return null;
    }
    return entry.value;
  }

  async set(key: string, value: string, ttlSeconds: number): Promise<void> {
    // Re-insert so Map iteration order tracks recency of writes.
    this.#entries.delete(key);
    this.#entries.set(key, { value, expiresAt: this.#now() + ttlSeconds * 1000 });
    while (this.#entries.size > this.#maxEntries) {
      const oldest = this.#entries.keys().next().value;
      if (oldest === undefined) break;
      this.#entries.delete(oldest);
    }
  }

  async delete(key: string): Promise<void> {
    this.#entries.delete(key);
  }

  get size(): number {
    return this.#entries.size;
  }
}

let store: CacheStore = new MemoryStore();

export function setCacheStore(next: CacheStore): void {
  store = next;
}

export function getCacheStore(): CacheStore {
  return store;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Keys                                                                       */
/* ────────────────────────────────────────────────────────────────────────── */

/** YYYY-MM-DD in UTC. Never local time: Vercel and a laptop must agree. */
export function utcDate(at: Date = new Date()): string {
  return at.toISOString().slice(0, 10);
}

export function cacheKey(slug: AgentSlug, ticker: string, at: Date = new Date()): string {
  return `valus:agent:${CACHE_VERSION}:${slug}:${ticker.trim().toUpperCase()}:${utcDate(at)}`;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* Read-through                                                               */
/* ────────────────────────────────────────────────────────────────────────── */

export interface CachedRunOptions<T> {
  slug: AgentSlug;
  ticker: string;
  ttlSeconds: number;
  now?: Date | undefined;
  /** Skip the read (still writes). For manual refreshes. */
  bypassRead?: boolean | undefined;
  /**
   * Extra write gate on top of `status === "ok"`. The runner uses it to keep a
   * verdict built on partial upstream data out of the cache.
   */
  shouldStore?: ((result: AgentResult<T>) => boolean) | undefined;
}

/**
 * Serve from cache when possible, otherwise run `produce` and store an ok
 * result. A broken store never breaks the agent: every store failure is
 * swallowed and the agent simply runs uncached.
 */
export async function withCache<T>(
  options: CachedRunOptions<T>,
  produce: () => Promise<AgentResult<T>>,
): Promise<AgentResult<T>> {
  const key = cacheKey(options.slug, options.ticker, options.now);

  if (!options.bypassRead) {
    const hit = await safeGet(key);
    if (hit) {
      try {
        const parsed = JSON.parse(hit) as AgentResult<T>;
        if (parsed.status === 'ok' && parsed.slug === options.slug) {
          return { ...parsed, meta: { ...parsed.meta, cached: true, latencyMs: 0 } };
        }
      } catch {
        // Corrupt entry: fall through and overwrite it with a fresh run.
      }
    }
  }

  const result = await produce();

  if (result.status === 'ok' && (options.shouldStore?.(result) ?? true)) {
    const ttl = Math.min(options.ttlSeconds, secondsUntilUtcMidnight(options.now));
    if (ttl > 0) {
      await safeSet(key, JSON.stringify(result), ttl);
    }
  }

  return result;
}

/**
 * The key already changes at UTC midnight; capping the TTL there too stops a
 * 23:50 write from sitting in the store for another 24h under a dead key.
 */
function secondsUntilUtcMidnight(now: Date = new Date()): number {
  const next = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1);
  return Math.max(1, Math.floor((next - now.getTime()) / 1000));
}

async function safeGet(key: string): Promise<string | null> {
  try {
    return await store.get(key);
  } catch {
    return null;
  }
}

async function safeSet(key: string, value: string, ttlSeconds: number): Promise<void> {
  try {
    await store.set(key, value, ttlSeconds);
  } catch {
    // Cache writes are best-effort.
  }
}
