/**
 * A local cache of runs and links, so a reload or a refresh tick does not refetch
 * everything.
 *
 * A 24h window is ~2000 runs across a dozen pages plus a dozen pages of events, about
 * ten seconds. Most of that is unchanged between one refresh and the next: a completed
 * run never changes again, and the window usually only extends at the `now` end.
 *
 * So the cache holds runs by id and remembers the span it covers. A refresh then needs
 * only the new slice at the end, plus the runs that were still in flight — everything
 * else is already known. Whatever is cached is drawn immediately, so a reload paints
 * before the network answers.
 *
 * Nothing here touches storage or the network: it decides *what* to fetch and merges
 * the answer. See `src/browser-cache.js` for the sessionStorage side.
 */

import { runStart, runEnd } from './time.js';
import { batchLabelsOf } from './links.js';

/** States a run can still leave. Anything else is final and never refetched. */
const NON_TERMINAL = new Set([
  'RUNNING', 'PENDING', 'SCHEDULED', 'PAUSED', 'CANCELLING', 'RETRYING', 'AWAITINGRETRY',
]);

export function isSettled(run) {
  return !NON_TERMINAL.has(String(run.state_type ?? '').toUpperCase());
}

/** How long a cached window is served without going back to the server at all. */
export const CACHE_FRESH_MS = 10_000;

/** Beyond this, the cache is treated as cold and the window is fetched in full. */
export const CACHE_STALE_MS = 30 * 60 * 1000;

/**
 * Overlap re-fetched at the leading edge, in case a run appeared with an
 * `expected_start_time` slightly before the moment we last looked.
 */
const LEADING_EDGE_SLACK_MS = 60 * 1000;

/**
 * Decides how to satisfy a request for [from, to] given what is cached.
 *
 * @param {object|null} cache      as stored: { from, to, fetchedAt, runs, links }
 * @param {number} from
 * @param {number} to
 * @param {number} now
 * @param {number} freshMs how recent a fetch has to be to answer outright. The caller
 *   lowers it to the auto-refresh interval, so choosing 5s means data 5s old rather than
 *   every other tick being served from the cache, and passes 0 for an explicit Refresh.
 * @returns {{mode: 'full'|'incremental'|'fresh', since: number|null, drawCached: boolean}}
 *   `fresh` means the cache answers it outright; `incremental` means fetch from
 *   `since` onwards and merge; `full` means start over. `drawCached` says whether
 *   there is anything worth painting before the fetch returns.
 */
export function planFetch(cache, from, to, now = Date.now(), freshMs = CACHE_FRESH_MS) {
  if (!cache || !cache.runs?.length) {
    return { mode: 'full', since: null, drawCached: false };
  }

  const age = now - cache.fetchedAt;
  if (age > CACHE_STALE_MS) return { mode: 'full', since: null, drawCached: false };

  // The cache has to already cover the *start* of the window; it can only be extended
  // forwards. Asking for an earlier start means a different span, so refetch it.
  const covers = cache.from <= from && cache.to >= from;
  if (!covers) return { mode: 'full', since: null, drawCached: false };

  const drawCached = true;
  if (cache.to >= to && age < freshMs) return { mode: 'fresh', since: null, drawCached };

  return {
    mode: 'incremental',
    since: Math.max(from, cache.to - LEADING_EDGE_SLACK_MS),
    drawCached,
  };
}

/** Cached runs that could still have changed, so a refresh has to ask about them. */
export function unsettledRunIds(cache, from) {
  return (cache?.runs ?? [])
    .filter((run) => !isSettled(run) && runEnd(run) >= from)
    .map((run) => run.id);
}

/**
 * Merges fetched runs over cached ones, newest wins, dropping anything that ended
 * before `keepFrom` so the cache cannot grow without bound.
 */
export function mergeRuns(cachedRuns, freshRuns, keepFrom) {
  const byId = new Map();

  for (const run of cachedRuns ?? []) byId.set(run.id, run);
  for (const run of freshRuns ?? []) byId.set(run.id, run); // a refetched run replaces its copy

  return [...byId.values()].filter((run) => runEnd(run) >= keepFrom);
}

/** Merges links, which are immutable once emitted, so cached ones are always still valid. */
export function mergeLinks(cachedLinks, freshLinks) {
  const byKey = new Map();

  for (const link of [...(cachedLinks ?? []), ...(freshLinks ?? [])]) {
    byKey.set(`${link.upstreamRunId}>${link.downstreamRunId}`, link);
  }
  return [...byKey.values()];
}

/**
 * Keeps only the fields the UI reads.
 *
 * A full run object is ~1.5KB and a 24h window holds a couple of thousand of them, which is
 * more than sessionStorage will take: 5.1MB against 724KB for this.
 *
 * `batchLabels` is derived rather than kept raw — the tags and parameters it comes from cost
 * three times as much (+332KB against +118KB on 24h) and nothing else reads them. Dropping
 * them entirely, as this first did, left a cached window with no labels at all, so every
 * link fell through to the weaker timing guess.
 */
export function projectRun(run, batchKeys = []) {
  return {
    batchLabels: [...batchLabelsOf(run, batchKeys)],
    id: run.id,
    flow_id: run.flow_id,
    name: run.name,
    state_type: run.state_type,
    state_name: run.state_name,
    start_time: run.start_time,
    end_time: run.end_time,
    expected_start_time: run.expected_start_time,
    created: run.created,
    parent_task_run_id: run.parent_task_run_id,
  };
}

/** The runs, projected and ordered, ready to store. */
export function projectForStorage(runs, batchKeys = []) {
  return [...runs].sort((a, b) => runStart(a) - runStart(b)).map((run) => projectRun(run, batchKeys));
}
