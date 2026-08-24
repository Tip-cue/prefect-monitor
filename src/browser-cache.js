/**
 * The storage side of the run cache.
 *
 * sessionStorage rather than localStorage: the cache is a speed-up for the tab you are
 * working in, and going stale across days would be a liability rather than a saving.
 * Everything is best-effort — a browser with storage disabled, or a quota refusal on a
 * large window, must degrade to "no cache", never to a broken page.
 */

import { projectForStorage } from './run-cache.js';

/**
 * The trailing number is the meaning of `from`, which changed: it used to be the range
 * that was asked for and is now the span actually read. An entry written under the old
 * meaning would be served incrementally over a gap it does not know it has, so the key
 * moves and the old entry is simply never read again.
 */
const KEY_PREFIX = 'prefect-monitor.cache.2.';

/** One entry per API base, so pointing at another server cannot serve its data. */
const keyFor = (apiBaseUrl) => `${KEY_PREFIX}${apiBaseUrl}`;

export function readCache(apiBaseUrl) {
  try {
    const raw = sessionStorage.getItem(keyFor(apiBaseUrl));
    if (!raw) return null;

    const cache = JSON.parse(raw);
    return Array.isArray(cache.runs) ? cache : null;
  } catch (error) {
    console.warn('cache unreadable, ignoring it', error);
    return null;
  }
}

/**
 * @returns {boolean} whether it was stored; false means carry on without a cache
 */
export function writeCache(
  apiBaseUrl,
  { from, to, fetchedAt, runs, links, flows, edges, linksFrom, batchKeys = [] },
) {
  const entry = {
    from,
    to,
    fetchedAt,
    // How far back the links reach. Dropping it was not harmless: the next refresh read
    // one minute of events, found no cached value to compare against, and concluded that
    // was all the coverage there was — so a healthy 6h chart warned it had no old links.
    linksFrom,
    // Projected with the keys in force, so the labels the fallback matches on survive.
    runs: projectForStorage(runs, batchKeys),
    links,
    flows: flows.map((flow) => ({ id: flow.id, name: flow.name })),
    // Cheap, and without them the instant paint would have no lane ordering or
    // aggregate connectors until the fetch returned.
    edges,
  };

  try {
    sessionStorage.setItem(keyFor(apiBaseUrl), JSON.stringify(entry));
    return true;
  } catch (error) {
    // Quota is the expected failure on a wide window. Drop what is there and carry on
    // uncached rather than leaving a half-written entry behind.
    console.warn('could not cache this window, continuing without', error);
    clearCache(apiBaseUrl);
    return false;
  }
}

export function clearCache(apiBaseUrl) {
  try {
    sessionStorage.removeItem(keyFor(apiBaseUrl));
  } catch {
    // Nothing to do: if it cannot be removed it also cannot have been written.
  }
}
