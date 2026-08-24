/**
 * Where the API is, and whether the reader may change it.
 *
 * Three sources, in this order:
 *
 *   1. `config.js`  — what the deployment says. A static page cannot read environment
 *                     variables, so a deployment that needs to configure anything writes
 *                     this file: the bundled server renders it from the environment, and a
 *                     container image can do the same at startup.
 *   2. the same origin — served next to the Prefect server, `/api` is right and needs no
 *                     configuring at all. This is the case worth optimising for: no CORS,
 *                     and run links open in the same tab.
 *   3. localStorage — what the reader last typed into the header field.
 *
 * Pure, so the precedence is testable: it is the kind of rule that otherwise breaks
 * silently, leaving a deployment pointed somewhere nobody intended.
 */

/** Hostnames that mean "a dev server", where the API is somewhere else. */
const LOCAL_HOSTS = ['localhost', '127.0.0.1', ''];

export const isSameOrigin = (hostname) => !LOCAL_HOSTS.includes(hostname);

/** Where to look when nothing is configured. */
export const FALLBACK_API_URL = 'http://localhost:4200/api';

/**
 * @param {{config?: object, hostname?: string, storedUrl?: string}} sources
 * @returns {string} the API base URL to use
 */
export function resolveApiUrl({ config = {}, hostname = '', storedUrl = '' } = {}) {
  // A configured URL is the deployment's decision and outranks a stored one, which may be
  // left over from a dev session against the same hostname.
  if (config.apiUrl) return config.apiUrl;

  const fallback = isSameOrigin(hostname) ? '/api' : FALLBACK_API_URL;
  return apiUrlEditable({ config, hostname }) && storedUrl ? storedUrl : fallback;
}

/**
 * Whether the header shows the API URL field.
 *
 * Hidden when there is nothing to choose: same-origin, or a URL the deployment pinned.
 * `apiUrlEditable` in `config.js` overrides either way — `true` to offer the field on a
 * deployment that wants it, `false` to take it away.
 */
export function apiUrlEditable({ config = {}, hostname = '' } = {}) {
  if (typeof config.apiUrlEditable === 'boolean') return config.apiUrlEditable;
  return !isSameOrigin(hostname) && !config.apiUrl;
}

/**
 * The payload keys to read a batch label from: the conventional ones, then whatever the
 * deployment adds.
 *
 * Appended rather than replacing, so a server whose events use a name of their own says
 * only that name — and a payload carrying both a conventional key and a private one still
 * prefers the conventional one.
 *
 * @param {object} config from `config.js`
 * @param {string[]} defaults the built-in list
 */
export function resolveBatchKeys(config = {}, defaults = []) {
  const extra = (config.extraBatchKeys ?? [])
    .map((key) => String(key).trim())
    .filter(Boolean);

  return [...new Set([...defaults, ...extra])];
}
