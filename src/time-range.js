/**
 * The selected time range, as text.
 *
 * Ranges are held as expressions rather than as resolved timestamps, the way Kibana
 * and Grafana do it: `now-6h` → `now` keeps meaning "the last six hours" after a
 * refresh, where a pair of absolute timestamps would silently freeze. That is also
 * what makes a URL worth pasting to someone else.
 */

/**
 * The longest range offered, or accepted.
 *
 * One fetch reads ~2200 runs, which on a busy server is about a day: past that the chart
 * showed a fraction of what was asked for and said so. Rather than keep explaining the
 * truncation, the picker stops at what can actually be drawn — and a longer range is more
 * useful as a *shifted* day ("yesterday, for 12h") than as a partial week.
 */
export const MAX_RANGE_MS = 24 * 60 * 60 * 1000;

/** Offered both as "the last X" and as "X from a start point". */
export const DURATIONS = [
  { label: '15m', long: '15 minutes', ms: 15 * 60 * 1000 },
  { label: '30m', long: '30 minutes', ms: 30 * 60 * 1000 },
  { label: '1h', long: '1 hour', ms: 60 * 60 * 1000 },
  { label: '3h', long: '3 hours', ms: 3 * 60 * 60 * 1000 },
  { label: '6h', long: '6 hours', ms: 6 * 60 * 60 * 1000 },
  { label: '12h', long: '12 hours', ms: 12 * 60 * 60 * 1000 },
  { label: '24h', long: '24 hours', ms: MAX_RANGE_MS },
];

/** Offered in the picker, and the shorthand the URL round-trips. */
export const QUICK_RANGES = DURATIONS.map(({ label, long }) => ({
  label: `Last ${long}`,
  from: `now-${label}`,
}));

/**
 * Start points worth a click, so a past day needs no typing: midnight of today, yesterday,
 * and a few days before that. Held as a day offset rather than a timestamp, since "midnight
 * yesterday" has to be resolved in the reader's own timezone and at the time they ask.
 */
export const START_ANCHORS = [
  { label: 'Today', days: 0 },
  { label: 'Yesterday', days: 1 },
  { label: '2 days ago', days: 2 },
  { label: '3 days ago', days: 3 },
  { label: '7 days ago', days: 7 },
];

/** Local midnight, `days` back. */
export function anchorStart(days, nowMs = Date.now()) {
  const at = new Date(nowMs);
  at.setHours(0, 0, 0, 0);
  at.setDate(at.getDate() - days);
  return at.getTime();
}

/** `YYYY-MM-DD HH:mm` local — what the picker shows, and what it will read back. */
export function formatLocal(ms) {
  const at = new Date(ms);
  const pad = (value) => String(value).padStart(2, '0');
  return `${at.getFullYear()}-${pad(at.getMonth() + 1)}-${pad(at.getDate())}`
    + ` ${pad(at.getHours())}:${pad(at.getMinutes())}`;
}

/**
 * The range "`durationMs` starting at `startMs`", as expressions.
 *
 * Absolute on both ends: a fixed start is the point of picking one, and it must not drift
 * on the next refresh the way a relative range does.
 */
export function rangeFromStart(startMs, durationMs) {
  const span = Math.min(durationMs, MAX_RANGE_MS);
  return { from: formatLocal(startMs), to: formatLocal(startMs + span) };
}

/**
 * The same range, shortened to MAX_RANGE_MS if it is longer.
 *
 * `anchor` is the end to keep — the one the reader just set, so the other moves. Setting
 * an end time and having the *end* silently move would be the one thing they were sure
 * about; the same the other way round for a start.
 *
 * Applied to the expressions and not only to the resolved pair, so a pasted `now-7d` link
 * ends up labelled as the day it is actually showing rather than lying about the week.
 *
 * @param {'start'|'end'} anchor defaults to the end: that is where "now" is, and where a
 *   link that has simply aged out should be trimmed to.
 */
export function capRange(range, nowMs = Date.now(), anchor = 'end') {
  const from = parseTimeExpression(range?.from, nowMs);
  const to = parseTimeExpression(range?.to, nowMs);

  // Unreadable or inverted: leave it alone and let resolveRange reject it.
  if (from === null || to === null || from >= to || to - from <= MAX_RANGE_MS) return range;

  if (anchor === 'start') return { from: range.from, to: formatLocal(from + MAX_RANGE_MS) };

  return range.to === 'now'
    ? { from: 'now-24h', to: 'now' }
    : { from: formatLocal(to - MAX_RANGE_MS), to: range.to };
}

/** The range "`durationMs` ending at `endMs`", as expressions. */
export function rangeToEnd(endMs, durationMs) {
  const span = Math.min(durationMs, MAX_RANGE_MS);
  return { from: formatLocal(endMs - span), to: formatLocal(endMs) };
}

export const DEFAULT_RANGE = { from: 'now-6h', to: 'now' };

const UNIT_MS = {
  s: 1000,
  m: 60 * 1000,
  h: 60 * 60 * 1000,
  d: 24 * 60 * 60 * 1000,
  w: 7 * 24 * 60 * 60 * 1000,
};

const RELATIVE = /^now(?:([+-])(\d+)([smhdw]))?$/;

/** `2026-08-19`, `2026-08-19 09:44`, `2026-08-19T09:44:00Z`, `…+03:00`. */
const ABSOLUTE = /^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?$/;

/**
 * Resolves one end of a range to epoch ms.
 *
 * Accepts `now`, `now-90m`, `now+1h`, an ISO 8601 timestamp, or the
 * `YYYY-MM-DD HH:mm` shape a person would type.
 *
 * @param {string} expression
 * @param {number} [nowMs] injected so this stays pure and testable
 * @returns {number|null} null if it cannot be read
 */
export function parseTimeExpression(expression, nowMs = Date.now()) {
  const text = String(expression ?? '').trim();
  if (!text) return null;

  const relative = RELATIVE.exec(text);
  if (relative) {
    const [, sign, amount, unit] = relative;
    if (!sign) return nowMs;
    return nowMs + (sign === '-' ? -1 : 1) * Number(amount) * UNIT_MS[unit];
  }

  // Date.parse is far too forgiving to hand raw input to — V8 reads "now-5" as
  // 2001-05-01 — so absolute values have to match an explicit shape first. A typo
  // must be rejected, not silently resolved to some date in 2001.
  if (!ABSOLUTE.test(text)) return null;

  // "2026-08-19 09:44" is not valid ISO; a T makes it parseable, as local time.
  const parsed = Date.parse(text.replace(' ', 'T'));
  return Number.isNaN(parsed) ? null : parsed;
}

/**
 * @returns {{from: number, to: number}|null} null if either end is unreadable, or
 *   the range is inverted or empty
 */
export function resolveRange(range, nowMs = Date.now()) {
  // Capped here as well as where it is set: a hand-edited URL never reaches the picker,
  // and nothing downstream should have to cope with a fetch it cannot satisfy.
  const capped = capRange(range, nowMs);
  const from = parseTimeExpression(capped?.from, nowMs);
  const to = parseTimeExpression(capped?.to, nowMs);
  if (from === null || to === null || from >= to) return null;
  return { from, to };
}

/** The quick range this expression pair corresponds to, if any. */
export function matchQuickRange(range) {
  if (range?.to !== 'now') return null;
  return QUICK_RANGES.find((quick) => quick.from === range.from) ?? null;
}

/** What the picker button shows. */
export function describeRange(range) {
  const quick = matchQuickRange(range);
  if (quick) return quick.label;

  // A start-plus-duration range is two absolute ends, and two full timestamps in the
  // header is a lot to read for what is usually one day: "Aug 19 00:00–06:00".
  const from = parseTimeExpression(range?.from);
  const to = parseTimeExpression(range?.to);
  if (from !== null && to !== null && !/^now/.test(range.from) && !/^now/.test(range.to)) {
    const day = (ms) => new Date(ms).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
    const clock = (ms) => formatLocal(ms).slice(-5);

    return new Date(from).toDateString() === new Date(to).toDateString()
      ? `${day(from)} ${clock(from)}–${clock(to)}`
      : `${day(from)} ${clock(from)} → ${day(to)} ${clock(to)}`;
  }

  const readable = (expression) => {
    if (/^now/.test(expression)) return expression;
    const ms = parseTimeExpression(expression);
    return ms === null ? expression : new Date(ms).toLocaleString();
  };
  return `${readable(range.from)} → ${readable(range.to)}`;
}

/**
 * Reads the view out of a query string, falling back to the default range.
 *
 * The open pop-up is part of the view too: clicking a run navigates to Prefect in this
 * tab, and coming back should land on what was open, not on a bare chart.
 *
 * @returns {{range: object, mode: string|null, states: string[], refresh: string|null,
 *   modal: {kind: string, id: string}|null}}
 */
export function viewFromQuery(search) {
  const params = new URLSearchParams(search);
  const from = params.get('from');
  const to = params.get('to');

  return {
    range: from && to ? capRange({ from, to }) : { ...DEFAULT_RANGE },
    mode: params.get('mode') === 'agg' || params.get('mode') === 'graph' ? params.get('mode') : null,
    states: (params.get('states') ?? '').split(',').map((s) => s.trim()).filter(Boolean),
    refresh: params.get('refresh'),
    modal: modalFromParams(params),
    zoom: zoomFromParams(params),
  };
}

/**
 * The zoom is absolute, not an expression: it is a slice of one particular loaded range,
 * and "now-20m" would mean somewhere else tomorrow. The range around it stays relative,
 * so the pair reads as "the last 6 hours, looking at this bit of it".
 */
function zoomFromParams(params) {
  const from = Date.parse(params.get('zoomFrom') ?? '');
  const to = Date.parse(params.get('zoomTo') ?? '');

  return Number.isFinite(from) && Number.isFinite(to) && to > from ? { from, to } : null;
}

const MODAL_PARAMS = {
  subflows: 'subflows',       // sub-flows of one run
  subflowsOf: 'subflowsOf',   // sub-flows of every run of one flow
  chain: 'chain',             // one run's chain, in its own window
};

function modalFromParams(params) {
  for (const kind of Object.keys(MODAL_PARAMS)) {
    const id = params.get(MODAL_PARAMS[kind]);
    if (id) return { kind, id };
  }
  return null;
}

/** The query string for a view — everything needed to reproduce it from a paste. */
export function viewToQuery({ range, mode, states, refresh, modal, zoom }) {
  const params = new URLSearchParams();
  params.set('from', range.from);
  params.set('to', range.to);
  if (mode) params.set('mode', mode);
  if (states?.length) params.set('states', states.join(','));
  if (refresh && refresh !== 'Off') params.set('refresh', refresh);
  if (modal && MODAL_PARAMS[modal.kind]) params.set(MODAL_PARAMS[modal.kind], modal.id);
  if (zoom) {
    params.set('zoomFrom', new Date(zoom.from).toISOString());
    params.set('zoomTo', new Date(zoom.to).toISOString());
  }
  return `?${params}`;
}

/**
 * The range that "a start, an end, a duration — two of them" adds up to.
 *
 * Pure, and separate from the DOM wiring, because the combinations are where this went
 * wrong: picking a start while an *earlier* absolute end was still in the field produced an
 * inverted range and an error, when what the reader asked for was that day at the duration
 * they were already looking at.
 *
 * A `durationMs` is a deliberate choice of span and wins over a remembered end. Without one,
 * a start and an end are taken as the pair they are — and an inverted pair is a mistake worth
 * reporting rather than quietly reinterpreting.
 *
 * @param {{startMs?: number|null, endMs?: number|null, durationMs?: number|null,
 *   spanMs: number}} choice
 * @returns {{from: string, to: string}} range expressions; may still be inverted, which the
 *   caller reports
 */
export function composeRange({ startMs = null, endMs = null, durationMs = null, spanMs }) {
  const span = durationMs ?? spanMs;

  if (durationMs === null && startMs !== null && endMs !== null) {
    return { from: formatLocal(startMs), to: formatLocal(endMs) };
  }
  if (startMs !== null) return rangeFromStart(startMs, span);
  if (endMs !== null) return rangeToEnd(endMs, span);

  // Neither end pinned: "the last N", which keeps sliding.
  const quick = QUICK_RANGES[DURATIONS.findIndex((each) => each.ms === span)];
  return quick ? { from: quick.from, to: 'now' } : { ...DEFAULT_RANGE };
}
