/**
 * Which clock the page reads in: the reader's own, or UTC.
 *
 * Every timestamp on screen goes through here — the axis, tooltips, the picker fields, the
 * calendar, the notices — because a page showing some times in one zone and some in another
 * is worse than either. It is a display setting: instants are epoch milliseconds throughout,
 * and only ever converted on the way in and out.
 *
 * UTC matters because pipelines are usually scheduled and labelled in it: a batch stamped
 * `2026-08-19T06:00:00+00:00` should be findable at 06:00 on the axis, not at 09:00 because
 * the reader happens to sit in +03:00.
 *
 * Module state rather than a threaded argument, deliberately: it is one global fact about
 * the page, and threading it through every formatter in five modules would be noise. Tests
 * set it explicitly.
 */

/** @type {'local'|'utc'} */
let zone = 'local';

export const getZone = () => zone;
export const isUtc = () => zone === 'utc';

/** @param {'local'|'utc'} next anything else is treated as local */
export function setZone(next) {
  zone = next === 'utc' ? 'utc' : 'local';
  return zone;
}

/** What to call it in the interface. */
export const zoneLabel = () => (zone === 'utc' ? 'UTC' : 'Local');

/** The calendar parts of an instant, in the active zone. */
export function partsOf(ms) {
  const at = new Date(ms);
  return zone === 'utc'
    ? {
      year: at.getUTCFullYear(),
      month: at.getUTCMonth(),
      day: at.getUTCDate(),
      hours: at.getUTCHours(),
      minutes: at.getUTCMinutes(),
    }
    : {
      year: at.getFullYear(),
      month: at.getMonth(),
      day: at.getDate(),
      hours: at.getHours(),
      minutes: at.getMinutes(),
    };
}

/**
 * The instant those parts name, in the active zone.
 *
 * Through the platform's own constructors, so a day that does not exist — the 31st of a
 * 30-day month, an hour a daylight-saving jump skips — resolves the way the platform
 * resolves it rather than parsing as garbage.
 */
export function instantOf({ year, month, day, hours = 0, minutes = 0 }) {
  return zone === 'utc'
    ? Date.UTC(year, month, day, hours, minutes, 0, 0)
    : new Date(year, month, day, hours, minutes, 0, 0).getTime();
}

/** Which weekday a date falls on, 0 = Sunday, in the active zone. */
export function weekdayOf(year, month, day) {
  const at = new Date(instantOf({ year, month, day }));
  return zone === 'utc' ? at.getUTCDay() : at.getDay();
}

/** Days in a month — day 0 of the next month is the last of this one. */
export function daysInMonth(year, month) {
  return zone === 'utc'
    ? new Date(Date.UTC(year, month + 1, 0)).getUTCDate()
    : new Date(year, month + 1, 0).getDate();
}

const pad = (value) => String(value).padStart(2, '0');

/** `YYYY-MM-DD HH:mm` — what the picker fields hold, and what they will read back. */
export function formatStamp(ms) {
  const { year, month, day, hours, minutes } = partsOf(ms);
  return `${year}-${pad(month + 1)}-${pad(day)} ${pad(hours)}:${pad(minutes)}`;
}

/** Intl options carrying the zone, so every formatted string agrees with the parts above. */
const withZone = (options) => (zone === 'utc' ? { ...options, timeZone: 'UTC' } : options);

/** `14:23` — axis ticks and compact labels. */
export const formatClock = (ms) =>
  new Date(ms).toLocaleTimeString(undefined, withZone({ hour: '2-digit', minute: '2-digit' }));

/** `Aug 19` */
export const formatDay = (ms) =>
  new Date(ms).toLocaleDateString(undefined, withZone({ month: 'short', day: 'numeric' }));

/** `14:23:07` — a tooltip wants the seconds. */
export const formatTime = (ms) => new Date(ms).toLocaleTimeString(undefined, withZone({}));

/** The whole thing, for a tooltip or a notice. */
export const formatFull = (ms) => new Date(ms).toLocaleString(undefined, withZone({}));
