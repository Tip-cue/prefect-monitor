/**
 * The month grid behind the date picker.
 *
 * Pure arithmetic, kept away from the DOM: the parts that go wrong in a calendar are the
 * ones you cannot see by looking at it — the padding on a month that starts on a Sunday,
 * February in a leap year, stepping from January back to December.
 *
 * Weeks start on Monday, and every date question is asked of src/zone.js, so the grid is the
 * reader's own month or the UTC one depending on the setting — a calendar that disagreed with
 * the axis beside it would be worse than no calendar.
 */

import { daysInMonth as daysIn, instantOf as at, partsOf as splitOf, weekdayOf } from './zone.js';

/** Monday first, so the weekend sits together at the end. */
export const WEEKDAYS = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'];

const MONTHS = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];

export const monthLabel = (year, month) => `${MONTHS[month]} ${year}`;

/** How many days that month has. */
export const daysInMonth = (year, month) => daysIn(year, month);

/** Which column the 1st falls in, with Monday at 0. */
const firstColumn = (year, month) => (weekdayOf(year, month, 1) + 6) % 7;

/**
 * The month as rows of seven cells, `null` where the row runs past the month.
 *
 * @returns {(number|null)[][]} whole weeks, so a row is always seven cells wide
 */
export function monthGrid(year, month) {
  const cells = [
    ...Array.from({ length: firstColumn(year, month) }, () => null),
    ...Array.from({ length: daysInMonth(year, month) }, (unused, index) => index + 1),
  ];
  while (cells.length % 7 !== 0) cells.push(null);

  return Array.from({ length: cells.length / 7 }, (unused, row) => cells.slice(row * 7, row * 7 + 7));
}

/**
 * The month `delta` months away, carrying the year with it.
 *
 * @returns {{year: number, month: number}}
 */
export function shiftMonth(year, month, delta) {
  const shifted = splitOf(at({ year, month: month + delta, day: 1 }));
  return { year: shifted.year, month: shifted.month };
}

/**
 * An instant assembled from a day in a month and a time of day.
 *
 * Built through the Date constructor rather than by string, so a day that does not exist —
 * the 31st of a 30-day month, an hour skipped by a daylight-saving jump — resolves the way
 * the platform resolves it instead of parsing as garbage.
 */
export function instantOf(parts) {
  return at(parts);
}

/** The month and day-of-month an instant falls in, for opening the calendar on it. */
export function partsOf(ms) {
  return splitOf(ms);
}

/**
 * A time of day, held to a real one.
 *
 * Typed input reaches here as anything at all — empty, 99, -3 — and `instantOf` would
 * happily roll 99 hours into four days' time. Clamping keeps a mistyped hour on the day
 * that was actually clicked.
 */
export function clampTime(hours, minutes) {
  const hold = (value, max) => Math.min(max, Math.max(0, Number.isFinite(value) ? Math.trunc(value) : 0));
  return { hours: hold(hours, 23), minutes: hold(minutes, 59) };
}
