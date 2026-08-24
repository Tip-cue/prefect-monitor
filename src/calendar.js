/**
 * The month grid behind the date picker.
 *
 * Pure arithmetic, kept away from the DOM: the parts that go wrong in a calendar are the
 * ones you cannot see by looking at it — the padding on a month that starts on a Sunday,
 * February in a leap year, stepping from January back to December.
 *
 * Weeks start on Monday, and everything is local time, matching how the range fields are
 * written and read.
 */

/** Monday first, so the weekend sits together at the end. */
export const WEEKDAYS = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'];

const MONTHS = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];

export const monthLabel = (year, month) => `${MONTHS[month]} ${year}`;

/** How many days that month has — day 0 of the next month is the last of this one. */
export const daysInMonth = (year, month) => new Date(year, month + 1, 0).getDate();

/** Which column the 1st falls in, with Monday at 0. */
const firstColumn = (year, month) => (new Date(year, month, 1).getDay() + 6) % 7;

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
  const shifted = new Date(year, month + delta, 1);
  return { year: shifted.getFullYear(), month: shifted.getMonth() };
}

/**
 * An instant assembled from a day in a month and a time of day.
 *
 * Built through the Date constructor rather than by string, so a day that does not exist —
 * the 31st of a 30-day month, an hour skipped by a daylight-saving jump — resolves the way
 * the platform resolves it instead of parsing as garbage.
 */
export function instantOf({ year, month, day, hours = 0, minutes = 0 }) {
  return new Date(year, month, day, hours, minutes, 0, 0).getTime();
}

/** The month and day-of-month an instant falls in, for opening the calendar on it. */
export function partsOf(ms) {
  const at = new Date(ms);
  return {
    year: at.getFullYear(),
    month: at.getMonth(),
    day: at.getDate(),
    hours: at.getHours(),
    minutes: at.getMinutes(),
  };
}
