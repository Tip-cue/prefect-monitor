/** Run timestamps, durations, and time-axis ticks. */

import { formatClock, formatDay } from './zone.js';

const SECOND = 1000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/**
 * When a run began. A scheduled or cancelled run may never have started, so fall
 * back to when it was meant to, and finally to when it was created.
 */
export function runStart(run) {
  return new Date(run.start_time || run.expected_start_time || run.created).getTime();
}

/**
 * When a run ended. A run still going is drawn up to now; anything else without an
 * end time (cancelled before it started) gets a nominal one second so it stays visible.
 */
export function runEnd(run) {
  if (run.end_time) return new Date(run.end_time).getTime();
  if (run.state_type === 'RUNNING') return Date.now();
  return runStart(run) + SECOND;
}

/**
 * Whether a run actually occupies any part of [from, to].
 *
 * Needed because runs are fetched on `expected_start_time`, which is when a run was
 * *meant* to begin. A run scheduled at 09:54 that only started at 09:55:43 matches a
 * window ending 09:55 but belongs outside it, and used to be drawn as a 4px sliver
 * pinned past the right edge.
 */
export function overlapsWindow(run, from, to) {
  return runEnd(run) >= from && runStart(run) <= to;
}

export function formatDuration(ms) {
  if (ms < MINUTE) return `${Math.round(ms / SECOND)}s`;
  if (ms < HOUR) return `${Math.round(ms / MINUTE)}m`;
  if (ms < DAY) return `${(ms / HOUR).toFixed(1)}h`;
  return `${(ms / DAY).toFixed(1)}d`; // no run lasts this long; a skipped span can
}

const TICK_STEPS = [
  MINUTE, 2 * MINUTE, 5 * MINUTE, 10 * MINUTE, 15 * MINUTE, 30 * MINUTE,
  HOUR, 2 * HOUR, 3 * HOUR, 6 * HOUR, 12 * HOUR,
  DAY, 2 * DAY, 7 * DAY,
];

const MAX_TICKS = 10;

/** Round tick timestamps covering [from, to], at most MAX_TICKS of them. */
export function timeTicks(from, to) {
  const step = TICK_STEPS.find((candidate) => (to - from) / candidate <= MAX_TICKS)
    ?? TICK_STEPS[TICK_STEPS.length - 1];

  const ticks = [];
  for (let t = Math.ceil(from / step) * step; t <= to; t += step) ticks.push(t);
  return ticks;
}

/** Windows longer than two days need the date, shorter ones only the clock. */
export function formatTick(timestamp, windowMs) {
  const clock = formatClock(timestamp);
  if (windowMs <= 2 * DAY) return clock;

  return `${formatDay(timestamp)} ${clock}`;
}
