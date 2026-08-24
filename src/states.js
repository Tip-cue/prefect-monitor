/**
 * Prefect run states: naming, colour, severity.
 *
 * The colours approximate prefect-ui-library's own state palette so the dashboard
 * looks like the server it monitors.
 */

export const STATE_COLORS = {
  COMPLETED: '#2ac769',
  FAILED: '#fb4e4e',
  CRASHED: '#f97316',
  RUNNING: '#1d7cf2',
  SCHEDULED: '#fcd14e',
  LATE: '#e08504',
  PENDING: '#a8b8d2',
  PAUSED: '#c0cbd8',
  CANCELLED: '#737d8a',
  CANCELLING: '#94a0ae',
  RETRYING: '#f5a623',
};

const FALLBACK_COLOR = '#888888';

/**
 * States drawn with a diagonal hatch on top of their colour.
 *
 * Prefect's red and green are only ~5.5 ΔE apart under deuteranopia, which is well
 * below the readable threshold. The hatch means a failure is never distinguished by
 * colour alone — the legend and tooltip also always spell the state out.
 */
const HATCHED_STATES = new Set(['FAILED', 'CRASHED', 'CANCELLED']);

/**
 * Severity, worst first. Decides which state a bin reports when it holds several
 * runs, and which mark is drawn on top when marks overlap.
 */
const SEVERITY_ORDER = [
  'CRASHED',
  'FAILED',
  'CANCELLED',
  'PAUSED',
  'LATE',
  'RETRYING',
  'RUNNING',
  'PENDING',
  'SCHEDULED',
  'COMPLETED',
];

/** Display name of a run's state, e.g. "Completed" or "InfrastructurePending". */
export function stateName(run) {
  return run.state_name || run.state?.name || run.state_type || '?';
}

/**
 * Composite state names ("InfrastructurePending") have no colour of their own, so
 * they fall back to the colour of their state type ("PENDING").
 */
export function stateColor(run) {
  return (
    STATE_COLORS[stateName(run).toUpperCase()] ||
    STATE_COLORS[run.state_type] ||
    FALLBACK_COLOR
  );
}

export function isHatched(run) {
  return HATCHED_STATES.has(stateName(run).toUpperCase()) || HATCHED_STATES.has(run.state_type);
}

/** Lower is worse. Unknown states sort last. */
export function severity(run) {
  const index = SEVERITY_ORDER.indexOf(String(run.state_type || '').toUpperCase());
  return index < 0 ? SEVERITY_ORDER.length : index;
}

/** The most severe run in a non-empty list. */
export function worstRun(runs) {
  return runs.reduce((worst, run) => (severity(run) < severity(worst) ? run : worst));
}

/** Sorts a list of state names worst-first, then alphabetically. */
export function compareStateNames(a, b) {
  return (
    severity({ state_type: a.toUpperCase() }) - severity({ state_type: b.toUpperCase() }) ||
    a.localeCompare(b)
  );
}
