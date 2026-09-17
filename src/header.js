/**
 * The header bar, and the slice of the view every page shares.
 *
 * One implementation rendered into whichever page mounts it, rather than the same ninety
 * lines of markup in two files: those ids are the picker's contract with its wiring, so a
 * copy that drifts by one of them does not fail — it half-works, silently, in the half
 * nobody clicked that day.
 *
 * What lives here is what a page cannot disagree with the header about: the range, the
 * clock, the refresh interval, and the API it reads. Everything else — what to draw, and
 * what else belongs in the bar — the page passes in.
 */

import { popoverOffset } from './render.js';
import {
  QUICK_RANGES, DURATIONS, START_ANCHORS, DEFAULT_RANGE, MAX_RANGE_MS,
  anchorStart, capRange, composeRange, formatLocal,
  resolveRange, describeRange, matchQuickRange, parseTimeExpression, restampRange,
  sharedFromQuery, sharedToParams,
} from './time-range.js';
import { getZone, setZone, zoneLabel } from './zone.js';
import {
  WEEKDAYS, clampTime, instantOf, monthGrid, monthLabel, partsOf, shiftMonth,
} from './calendar.js';
import { apiUrlEditable, resolveApiUrl } from './settings.js';

/** What `config.js` said, if the deployment wrote one. */
const CONFIG = globalThis.PREFECT_MONITOR ?? {};
const API_URL_EDITABLE = apiUrlEditable({ config: CONFIG, hostname: location.hostname });

/** Grafana's ladder. "Off" is first so it is the safe default. */
export const REFRESH_INTERVALS = [
  { label: 'Off', ms: 0 },
  { label: '5s', ms: 5_000 },
  { label: '10s', ms: 10_000 },
  { label: '30s', ms: 30_000 },
  { label: '1m', ms: 60_000 },
  { label: '5m', ms: 5 * 60_000 },
  { label: '15m', ms: 15 * 60_000 },
  { label: '30m', ms: 30 * 60_000 },
  { label: '1h', ms: 60 * 60_000 },
];

/** Falls back to this when the current range cannot be read, which the default matches. */
const DEFAULT_SPAN_MS = 6 * 60 * 60 * 1000;

/**
 * Where the nav can go. Relative hrefs with the extension on them, because the server maps
 * `/` to index.html and nothing else — `/metrics` is a 404.
 */
export const PAGES = [
  { id: 'timeline', href: 'index.html', label: 'Timeline' },
  { id: 'metrics', href: 'metrics.html', label: 'Metrics' },
];

const $ = (selector) => document.querySelector(selector);

/** The three facts a page must not disagree with the header about, plus which page it is. */
const shell = {
  page: 'timeline',
  /** Range as expressions ("now-6h" → "now"), so a refresh keeps meaning the same. */
  range: { ...DEFAULT_RANGE },
  /** Auto-refresh interval label, "Off" when disabled. */
  refresh: 'Off',
  apiUrl: '',
};

let host = {};
let autoRefreshTimer = null;
let mounted = false;

/* --------------------------------- markup --------------------------------- */

/**
 * The bar, built once.
 *
 * `extras` is a slot rather than a list of sections the header knows about: the mode
 * buttons and the flow filter are the timeline's, and a header that enumerated them would
 * be coupled to a page it is meant to be independent of. The divider rides inside the slot,
 * or a page without extras shows two of them with nothing between.
 */
function headerHtml({ extras }) {
  return `
    <h1>Prefect Monitor</h1>
    <nav id="pageNav">${navHtml()}</nav>
    <input id="apiUrl" title="Prefect API URL" spellcheck="false">
    ${extras ? `<span class="sep"></span>${extras}` : ''}

    <span class="sep"></span>
    <div id="rangePicker">
      <button id="rangeButton" aria-haspopup="dialog" aria-expanded="false">
        <svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="9"/>
          <polyline points="12 7 12 12 16 14"/>
        </svg>
        <span id="rangeLabel">Last 6 hours</span>
      </button>
      <!-- Two ways in: the last N, or a start point and how long from there. No end time
           to fill in — it is the start plus the duration. -->
      <div id="rangePanel" role="dialog" aria-label="Time range" hidden>
        <div class="rangeColumn">
          <h2>Ending now</h2>
          <div id="quickRanges"></div>
        </div>
        <div class="rangeColumn hasCalendar">
          <h2>Starting at</h2>
          <div id="startAnchors"></div>
          <label for="rangeFrom">or a time of your own</label>
          <div class="withCalendar">
            <input id="rangeFrom" spellcheck="false" placeholder="2026-08-19 09:44">
            <button class="calendarToggle" data-field="rangeFrom" title="Pick a date"
                    aria-haspopup="dialog" aria-expanded="false"><svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="5" width="18" height="16" rx="2"/><line x1="3" y1="10" x2="21" y2="10"/><line x1="8" y1="3" x2="8" y2="7"/><line x1="16" y1="3" x2="16" y2="7"/></svg></button>
          </div>
          <div class="calendarHost" data-field="rangeFrom"></div>
        </div>
        <div class="rangeColumn hasCalendar">
          <h2>For</h2>
          <div id="durations"></div>
          <button id="untilNow">Until now</button>
          <label for="rangeTo">or an exact end</label>
          <div class="withCalendar">
            <input id="rangeTo" spellcheck="false" placeholder="2026-08-19 15:44">
            <button class="calendarToggle" data-field="rangeTo" title="Pick a date"
                    aria-haspopup="dialog" aria-expanded="false"><svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="5" width="18" height="16" rx="2"/><line x1="3" y1="10" x2="21" y2="10"/><line x1="8" y1="3" x2="8" y2="7"/><line x1="16" y1="3" x2="16" y2="7"/></svg></button>
          </div>
          <div class="calendarHost" data-field="rangeTo"></div>
          <p class="muted">
            Up to 24h. Longer falls back to 24h from whichever end you just set:
            back from an end, forward from a start.
          </p>
          <div id="rangeActions">
            <span id="rangeError" class="muted"></span>
          </div>
        </div>
      </div>
    </div>

    <!-- One calendar, moved into the host under whichever field opened it. -->
    <div id="calendar" role="dialog" aria-label="Pick a date" hidden></div>

    <button id="zoneToggle" title="Show times in UTC or in your own timezone">
      <svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="12" cy="12" r="9"/>
        <path d="M3 12h18M12 3a15 15 0 0 1 0 18a15 15 0 0 1 0-18"/>
      </svg>
      <span id="zoneLabel">Local</span>
    </button>

    <span class="sep"></span>
    <div id="refreshControl">
      <button id="refresh" title="Refresh now">
        <svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
          <path d="M20.5 12a8.5 8.5 0 1 1-2.6-6.1"/>
          <polyline points="20.5 4 20.5 9 15.5 9"/>
        </svg>
        Refresh
      </button>
      <button id="refreshIntervalButton" aria-haspopup="listbox" aria-expanded="false"
              title="Auto refresh interval">
        <span id="refreshIntervalLabel">Off</span>
        <svg class="icon chevron" viewBox="0 0 24 24" aria-hidden="true">
          <polyline points="7 10 12 15 17 10"/>
        </svg>
      </button>
      <div id="refreshMenu" role="listbox" aria-label="Auto refresh interval" hidden></div>
    </div>`;
}

function navHtml() {
  return PAGES
    .map((page) => `<a data-page="${page.id}" href="${page.href}">${page.label}</a>`)
    .join('');
}

/**
 * Re-stamps both links with the view as it stands.
 *
 * The range rides across the jump, so the other page opens on what you were looking at
 * rather than on the default. Written into the href rather than handled on click, which is
 * what keeps cmd-click and "open in a new tab" working.
 *
 * Only the shared params cross: `mode=agg` means nothing on a page with no chart, and a
 * `chain=<run-id>` carried over is a pop-up that can never open.
 */
function syncNav() {
  const search = query();
  document.querySelectorAll('#pageNav a').forEach((link) => {
    const page = PAGES.find((each) => each.id === link.dataset.page);
    link.href = page.href + search;
    link.classList.toggle('on', link.dataset.page === shell.page);
    link.toggleAttribute('aria-current', link.dataset.page === shell.page);
  });
}

/* --------------------------------- the view -------------------------------- */

/** The chosen auto-refresh interval in ms, 0 when it is off. */
function refreshMs() {
  return REFRESH_INTERVALS.find((option) => option.label === shell.refresh)?.ms ?? 0;
}

function params() {
  return sharedToParams({ range: shell.range, refresh: shell.refresh, zone: getZone() });
}

function query() {
  return `?${params()}`;
}

/**
 * Reads the shared half of the view out of a query string.
 *
 * `setZone` belongs here rather than in a page because the zone is a module-level
 * singleton and a module registry is per document: a second page starts out local however
 * the first one was left, and would render local times under a URL saying `tz=utc`. It runs
 * before anything is labelled, since every formatter reads the singleton.
 */
function readParams(search) {
  const { range, refresh, zone } = sharedFromQuery(search);
  setZone(zone ?? localStorage.zone ?? 'local');
  shell.range = range;
  shell.refresh = refresh ?? localStorage.refresh ?? 'Off';
}

/* -------------------------------- controls -------------------------------- */

/** @param {{from: string, to: string}} range expressions, not timestamps */
/**
 * @param {'start'|'end'} anchor which end to keep if the range is too long — the one just
 *   set. Capped here as well as when it is resolved, so the button never labels the chart
 *   with a week it is not showing.
 */
function setRange(range, anchor = 'end') {
  shell.range = capRange(range, Date.now(), anchor);
  syncHeader();
  // Before the page hears about it: whatever it does next, the panel should already be shut.
  closeRangePanel();
  host.onViewChange?.({ reason: 'range' });
}

function showRangeError(message) {
  $('#rangeError').textContent = message;
}

/* ----------------------------- auto refresh ------------------------------- */

/**
 * @param {{silent?: boolean}} options `silent` installs the timer without telling the page
 *   the view changed — for the mount, where the page has not finished reading its own half
 *   of the URL and would write one missing it.
 */
function setRefresh(label, { silent = false } = {}) {
  shell.refresh = localStorage.refresh = label;
  clearInterval(autoRefreshTimer);

  const chosen = REFRESH_INTERVALS.find((option) => option.label === label);
  // Wrapped: setInterval hands its callback a tick count, which load would read as options.
  if (chosen?.ms) autoRefreshTimer = setInterval(() => host.onReload?.({}), chosen.ms);

  syncHeader();
  if (!silent) host.onViewChange?.({ reason: 'refresh' });
  $('#refreshMenu').hidden = true;
  $('#refreshIntervalButton').setAttribute('aria-expanded', 'false');
}

function buildRefreshControl() {
  $('#refreshMenu').innerHTML = REFRESH_INTERVALS
    .map((option) => `<button data-label="${option.label}">${option.label}</button>`)
    .join('');

  document.querySelectorAll('#refreshMenu button').forEach((button) => {
    button.onclick = () => setRefresh(button.dataset.label);
  });

  $('#refreshIntervalButton').onclick = () => {
    const menu = $('#refreshMenu');
    menu.hidden = !menu.hidden;
    $('#refreshIntervalButton').setAttribute('aria-expanded', String(!menu.hidden));
  };

  document.addEventListener('click', (event) => {
    if (!$('#refreshMenu').hidden && !event.target.closest('#refreshControl')) {
      $('#refreshMenu').hidden = true;
      $('#refreshIntervalButton').setAttribute('aria-expanded', 'false');
    }
  });
}

/** Everything in the bar that both pages share. The page syncs its own extras. */
function syncHeader() {
  $('#rangeLabel').textContent = describeRange(shell.range);

  const active = matchQuickRange(shell.range);
  document.querySelectorAll('#quickRanges button').forEach((button) => {
    button.classList.toggle('on', Boolean(active) && button.dataset.from === active.from);
  });

  // A fixed start shows in the input; "the last N" leaves it empty, since there is nothing
  // to type — the start is wherever `now` happens to be.
  const startsAt = active ? null : parseTimeExpression(shell.range.from);
  $('#rangeFrom').value = startsAt === null ? '' : formatLocal(startsAt);

  // An end only shows when it is a fixed one: "ending now" has no timestamp to put there.
  const endsAt = active || shell.range.to === 'now' ? null : parseTimeExpression(shell.range.to);
  $('#rangeTo').value = endsAt === null ? '' : formatLocal(endsAt);

  // Empty means "wherever the range currently reaches", so say where that is rather than
  // showing a date baked into the markup.
  $('#rangeFrom').placeholder = formatLocal(fieldInstant('rangeFrom'));
  $('#rangeTo').placeholder = formatLocal(fieldInstant('rangeTo'));
  document.querySelectorAll('#startAnchors button').forEach((button) => {
    button.classList.toggle('on',
      startsAt !== null && startsAt === anchorStart(Number(button.dataset.days)));
  });

  // "Until now" and a fixed duration are alternatives, so exactly one of them is lit.
  const openEnded = startsAt !== null && shell.range.to === 'now';
  $('#untilNow').classList.toggle('on', openEnded);

  const span = rangeSpan();
  document.querySelectorAll('#durations button').forEach((button) => {
    button.classList.toggle('on', !openEnded && Number(button.dataset.ms) === span);
  });

  $('#zoneLabel').textContent = zoneLabel();
  $('#zoneToggle').classList.toggle('on', getZone() === 'utc');

  $('#refreshIntervalLabel').textContent = shell.refresh;
  $('#refreshIntervalButton').classList.toggle('on', shell.refresh !== 'Off');
  document.querySelectorAll('#refreshMenu button').forEach((button) => {
    button.classList.toggle('on', button.dataset.label === shell.refresh);
  });

  syncNav();
}

/* ------------------------------ range picker ------------------------------ */

function openRangePanel() {
  showRangeError('');
  const panel = $('#rangePanel');
  panel.hidden = false;
  $('#rangeButton').setAttribute('aria-expanded', 'true');

  // Measured once it is laid out: the panel is wider than the button, and where the button
  // sits depends on what else the header is showing.
  panel.style.left = `${popoverOffset({
    pickerLeft: $('#rangePicker').getBoundingClientRect().left,
    panelWidth: panel.offsetWidth,
    viewportWidth: window.innerWidth,
  })}px`;
}

function closeRangePanel() {
  closeCalendar();
  $('#rangePanel').hidden = true;
  $('#rangeButton').setAttribute('aria-expanded', 'false');
}

/** The span currently selected, so picking a start keeps the duration and vice versa. */
function rangeSpan() {
  const resolved = resolveRange(shell.range);
  return resolved ? Math.min(resolved.to - resolved.from, MAX_RANGE_MS) : DEFAULT_SPAN_MS;
}

/**
 * What one of the timestamp fields says.
 *
 * @returns {number|null|NaN} null when it is empty, NaN when it holds something that
 *   cannot be read — a difference that matters, since empty means "use the other end"
 *   and unreadable has to be reported.
 */
function typedTime(selector) {
  const text = $(selector).value.trim();
  if (!text) return null;

  const ms = parseTimeExpression(text);
  return ms === null ? NaN : ms;
}

/**
 * Applies whatever the panel now says, from the piece that was just chosen.
 *
 * Any one of the three — start, end, duration — can be set at a time, and the others keep
 * what they had, so there is no order to learn and nothing to confirm. Which two are
 * present decides the third:
 *
 *   start + end        exactly that, capped towards `anchor`
 *   start + duration   start, running on for that long
 *   end + duration     that long, ending there
 *   duration alone     the last N, still ending now
 *
 * @param {{startMs?: number, endMs?: number, durationMs?: number, untilNow?: boolean,
 *   anchor?: 'start'|'end'}} choice `untilNow` runs the window on from the start rather
 *   than ending it a fixed span later, which is what you want for today: the chart then
 *   keeps up as runs come in.
 */
function applyRange({ startMs, endMs, durationMs, untilNow = false, anchor = 'end' } = {}) {
  const start = startMs ?? typedTime('#rangeFrom');
  const end = endMs ?? typedTime('#rangeTo');

  if (Number.isNaN(start) || Number.isNaN(end)) {
    showRangeError(`unreadable ${Number.isNaN(start) ? 'start' : 'end'} time`);
    return;
  }

  if (untilNow) {
    if (start === null) {
      showRangeError('pick a start point first');
      return;
    }
    // Capping is silent everywhere else, but here it throws away the start just clicked.
    if (Date.now() - start > MAX_RANGE_MS) showRangeError('more than 24h ago — showing the last 24h');
    setRange({ from: formatLocal(start), to: 'now' });
    return;
  }

  const range = composeRange({
    startMs: start,
    endMs: end,
    durationMs: durationMs ?? null,
    spanMs: rangeSpan(),
  });

  if (!resolveRange(range)) {
    showRangeError('ends before it starts');
    return;
  }
  setRange(range, anchor);
}

/**
 * The instant a timestamp field stands for: what it holds, or — while it is empty, which is
 * what a relative range leaves it — where that range currently starts or ends.
 *
 * So the calendar opens on the range you are looking at rather than on today, and an empty
 * field hints at the boundary it would replace rather than at a date typed into the markup
 * once and left there.
 */
function fieldInstant(field) {
  const typed = parseTimeExpression($(`#${field}`).value.trim());
  if (typed !== null) return typed;

  const resolved = resolveRange(shell.range);
  if (!resolved) return Date.now();
  return field === 'rangeFrom' ? resolved.from : resolved.to;
}

/**
 * Which field the calendar is editing, the month it is showing, and the time it will apply.
 *
 * The time lives here rather than being read back out of the inputs, because redrawing —
 * stepping to another month — replaces them. Reading the field instead meant every redraw,
 * including the one on a stepper click, snapped the time back to what the field said.
 */
let calendar = null;

/**
 * Draws the calendar for whichever field opened it.
 *
 * In flow beneath that field rather than floating: the panel simply grows, so there is no
 * second popover to keep inside the window — a problem the range panel itself needed
 * measuring to solve.
 */
function drawCalendar() {
  if (!calendar) return;
  const { year, month, day: picked, hours, minutes } = calendar;

  // The highlight follows what has been clicked, not what the field still says: a day is
  // chosen first and confirmed after, so the two disagree in between.
  const isSelected = (day) => day === picked
    && year === calendar.year && month === calendar.month;

  const rows = monthGrid(year, month).map((week) => `<tr>${week.map((day) => (
    day === null
      ? '<td></td>'
      : `<td><button data-day="${day}" class="${isSelected(day) ? 'on' : ''}">${day}</button></td>`
  )).join('')}</tr>`).join('');

  $('#calendar').innerHTML = `
    <div id="calendarHead">
      <button data-step="-1" title="Previous month">&#8249;</button>
      <b>${monthLabel(year, month)}</b>
      <button data-step="1" title="Next month">&#8250;</button>
    </div>
    <table>
      <thead><tr>${WEEKDAYS.map((day) => `<th>${day}</th>`).join('')}</tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <div id="calendarTime">
      time
      <input id="calendarHours" type="number" min="0" max="23" value="${hours}">
      :
      <input id="calendarMinutes" type="number" min="0" max="59" step="5" value="${minutes}">
      <button id="calendarApply" title="Use this date and time">&#10003;</button>
    </div>`;
}

/** Opens the calendar under `field`, on the month that field is already showing. */
function openCalendar(field) {
  const at = partsOf(fieldInstant(field));
  calendar = {
    field, year: at.year, month: at.month, day: at.day, hours: at.hours, minutes: at.minutes,
  };

  const host = document.querySelector(`.calendarHost[data-field="${field}"]`);
  host.appendChild($('#calendar'));
  $('#calendar').hidden = false;
  document.querySelectorAll('.calendarToggle').forEach((button) => {
    button.setAttribute('aria-expanded', String(button.dataset.field === field));
  });
  drawCalendar();
}

function closeCalendar() {
  calendar = null;
  $('#calendar').hidden = true;
  document.querySelectorAll('.calendarToggle').forEach((button) => {
    button.setAttribute('aria-expanded', 'false');
  });
}

/** Writes the picked instant into the field and applies it, as clicking an anchor does. */
function applyCalendar() {
  const { year, month, day } = calendar;
  const { hours, minutes } = clampTime(calendar.hours, calendar.minutes);
  const picked = instantOf({ year, month, day, hours, minutes });

  $(`#${calendar.field}`).value = formatLocal(picked);
  // The field just edited is the one to keep if the pair is longer than a day.
  applyRange({ anchor: calendar.field === 'rangeFrom' ? 'start' : 'end' });
}

function buildCalendar() {
  document.querySelectorAll('.calendarToggle').forEach((button) => {
    button.onclick = () => {
      if (calendar?.field === button.dataset.field) closeCalendar();
      else openCalendar(button.dataset.field);
    };
  });

  $('#calendar').addEventListener('click', (event) => {
    const stepper = event.target.closest('[data-step]');
    if (stepper) {
      calendar = { ...calendar, ...shiftMonth(calendar.year, calendar.month, Number(stepper.dataset.step)) };
      drawCalendar();
      return;
    }

    const day = event.target.closest('[data-day]');
    if (day) {
      // Chosen, not applied: the time may still need setting, and applying here would close
      // the panel before it could be.
      calendar.day = Number(day.dataset.day);
      drawCalendar();
      return;
    }

    if (event.target.closest('#calendarApply')) applyCalendar();
  });

  // Enter is the same as the tick: a time typed rather than stepped should not need the mouse.
  $('#calendar').addEventListener('keydown', (event) => {
    if (event.key === 'Enter') applyCalendar();
  });

  // Remembered, not redrawn. Redrawing here replaced the very input being clicked, so a
  // stepper set the value and then lost it in the same breath.
  $('#calendar').addEventListener('input', (event) => {
    if (event.target.id === 'calendarHours') calendar.hours = Number(event.target.value);
    if (event.target.id === 'calendarMinutes') calendar.minutes = Number(event.target.value);
  });
}

function buildRangePicker() {
  $('#quickRanges').innerHTML = QUICK_RANGES
    .map((quick) => `<button data-from="${quick.from}">${quick.label}</button>`)
    .join('');
  $('#startAnchors').innerHTML = START_ANCHORS
    .map((anchor) => `<button data-days="${anchor.days}">${anchor.label}`
      + ` <span class="muted">00:00</span></button>`)
    .join('');
  $('#durations').innerHTML = DURATIONS
    .map((duration) => `<button data-ms="${duration.ms}">${duration.label}</button>`)
    .join('');

  document.querySelectorAll('#quickRanges button').forEach((button) => {
    button.onclick = () => setRange({ from: button.dataset.from, to: 'now' });
  });
  document.querySelectorAll('#startAnchors button').forEach((button) => {
    // A start point replaces the start and keeps the span, rather than keeping an end that
    // may now be before it: 7 days ago → 3 days ago used to invert the range and fail.
    button.onclick = () => applyRange({
      startMs: anchorStart(Number(button.dataset.days)),
      durationMs: rangeSpan(),
      anchor: 'start',
    });
  });
  document.querySelectorAll('#durations button').forEach((button) => {
    button.onclick = () => applyRange({ durationMs: Number(button.dataset.ms) });
  });
  $('#untilNow').onclick = () => applyRange({ untilNow: true });

  $('#rangeButton').onclick = () => {
    if ($('#rangePanel').hidden) openRangePanel();
    else closeRangePanel();
  };
  // Editing one end keeps it and moves the other, if the pair is longer than a day.
  $('#rangeFrom').onkeydown = (event) => {
    if (event.key === 'Enter') applyRange({ anchor: 'start' });
  };
  $('#rangeTo').onkeydown = (event) => {
    if (event.key === 'Enter') applyRange({ anchor: 'end' });
  };

  // Clicking anywhere else closes it, like every other popover.
  document.addEventListener('click', (event) => {
    if (!$('#rangePanel').hidden && !event.target.closest('#rangePicker')) closeRangePanel();
  });
}

/* ---------------------------------- mount ---------------------------------- */

/**
 * Renders the bar into the page's `<header>` and returns the handle it reads back through.
 *
 * Options in and a controller out, rather than events: the page reads `range` and
 * `refreshMs()` on every load and calls `showRangeError` back — traffic an event only
 * models one direction of. There is exactly one host per document, and it must handle every
 * notification, which is what a listener list is wrong for.
 *
 * `#calendar` is re-parented into whichever field opened it, so rendering twice would orphan
 * it. Once per document, and the guard says so rather than leaving a stale bar behind.
 *
 * @param {{page: string, extras?: string, search?: string,
 *   onViewChange?: (change: {reason: 'range'|'refresh'|'zone'}) => void,
 *   onReload?: (options: {force?: boolean}) => void,
 *   onApiUrlChange?: (url: string) => void}} options
 */
export function mountHeader({
  page, extras = '', search = window.location.search,
  onViewChange, onReload, onApiUrlChange,
} = {}) {
  if (mounted) throw new Error('the header mounts once per document');
  mounted = true;

  host = { onViewChange, onReload, onApiUrlChange };
  shell.page = page;
  shell.apiUrl = resolveApiUrl({
    config: CONFIG,
    hostname: location.hostname,
    storedUrl: localStorage.apiUrl,
  });

  readParams(search); // sets the zone before anything is labelled
  document.querySelector('header').innerHTML = headerHtml({ extras });

  buildRangePicker();
  buildCalendar();
  buildRefreshControl();

  $('#apiUrl').value = shell.apiUrl;
  // Nothing to choose when the API is next to us, or when a deployment pinned it: the field
  // is hidden rather than shown filled in and ignored.
  $('#apiUrl').hidden = !API_URL_EDITABLE;
  if (API_URL_EDITABLE) {
    $('#apiUrl').onchange = () => {
      shell.apiUrl = localStorage.apiUrl = $('#apiUrl').value;
      onApiUrlChange?.(shell.apiUrl);
    };
  }

  $('#refresh').onclick = () => host.onReload?.({ force: true });

  // A display setting: nothing is refetched, everything is relabelled. The range keeps its
  // meaning too — "now-6h" is the same six hours whichever clock names them.
  $('#zoneToggle').onclick = () => {
    // Resolved on the old clock, rewritten for the new one: a range fixed at 11:00 in
    // +03:00 becomes 08:00 in UTC and covers the same runs, rather than jumping by the
    // offset because a bare stamp is read in whichever zone is current.
    const showing = resolveRange(shell.range);

    localStorage.zone = setZone(getZone() === 'utc' ? 'local' : 'utc');
    if (showing) shell.range = restampRange(shell.range, showing);

    syncHeader();
    host.onViewChange?.({ reason: 'zone' });
  };

  // Silent: the page has not written its own half of the URL yet, and a notification here
  // would have it replace the entry with one missing everything it has not read.
  setRefresh(shell.refresh, { silent: true });

  return {
    get range() { return shell.range; },
    get refresh() { return shell.refresh; },
    get apiUrl() { return shell.apiUrl; },
    refreshMs,
    params,
    query,
    sync: syncHeader,
    showRangeError,
    readParams,
  };
}
