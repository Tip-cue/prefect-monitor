/** Wires the DOM to the Prefect API and the renderer. */

import { PrefectApi, BATCH_PAYLOAD_KEYS } from './prefect-api.js';
import { runStart, runEnd, formatDuration } from './time.js';
import {
  filterToChainsWithState, inferredLinks, runChain, sequentialChainLinks,
} from './links.js';
import {
  renderTimeline, legendHtml, tooltipHtml, tooltipPosition, popoverOffset, escapeHtml,
} from './render.js';
import {
  QUICK_RANGES, DURATIONS, START_ANCHORS, DEFAULT_RANGE, MAX_RANGE_MS,
  anchorStart, capRange, composeRange, formatLocal,
  resolveRange, describeRange, matchQuickRange, parseTimeExpression, restampRange,
  viewFromQuery, viewToQuery,
} from './time-range.js';
import {
  CACHE_FRESH_MS, planFetch, unsettledRunIds, mergeRuns, mergeLinks,
} from './run-cache.js';
import {
  DRAG_THRESHOLD_PX, clampZoom, thumbGeometry, zoomFromDrag, zoomFromThumb,
} from './zoom.js';
import { readCache, writeCache, clearCache } from './browser-cache.js';
import { apiUrlEditable, resolveApiUrl, resolveBatchKeys } from './settings.js';
import { formatFull, getZone, setZone, zoneLabel } from './zone.js';
import {
  WEEKDAYS, clampTime, instantOf, monthGrid, monthLabel, partsOf, shiftMonth,
} from './calendar.js';

/**
/** What `config.js` said, if the deployment wrote one. */
const CONFIG = globalThis.PREFECT_MONITOR ?? {};

const API_URL_EDITABLE = apiUrlEditable({ config: CONFIG, hostname: location.hostname });
const BATCH_KEYS = resolveBatchKeys(CONFIG, BATCH_PAYLOAD_KEYS);

/** Every client is built the same way, so config applies wherever the server changes. */
const clientFor = (baseUrl) => new PrefectApi(baseUrl, { batchKeys: BATCH_KEYS });

/** Grafana's ladder. "Off" is first so it is the safe default. */
const REFRESH_INTERVALS = [
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
const RESIZE_DEBOUNCE_MS = 150;

/** Falls back to this when the current range cannot be read, which the default matches. */
const DEFAULT_SPAN_MS = 6 * 60 * 60 * 1000;

const $ = (selector) => document.querySelector(selector);

const state = {
  mode: 'graph',
  /** Range as expressions ("now-6h" → "now"), so a refresh keeps meaning the same. */
  range: { ...DEFAULT_RANGE },
  /** @type {{runs, edges, exactLinks, from: number, to: number}|null} */
  view: null,
  flowNames: new Map(),
  /** Legend filter: show only pipelines containing a run in one of these states. */
  selectedStates: new Set(),
  /** Shift-clicked run whose chain is isolated, if any. */
  isolatedRunId: null,
  /** Auto-refresh interval label, "Off" when disabled. */
  refresh: 'Off',
  /** Which pop-up is open, mirrored in the URL: { kind, id } or null. */
  modal: null,
  /**
   * Dragged-out slice of the range being looked at, or null for all of it. Absolute, and
   * never replaces the range: the picker still says "Last 6 hours", so however deep you
   * zoom, one click on the bar's ✕ is the way back.
   */
  zoom: null,
};

let api = clientFor(resolveApiUrl({
  config: CONFIG,
  hostname: location.hostname,
  storedUrl: localStorage.apiUrl,
}));
let autoRefreshTimer = null;
let resizeTimer = null;

const flowName = (flowId) => state.flowNames.get(flowId) || String(flowId).slice(0, 8);

/** The chosen auto-refresh interval in ms, 0 when it is off. */
const refreshMs = () => REFRESH_INTERVALS.find((option) => option.label === state.refresh)?.ms ?? 0;

/* ------------------------------ loading data ------------------------------ */

/** Reads the view out of the URL so a refresh — or a pasted link — restores it. */
function readUrl() {
  const { range, mode, states, refresh, modal, zoom, zone } = viewFromQuery(window.location.search);
  setZone(zone ?? localStorage.zone ?? 'local');
  state.range = range;
  state.mode = mode ?? localStorage.mode ?? 'graph';
  state.selectedStates = new Set(states);
  state.refresh = refresh ?? localStorage.refresh ?? 'Off';
  state.modal = modal;
  state.zoom = zoom;
}

/**
 * Mirrors the view into the URL.
 *
 * Replace by default, so changing a range or a filter does not fill the history. But
 * *push* when a pop-up opens: clicking a run navigates to Prefect in this tab, and Back
 * should return to the pop-up that was open rather than to a bare chart.
 */
function writeUrl({ push = false } = {}) {
  const query = viewToQuery({
    range: state.range,
    mode: state.mode,
    states: [...state.selectedStates],
    refresh: state.refresh,
    modal: state.modal,
    zoom: state.zoom,
    zone: getZone(),
  });
  if (push) window.history.pushState(null, '', query);
  else window.history.replaceState(null, '', query);
}

/**
 * @param {{force?: boolean}} options `force` skips the cache-freshness check, for the
 *   Refresh button: clicking it and being handed data from a few seconds ago reads as the
 *   button not working.
 */
/**
 * Event links plus whatever can be assumed for the runs they do not explain.
 *
 * Derived at every paint rather than stored: the cache holds event links only, so the
 * instant-paint path used to show none at all — on an old window, where every link is
 * assumed, the chart lost its lines a few seconds after loading them. Too slow to put in
 * draw() (~130ms for 2000 runs, and draw runs on every frame of a zoom-pan), so it happens
 * once per load, in both places a load can paint from.
 */
function withAssumedLinks(runs, edges, eventLinks) {
  const guessed = inferredLinks(runs, { edges, batchKeys: BATCH_KEYS, exactLinks: eventLinks });

  return {
    links: [...eventLinks, ...guessed],
    assumed: {
      label: guessed.filter((link) => link.basis === 'label').length,
      timing: guessed.filter((link) => link.basis === 'timing').length,
    },
  };
}

async function load({ force = false } = {}) {
  $('#err').style.display = 'none';
  const resolved = resolveRange(state.range);
  if (!resolved) {
    showRangeError('that range does not resolve');
    return;
  }
  const { from, to } = resolved;
  const modalWasOpen = $('#modal').style.display === 'flex';

  const cache = readCache(api.baseUrl);
  // A 5s auto-refresh means 5s, not "every other tick, because the cache was still fresh".
  const plan = planFetch(cache, from, to, Date.now(), force ? 0 : refreshMs() || CACHE_FRESH_MS);

  // Paint what is already known before going to the network. On a reload or a refresh
  // tick this is the whole window, so the chart is there immediately.
  if (plan.drawCached) {
    state.flowNames = new Map(cache.flows.map((flow) => [flow.id, flow.name]));
    const cachedEdges = cache.edges ?? [];
    const { links, assumed } = withAssumedLinks(cache.runs, cachedEdges, cache.links);
    state.view = {
      runs: cache.runs, edges: cachedEdges, exactLinks: links, assumed,
      from, to, linkError: null, linksFrom: cache.linksFrom, runsFrom: cache.from,
      asOf: cache.fetchedAt,
    };
    draw();
    restoreModalFromUrl();
  }
  if (plan.mode === 'fresh') return; // cached and recent enough to leave alone

  let linkError = null;
  document.body.classList.add('loading');
  try {
    const fetchFrom = plan.mode === 'incremental' ? plan.since : from;

    const [runResult, refreshedRuns, flows, linkResult] = await Promise.all([
      // Overlapping, not just started-inside: a run that began before the window but
      // ran into it belongs on the chart.
      api.fetchRunsOverlapping(fetchFrom, to),
      // The only cached runs that can have changed are the ones still in flight.
      plan.mode === 'incremental' ? api.fetchRunsByIds(unsettledRunIds(cache, from)) : [],
      api.fetchFlows(),
      // The server's own record of which run each automation started, and why.
      // Links are only ever drawn from this, so a failure here means no links at
      // all — which must be said out loud, not left looking like "no links exist".
      api.fetchAutomationRunLinks(fetchFrom, to).catch((error) => {
        linkError = error.message;
        console.warn('automation events unavailable, so no links can be drawn', error);
        return { links: [], coveredFrom: fetchFrom };
      }),
    ]);
    const edges = await api.fetchFlowEdges(flows);

    const runs = plan.mode === 'incremental'
      ? mergeRuns(cache.runs, [...runResult.runs, ...refreshedRuns], from)
      : runResult.runs;
    const eventLinks = plan.mode === 'incremental'
      ? mergeLinks(cache.links, linkResult.links)
      : linkResult.links;

    // Where the events do not reach, assume what can be assumed — never over an event.
    const { links: exactLinks, assumed } = withAssumedLinks(runs, edges, eventLinks);

    // The earliest instant runs are known to be complete from. An incremental fetch only
    // read the leading slice, so coverage is whatever the cache had — but no earlier than
    // `from`, since mergeRuns just dropped everything that ended before it.
    const runsFrom = plan.mode === 'incremental'
      ? Math.max(cache.from, from)
      : runResult.coveredFrom;

    // How far back links are known — reported by the fetch, not assumed, because a wide
    // window exceeds the event page budget and reaches only part way back. An incremental
    // fetch read only the leading slice and so says nothing about older coverage: the
    // cached links are still held, and with them the reach they were fetched with.
    const linksFrom = plan.mode === 'incremental'
      ? cache.linksFrom ?? from
      : linkResult.coveredFrom;

    // Nothing to draw links from: ask whether the server has any events left for this
    // window, so the legend can say which of the two reasons it is.
    const eventsPruned = exactLinks.length === 0 && !linkError
      ? !await api.hasEventsIn(from, to).catch(() => true)
      : false;

    state.flowNames = new Map(flows.map((flow) => [flow.id, flow.name]));
    state.view = {
      runs, edges, exactLinks, from, to, linkError, linksFrom, runsFrom,
      eventsPruned, assumed, asOf: Date.now(),
    };
    draw();
    await restoreModalFromUrl({ rerender: modalWasOpen });

    // The span actually covered, not the one asked for: a relative range keeps sliding,
    // and a window past the run cap reaches only part way back.
    writeCache(api.baseUrl, {
      from: runsFrom,
      to,
      fetchedAt: Date.now(),
      runs,
      links: eventLinks,
      flows,
      edges,
      linksFrom,
      batchKeys: BATCH_KEYS,
    });
  } catch (error) {
    showError(error);
  } finally {
    document.body.classList.remove('loading');
  }
}

/**
 * Draws the pop-up named in the URL: opening it after a reload or a Back from Prefect, and
 * on `rerender`, redrawing one that is already open. Runs not in the loaded window are
 * fetched by id rather than given up on.
 *
 * A pop-up used to be fetched once when opened and then left alone, so a running sub-flow
 * stayed running on screen however long the auto-refresh ran behind it.
 */
async function restoreModalFromUrl({ rerender = false } = {}) {
  const wanted = state.modal;
  if (!wanted) return;
  if (!rerender && $('#modal').style.display === 'flex') return;

  const runs = state.view?.runs ?? [];
  const chart = $('#chart');

  if (wanted.kind === 'subflowsOf') {
    const laneRuns = runs.filter((run) => run.flow_id === wanted.id);
    if (laneRuns.length === 0) return;
    showSubflows(laneRuns, `Sub-flows of ${flowName(wanted.id)} (${laneRuns.length} runs)`,
      { remember: false });
    return;
  }

  let run = runs.find((each) => each.id === wanted.id);
  if (!run) [run] = await api.fetchRunsByIds([wanted.id]).catch(() => []);
  if (!run) return; // the run is gone; leave the chart as it is

  if (wanted.kind === 'chain') showChainWindow(chart, run, { remember: false });
  else {
    showSubflows([run], `Sub-flows of ${flowName(run.flow_id)} / ${run.name}`, { remember: false });
  }
}

/**
 * The span actually on screen: the range asked for, but starting no earlier than the runs
 * reach. On 7d that is the most recent day — plotted against a 7d axis, the six unread days
 * read as an outage rather than as the gap in the data that they are. runNotice says so.
 *
 * The link-coverage notice measures against this too, since a stretch that is not drawn
 * cannot be missing links.
 */
function drawnFrom() {
  const { from, runsFrom } = state.view ?? {};
  return Math.max(from ?? 0, runsFrom ?? 0);
}

/**
 * The span the chart is showing: everything loaded, or the dragged-out slice of it.
 *
 * Re-clamped on every draw rather than once when it is set, because the range underneath
 * keeps moving — a relative range slides on each refresh.
 */
function drawnWindow() {
  const loaded = { from: drawnFrom(), to: state.view?.to ?? 0 };
  state.zoom = clampZoom(state.zoom, loaded.from, loaded.to);

  return { loaded, window: state.zoom ?? loaded };
}

/** Re-render from what is already loaded — filtering and mode changes need no refetch. */
function draw() {
  if (!state.view) return;
  const { runs, edges, exactLinks } = state.view;
  const { loaded, window: shown } = drawnWindow();

  state.isolatedRunId = null;
  $('#legend').innerHTML = legendHtml(runs, state.selectedStates)
    + runNotice() + linkNotice() + asOfNotice();
  renderTimeline($('#chart'), filterByState(runs), {
    from: shown.from,
    to: shown.to,
    edges,
    exactLinks,
    aggregated: state.mode === 'agg',
    flowName,
  });
  drawZoomBar(loaded);
}

/**
 * The zoom bar: the whole loaded range as a track, the part on screen as a thumb, lined up
 * under the plot so the thumb sits beneath what it refers to. Hidden when not zoomed —
 * a full-width thumb and a ✕ that does nothing are just noise.
 */
function drawZoomBar(loaded) {
  const bar = $('#zoomBar');
  const plot = $('#chart').timelinePlot;

  if (!state.zoom || !plot) {
    bar.hidden = true;
    return;
  }

  bar.hidden = false;
  const track = $('#zoomTrack');
  track.style.marginLeft = `${plot.left}px`; // line the start of the track up with the plot

  // Measured, not assumed: the track takes what the row has left, which depends on the
  // label's text. Reading the width forces layout, so this is the width just applied.
  const { left, width } = thumbGeometry(loaded, state.zoom, trackWidth());
  const thumb = $('#zoomThumb');
  thumb.style.left = `${left}px`;
  thumb.style.width = `${width}px`;

  $('#zoomLabel').textContent = `${formatDuration(state.zoom.to - state.zoom.from)} of `
    + `${formatDuration(loaded.to - loaded.from)}`;
}

/** The zoom bar's track, as laid out — the thumb and any pan have to agree with it. */
function trackWidth() {
  return $('#zoomTrack').getBoundingClientRect().width;
}

/** Applies a zoom (or null to clear it) — presentation only, so no refetch. */
function setZoom(zoom) {
  state.zoom = zoom;
  writeUrl();
  draw();
}

/**
 * Says so when the range holds more runs than one fetch will read, since the axis then
 * starts later than the range asked for. Unlike the links, this does not fill in on later
 * refreshes: there is no more room for it, only a narrower range.
 */
function runNotice() {
  const { runsFrom, from } = state.view ?? {};
  if (!runsFrom || !from || runsFrom <= from + 60_000) return '';

  const skipped = formatDuration(runsFrom - from);
  return '<span class="chip" style="color:#e0c48a">⚠ too many runs to load the whole range'
    + ` · showing from ${formatFull(runsFrom)}, the ${skipped} before that`
    + ' is not drawn</span>';
}

/** Cached data must never look live, so the legend row says when it was fetched. */
function asOfNotice() {
  const asOf = state.view?.asOf;
  if (!asOf) return '';

  const secondsAgo = Math.round((Date.now() - asOf) / 1000);
  if (secondsAgo < 3) return '';
  const age = secondsAgo < 90 ? `${secondsAgo}s ago` : `${Math.round(secondsAgo / 60)}m ago`;
  return `<span class="chip muted" title="${formatFull(asOf)}">as of ${age}</span>`;
}

function linkNotice() {
  const { exactLinks, linkError, linksFrom, eventsPruned, assumed } = state.view ?? {};
  const inferredCount = (assumed?.label ?? 0) + (assumed?.timing ?? 0);
  const from = drawnFrom();

  if (linksFrom && from && linksFrom > from + 60_000) {
    return '<span class="chip" style="color:#e0c48a">⚠ links only reach back to'
      + ` ${formatFull(linksFrom)} · earlier runs are drawn unlinked</span>`;
  }

  if (linkError) {
    return `<span class="chip" style="color:#f3b8b8">⚠ no links: automation events
            unavailable (${escapeHtml(linkError)})</span>`;
  }
  if (inferredCount > 0) {
    const how = [
      assumed.label ? `${assumed.label} matched by batch label` : '',
      assumed.timing ? `${assumed.timing} assumed from timing, drawn dashed` : '',
    ].filter(Boolean).join(' · ');

    return `<span class="chip muted">no events for these runs: ${how}</span>`;
  }
  if (exactLinks?.length === 0) {
    // Runs outlive the events that explain them, so an old window has no links to draw and
    // never will. Worth distinguishing from a window where simply nothing was triggered.
    return eventsPruned
      ? '<span class="chip muted">no links: the server has no events left for this window,'
        + ' so what triggered these runs is no longer recorded</span>'
      : '<span class="chip muted">no run links in this window — no automation fired</span>';
  }
  return '';
}

const filterByState = (runs) =>
  filterToChainsWithState(runs, state.selectedStates, state.view?.exactLinks ?? []);

function toggleStateFilter(stateLabel) {
  if (!stateLabel) state.selectedStates.clear();
  else if (state.selectedStates.has(stateLabel)) state.selectedStates.delete(stateLabel);
  else state.selectedStates.add(stateLabel);
  writeUrl();
  draw();
}

function showError(error) {
  $('#err').textContent =
    `Failed to query Prefect API at ${api.baseUrl} — ${error.message}. ` +
    'Check the URL / port-forward, and CORS (PREFECT_SERVER_API_CORS_ALLOWED_ORIGINS).';
  $('#err').style.display = 'block';
}

/* --------------------------------- modal ---------------------------------- */

function openModal(titleHtml) {
  const wasOpen = $('#modal').style.display === 'flex';
  $('#modal').style.display = 'flex';
  $('#modalTitle').innerHTML = titleHtml;

  // Only blank it when it is actually opening. A refresh tick re-renders an open pop-up,
  // and clearing it first would flash "loading…" over the chart every few seconds.
  if (!wasOpen) $('#modalChart').innerHTML = '<p class="muted">loading…</p>';
}

function closeModal() {
  $('#modal').style.display = 'none';
  if (state.modal) {
    state.modal = null;
    writeUrl();
  }
}

/**
 * Deployed under the Prefect host these are same-origin, so they navigate in this
 * tab exactly like a link inside the Prefect UI does.
 */
function prefectLink(runId, text) {
  return `<a class="ext" href="${api.runUrl(runId)}">${text} →</a>`;
}

function openRunInPrefect(runId) {
  window.location.assign(api.runUrl(runId));
}

/**
 * Bumped per pop-up render. Refresh ticks re-render an open pop-up, so a slow fetch can
 * still be in flight when the next one starts; the older one must not paint over it.
 */
let modalRender = 0;

/** Sub-flow runs of the given parents, drawn as their own timeline. */
async function showSubflows(parentRuns, title, { remember = true } = {}) {
  const single = parentRuns.length === 1 ? parentRuns[0] : null;
  const render = ++modalRender;

  if (remember) {
    state.modal = single
      ? { kind: 'subflows', id: single.id }
      : { kind: 'subflowsOf', id: parentRuns[0]?.flow_id };
    writeUrl({ push: true });
  }
  openModal(escapeHtml(title) + (single ? ` ${prefectLink(single.id, 'open in Prefect')}` : ''));

  try {
    const { runs: subflowRuns, parentByRun } = await api.fetchSubflowRuns(
      parentRuns.slice(0, 200).map((run) => run.id),
    );
    if (render !== modalRender) return; // superseded while waiting

    if (subflowRuns.length === 0) {
      const fallback = single ? ` — ${prefectLink(single.id, 'open this run in Prefect')}` : '';
      $('#modalChart').innerHTML = `<p class="muted">no sub-flow runs${fallback}</p>`;
      return;
    }

    const from = Math.min(...subflowRuns.map(runStart));
    const to = Math.max(...subflowRuns.map(runEnd));
    const padding = (to - from) * 0.02 + 1000;

    $('#modalChart').innerHTML = '';
    renderTimeline($('#modalChart'), subflowRuns, {
      from: from - padding,
      to: to + padding,
      edges: [],
      // Within one parent run the order sub-flows ran in is a fact about that parent,
      // so consecutive siblings are linked. Not the same claim as an automation link.
      exactLinks: sequentialChainLinks(subflowRuns, parentByRun),
      aggregated: false, // a handful of runs; binning them would hide the sequence
      flowName,
    });
    $('#modalChart').insertAdjacentHTML(
      'beforeend',
      '<p class="muted" style="margin:8px 0 0">click any sub-flow run to open it in Prefect</p>',
    );
  } catch (error) {
    if (render === modalRender) $('#modalChart').textContent = error.message;
  }
}

/**
 * Right-click: the same chain shift-click highlights in place, but opened as its own
 * window — one batch on its own axis, with the rest of the window gone rather than
 * dimmed.
 */
/**
 * The batch a chain is about, taken from the events that linked it — the payload of
 * the event that fired the automation. Read, not inferred from run tags.
 */
function batchOfChain(chainRuns) {
  const ids = new Set(chainRuns.map((each) => each.id));
  const batches = new Set(
    (state.view?.exactLinks ?? [])
      .filter((link) => ids.has(link.downstreamRunId) || ids.has(link.upstreamRunId))
      .map((link) => link.batch)
      .filter(Boolean),
  );
  return batches.size === 1 ? [...batches][0] : null;
}

function showChainWindow(chart, run, { remember = true } = {}) {
  if (remember) {
    state.modal = { kind: 'chain', id: run.id };
    writeUrl({ push: true });
  }
  const chain = runChain(chart.timelineLinks ?? [], run.id);

  // Resolve ids against what is actually drawn, so the window matches the chart.
  const drawn = new Map();
  for (const mark of chart.timelineMarks ?? []) {
    for (const each of mark.runs) drawn.set(each.id, each);
  }
  const chainRuns = [...chain].map((id) => drawn.get(id)).filter(Boolean);

  const flowCount = new Set(chainRuns.map((each) => each.flow_id)).size;
  const label = batchOfChain(chainRuns);
  const heading = chainRuns.length > 1
    ? `Chain of ${flowName(run.flow_id)} / ${run.name} — ${chainRuns.length} runs across ${flowCount} flows`
    : `${flowName(run.flow_id)} / ${run.name} — no linked runs`;

  openModal(
    escapeHtml(heading)
    + (label ? ` <span class="muted">batch ${escapeHtml(label)}</span>` : '')
    + ` ${prefectLink(run.id, 'open in Prefect')}`,
  );

  const from = Math.min(...chainRuns.map(runStart));
  const to = Math.max(...chainRuns.map(runEnd));
  const padding = (to - from) * 0.05 + 1000;

  $('#modalChart').innerHTML = '';
  renderTimeline($('#modalChart'), chainRuns, {
    from: from - padding,
    to: to + padding,
    edges: state.view?.edges ?? [],
    exactLinks: state.view?.exactLinks ?? [],
    aggregated: false, // a chain is individual runs; binning them would hide the point
    flowName,
  });
  $('#modalChart').insertAdjacentHTML(
    'beforeend',
    '<p class="muted" style="margin:8px 0 0">click any run to open it in Prefect</p>',
  );
}

/* -------------------------------- pointer --------------------------------- */

const isRunMark = (element) => element.classList?.contains('run');
const chartOf = (element) => element.closest('#chart,#modalChart');
const markFor = (element) => chartOf(element)?.timelineMarks[Number(element.dataset.i)];

/** Fades every link that does not touch the hovered run, so one chain stays traceable. */
function highlightLinks(chart, runIds) {
  chart?.querySelectorAll('.lnk').forEach((link) => {
    const touches = !runIds || link.dataset.r.split(' ').some((id) => runIds.has(id));
    let opacity = '.45'; // nothing hovered
    if (runIds) opacity = touches ? '.95' : '.06';
    link.setAttribute('stroke-opacity', opacity);
  });
}

/** Shift-click: dim everything that is not part of this run's chain. */
function isolateChain(chart, runId) {
  state.isolatedRunId = runId;
  const chain = runChain(chart.timelineLinks ?? [], runId);

  chart.querySelectorAll('.run').forEach((element) => {
    const mark = chart.timelineMarks[Number(element.dataset.i)];
    const inChain = mark.runs.some((run) => chain.has(run.id));
    element.style.opacity = inChain ? '1' : '0.15';
  });
  chart.querySelectorAll('.lnk').forEach((link) => {
    const [upstream, downstream] = link.dataset.r.split(' ');
    const inChain = chain.has(upstream) && chain.has(downstream);
    link.setAttribute('stroke-opacity', inChain ? '.95' : '.04');
  });
  return chain.size;
}

function clearIsolation() {
  if (!state.isolatedRunId) return;
  state.isolatedRunId = null;
  document.querySelectorAll('#chart,#modalChart').forEach((chart) => {
    chart.querySelectorAll('.run').forEach((element) => { element.style.opacity = ''; });
    highlightLinks(chart, null);
  });
}

document.addEventListener('mousemove', (event) => {
  const tooltip = $('#tooltip');
  const target = event.target;

  if (!isRunMark(target)) {
    tooltip.style.display = 'none';
    // An isolated chain is a deliberate selection; hovering elsewhere must not undo it.
    if (!state.isolatedRunId) {
      document.querySelectorAll('#chart,#modalChart').forEach((chart) => highlightLinks(chart, null));
    }
    return;
  }

  const mark = markFor(target);
  tooltip.innerHTML = tooltipHtml(mark, { flowName, inModal: Boolean(target.closest('#modalChart')) });
  tooltip.style.display = 'block';

  // Measured after the content is in, since the width depends on the names shown.
  const { width, height } = tooltip.getBoundingClientRect();
  const { x, y } = tooltipPosition({
    cursorX: event.clientX,
    cursorY: event.clientY,
    width,
    height,
    viewportWidth: window.innerWidth,
    viewportHeight: window.innerHeight,
  });
  tooltip.style.left = `${x}px`;
  tooltip.style.top = `${y}px`;

  if (!state.isolatedRunId) highlightLinks(chartOf(target), new Set(mark.runs.map((run) => run.id)));
});

/* ------------------------- dragging out a time range ----------------------- */

/**
 * A drag across the plot selects a time range, the way Grafana's charts do.
 *
 * The same gesture starts as a click, so nothing is committed until the pointer has moved
 * past a few pixels — under that it stays a click and opens the run's sub-flows. Past it,
 * the click that follows the release has to be swallowed, or letting go over a run would
 * both zoom and open a pop-up.
 */
let drag = null;

$('#chart').addEventListener('mousedown', (event) => {
  const plot = $('#chart').timelinePlot;
  // Shift is isolate-the-chain and right-click opens it; neither is a drag.
  if (!plot || event.button !== 0 || event.shiftKey) return;

  const svg = $('#chart').querySelector('svg');
  const x = event.clientX - svg.getBoundingClientRect().left;
  if (x < plot.left) return; // the label gutter is not part of the time axis

  drag = { startX: x, x, moved: false, plot };
});

document.addEventListener('mousemove', (event) => {
  if (!drag) return;
  const svg = $('#chart').querySelector('svg');
  if (!svg) return;

  const rect = svg.getBoundingClientRect();
  drag.x = Math.max(drag.plot.left, Math.min(event.clientX - rect.left, drag.plot.left + drag.plot.width));
  if (Math.abs(drag.x - drag.startX) < DRAG_THRESHOLD_PX) return;

  drag.moved = true;
  document.body.classList.add('dragging');
  $('#tooltip').style.display = 'none'; // the tooltip would follow the cursor across it

  // Positioned against #main, measured rather than assumed, so the chart's padding and
  // the legend's height above it do not have to be known here.
  const main = $('#main').getBoundingClientRect();
  const selection = $('#dragSelect');
  selection.hidden = false;
  selection.style.top = `${rect.top - main.top}px`;
  selection.style.left = `${rect.left - main.left + Math.min(drag.startX, drag.x)}px`;
  selection.style.width = `${Math.abs(drag.x - drag.startX)}px`;
  selection.style.height = `${rect.height}px`;
});

document.addEventListener('mouseup', () => {
  if (!drag) return;
  const finished = drag;
  drag = null;

  document.body.classList.remove('dragging');
  $('#dragSelect').hidden = true;
  if (!finished.moved) return; // it was a click after all

  // Nested zooms keep the range they were dragged out of, so ✕ still returns to it.
  const zoom = zoomFromDrag(finished.plot, finished.startX, finished.x);
  if (zoom) {
    suppressClick = true;
    setZoom(zoom);
  }
});

/**
 * Dragging the thumb pans the window through the range, CloudWatch-style: the span stays,
 * the position moves. Panning is presentation too — the runs either side are already
 * loaded — so it follows the pointer without a fetch.
 */
let thumbDrag = null;

$('#zoomThumb').addEventListener('mousedown', (event) => {
  if (!state.zoom) return;
  event.preventDefault(); // or the browser starts a text selection instead
  thumbDrag = { x: event.clientX, left: parseFloat($('#zoomThumb').style.left) || 0 };
  document.body.classList.add('dragging');
});

document.addEventListener('mousemove', (event) => {
  if (!thumbDrag) return;
  const loaded = { from: drawnFrom(), to: state.view.to };

  const left = thumbDrag.left + (event.clientX - thumbDrag.x);
  state.zoom = zoomFromThumb(loaded, state.zoom, trackWidth(), left);
  draw();
});

document.addEventListener('mouseup', () => {
  if (!thumbDrag) return;
  thumbDrag = null;
  document.body.classList.remove('dragging');
  writeUrl(); // only once the pan settles, rather than on every frame of it
});

$('#zoomReset').addEventListener('click', () => setZoom(null));

/** Set when a drag ends, so the click that follows the release does not also fire. */
let suppressClick = false;

document.addEventListener('click', (event) => {
  if (suppressClick) {
    suppressClick = false;
    return;
  }
  const target = event.target;

  const chip = target.closest?.('.chip.filter');
  if (chip) {
    toggleStateFilter(chip.dataset.state);
    return;
  }

  if (isRunMark(target)) {
    const mark = markFor(target);
    const run = mark.worst;

    // Shift-click isolates this run's whole chain instead of drilling into it.
    if (event.shiftKey) {
      const chart = chartOf(target);
      if (state.isolatedRunId === run.id) clearIsolation();
      else isolateChain(chart, run.id);
      return;
    }
    if (state.isolatedRunId) clearIsolation();

    if (target.closest('#modalChart')) {
      openRunInPrefect(run.id); // in the pop-up: straight to the run in Prefect
    } else {
      // A bin holds many runs; a graph mark holds one. Otherwise identical.
      showSubflows(mark.runs, mark.runs.length > 1
        ? `Sub-flows of ${flowName(run.flow_id)} — ${mark.runs.length} runs`
        : `Sub-flows of ${flowName(run.flow_id)} / ${run.name}`);
    }
    return;
  }

  if (target.classList?.contains('lanelabel') && state.view) {
    const flowId = target.dataset.lane;
    const laneRuns = state.view.runs.filter((run) => run.flow_id === flowId);
    if (laneRuns.length > 0) {
      showSubflows(laneRuns, `Sub-flows of ${flowName(flowId)} (${laneRuns.length} runs)`);
    }
    return;
  }

  if (target.id === 'modalClose' || target.id === 'modal') {
    closeModal();
    return;
  }
  clearIsolation(); // clicking the background drops the selection
});

// Right-click on a run opens its chain. No modifier: Firefox forces its own menu on
// shift-right-click and never delivers the event.
document.addEventListener('contextmenu', (event) => {
  if (!isRunMark(event.target)) return; // anywhere else, leave the normal menu alone
  event.preventDefault();
  showChainWindow(chartOf(event.target), markFor(event.target).worst);
});

document.addEventListener('keydown', (event) => {
  if (event.key !== 'Escape') return;
  if ($('#modal').style.display === 'flex') closeModal();
  else if (state.isolatedRunId) clearIsolation();
  else if (state.zoom) setZoom(null); // same way out as the bar's ✕
});

/* -------------------------------- controls -------------------------------- */

function setMode(mode) {
  state.mode = localStorage.mode = mode;
  syncControls();
  writeUrl();
  draw(); // same data, different shape — no refetch
}

/** @param {{from: string, to: string}} range expressions, not timestamps */
/**
 * @param {'start'|'end'} anchor which end to keep if the range is too long — the one just
 *   set. Capped here as well as when it is resolved, so the button never labels the chart
 *   with a week it is not showing.
 */
function setRange(range, anchor = 'end') {
  state.range = capRange(range, Date.now(), anchor);
  syncControls();
  writeUrl();
  closeRangePanel();
  load();
}

function showRangeError(message) {
  $('#rangeError').textContent = message;
}

/* ----------------------------- auto refresh ------------------------------- */

function setRefresh(label) {
  state.refresh = localStorage.refresh = label;
  clearInterval(autoRefreshTimer);

  const chosen = REFRESH_INTERVALS.find((option) => option.label === label);
  // Wrapped: setInterval hands its callback a tick count, which load would read as options.
  if (chosen?.ms) autoRefreshTimer = setInterval(() => load(), chosen.ms);

  syncControls();
  writeUrl();
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

function syncControls() {
  $('#mGraph').classList.toggle('on', state.mode === 'graph');
  $('#mAgg').classList.toggle('on', state.mode === 'agg');

  $('#rangeLabel').textContent = describeRange(state.range);

  const active = matchQuickRange(state.range);
  document.querySelectorAll('#quickRanges button').forEach((button) => {
    button.classList.toggle('on', Boolean(active) && button.dataset.from === active.from);
  });

  // A fixed start shows in the input; "the last N" leaves it empty, since there is nothing
  // to type — the start is wherever `now` happens to be.
  const startsAt = active ? null : parseTimeExpression(state.range.from);
  $('#rangeFrom').value = startsAt === null ? '' : formatLocal(startsAt);

  // An end only shows when it is a fixed one: "ending now" has no timestamp to put there.
  const endsAt = active || state.range.to === 'now' ? null : parseTimeExpression(state.range.to);
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
  const openEnded = startsAt !== null && state.range.to === 'now';
  $('#untilNow').classList.toggle('on', openEnded);

  const span = rangeSpan();
  document.querySelectorAll('#durations button').forEach((button) => {
    button.classList.toggle('on', !openEnded && Number(button.dataset.ms) === span);
  });

  $('#zoneLabel').textContent = zoneLabel();
  $('#zoneToggle').classList.toggle('on', getZone() === 'utc');

  $('#refreshIntervalLabel').textContent = state.refresh;
  $('#refreshIntervalButton').classList.toggle('on', state.refresh !== 'Off');
  document.querySelectorAll('#refreshMenu button').forEach((button) => {
    button.classList.toggle('on', button.dataset.label === state.refresh);
  });
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
  const resolved = resolveRange(state.range);
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

  const resolved = resolveRange(state.range);
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

function init() {
  $('#apiUrl').value = api.baseUrl;
  readUrl();
  buildRangePicker();
  buildCalendar();
  buildRefreshControl();

  $('#mGraph').onclick = () => setMode('graph');
  $('#mAgg').onclick = () => setMode('agg');
  $('#refresh').onclick = () => load({ force: true });

  // A display setting: nothing is refetched, everything is relabelled. The range keeps its
  // meaning too — "now-6h" is the same six hours whichever clock names them.
  $('#zoneToggle').onclick = () => {
    // Resolved on the old clock, rewritten for the new one: a range fixed at 11:00 in
    // +03:00 becomes 08:00 in UTC and covers the same runs, rather than jumping by the
    // offset because a bare stamp is read in whichever zone is current.
    const showing = resolveRange(state.range);

    localStorage.zone = setZone(getZone() === 'utc' ? 'local' : 'utc');
    if (showing) state.range = restampRange(state.range, showing);

    syncControls();
    writeUrl();
    draw();
  };

  // Nothing to choose when the API is next to us, or when a deployment pinned it: the field
  // is hidden rather than shown filled in and ignored.
  $('#apiUrl').hidden = !API_URL_EDITABLE;
  if (API_URL_EDITABLE) {
    $('#apiUrl').onchange = () => {
      localStorage.apiUrl = $('#apiUrl').value;
      clearCache(api.baseUrl);
      api = clientFor($('#apiUrl').value); // a new server means new caches
      load();
    };
  }

  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      draw();
    }, RESIZE_DEBOUNCE_MS);
  });

  // Back/forward should move through views, not reload the page.
  window.addEventListener('popstate', () => {
    readUrl();
    syncControls();
    // Whatever the entry says, not what is on screen: Back out of a pop-up closes it.
    if (!state.modal) $('#modal').style.display = 'none';
    load();
  });

  setRefresh(state.refresh); // installs the timer and syncs the control
  writeUrl();
  load();
}

init();
