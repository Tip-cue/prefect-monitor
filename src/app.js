/** Wires the DOM to the Prefect API and the renderer. */

import { PrefectApi, BATCH_PAYLOAD_KEYS } from './prefect-api.js';
import { runStart, runEnd, formatDuration } from './time.js';
import {
  filterToChainsWithState, inferredLinks, runChain, sequentialChainLinks,
} from './links.js';
import {
  renderTimeline, legendHtml, tooltipHtml, tooltipPosition, escapeHtml,
} from './render.js';
import { resolveRange, viewFromQuery, viewToQuery } from './time-range.js';
import {
  CACHE_FRESH_MS, planFetch, unsettledRunIds, mergeRuns, mergeLinks,
} from './run-cache.js';
import {
  DRAG_THRESHOLD_PX, clampZoom, thumbGeometry, zoomFromDrag, zoomFromThumb, zoomFromWheel,
} from './zoom.js';
import { readCache, writeCache, clearCache } from './browser-cache.js';
import { resolveBatchKeys } from './settings.js';
import { formatFull, getZone } from './zone.js';
import { mountHeader } from './header.js';

/**
/** What `config.js` said, if the deployment wrote one. */
const CONFIG = globalThis.PREFECT_MONITOR ?? {};

const BATCH_KEYS = resolveBatchKeys(CONFIG, BATCH_PAYLOAD_KEYS);

/** Every client is built the same way, so config applies wherever the server changes. */
const clientFor = (baseUrl) => new PrefectApi(baseUrl, { batchKeys: BATCH_KEYS });

const RESIZE_DEBOUNCE_MS = 150;

const $ = (selector) => document.querySelector(selector);

/**
 * The part of the bar that is this page's and not the header's: which shape to draw the
 * window in, and which flows to ask the server for.
 */
const EXTRAS = `
    <button id="mGraph">Flow graph</button>
    <button id="mAgg">Aggregate</button>

    <span class="sep"></span>
    <!-- Applied on Enter or the tick, not per keystroke: the filter goes to the server. -->
    <div id="flowFilterBox">
      <input id="flowFilter" placeholder="filter flows…" title="Show only flows whose name contains this — Enter to apply" spellcheck="false">
      <button id="flowFilterApply" title="Apply the filter (Enter)">&#10003;</button>
    </div>`;

const state = {
  mode: 'graph',
  /** @type {{runs, edges, exactLinks, from: number, to: number}|null} */
  view: null,
  flowNames: new Map(),
  /** Legend filter: show only pipelines containing a run in one of these states. */
  selectedStates: new Set(),
  /** Text filter: only flows whose name contains this (case-insensitive). */
  flowFilter: '',
  /** Width of the flow name column, px, once dragged; undefined fits the longest name. */
  nameWidth: Number(localStorage.nameWidth) || undefined,
  /** Shift-clicked run whose chain is isolated, if any. */
  isolatedRunId: null,
  /** Which pop-up is open, mirrored in the URL: { kind, id } or null. */
  modal: null,
  /**
   * Dragged-out slice of the range being looked at, or null for all of it. Absolute, and
   * never replaces the range: the picker still says "Last 6 hours", so however deep you
   * zoom, one click on the bar's ✕ is the way back.
   */
  zoom: null,
};

/** Both built in init(), against the API the header resolved. */
let api = null;
let header = null;
let resizeTimer = null;

const flowName = (flowId) => state.flowNames.get(flowId) || String(flowId).slice(0, 8);

/**
 * Measures a lane name exactly as the chart draws it: an SVG text with the label's own
 * class, kept off to the side. A canvas measurement drifted a few percent from the real
 * glyphs and left a blank strip before the column's edge.
 */
const labelRuler = document.body.appendChild(document.createElementNS('http://www.w3.org/2000/svg', 'svg'));
labelRuler.setAttribute('style', 'position:absolute;left:-9999px;top:0;width:1px;height:1px;visibility:hidden');
labelRuler.innerHTML = '<text class="lanelabel"></text>';
const textWidth = (text) => {
  labelRuler.firstChild.textContent = text;
  return labelRuler.firstChild.getComputedTextLength();
};


/* ------------------------------ loading data ------------------------------ */

/**
 * Reads the timeline's half of the view out of the URL so a refresh — or a pasted link —
 * restores it. The range, the clock and the refresh interval are the header's half, and it
 * has already read them by the time this runs.
 */
function readUrl() {
  const { mode, states, modal, zoom, flow } = viewFromQuery(window.location.search);
  state.mode = mode ?? localStorage.mode ?? 'graph';
  state.selectedStates = new Set(states);
  state.flowFilter = flow;
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
    range: header.range,
    mode: state.mode,
    states: [...state.selectedStates],
    flow: state.flowFilter,
    refresh: header.refresh,
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
  const resolved = resolveRange(header.range);
  if (!resolved) {
    header.showRangeError('that range does not resolve');
    return;
  }
  const { from, to } = resolved;
  const modalWasOpen = $('#modal').style.display === 'flex';

  // The filter is part of what was fetched: the server only sent matching runs.
  const flowFilter = state.flowFilter.trim();
  const cache = readCache(api.baseUrl, flowFilter);
  // A 5s auto-refresh means 5s, not "every other tick, because the cache was still fresh".
  const plan = planFetch(cache, from, to, Date.now(), force ? 0 : header.refreshMs() || CACHE_FRESH_MS);

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
      api.fetchRunsOverlapping(fetchFrom, to, { flowName: flowFilter }),
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

    // Typed on while this was in flight: the answer is for a filter no longer shown, so
    // it must not be painted or cached as the current one. The later load draws its own.
    if (state.flowFilter.trim() !== flowFilter) return;

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
    }, flowFilter);
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
  const named = filterByFlowName(runs);
  $('#legend').innerHTML = legendHtml(named, state.selectedStates)
    + runNotice() + linkNotice() + asOfNotice();
  renderTimeline($('#chart'), filterByState(named), {
    from: shown.from,
    to: shown.to,
    edges,
    exactLinks,
    aggregated: state.mode === 'agg',
    flowName,
    nameWidth: state.nameWidth,
    textWidth,
    range: loaded, // lanes from the whole loaded span, so the zoom only moves the marks
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

  if (!state.zoom) {
    bar.hidden = true;
    return;
  }

  // Shown whenever there is a zoom, plot or no plot, so the ✕ is always there.
  bar.hidden = false;
  const track = $('#zoomTrack');
  track.style.marginLeft = `${plot?.left ?? 0}px`; // line the start of the track up with the plot

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

/** Pending while what is typed differs from what is applied; lit once a filter is applied. */
function syncFlowFilterBox() {
  const typed = $('#flowFilter').value.trim();
  const applied = state.flowFilter.trim();
  $('#flowFilterBox').classList.toggle('pending', typed !== applied);
  $('#flowFilterBox').classList.toggle('on', typed === applied && applied !== '');
}

function applyFlowFilter() {
  const typed = $('#flowFilter').value.trim();
  if (typed === state.flowFilter.trim()) return syncFlowFilterBox();
  state.flowFilter = typed;
  syncFlowFilterBox();
  writeUrl();
  draw(); // narrow what is loaded at once; the fetch fills in what the server has
  load();
}

const filterByFlowName = (runs) => {
  const needle = state.flowFilter.trim().toLowerCase();
  return needle ? runs.filter((run) => flowName(run.flow_id).toLowerCase().includes(needle)) : runs;
};

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

let columnDrag = null;

$('#chart').addEventListener('mousedown', (event) => {
  const plot = $('#chart').timelinePlot;
  // Shift is isolate-the-chain and right-click opens it; neither is a drag.
  if (!plot || event.button !== 0 || event.shiftKey) return;

  // The grab strip on the name column's edge resizes the column instead of selecting time.
  if (event.target.classList.contains('colHandle')) {
    const start = state.nameWidth ?? event.target.getBBox().x + 10; // the strip straddles the edge
    columnDrag = { startX: event.clientX, start };
    document.body.classList.add('dragging');
    event.preventDefault();
    return;
  }

  const svg = $('#chart').querySelector('svg');
  const x = event.clientX - svg.getBoundingClientRect().left;
  if (x < plot.left) return; // the label gutter is not part of the time axis

  drag = { startX: x, x, moved: false, plot };
});

document.addEventListener('mousemove', (event) => {
  if (columnDrag) {
    state.nameWidth = Math.max(80, columnDrag.start + event.clientX - columnDrag.startX);
    draw();
    return;
  }
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
  if (columnDrag) {
    columnDrag = null;
    document.body.classList.remove('dragging');
    localStorage.nameWidth = state.nameWidth;
    suppressClick = true; // letting go over a lane label must not open it
    return;
  }
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

// The wheel over the plot zooms about the pointer: up to zoom in, down to zoom out, and out
// past the loaded range is the same as the ✕. Over the label gutter it still scrolls the
// page, so a tall chart can be scrolled from there.
document.addEventListener('wheel', (event) => {
  if (!state.view || event.target.closest('header, #modal')) return;

  let plot = $('#chart').timelinePlot;
  let x;
  if (plot) {
    const svg = $('#chart').querySelector('svg');
    x = event.clientX - svg.getBoundingClientRect().left;
    if (x < plot.left) return;
  } else if (state.zoom) {
    // Nothing is drawn to point at, so the whole page is the zoom surface, anchored on
    // the slice's middle, and scrolling out is the way back.
    plot = { left: 0, width: 1, from: state.zoom.from, to: state.zoom.to };
    x = 0.5;
  } else {
    return;
  }

  event.preventDefault();
  const { loaded } = drawnWindow();
  const zoom = zoomFromWheel(plot, loaded, x, event.deltaY);
  if (zoom?.from === plot.from && zoom?.to === plot.to) return; // nothing changes
  $('#tooltip').style.display = 'none'; // the marks move under the cursor
  setZoom(zoom);
}, { passive: false });

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

/** The controls this page adds to the bar. The header syncs its own. */
function syncControls() {
  $('#mGraph').classList.toggle('on', state.mode === 'graph');
  $('#mAgg').classList.toggle('on', state.mode === 'agg');

  $('#flowFilter').value = state.flowFilter;
  syncFlowFilterBox();

  header.sync();
}

function init() {
  header = mountHeader({
    page: 'timeline',
    extras: EXTRAS,
    onViewChange: ({ reason }) => {
      writeUrl();
      // A clock is a display setting: everything is relabelled, nothing is refetched.
      if (reason === 'zone') draw();
      else load();
    },
    onReload: ({ force = false } = {}) => load({ force }),
    onApiUrlChange: (url) => {
      clearCache(api.baseUrl); // the server we are leaving, before the handle changes
      api = clientFor(url); // a new server means new caches
      load();
    },
  });
  api = clientFor(header.apiUrl);
  readUrl();

  $('#mGraph').onclick = () => setMode('graph');
  $('#mAgg').onclick = () => setMode('agg');
  // Double-click the column's edge to go back to fitting the longest name.
  $('#chart').addEventListener('dblclick', (event) => {
    if (!event.target.classList.contains('colHandle')) return;
    state.nameWidth = undefined;
    delete localStorage.nameWidth;
    draw();
  });
  // The filter goes to the server, so it is applied on Enter or the tick rather than on
  // every keystroke — typing only marks the box as pending.
  $('#flowFilter').oninput = syncFlowFilterBox;
  $('#flowFilter').onkeydown = (event) => {
    if (event.key === 'Enter') applyFlowFilter();
    if (event.key === 'Escape') { $('#flowFilter').value = state.flowFilter; syncFlowFilterBox(); }
  };
  $('#flowFilterApply').onclick = applyFlowFilter;

  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      draw();
    }, RESIZE_DEBOUNCE_MS);
  });

  // Back/forward should move through views, not reload the page.
  window.addEventListener('popstate', () => {
    header.readParams(window.location.search); // the clock, before anything is labelled
    readUrl();
    syncControls();
    // Whatever the entry says, not what is on screen: Back out of a pop-up closes it.
    if (!state.modal) $('#modal').style.display = 'none';
    load();
  });

  syncControls();
  writeUrl();
  load();
}

init();
