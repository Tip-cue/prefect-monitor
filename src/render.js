/**
 * Drawing the timeline, legend, tooltip and state table.
 *
 * Everything here builds markup from data and returns it (or writes it into a given
 * container). Nothing reaches for the document or app state.
 */

import { stateColor, stateName, isHatched, severity, worstRun, compareStateNames } from './states.js';
import {
  runStart, runEnd, overlapsWindow, formatDuration, timeTicks, formatTick,
} from './time.js';
import { laneGroups, laneOrder, packRunsIntoRows } from './layout.js';
import { runLinkPairs } from './links.js';
import { formatFull, formatTime } from './zone.js';

const LAYOUT = {
  // Default width of the flow name column; the user can drag it (options.nameWidth).
  nameWidth: 215,
  minNameWidth: 80,
  /** A lane name starts 14px in and must end 12px short of the column's edge. */
  nameInset: 26,
  /** One column per state present, plus a total, in aggregate mode's label gutter.
      Wide enough for "180 (99%)" at 11px. */
  stateColumnWidth: 78,
  totalColumnWidth: 52,
  /** A lane of its own for aggregate mode's connectors, so they miss the totals. */
  connectorWidth: 24,
  rightPadding: 48,
  axisHeight: 26,
  minChartWidth: 640,
  groupGap: 12,
  groupCaptionHeight: 17,
  groupPadding: 4,
  runMarkHeight: 14,
  aggregatedMarkHeight: 30,
  subRowGap: 3,
  minLaneHeight: 32,
  minAggregatedLaneHeight: 46,
  laneVerticalPadding: 14,
};

/** Width in pixels of one aggregated bin. */
const BIN_WIDTH = 7;

/** Gap left between abutting marks so they read as separate runs. */
const MARK_GAP = 1.5;

/**
 * A ceiling, not a working limit. It was 150, from when links came from fuzzy matching
 * and a 6h window produced ~1000 of them; exact links are far fewer (486 over 24h), and
 * 150 silently cut a 24h view down to its last 7 hours. Runs are capped at 2200 a window
 * so links cannot much exceed that either — this only guards a pathological case, and
 * the chart says so when it bites.
 */
const MAX_LINKS_DRAWN = 2500;

export function escapeHtml(value) {
  return String(value).replace(/[&<>"]/g, (char) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[char]
  ));
}

/**
 * Draws the timeline into `container`.
 *
 * Each mark drawn is recorded on `container.timelineMarks`, indexed by the mark's
 * `data-i` attribute, so hover and click handlers can map an element back to its runs.
 *
 * @param {HTMLElement} container
 * @param {object[]} runs
 * @param {object} options
 * @param {number} options.from        window start, epoch ms
 * @param {number} options.to          window end, epoch ms
 * @param {{src: string, dst: string, name: string}[]} options.edges
 * @param {boolean} options.aggregated bin runs per lane instead of drawing each one
 * @param {(flowId: string) => string} options.flowName
 * @param {number} [options.nameWidth] width of the flow name column, px
 * @param {(text: string) => number} [options.textWidth] measures a lane name at 12px;
 *   the browser passes a canvas measurement, tests get an estimate
 */
export function renderTimeline(container, runs, options) {
  const {
    from, to, edges = [], aggregated = false, flowName = String, exactLinks = [],
    nameWidth = LAYOUT.nameWidth, textWidth = estimateTextWidth,
  } = options;

  // Only what genuinely occupies the window: runs are fetched on expected start, so
  // one that was scheduled inside it but began after it does not belong here.
  const visible = runs.filter((run) => overlapsWindow(run, from, to));

  const runsByFlow = groupBy(visible, (run) => run.flow_id);

  // Computed before the lanes are ordered, because the ordering follows them. Also
  // needed in aggregate mode, where they are not drawn: shift-click and right-click
  // walk chains in both modes.
  const pairs = runLinkPairs(visible, exactLinks);

  // Lane order follows what is actually linked. The sub-flow view passes no flow-level
  // edges — its links are between runs — so without this its lanes fell back to
  // alphabetical and read out of sequence.
  const groups = laneGroups([...runsByFlow.keys()], [...edges, ...flowEdgesFrom(pairs)], flowName);

  if (groups.length === 0) {
    container.innerHTML = '<p class="muted" style="padding:16px">no flow runs in this window</p>';
    container.timelineMarks = [];
    container.timelinePlot = null; // nothing drawn, so there is nothing to drag across
    return;
  }

  // The counts table is aggregate mode's job; the graph shows each run's state directly.
  const states = aggregated ? [...new Set(visible.map(stateName))].sort(compareStateNames) : [];
  const geometry = computeGeometry(container, groups, runsByFlow, { from, to, aggregated, states, nameWidth });
  const marks = [];

  const svg = [
    svgDefs(geometry),
    aggregated
      ? aggregatedConnectors(edges, runsByFlow, geometry)
      : runLinks(pairs, geometry),
    groupBoxes(groups, geometry, flowName, textWidth),
    timeAxis(geometry),
    countColumns(geometry),
    laneLabels(groups, runsByFlow, geometry, { flowName, aggregated, textWidth }),
    laneMarks(groups, runsByFlow, geometry, { aggregated, marks }),
    columnHandle(geometry),
  ].join('');

  container.innerHTML = `<svg width="${geometry.width}" height="${geometry.height}">${svg}</svg>`;
  container.timelineMarks = marks;
  // What the plot area covers, so a drag across it can be read as a time span and the
  // zoom bar underneath can line up with it.
  container.timelinePlot = { left: geometry.labelWidth, width: geometry.plotWidth, from, to };
  // Every link, not just the drawn ones, so a chain can be walked past the draw cap.
  container.timelineLinks = pairs.map(([upstream, downstream]) => [upstream.id, downstream.id]);
}

/** The distinct flow-to-flow edges implied by a set of run links. */
function flowEdgesFrom(pairs) {
  const seen = new Set();
  const edges = [];

  for (const [upstream, downstream] of pairs) {
    const key = `${upstream.flow_id}>${downstream.flow_id}`;
    if (upstream.flow_id === downstream.flow_id || seen.has(key)) continue;
    seen.add(key);
    edges.push({ src: upstream.flow_id, dst: downstream.flow_id });
  }
  return edges;
}

/**
 * Works out where everything goes: lane heights (which depend on how many runs
 * overlap), group boxes, and the time-to-x mapping.
 */
function computeGeometry(container, groups, runsByFlow, { from, to, aggregated, states, nameWidth }) {
  nameWidth = Math.max(LAYOUT.minNameWidth, nameWidth);
  const width = Math.max(container.clientWidth - 16, LAYOUT.minChartWidth);
  // In aggregate mode the gutter is a table: the flow name, a column per state, then
  // the row total. Everywhere else it is just the name.
  const labelWidth = states.length === 0
    ? nameWidth
    : nameWidth + states.length * LAYOUT.stateColumnWidth
      + LAYOUT.totalColumnWidth + LAYOUT.connectorWidth;
  const markHeight = aggregated ? LAYOUT.aggregatedMarkHeight : LAYOUT.runMarkHeight;
  const subRowHeight = markHeight + LAYOUT.subRowGap;

  // A lane is as tall as its busiest moment needs.
  const packing = new Map();
  const laneHeight = new Map();

  for (const flowId of groups.flat()) {
    const packed = aggregated
      ? { rows: 1, rowOf: new Map() }
      : packRunsIntoRows(runsByFlow.get(flowId));

    packing.set(flowId, packed);
    laneHeight.set(flowId, Math.max(
      aggregated ? LAYOUT.minAggregatedLaneHeight : LAYOUT.minLaneHeight,
      packed.rows * subRowHeight + LAYOUT.laneVerticalPadding,
    ));
  }

  const laneTop = new Map();
  const boxes = [];
  let y = LAYOUT.axisHeight + 8;

  for (const group of groups) {
    const captionHeight = group.length > 1 ? LAYOUT.groupCaptionHeight : 0;
    const top = y;

    let laneY = y + captionHeight + LAYOUT.groupPadding;
    for (const flowId of group) {
      laneTop.set(flowId, laneY);
      laneY += laneHeight.get(flowId);
    }

    boxes.push({ group, top, bottom: laneY + LAYOUT.groupPadding, captionHeight });
    y = laneY + LAYOUT.groupPadding + LAYOUT.groupGap;
  }

  const plotWidth = width - labelWidth - LAYOUT.rightPadding;

  return {
    width,
    height: y + 4,
    from,
    to,
    labelWidth,
    plotWidth,
    states,
    nameWidth,
    /** Right edge of the state column at `index`, or of the total column at states.length. */
    columnRight: (index) => nameWidth
      + (index + 1) * LAYOUT.stateColumnWidth
      + (index === states.length ? LAYOUT.totalColumnWidth - LAYOUT.stateColumnWidth : 0),
    markHeight,
    subRowHeight,
    boxes,
    /** Timestamp to x pixel. */
    x: (timestamp) => labelWidth + ((timestamp - from) / (to - from)) * plotWidth,
    laneMiddle: (flowId) => laneTop.get(flowId) + laneHeight.get(flowId) / 2,
    /** Top edge of one sub-row within a lane. */
    subRowTop: (flowId, row) => {
      const stackHeight = packing.get(flowId).rows * subRowHeight;
      return laneTop.get(flowId) + (laneHeight.get(flowId) - stackHeight) / 2 + row * subRowHeight;
    },
    subRowOf: (run) => packing.get(run.flow_id).rowOf.get(run.id) ?? 0,
    laneTop: (flowId) => laneTop.get(flowId),
    laneHeight: (flowId) => laneHeight.get(flowId),
  };
}

function svgDefs({ height, nameWidth }) {
  return `
    <defs>
      <pattern id="hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
        <line x1="0" y1="0" x2="0" y2="6" stroke="#000" stroke-opacity=".38" stroke-width="2.5"/>
      </pattern>
      <!-- Lane names are clipped to their column. A run that began before the window
           is drawn hard against the plot's left edge, so an over-long name would
           otherwise collide with it. -->
      <clipPath id="laneLabelClip">
        <rect x="0" y="0" width="${nameWidth - 10}" height="${height}"/>
      </clipPath>
    </defs>`;
}

/**
 * A rectangle whose left and right ends are independently rounded or square.
 *
 * A square end means "this run continues past the edge of the window" — it started
 * before it, or has not finished — the same convention a Gantt chart uses. Rounded
 * ends are a run that begins and ends inside what you are looking at.
 */
export function markPath(x, y, width, height, { squareLeft = false, squareRight = false } = {}) {
  const radius = Math.min(3, width / 2, height / 2);
  const left = squareLeft ? 0 : radius;
  const right = squareRight ? 0 : radius;

  return [
    `M ${x + left} ${y}`,
    `H ${x + width - right}`,
    right ? `A ${right} ${right} 0 0 1 ${x + width} ${y + right}` : '',
    `V ${y + height - right}`,
    right ? `A ${right} ${right} 0 0 1 ${x + width - right} ${y + height}` : '',
    `H ${x + left}`,
    left ? `A ${left} ${left} 0 0 1 ${x} ${y + height - left}` : '',
    `V ${y + left}`,
    left ? `A ${left} ${left} 0 0 1 ${x + left} ${y}` : '',
    'Z',
  ].filter(Boolean).join(' ');
}

/**
 * The gutter's column headers and separators.
 *
 * The state counts live here rather than in a panel: they are read against the rows
 * they describe, so they belong on the same line as the flow, always visible.
 */
function countColumns(geometry) {
  const { states, height } = geometry;
  if (states.length === 0) return ''; // no table outside aggregate mode
  const top = LAYOUT.axisHeight - 10;

  const headers = states.map((state, index) => `
    <text x="${geometry.columnRight(index) - 6}" y="${top}" text-anchor="end">
      ${escapeHtml(truncate(state, 9))}
    </text>`).join('');

  const totalHeader = `
    <text x="${geometry.columnRight(states.length) - 6}" y="${top}" text-anchor="end">total</text>`;

  // A separator before each column and one closing the gutter, so the counts read as
  // a table rather than as floating numbers.
  const separators = [...states.keys(), states.length].map((index) => {
    const x = geometry.columnRight(index) - LAYOUT.stateColumnWidth
      + (index === states.length ? LAYOUT.stateColumnWidth - LAYOUT.totalColumnWidth : 0);
    return `<line x1="${x}" y1="${LAYOUT.axisHeight - 6}" x2="${x}" y2="${height - 6}"
                  stroke="var(--line)" stroke-opacity=".7"/>`;
  }).join('');

  const totalsEdge = geometry.labelWidth - LAYOUT.connectorWidth;
  const gutterEdge = `
    <line x1="${totalsEdge}" y1="${LAYOUT.axisHeight - 6}" x2="${totalsEdge}" y2="${height - 6}"
          stroke="var(--line)"/>
    <line x1="${geometry.labelWidth}" y1="${LAYOUT.axisHeight - 6}"
          x2="${geometry.labelWidth}" y2="${height - 6}" stroke="var(--line)"/>`;

  return `${separators}${gutterEdge}${headers}${totalHeader}`;
}

/** One rounded area per pipeline, captioned with its root flow. */
function groupBoxes(groups, geometry, flowName, textWidth) {
  return geometry.boxes.map(({ group, top, bottom, captionHeight }) => {
    const box = `<rect x="4" y="${top}" width="${geometry.width - 10}" height="${bottom - top}"
                       rx="10" fill="#7f9dd10a" stroke="var(--line)"/>`;
    if (!captionHeight) return box;

    // Trimmed to the name column: the gutter beyond it is the counts table.
    // Captions are 11px against the labels' 12px, so the measurement is scaled to match.
    const suffix = ` · ${group.length} flows`;
    const room = geometry.nameWidth - LAYOUT.nameInset - textWidth(suffix) * (11 / 12);
    const caption = fitText(flowName(group[0]), room, (text) => textWidth(text) * (11 / 12)) + suffix;
    return `${box}<text x="14" y="${top + 13}" fill="var(--accent)" opacity=".85"
                        clip-path="url(#laneLabelClip)">${escapeHtml(caption)}</text>`;
  }).join('');
}

function timeAxis(geometry) {
  const { from, to, height } = geometry;

  return timeTicks(from, to).map((tick) => {
    const x = geometry.x(tick);
    return `
      <line x1="${x}" y1="${LAYOUT.axisHeight - 6}" x2="${x}" y2="${height - 6}"
            stroke="var(--line)" stroke-opacity=".55"/>
      <text x="${x}" y="${LAYOUT.axisHeight - 10}" text-anchor="middle">${formatTick(tick, to - from)}</text>`;
  }).join('');
}

function laneLabels(groups, runsByFlow, geometry, { flowName, aggregated, textWidth }) {
  // Aggregate mode also draws its edge connectors in the gutter, so it gets less room.
  const room = geometry.nameWidth - LAYOUT.nameInset - (aggregated ? 16 : 0);

  return groups.flatMap((group) => group.map((flowId, indexInGroup) => {
    const laneRuns = runsByFlow.get(flowId);
    const name = flowName(flowId);
    const middle = geometry.laneMiddle(flowId);

    // A guide from the name to its marks: with a wide name column the eye has a long way
    // to travel, so the line is drawn to be seen.
    const divider = indexInGroup === 0 ? '' : `
      <line x1="14" y1="${geometry.laneTop(flowId)}"
            x2="${geometry.width - LAYOUT.rightPadding}" y2="${geometry.laneTop(flowId)}"
            stroke="var(--muted)" stroke-opacity=".35"/>`;

    const label = `
      <text class="lanelabel" clip-path="url(#laneLabelClip)"
            data-lane="${escapeHtml(flowId)}" x="14" y="${middle + 4}">
        ${escapeHtml(fitText(name, room, textWidth))}
        <title>${escapeHtml(name)} — click for sub-flows</title>
      </text>`;

    return divider + label + laneCounts(laneRuns, flowId, geometry, middle);
  })).join('');
}

/**
 * This lane's runs counted per state, in the same cell format the table had: a tinted
 * background, a stripe of the state's colour, the count and its share of the row.
 */
function laneCounts(laneRuns, flowId, geometry, middle) {
  if (geometry.states.length === 0) return '';

  const byState = new Map();
  for (const run of laneRuns) byState.set(stateName(run), (byState.get(stateName(run)) ?? 0) + 1);

  const top = geometry.laneTop(flowId) + 1;
  const height = geometry.laneHeight(flowId) - 2;

  const cells = geometry.states.map((state, index) => {
    const right = geometry.columnRight(index);
    const left = right - LAYOUT.stateColumnWidth;
    const count = byState.get(state) ?? 0;

    // A zero is a dot: the eye should land on the counts that exist.
    if (count === 0) {
      return `<text x="${right - 8}" y="${middle + 4}" text-anchor="end" opacity=".3">·</text>`;
    }

    const color = stateColor(laneRuns.find((run) => stateName(run) === state));
    const percent = Math.round((count / laneRuns.length) * 100);

    return `
      <rect x="${left}" y="${top}" width="${LAYOUT.stateColumnWidth}" height="${height}"
            fill="${color}" fill-opacity=".13"/>
      <rect x="${left}" y="${top}" width="3" height="${height}" fill="${color}"/>
      <text x="${right - 8}" y="${middle + 4}" text-anchor="end" fill="var(--ink)">
        ${count}<tspan fill="var(--muted)" font-size="10"> (${percent}%)</tspan>
      </text>`;
  }).join('');

  const total = `
    <text x="${geometry.columnRight(geometry.states.length) - 8}" y="${middle + 4}"
          text-anchor="end" opacity=".7">${laneRuns.length}</text>`;

  return cells + total;
}

/**
 * The name column's right edge: a line always drawn, so the names read as a column, and
 * a grab strip over it. Dragging is app.js's job (it needs the document's mouse events).
 */
function columnHandle(geometry) {
  const x = geometry.nameWidth - 6;
  const top = LAYOUT.axisHeight - 6;
  const height = geometry.height - LAYOUT.axisHeight;
  return `
    <line x1="${x}" y1="${top}" x2="${x}" y2="${top + height}" stroke="var(--muted)" stroke-opacity=".35"/>
    <rect class="colHandle" x="${x - 4}" y="${top}" width="8" height="${height}" fill="transparent"/>`;
}

/**
 * Aggregate mode: one connector per flow pair, drawn as a bracket in the left
 * gutter. The runs are binned, so per-run links would have nothing to point at.
 */
function aggregatedConnectors(edges, runsByFlow, geometry) {
  const drawn = new Set();

  return edges.map(({ src, dst }) => {
    const key = `${src}>${dst}`;
    if (!runsByFlow.has(src) || !runsByFlow.has(dst) || drawn.has(key)) return '';
    drawn.add(key);

    const y1 = geometry.laneMiddle(src);
    const y2 = geometry.laneMiddle(dst);
    const x = geometry.labelWidth - LAYOUT.connectorWidth / 2;
    const bulge = Math.min(26, 8 + Math.abs(y2 - y1) / 4);

    return `
      <path d="M ${x} ${y1} C ${x - bulge} ${y1}, ${x - bulge} ${y2}, ${x} ${y2}"
            fill="none" stroke="var(--accent)" stroke-opacity=".7" stroke-width="1.5"/>
      <circle cx="${x}" cy="${y2}" r="2.5" fill="var(--accent)"/>`;
  }).join('');
}

/** Flow graph mode: a curve from each upstream run to the run it triggered. */
function runLinks(pairs, geometry) {
  pairs = [...pairs].sort(([, a], [, b]) => runStart(b) - runStart(a)); // most recent first

  const midOf = (run) => geometry.subRowTop(run.flow_id, geometry.subRowOf(run)) + geometry.markHeight / 2;

  const curves = pairs.slice(0, MAX_LINKS_DRAWN).map(([upstream, downstream, basis]) => {
    const x1 = geometry.x(runEnd(upstream));
    const x2 = geometry.x(runStart(downstream));
    const y1 = midOf(upstream);
    const y2 = midOf(downstream);
    // Dashed only for a link guessed from timing. A shared batch label is evidence — the
    // runs say they are about the same thing — so it draws solid like an event link; a
    // nearest-preceding-run guess is not, and should not look like one.
    const dash = basis === 'timing' ? ' stroke-dasharray="4 3"' : '';

    return `<path class="lnk" data-r="${upstream.id} ${downstream.id}"
                  d="M ${x1} ${y1} C ${x1 + 45} ${y1}, ${x2 - 45} ${y2}, ${x2} ${y2}"
                  fill="none" stroke="var(--accent)" stroke-opacity=".45" stroke-width="1.5"${dash}/>`;
  }).join('');

  if (pairs.length <= MAX_LINKS_DRAWN) return curves;

  return curves + `
    <text x="${geometry.width - LAYOUT.rightPadding}" y="${geometry.height - 2}" text-anchor="end">
      showing the ${MAX_LINKS_DRAWN} most recent of ${pairs.length} run links
    </text>`;
}

function laneMarks(groups, runsByFlow, geometry, { aggregated, marks }) {
  const drawMark = (x, width, y, mark, ends) => {
    marks.push(mark);
    const d = markPath(x, y, width, geometry.markHeight, ends);
    const attrs = `class="run" data-i="${marks.length - 1}" d="${d}"`;
    const fill = `<path ${attrs} fill="${stateColor(mark.worst)}"/>`;
    return isHatched(mark.worst) ? `${fill}<path ${attrs} fill="url(#hatch)"/>` : fill;
  };

  return groups.flat().map((flowId) => (aggregated
    ? aggregatedLane(runsByFlow.get(flowId), flowId, geometry, drawMark)
    : runLane(runsByFlow.get(flowId), flowId, geometry, drawMark)
  )).join('');
}

/**
 * A 6h window holds ~500 runs per lane, so per-run blocks abut into one solid bar and
 * a two-pixel failure vanishes under the next success. Bin by pixel column and let
 * the worst state in each bin win, so a failure can never be painted over.
 */
function aggregatedLane(laneRuns, flowId, geometry, drawMark) {
  const y = geometry.subRowTop(flowId, 0);
  const bins = new Map();

  for (const run of laneRuns) {
    const column = Math.round((geometry.x(Math.max(runStart(run), geometry.from)) - geometry.labelWidth) / BIN_WIDTH);
    if (!bins.has(column)) bins.set(column, []);
    bins.get(column).push(run);
  }

  return [...bins.entries()]
    .sort(([a], [b]) => a - b)
    .map(([column, binRuns]) => drawMark(
      geometry.labelWidth + column * BIN_WIDTH,
      BIN_WIDTH - MARK_GAP,
      y,
      { runs: binRuns, worst: worstRun(binRuns) },
    ))
    .join('');
}

function runLane(laneRuns, flowId, geometry, drawMark) {
  // Least severe first, so a failure is drawn last and never covered by a success.
  return [...laneRuns]
    .sort((a, b) => severity(b) - severity(a))
    .map((run) => {
      // A run can extend past either edge: it started before the window, or has not
      // finished. Clamp it to the plot and square off whichever end was cut.
      const startsEarlier = runStart(run) < geometry.from;
      const endsLater = runEnd(run) > geometry.to;
      const startX = geometry.x(startsEarlier ? geometry.from : runStart(run));
      const endX = geometry.x(endsLater ? geometry.to : runEnd(run));

      return drawMark(
        startX,
        Math.max(endX - startX - (endsLater ? 0 : MARK_GAP), 4),
        geometry.subRowTop(flowId, geometry.subRowOf(run)),
        { runs: [run], worst: run },
        { squareLeft: startsEarlier, squareRight: endsLater },
      );
    })
    .join('');
}

/**
 * A chip per state present, with its run count. Each chip is a filter toggle —
 * see `selectedStates` in app.js.
 *
 * @param {object[]} runs all runs in the window, before filtering, so a state can
 *   always be toggled back on
 * @param {Set<string>} selectedStates empty means no filter
 */
export function legendHtml(runs, selectedStates = new Set()) {
  // Keep one real run per state so a chip uses exactly the colour its marks use.
  const exampleByState = new Map();
  const countByState = new Map();

  for (const run of runs) {
    const name = stateName(run);
    if (!exampleByState.has(name)) exampleByState.set(name, run);
    countByState.set(name, (countByState.get(name) ?? 0) + 1);
  }

  const filtering = selectedStates.size > 0;

  const chips = [...exampleByState.entries()]
    .sort(([, a], [, b]) => severity(a) - severity(b))
    .map(([name, example]) => {
      const hatch = isHatched(example)
        ? ';background-image:repeating-linear-gradient(45deg,rgba(0,0,0,.38) 0 2px,transparent 2px 4px)'
        : '';
      const selected = selectedStates.has(name);
      const classes = ['chip', 'filter', filtering && !selected ? 'off' : '', selected ? 'on' : '']
        .filter(Boolean).join(' ');

      return `<button class="${classes}" data-state="${escapeHtml(name)}" aria-pressed="${selected}"
                      title="Show only pipelines with a ${escapeHtml(name)} run">
                <i style="background:${stateColor(example)}${hatch}"></i>${escapeHtml(name)}
                <span style="opacity:.6">${countByState.get(name)}</span>
              </button>`;
    });

  if (filtering) {
    chips.push('<button class="chip filter clear" data-state="">clear filter</button>');
  }
  return chips.join('');
}

/** Gap between the cursor and the tooltip, and the minimum from any viewport edge. */
const TOOLTIP_OFFSET = 14;
const VIEWPORT_MARGIN = 8;

/**
 * Where to put the tooltip so it never leaves the viewport.
 *
 * It prefers down-and-right of the cursor and flips to the other side when there is
 * no room, rather than sliding under the cursor. The size has to be **measured** by
 * the caller: the tooltip's width depends on the flow and run names in it, and the
 * previous fixed guess of 380px clipped it against the edges.
 *
 * @returns {{x: number, y: number}}
 */
/**
 * Horizontal offset for a popover anchored to `pickerLeft`, kept inside the viewport.
 *
 * Neither CSS anchor works on its own: hung off the left edge the range panel's third column
 * ran past the right of the window, and hung off the right edge — after the API URL field
 * was hidden and the header got shorter — its first column ran past the left. The answer
 * depends on where the button ends up, which only measurement knows.
 *
 * @returns {number} pixels to offset by, relative to the anchor's own left edge
 */
export function popoverOffset({ pickerLeft, panelWidth, viewportWidth, margin = 8 }) {
  const lower = margin - pickerLeft;                                  // flush to the left edge
  const upper = viewportWidth - margin - panelWidth - pickerLeft;     // flush to the right

  // Prefer aligned with the anchor (0), pull back when that overflows the right, and never
  // past the left — a panel wider than the window shows its start rather than its middle.
  return Math.max(lower, Math.min(0, upper));
}

export function tooltipPosition({ cursorX, cursorY, width, height, viewportWidth, viewportHeight }) {
  const maxX = viewportWidth - width - VIEWPORT_MARGIN;
  const maxY = viewportHeight - height - VIEWPORT_MARGIN;

  const preferredX = cursorX + TOOLTIP_OFFSET;
  const preferredY = cursorY + TOOLTIP_OFFSET;

  const x = preferredX > maxX ? cursorX - width - TOOLTIP_OFFSET : preferredX;
  const y = preferredY > maxY ? cursorY - height - TOOLTIP_OFFSET : preferredY;

  // Clamp last, so a tooltip larger than the viewport still starts on screen.
  return {
    x: Math.max(VIEWPORT_MARGIN, Math.min(x, Math.max(VIEWPORT_MARGIN, maxX))),
    y: Math.max(VIEWPORT_MARGIN, Math.min(y, Math.max(VIEWPORT_MARGIN, maxY))),
  };
}

/**
 * @param {{runs: object[], worst: object}} mark
 * @param {{flowName: Function, inModal: boolean}} context
 */
export function tooltipHtml(mark, { flowName, inModal }) {
  const { runs, worst } = mark;
  const isBin = runs.length > 1;

  const heading = isBin
    ? `<b>${escapeHtml(flowName(worst.flow_id))}</b> — ${runs.length} runs in this slice`
    : `<b>${escapeHtml(flowName(worst.flow_id))}</b> / ${escapeHtml(worst.name)}`;

  const counts = new Map();
  for (const run of runs) counts.set(stateName(run), (counts.get(stateName(run)) ?? 0) + 1);

  const states = [...counts.entries()]
    .sort(([, a], [, b]) => b - a)
    .map(([name, count]) => {
      const color = stateColor({ state_name: name, state_type: name.toUpperCase() });
      return `<span class="chip"><i style="background:${color}"></i>${escapeHtml(name)}${isBin ? ` × ${count}` : ''}</span>`;
    })
    .join(' ');

  const when = isBin
    ? `${formatTime(Math.min(...runs.map(runStart)))} – ${formatTime(Math.max(...runs.map(runEnd)))}`
    : `${formatFull(runStart(worst))}<br>duration ${formatDuration(runEnd(worst) - runStart(worst))}`;

  let hint = 'click for sub-flows · shift-click to isolate its chain, right-click to open it';
  if (inModal) hint = 'click to open in Prefect';

  return `${heading}<br>${states}<br>${when}<br><span class="muted">${hint}</span>`;
}

/**
 * One row per flow, one column per state, counts over the whole window.
 * Only the cells carry colour; the numbers stay in text ink so they stay readable.
 */
export function statusTableHtml(runs, edges, flowName) {
  const runsByFlow = groupBy(runs, (run) => run.flow_id);
  const lanes = laneOrder([...runsByFlow.keys()], edges ?? [], flowName);
  const states = [...new Set(runs.map(stateName))].sort(compareStateNames);

  const header = states.map((state) => `<th>${escapeHtml(state)}</th>`).join('');

  const rows = lanes.map((flowId) => {
    const laneRuns = runsByFlow.get(flowId);

    const cells = states.map((state) => {
      const matching = laneRuns.filter((run) => stateName(run) === state);
      if (matching.length === 0) return '<td class="z">·</td>';

      const color = stateColor(matching[0]);
      const percent = Math.round((matching.length / laneRuns.length) * 100);
      return `<td style="background:${color}22;box-shadow:inset 3px 0 0 ${color}">
                ${matching.length}<span class="pct">(${percent}%)</span>
              </td>`;
    }).join('');

    const name = escapeHtml(flowName(flowId));
    return `<tr><th class="rowh" title="${name}">${name}</th>${cells}<td class="tot">${laneRuns.length}</td></tr>`;
  }).join('');

  return `<table class="stat">
            <thead><tr><th></th>${header}<th class="tot">total</th></tr></thead>
            <tbody>${rows}</tbody>
          </table>`;
}

function truncate(text, maxLength) {
  return text.length > maxLength ? `${text.slice(0, maxLength - 1)}…` : text;
}

/** Uppercase names in the system font at 12px average about this. */
const ESTIMATED_CHAR_PX = 7.2;
const estimateTextWidth = (text) => text.length * ESTIMATED_CHAR_PX;

/**
 * `text` if it fits in `maxPx`, otherwise as much of it as does, ending in an ellipsis.
 *
 * Starts from a proportional guess and steps from there, so a measurement that costs a
 * layout is taken a few times per name rather than once per character.
 */
export function fitText(text, maxPx, textWidth = estimateTextWidth) {
  const full = textWidth(text);
  if (full <= maxPx) return text;

  const cut = (keep) => `${text.slice(0, keep)}…`;
  let keep = Math.max(1, Math.min(text.length - 1, Math.floor((text.length * maxPx) / full)));
  while (keep > 1 && textWidth(cut(keep)) > maxPx) keep -= 1;
  while (keep < text.length - 1 && textWidth(cut(keep + 1)) <= maxPx) keep += 1;
  return cut(keep);
}

function groupBy(items, keyOf) {
  const grouped = new Map();
  for (const item of items) {
    const key = keyOf(item);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(item);
  }
  return grouped;
}
