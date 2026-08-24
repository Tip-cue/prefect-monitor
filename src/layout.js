/**
 * Arranging flows into lanes: which pipeline each flow belongs to, what order the
 * lanes go in, and how concurrent runs stack inside one lane.
 */

import { runStart, runEnd } from './time.js';


/**
 * Groups flows into connected pipelines and orders the lanes within each.
 *
 * A group is one whole pipeline — the first flow and everything it transitively
 * triggers — and
 * is drawn as a single enclosing area, so the chart reads as a map of pipelines
 * rather than a flat list of flows.
 *
 * Ordering inside a group is depth-first from its roots, so a chain occupies
 * consecutive lanes and its links stay short and un-crossed. (A plain topological
 * sort interleaves unrelated flows between a parent and its child, which is what
 * produced long crossing curves.) Children are visited leaves-first so that a large
 * subtree lands last and does not push its parent's other edges across the chart.
 *
 * @param {string[]} flowIds
 * @param {{src: string, dst: string}[]} edges
 * @param {(flowId: string) => string} [label] used only for stable alphabetic tie-breaks
 * @returns {string[][]} groups, largest pipeline first, isolated flows last
 */
export function laneGroups(flowIds, edges, label = String) {
  const graph = buildGraph(flowIds, edges);
  const byName = (a, b) => String(label(a)).localeCompare(String(label(b)));

  return connectedComponents(flowIds, graph.undirected)
    .map((members) => orderComponent(members, graph, byName))
    .sort((a, b) => b.length - a.length || byName(a[0], b[0]));
}

/** The lanes of every pipeline, flattened top to bottom. */
export function laneOrder(flowIds, edges, label) {
  return laneGroups(flowIds, edges, label).flat();
}

function buildGraph(flowIds, edges) {
  const known = new Set(flowIds);
  const children = new Map(flowIds.map((id) => [id, []]));
  const undirected = new Map(flowIds.map((id) => [id, []]));
  const inDegree = new Map(flowIds.map((id) => [id, 0]));
  const seen = new Set();

  for (const { src, dst } of edges) {
    if (!known.has(src) || !known.has(dst) || src === dst) continue;

    const key = `${src}>${dst}`;
    if (seen.has(key)) continue;
    seen.add(key);

    children.get(src).push(dst);
    inDegree.set(dst, inDegree.get(dst) + 1);
    undirected.get(src).push(dst);
    undirected.get(dst).push(src);
  }

  return { children, undirected, inDegree };
}

function connectedComponents(flowIds, undirected) {
  const assigned = new Set();
  const components = [];

  for (const start of flowIds) {
    if (assigned.has(start)) continue;

    const members = [];
    const stack = [start];
    assigned.add(start);

    while (stack.length > 0) {
      const node = stack.pop();
      members.push(node);
      for (const neighbour of undirected.get(node)) {
        if (assigned.has(neighbour)) continue;
        assigned.add(neighbour);
        stack.push(neighbour);
      }
    }
    components.push(members);
  }
  return components;
}

function orderComponent(members, { children, inDegree }, byName) {
  const subtreeSizes = new Map();

  /** Number of flows reachable from `node`, itself included. */
  const subtreeSize = (node, visiting = new Set()) => {
    if (subtreeSizes.has(node)) return subtreeSizes.get(node);
    if (visiting.has(node)) return 0; // a cycle contributes nothing further

    visiting.add(node);
    const size = 1 + children.get(node).reduce((sum, kid) => sum + subtreeSize(kid, visiting), 0);
    subtreeSizes.set(node, size);
    return size;
  };

  const smallestSubtreeFirst = (a, b) => subtreeSize(a) - subtreeSize(b) || byName(a, b);
  const largestSubtreeFirst = (a, b) => subtreeSize(b) - subtreeSize(a) || byName(a, b);

  const inComponent = new Set(members);
  const ordered = [];
  const placed = new Set();

  const visit = (node) => {
    if (placed.has(node) || !inComponent.has(node)) return;
    placed.add(node);
    ordered.push(node);
    [...children.get(node)].sort(smallestSubtreeFirst).forEach(visit);
  };

  members.filter((id) => inDegree.get(id) === 0).sort(largestSubtreeFirst).forEach(visit);
  [...members].sort(byName).forEach(visit); // whatever a cycle left unreachable

  return ordered;
}

/**
 * Beyond this many concurrent runs a lane stops growing and reuses the row that
 * frees up first. Overlaps can reappear, but the chart stays a sane height.
 */
export const MAX_SUB_ROWS = 8;

/**
 * Assigns each run a sub-row so concurrent runs of one flow sit side by side
 * instead of on top of each other (where only the last drawn would be visible).
 *
 * Standard interval partitioning: runs in start order, each taking the first row
 * that is free at its start time.
 *
 * @returns {{rowOf: Map<string, number>, rows: number}}
 */
export function packRunsIntoRows(runs) {
  const rowOf = new Map();
  const rowFreeFrom = [];

  for (const run of [...runs].sort((a, b) => runStart(a) - runStart(b))) {
    const start = runStart(run);

    let row = rowFreeFrom.findIndex((freeFrom) => freeFrom <= start);
    if (row < 0) {
      row = rowFreeFrom.length < MAX_SUB_ROWS
        ? rowFreeFrom.length
        : rowFreeFrom.indexOf(Math.min(...rowFreeFrom));
    }

    rowFreeFrom[row] = Math.max(rowFreeFrom[row] ?? 0, runEnd(run));
    rowOf.set(run.id, row);
  }

  return { rowOf, rows: Math.max(1, rowFreeFrom.length) };
}
