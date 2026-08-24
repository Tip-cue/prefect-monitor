/**
 * Linking a downstream run to the upstream run that triggered it, and resolving the
 * flow slugs that automation triggers name.
 *
 * Links between top-level runs come from the server's automation events. Where those are
 * gone — Prefect prunes events in days and runs in weeks — a run can instead be matched to
 * the upstream run that shares its batch label. That is inference, so it never overrides an
 * event, and what it produces is marked `inferred` and drawn dashed.
 *
 * The earlier attempt at this disagreed with the events on 68 of 119 links, because it took
 * the *first* upstream run sharing the label. A batch usually has many: an ingest flow that
 * runs each minute tags every run with the quarter-hour it belongs to, and the automation
 * fires on the last of them. Matching the one that **finished last** is what makes it agree
 * — see inferredLinks.
 *
 * Sub-flows are the one exception, and a narrower claim: inside one parent run their
 * order is a fact about that parent. See sequentialChainLinks.
 */

import { stateName } from './states.js';
import { runStart, runEnd } from './time.js';

/**
 * Every run-to-run link, from the automation events.
 *
 * @param {object[]} runs
 * @param {{upstreamRunId: string, downstreamRunId: string, basis?: string}[]} exactLinks
 * @returns {[object, object, string][]} [upstream, downstream, basis] triples, both run ends
 *   present in `runs`. `basis` is 'event' unless the link was inferred — see inferredLinks.
 */
export function runLinkPairs(runs, exactLinks = []) {
  const byId = new Map(runs.map((run) => [run.id, run]));
  const pairs = [];

  for (const { upstreamRunId, downstreamRunId, basis = 'event' } of exactLinks) {
    const upstream = byId.get(upstreamRunId);
    const downstream = byId.get(downstreamRunId);
    if (upstream && downstream) pairs.push([upstream, downstream, basis]);
  }
  return pairs;
}

/**
 * Links each sub-flow run to the sibling that finished most recently before it started.
 *
 * This is a different claim from the automation links, and a weaker one: it is the
 * order the parent flow called them, which is a fact about the parent, not evidence
 * that one sub-flow triggered the next. It is only used inside the sub-flow pop-up,
 * where every run shown belongs to a known parent.
 *
 * Predecessor rather than a linear chain, because sub-flows fan out: one run's
 * datadispatcher is followed by *both* the delta writer and post process, which start
 * together a second after it ends. Chaining consecutive siblings in start order kept
 * only one of those and left the other looking unrelated.
 *
 * A run with nothing finished before it — the first, or one that overlapped everything
 * before it — is left unlinked. That also means a link can never point backwards in
 * time, which chaining by start order could.
 *
 * @param {object[]} runs sub-flow runs
 * @param {Map<string, string>} parentByRun run id → parent flow run id
 * @returns {{upstreamRunId: string, downstreamRunId: string}[]}
 */
export function sequentialChainLinks(runs, parentByRun) {
  const byParent = new Map();

  for (const run of runs) {
    const parent = parentByRun.get(run.id);
    if (!parent) continue; // no known parent, so no sequence to place it in
    if (!byParent.has(parent)) byParent.set(parent, []);
    byParent.get(parent).push(run);
  }

  const links = [];
  for (const siblings of byParent.values()) {
    for (const run of siblings) {
      let predecessor = null;

      for (const other of siblings) {
        if (other === run || runEnd(other) > runStart(run)) continue;
        if (!predecessor || runEnd(other) > runEnd(predecessor)) predecessor = other;
      }
      if (predecessor) links.push({ upstreamRunId: predecessor.id, downstreamRunId: run.id });
    }
  }
  return links;
}

/**
 * Groups run ids into chains — one entry per run, mapped to a stable id shared by
 * everything reachable from it. A run with no links is a chain of one.
 *
 * @param {[string, string][]} links
 * @returns {Map<string, string>} run id → chain id
 */
function chainIds(runIds, links) {
  const neighbours = new Map();
  const connect = (from, to) => {
    if (!neighbours.has(from)) neighbours.set(from, []);
    neighbours.get(from).push(to);
  };
  for (const [upstream, downstream] of links) {
    connect(upstream, downstream);
    connect(downstream, upstream);
  }

  const chainOf = new Map();
  for (const start of runIds) {
    if (chainOf.has(start)) continue;

    const queue = [start];
    chainOf.set(start, start);
    while (queue.length > 0) {
      for (const next of neighbours.get(queue.pop()) ?? []) {
        if (chainOf.has(next)) continue;
        chainOf.set(next, start);
        queue.push(next);
      }
    }
  }
  return chainOf;
}

/**
 * Keeps only the runs belonging to a **chain** that contains a run in one of
 * `selectedStates`, and drops everything else outright.
 *
 * Per chain, not per flow: the same pipeline runs many times in a window, and
 * selecting "Failed" should leave the batches that actually failed — with their
 * upstream and downstream runs for context — rather than every batch of a pipeline
 * that failed once. A flow whose runs are all filtered away loses its lane, and a
 * pipeline with nothing left loses its box.
 *
 * @param {Set<string>} selectedStates empty means no filtering
 */
export function filterToChainsWithState(runs, selectedStates, exactLinks = []) {
  if (!selectedStates || selectedStates.size === 0) return runs;

  const links = runLinkPairs(runs, exactLinks)
    .map(([upstream, downstream]) => [upstream.id, downstream.id]);
  const chainOf = chainIds(runs.map((run) => run.id), links);

  const keep = new Set();
  for (const run of runs) {
    if (selectedStates.has(stateName(run))) keep.add(chainOf.get(run.id));
  }
  return runs.filter((run) => keep.has(chainOf.get(run.id)));
}

/**
 * Every run reachable from `startRunId` by following links in either direction —
 * the whole batch it belongs to, upstream and downstream.
 *
 * @param {[string, string][]} links [upstreamId, downstreamId] pairs
 * @returns {Set<string>}
 */
export function runChain(links, startRunId) {
  const neighbours = new Map();
  const connect = (from, to) => {
    if (!neighbours.has(from)) neighbours.set(from, []);
    neighbours.get(from).push(to);
  };

  for (const [upstream, downstream] of links) {
    connect(upstream, downstream);
    connect(downstream, upstream);
  }

  const chain = new Set([startRunId]);
  const queue = [startRunId];

  while (queue.length > 0) {
    for (const next of neighbours.get(queue.pop()) ?? []) {
      if (chain.has(next)) continue;
      chain.add(next);
      queue.push(next);
    }
  }
  return chain;
}

const normalizeSlug = (value) =>
  String(value ?? '').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');

/**
 * Resolves the flow slug in an automation trigger to a flow id.
 *
 * Event resource ids are slugs of the emitting flow's name
 * ("ingest_main_flow"). Sub-flows prefix theirs with the parent pipeline
 * ("transform_sub_flow_delta_writer" for "sub flow delta writer"), so an exact
 * match is tried first and a unique suffix match second.
 *
 * @returns {string|null} flow id, or null if nothing matched unambiguously
 */
export function resolveFlowSlug(slug, flows) {
  const wanted = normalizeSlug(slug);
  if (!wanted || wanted.includes('*')) return null; // e.g. "prefect.flow-run.*"

  const exact = flows.find((flow) => normalizeSlug(flow.name) === wanted);
  if (exact) return exact.id;

  const suffixMatches = flows.filter((flow) => {
    const name = normalizeSlug(flow.name);
    return name && wanted.endsWith(name);
  });

  return suffixMatches.length === 1 ? suffixMatches[0].id : null;
}

/** A tag or parameter value that reads as an instant, which is what batch labels are. */
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/;

/**
 * How long after an upstream run finishes its trigger can still plausibly have started the
 * downstream one.
 *
 * An automation fires immediately: measured across 122 event-confirmed links, the gap ran
 * from a few seconds to 96, with medians of 11-81 depending on the pair. Fifteen minutes is
 * generous enough to absorb a queue or a retry while still refusing to pair a run with
 * something that finished an hour earlier.
 */
export const MAX_TRIGGER_GAP_MS = 15 * 60 * 1000;

/**
 * Slack on "finished before it started".
 *
 * Some confirmed pairs have the downstream starting up to 14 seconds *before* the upstream's
 * recorded end — the trigger fires on the state change, which is written after the run is
 * done. Without this they would be refused as impossible.
 */
const START_TOLERANCE_MS = 60 * 1000;

/**
 * The batch labels a run carries, from its parameters and its tags.
 *
 * Parameters are read under the configured keys. Tags are read by shape rather than by name
 * — a bare timestamp, or the value half of `SOMETHING:<timestamp>` — because a flow tags its
 * runs however it likes and the shape is the only thing that generalises.
 *
 * A run that already carries `batchLabels` is taken at its word: the cache keeps the derived
 * labels rather than the tags and parameters they came from, which is a fifth of the size —
 * and without them a cached window had no labels at all, so every link fell to the weaker
 * timing guess.
 *
 * @param {object} run
 * @param {string[]} keys parameter names to read (see BATCH_PAYLOAD_KEYS)
 * @returns {Set<string>} usually one value; an ingest run carries both its own minute and
 *   the batch that minute belongs to
 */
export function batchLabelsOf(run, keys = []) {
  if (Array.isArray(run?.batchLabels)) return new Set(run.batchLabels);

  const labels = new Set();

  for (const key of keys) {
    const value = run?.parameters?.[key];
    if (value !== null && value !== undefined && typeof value !== 'object') labels.add(String(value));
  }
  for (const tag of run?.tags ?? []) {
    // The whole tag first: a bare ISO timestamp is full of colons, and splitting on the
    // first one turned `2026-08-20T11:00:00+00:00` into `00:00+00:00` and lost it.
    const text = String(tag);
    const value = TIMESTAMP.test(text) ? text : text.slice(text.indexOf(':') + 1);
    if (TIMESTAMP.test(value)) labels.add(value);
  }
  return labels;
}

/**
 * Links inferred from batch labels, for runs the events do not explain.
 *
 * Two tiers, both confined to `edges` — the flow-to-flow pairs the automations themselves
 * define — so nothing is ever invented between unrelated flows, and both only for runs with
 * no exact link, so an event always wins:
 *
 *   `basis: 'label'`   the upstream run sharing this run's batch label that **finished last**
 *                      before it started. They all belong to the batch; only the last one's
 *                      completion could have triggered it. This is the stronger claim.
 *   `basis: 'timing'`  where no label is available on either side — some flows publish none —
 *                      the nearest preceding run along the same edge, within
 *                      MAX_TRIGGER_GAP_MS. A guess, and drawn as one.
 *
 * @param {object[]} runs
 * @param {{edges: {src: string, dst: string}[], batchKeys?: string[],
 *   exactLinks?: {downstreamRunId: string}[]}} options
 * @returns {{upstreamRunId: string, downstreamRunId: string, batch: string,
 *   inferred: true}[]}
 */
export function inferredLinks(runs, { edges = [], batchKeys = [], exactLinks = [] } = {}) {
  const explained = new Set(exactLinks.map((link) => link.downstreamRunId));
  const upstreamFlows = new Map(); // dst flow → src flows that feed it
  for (const { src, dst } of edges) {
    if (!upstreamFlows.has(dst)) upstreamFlows.set(dst, new Set());
    upstreamFlows.get(dst).add(src);
  }

  const labelled = runs.map((run) => ({ run, labels: batchLabelsOf(run, batchKeys) }));
  const links = [];

  for (const { run, labels } of labelled) {
    if (explained.has(run.id)) continue;

    const sources = upstreamFlows.get(run.flow_id);
    if (!sources) continue; // nothing upstream of this flow, so nothing to match

    // Anything upstream of this run that could have triggered it: the right flow, finished
    // before it started (within the tolerance), and not so long before as to be unrelated.
    const plausible = labelled.filter(({ run: candidate }) => (
      sources.has(candidate.flow_id)
      && runEnd(candidate) <= runStart(run) + START_TOLERANCE_MS
      && runStart(run) - runEnd(candidate) <= MAX_TRIGGER_GAP_MS
    ));

    // A shared batch label is the stronger claim, so it is tried first. Failing that — the
    // run carries no label, or none of the candidates share it — the nearest preceding run
    // along the same automation edge is the best available guess.
    let best = null;
    let batch = null;
    for (const candidate of plausible) {
      const shared = [...candidate.labels].find((label) => labels.has(label));
      if (!shared) continue;
      if (!best || runEnd(candidate.run) > runEnd(best)) {
        best = candidate.run;
        batch = shared;
      }
    }
    if (best) {
      links.push({
        upstreamRunId: best.id, downstreamRunId: run.id, batch, basis: 'label', inferred: true,
      });
      continue;
    }

    // Only when this run carries no label at all. A run that *has* one and finds no
    // candidate sharing it is evidence against the nearest-preceding run being its trigger —
    // we know which batch it belongs to and no run from that batch is here. Guessing anyway
    // produced 931 dashed lines on one 6h window where the events explained 975.
    if (labels.size > 0) continue;

    for (const candidate of plausible) {
      if (!best || runEnd(candidate.run) > runEnd(best)) best = candidate.run;
    }
    if (best) {
      links.push({
        upstreamRunId: best.id, downstreamRunId: run.id, batch: null, basis: 'timing', inferred: true,
      });
    }
  }
  return links;
}
