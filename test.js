/**
 * Self-check for the pure logic: `npm test`.
 *
 * Everything here runs without a browser or a Prefect server. Rendering is left to
 * the eye, but the decisions behind it — lane order, run linking, binning — are not.
 */

import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

import { stateColor, worstRun, STATE_COLORS } from './src/states.js';
import { timeTicks, overlapsWindow } from './src/time.js';
import { laneGroups, laneOrder, packRunsIntoRows, MAX_SUB_ROWS } from './src/layout.js';
import {
  resolveFlowSlug, runChain, filterToChainsWithState, runLinkPairs, sequentialChainLinks,
  batchLabelsOf, inferredLinks,
} from './src/links.js';
import { markPath, tooltipPosition, popoverOffset, fitText } from './src/render.js';
import {
  monthGrid, monthLabel, shiftMonth, daysInMonth, instantOf, partsOf, clampTime, WEEKDAYS,
} from './src/calendar.js';
import { setZone, getZone, formatStamp, zoneLabel } from './src/zone.js';
import {
  planFetch, mergeRuns, mergeLinks, unsettledRunIds, projectForStorage,
} from './src/run-cache.js';
import {
  MIN_THUMB_PX, clampZoom, thumbGeometry, zoomFromDrag, zoomFromThumb,
} from './src/zoom.js';
import {
  runUrlFromApiBase, PrefectApi, batchFromPayload, BATCH_PAYLOAD_KEYS,
} from './src/prefect-api.js';
import { readCache, writeCache } from './src/browser-cache.js';
import {
  resolveApiUrl, apiUrlEditable, resolveBatchKeys, FALLBACK_API_URL,
} from './src/settings.js';
import {
  parseTimeExpression, resolveRange, describeRange, viewFromQuery, viewToQuery, DEFAULT_RANGE,
  QUICK_RANGES, DURATIONS, MAX_RANGE_MS, anchorStart, capRange, composeRange, formatLocal,
  rangeFromStart, rangeToEnd, restampRange,
} from './src/time-range.js';

const iso = (ms) => new Date(ms).toISOString();
const run = (id, startMs, endMs, extra = {}) => ({
  id,
  start_time: iso(startMs),
  end_time: iso(endMs),
  ...extra,
});

const MINUTE = 60_000;

test('a lane name is cut to the room it has, ending in an ellipsis', () => {
  const width = (text) => text.length * 10;
  assert.equal(fitText('ABCDEF', 60, width), 'ABCDEF', 'fits exactly');
  assert.equal(fitText('ABCDEF', 50, width), 'ABCD…', 'the ellipsis counts');
  assert.equal(fitText('ABCDEF', 5, width), 'A…', 'never less than one character');
});

test('time axis keeps a readable number of ticks', () => {
  const ticks = timeTicks(0, 60 * MINUTE);
  assert.ok(ticks.length >= 4 && ticks.length <= 10, `got ${ticks.length} ticks`);
});

test('states', async (t) => {
  await t.test('colour comes from the state', () => {
    assert.equal(stateColor({ state_type: 'COMPLETED' }), STATE_COLORS.COMPLETED);
  });

  await t.test('composite state names fall back to their state type', () => {
    const color = stateColor({ state_name: 'InfrastructurePending', state_type: 'PENDING' });
    assert.equal(color, STATE_COLORS.PENDING);
  });

  await t.test('a bin reports its worst state', () => {
    const runs = [{ state_type: 'COMPLETED' }, { state_type: 'FAILED' }, { state_type: 'COMPLETED' }];
    assert.equal(worstRun(runs).state_type, 'FAILED');
  });

  await t.test('crashed outranks failed', () => {
    assert.equal(worstRun([{ state_type: 'FAILED' }, { state_type: 'CRASHED' }]).state_type, 'CRASHED');
  });
});

test('links come only from the automation events', async (t) => {
  const emitter = run('emitter', 0, 60_000, { flow_id: 'ingest' });
  const later = run('later', 70_000, 90_000, { flow_id: 'ingest' });
  const triggered = run('triggered', 120_000, 180_000, { flow_id: 'transform' });
  const runs = [emitter, later, triggered];

  await t.test('the event decides, not whichever run looks likelier', () => {
    // `later` finished more recently and would win any time-based guess.
    const pairs = runLinkPairs(runs, [{ upstreamRunId: 'emitter', downstreamRunId: 'triggered' }]);
    assert.equal(pairs.length, 1);
    assert.deepEqual([pairs[0][0].id, pairs[0][1].id], ['emitter', 'triggered']);
  });

  await t.test('a run whose trigger is off-window gets no link at all', () => {
    // Drawing some visible upstream instead would be a plausible lie.
    const links = [{ upstreamRunId: 'not-in-window', downstreamRunId: 'triggered' }];
    assert.deepEqual(runLinkPairs(runs, links), []);
  });

  await t.test('no events means no links — nothing is inferred', () => {
    assert.deepEqual(runLinkPairs(runs, []), []);
  });
});

test('batch labels a run carries', async (t) => {
  await t.test('parameters under the configured keys', () => {
    const run = { parameters: { run_partition: '2026-08-20T11:00:00+00:00' }, tags: [] };
    assert.deepEqual([...batchLabelsOf(run, ['run_partition'])], ['2026-08-20T11:00:00+00:00']);
  });

  await t.test('tags by shape, bare or prefixed', () => {
    // A bare ISO timestamp is full of colons; splitting on the first one used to turn
    // 2026-08-20T11:00:00+00:00 into 00:00+00:00 and lose the batch entirely.
    const run = {
      tags: ['auto-scheduled', '2026-08-20T11:12:00+00:00',
        'MINUTE_COMPLETE:2026-08-20T11:12:00+00:00', '2026-08-20T11:00:00+00:00', 'INCOMPLETE'],
      parameters: {},
    };
    assert.deepEqual([...batchLabelsOf(run, [])].sort(),
      ['2026-08-20T11:00:00+00:00', '2026-08-20T11:12:00+00:00']);
  });

  await t.test('a run with nothing timestamp-shaped carries no label', () => {
    assert.equal(batchLabelsOf({ tags: ['external_job:8675309'], parameters: {} }).size, 0);
  });
});

test('links inferred where the events do not reach', async (t) => {
  const edges = [{ src: 'ingest', dst: 'transform' }];
  const base = Date.parse('2026-08-20T11:00:00Z');
  const at = (seconds) => base + seconds * 1000;
  const batch = '2026-08-20T11:00:00+00:00';

  const ingestRun = (id, endsAt, tags = [batch]) => ({
    ...run(id, endsAt - 60_000, endsAt), flow_id: 'ingest', tags, parameters: {},
  });
  const transformRun = (id, startsAt, params = { batch }) => ({
    ...run(id, startsAt, startsAt + 60_000), flow_id: 'transform', tags: [], parameters: params,
  });

  // Three ingest runs in the same batch, a minute apart; the transform starts 10s after the
  // last of them finishes — the shape the events actually show.
  const labelled = [
    ingestRun('i1', at(0)), ingestRun('i2', at(60)), ingestRun('i3', at(120)),
    transformRun('t1', at(130)),
  ];

  await t.test('the upstream that finished last carries the label match', () => {
    // The first attempt at this took the *first* run carrying the label and disagreed with
    // the events on 68 of 119 links: an ingest flow running every minute tags every run
    // with the batch, and the automation fires on the last one.
    assert.deepEqual(inferredLinks(labelled, { edges, batchKeys: ['batch'] }), [{
      upstreamRunId: 'i3', downstreamRunId: 't1', batch, basis: 'label', inferred: true,
    }]);
  });

  await t.test('a downstream that started just before the upstream ended still links', () => {
    // Confirmed pairs run to 14s of overlap: the trigger fires on the state change, which
    // is written after the run has finished.
    const overlapping = [ingestRun('i9', at(140)), transformRun('t9', at(126))];
    const [link] = inferredLinks(overlapping, { edges, batchKeys: ['batch'] });
    assert.equal(link.upstreamRunId, 'i9');
  });

  await t.test('never an upstream that finished well after it started', () => {
    const late = [...labelled, ingestRun('i4', at(600))];
    assert.equal(inferredLinks(late, { edges, batchKeys: ['batch'] })[0].upstreamRunId, 'i3');
  });

  await t.test('an event link is never second-guessed', () => {
    const links = inferredLinks(labelled, {
      edges, batchKeys: ['batch'], exactLinks: [{ downstreamRunId: 't1' }],
    });
    assert.deepEqual(links, []);
  });

  await t.test('only along the edges the automations define', () => {
    const stranger = { ...transformRun('x1', at(130)), flow_id: 'unrelated' };
    const links = inferredLinks([...labelled, stranger], { edges, batchKeys: ['batch'] });
    assert.deepEqual(links.map((l) => l.downstreamRunId), ['t1']);
  });

  await t.test('with no label on either side, the nearest preceding run — marked as timing', () => {
    // Some flows publish no batch anywhere. Guessing from timing is worth more than an
    // unlinked chain, as long as the chart says it is a guess.
    const unlabelled = [
      ingestRun('u1', at(0), []), ingestRun('u2', at(60), []),
      transformRun('u3', at(70), {}),
    ];
    assert.deepEqual(inferredLinks(unlabelled, { edges, batchKeys: ['batch'] }), [{
      upstreamRunId: 'u2', downstreamRunId: 'u3', batch: null, basis: 'timing', inferred: true,
    }]);
  });

  await t.test('a labelled run whose batch-mate is absent gets no timing guess', () => {
    // It says which batch it belongs to and no run from that batch is here — that is
    // evidence against the nearest preceding run, not for it. Guessing anyway produced 931
    // dashed lines on one 6h window whose events explained 975.
    const orphan = [ingestRun('o1', at(0), ['2026-08-20T09:00:00+00:00']), transformRun('o2', at(10))];
    assert.deepEqual(inferredLinks(orphan, { edges, batchKeys: ['batch'] }), []);
  });

  await t.test('nothing is invented across an implausible gap', () => {
    const distant = [
      ingestRun('d1', at(0), []),
      transformRun('d2', at(60 * 60), {}), // an hour later: no automation waits that long
    ];
    assert.deepEqual(inferredLinks(distant, { edges, batchKeys: ['batch'] }), []);
  });

  await t.test('the basis reaches the renderer, which dashes only a timing guess', () => {
    // A shared batch label is evidence and draws solid, like an event link. A
    // nearest-preceding-run guess is not, and must not look like one.
    const [labelLink] = inferredLinks(labelled, { edges, batchKeys: ['batch'] });
    assert.equal(runLinkPairs(labelled, [labelLink])[0][2], 'label');

    const unlabelled = [ingestRun('u1', at(0), []), transformRun('u2', at(10), {})];
    const [timingLink] = inferredLinks(unlabelled, { edges, batchKeys: ['batch'] });
    assert.equal(runLinkPairs(unlabelled, [timingLink])[0][2], 'timing');

    const fromEvent = [{ upstreamRunId: 'i1', downstreamRunId: 't1' }];
    assert.equal(runLinkPairs(labelled, fromEvent)[0][2], 'event', 'the default');
  });
});

test('resolving an automation trigger to a flow', async (t) => {
  const flows = [
    { id: 'F1', name: 'INGEST_MAIN_FLOW' },
    { id: 'F2', name: 'sub flow delta writer' },
  ];

  await t.test('exact slug match', () => {
    assert.equal(resolveFlowSlug('ingest_main_flow', flows), 'F1');
  });

  await t.test('sub-flow emitters match on their suffix', () => {
    assert.equal(resolveFlowSlug('transform_sub_flow_delta_writer', flows), 'F2');
  });

  await t.test('wildcard resources are ignored', () => {
    assert.equal(resolveFlowSlug('prefect.flow-run.*', flows), null);
  });
});

test('lane order', async (t) => {
  await t.test('puts a source above its target', () => {
    assert.equal(laneOrder(['b', 'a'], [{ src: 'a', dst: 'b' }])[0], 'a');
  });

  await t.test('keeps a chain contiguous and sinks isolated flows', () => {
    // Otherwise the chain's links cross every unrelated lane in between.
    const lanes = laneOrder(['x', 'ingest', 'transform', 'export'], [
      { src: 'ingest', dst: 'transform' },
      { src: 'transform', dst: 'export' },
    ]);
    assert.deepEqual(lanes, ['ingest', 'transform', 'export', 'x']);
  });

  await t.test('still emits every lane when edges form a cycle', () => {
    const lanes = laneOrder(['a', 'b'], [{ src: 'a', dst: 'b' }, { src: 'b', dst: 'a' }]);
    assert.equal(lanes.length, 2);
  });
});

test('pipeline grouping', () => {
  const groups = laneGroups(['x', 'ingest', 'transform', 'export', 'y', 'lonely'], [
    { src: 'ingest', dst: 'transform' },
    { src: 'transform', dst: 'export' },
    { src: 'x', dst: 'y' },
  ]);

  assert.equal(groups.length, 3, 'one group per connected pipeline');
  assert.deepEqual(groups[0], ['ingest', 'transform', 'export'], 'biggest pipeline first, kept contiguous');
  assert.deepEqual(groups[1], ['x', 'y']);
  assert.deepEqual(groups[2], ['lonely'], 'unconnected flow is its own group, last');
});

test('packing concurrent runs into sub-rows', async (t) => {
  const packed = packRunsIntoRows([
    run('a', 0, 10 * MINUTE),
    run('b', 5 * MINUTE, 15 * MINUTE), // overlaps a
    run('c', 20 * MINUTE, 25 * MINUTE), // starts after a ended
  ]);

  await t.test('overlapping runs never share a row', () => {
    assert.equal(packed.rows, 2);
    assert.notEqual(packed.rowOf.get('a'), packed.rowOf.get('b'));
  });

  await t.test('a later run reuses a freed row', () => {
    assert.equal(packed.rowOf.get('c'), packed.rowOf.get('a'));
  });

  await t.test('sub-rows are capped so one burst cannot blow up the lane height', () => {
    const allOverlapping = Array.from({ length: 20 }, (_, i) => run(`r${i}`, 0, 15 * MINUTE));
    assert.equal(packRunsIntoRows(allOverlapping).rows, MAX_SUB_ROWS);
  });
});

test('filtering by state keeps matching chains and drops the rest', async (t) => {
  const ok = (id, flow, at) => run(id, at, at + 30_000,
    { flow_id: flow, state_name: 'Completed', state_type: 'COMPLETED' });
  const failed = (id, flow, at) => ({ ...ok(id, flow, at), state_name: 'Failed', state_type: 'FAILED' });

  // Two runs of the same pipeline: batch A is healthy, batch B fails downstream.
  const runs = [
    ok('ingestA', 'ingest', 3600_000), ok('transformA', 'transform', 3600_000 + 60_000),
    ok('ingestB', 'ingest', 7200_000), failed('transformB', 'transform', 7200_000 + 60_000),
  ];
  const links = [
    { upstreamRunId: 'ingestA', downstreamRunId: 'transformA' },
    { upstreamRunId: 'ingestB', downstreamRunId: 'transformB' },
  ];
  const kept = filterToChainsWithState(runs, new Set(['Failed']), links);

  await t.test('keeps the failing chain, upstream included for context', () => {
    assert.deepEqual(kept.map((r) => r.id).sort(), ['ingestB', 'transformB']);
  });

  // The point of chain-scoping: the healthy batch of the *same* pipeline is gone.
  await t.test('drops other runs of the same flows', () => {
    assert.ok(!kept.some((r) => r.id === 'ingestA' || r.id === 'transformA'));
  });

  await t.test('an unset filter changes nothing', () => {
    assert.equal(filterToChainsWithState(runs, new Set(), links).length, 4);
  });

  await t.test('a state nothing is in leaves nothing on screen', () => {
    assert.equal(filterToChainsWithState(runs, new Set(['Crashed']), links).length, 0);
  });

  await t.test('an unlinked run stands alone and is kept on its own merit', () => {
    const lonely = failed('solo', 'other', 3600_000);
    const out = filterToChainsWithState([...runs, lonely], new Set(['Failed']), links);
    assert.deepEqual(out.map((r) => r.id).sort(), ['ingestB', 'solo', 'transformB']);
  });
});

test('a run chain spans the links in both directions', async (t) => {
  //   a -> b -> c      and a separate  x -> y
  const links = [['a', 'b'], ['b', 'c'], ['x', 'y']];

  await t.test('from the middle, reaches both ends', () => {
    assert.deepEqual([...runChain(links, 'b')].sort(), ['a', 'b', 'c']);
  });

  await t.test('does not leak into an unrelated chain', () => {
    assert.ok(!runChain(links, 'a').has('x'));
  });

  await t.test('an unlinked run is a chain of one', () => {
    assert.deepEqual([...runChain(links, 'lonely')], ['lonely']);
  });

  await t.test('a cycle terminates', () => {
    assert.equal(runChain([['a', 'b'], ['b', 'a']], 'a').size, 2);
  });
});

test('the batch a triggering event was about', async (t) => {
  await t.test('read from the payload, first key that is there', () => {
    assert.equal(batchFromPayload({ batch: '2026-08-19T06:00' }), '2026-08-19T06:00');
    assert.equal(batchFromPayload({ interval_start: '2026-08-19T06:00' }), '2026-08-19T06:00');
  });

  await t.test('earlier keys win, so a server can carry more than one', () => {
    const payload = { batch: 'wanted', datetime: 'also here' };
    assert.ok(BATCH_PAYLOAD_KEYS.indexOf('batch') < BATCH_PAYLOAD_KEYS.indexOf('datetime'));
    assert.equal(batchFromPayload(payload), 'wanted');
  });

  await t.test('a payload that does not say gets no label rather than a guess', () => {
    // The alternative — falling back to when the event fired — reads as a batch key and
    // is not one. Two chains a second apart would look like different batches.
    assert.equal(batchFromPayload({ unrelated: 1 }), null);
    assert.equal(batchFromPayload(undefined), null);
    assert.equal(batchFromPayload({ batch: null }), null);
  });

  await t.test('structure is not a label', () => {
    assert.equal(batchFromPayload({ batch: { from: 1, to: 2 } }), null);
    assert.equal(batchFromPayload({ batch: ['a'] }), null);
  });

  await t.test('a non-string scalar still labels', () => {
    assert.equal(batchFromPayload({ partition: 20260819 }), '20260819');
  });

  await t.test('the keys can be replaced for a server that names it something else', () => {
    assert.equal(batchFromPayload({ run_key: 'k' }, ['run_key']), 'k');
  });
});

test('where the API is, and who may change it', async (t) => {
  const deployed = 'monitor.example.com';

  await t.test('served next to the server, the API is same-origin and fixed', () => {
    assert.equal(resolveApiUrl({ hostname: deployed }), '/api');
    assert.equal(apiUrlEditable({ hostname: deployed }), false, 'nothing to choose');
  });

  await t.test('a stored URL cannot redirect a same-origin page', () => {
    // Left behind by a dev session on the same hostname; the deployed page must ignore it.
    assert.equal(resolveApiUrl({ hostname: deployed, storedUrl: 'http://elsewhere/api' }), '/api');
  });

  await t.test('on a dev server the field appears and what was typed wins', () => {
    assert.equal(resolveApiUrl({ hostname: 'localhost' }), FALLBACK_API_URL);
    assert.equal(apiUrlEditable({ hostname: 'localhost' }), true);
    assert.equal(resolveApiUrl({ hostname: 'localhost', storedUrl: 'http://typed/api' }),
      'http://typed/api');
  });

  await t.test('a configured URL outranks both, and takes the field away with it', () => {
    const config = { apiUrl: 'https://prefect.example.com/api' };
    assert.equal(resolveApiUrl({ config, hostname: 'localhost', storedUrl: 'http://typed/api' }),
      config.apiUrl);
    assert.equal(apiUrlEditable({ config, hostname: 'localhost' }), false);
  });

  await t.test('the field can be forced either way', () => {
    assert.equal(apiUrlEditable({ config: { apiUrlEditable: true }, hostname: deployed }), true);
    assert.equal(apiUrlEditable({ config: { apiUrlEditable: false }, hostname: 'localhost' }), false);
  });
});

test('batch keys a deployment adds', async (t) => {
  const defaults = ['batch', 'partition'];

  await t.test('appended, so the conventional names still win', () => {
    const keys = resolveBatchKeys({ extraBatchKeys: ['run_partition'] }, defaults);
    assert.deepEqual(keys, ['batch', 'partition', 'run_partition']);
    assert.ok(keys.indexOf('batch') < keys.indexOf('run_partition'));
  });

  await t.test('nothing configured leaves the defaults alone', () => {
    assert.deepEqual(resolveBatchKeys({}, defaults), defaults);
  });

  await t.test('blank and duplicate entries are dropped', () => {
    assert.deepEqual(resolveBatchKeys({ extraBatchKeys: [' x ', '', 'batch'] }, defaults),
      ['batch', 'partition', 'x']);
  });
});

test('a client reads the batch keys it was given', () => {
  // The default list is the module's; a deployment's list has to reach the reading code.
  const api = new PrefectApi('http://p/api', { batchKeys: ['run_partition'] });
  assert.deepEqual(api.batchKeys, ['run_partition']);
  assert.deepEqual(new PrefectApi('http://p/api').batchKeys, BATCH_PAYLOAD_KEYS);
});

test('whether the server still has events for a window', async (t) => {
  const realFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = realFetch; });
  const answer = (total) => {
    globalThis.fetch = async () => ({ ok: true, json: async () => ({ total, events: [] }) });
  };

  await t.test('some events means links were simply not triggered', async () => {
    answer(9554);
    assert.equal(await new PrefectApi('http://p/api').hasEventsIn(0, 1), true);
  });

  await t.test('none means they are gone, and no link can ever be drawn', async () => {
    // Runs outlive their events: Prefect prunes events in days and runs in weeks, so an old
    // window has runs with nothing left to explain them.
    answer(0);
    assert.equal(await new PrefectApi('http://p/api').hasEventsIn(0, 1), false);
  });

  await t.test('a server that reports no total is read from the page instead', async () => {
    globalThis.fetch = async () => ({ ok: true, json: async () => ({ events: [{ id: 'e' }] }) });
    assert.equal(await new PrefectApi('http://p/api').hasEventsIn(0, 1), true);
  });
});

test('runs deep-link into the Prefect UI beside the API', async (t) => {
  await t.test('from an absolute API url', () => {
    assert.equal(
      runUrlFromApiBase('https://p.example/api', 'abc'),
      'https://p.example/runs/flow-run/abc',
    );
  });

  // Deployed under the Prefect host the API is same-origin, so the link is a bare
  // path and navigating to it stays in the tab.
  await t.test('from a same-origin relative API url', () => {
    assert.equal(runUrlFromApiBase('/api', 'abc'), '/runs/flow-run/abc');
  });
});

test('time range expressions', async (t) => {
  const now = Date.parse('2026-08-19T10:00:00Z');

  await t.test('now, and offsets from it', () => {
    assert.equal(parseTimeExpression('now', now), now);
    assert.equal(parseTimeExpression('now-90m', now), now - 90 * MINUTE);
    assert.equal(parseTimeExpression('now-7d', now), now - 7 * 24 * 60 * MINUTE);
    assert.equal(parseTimeExpression('now+1h', now), now + 60 * MINUTE);
  });

  await t.test('absolute timestamps, ISO or typed', () => {
    assert.equal(parseTimeExpression('2026-08-19T09:44:00Z', now), Date.parse('2026-08-19T09:44:00Z'));
    // The shape a person types is not valid ISO; it is read as local time.
    assert.equal(parseTimeExpression('2026-08-19 09:44', now), Date.parse('2026-08-19T09:44'));
  });

  await t.test('nonsense is rejected rather than guessed at', () => {
    assert.equal(parseTimeExpression('yesterday', now), null);
    assert.equal(parseTimeExpression('', now), null);
    assert.equal(parseTimeExpression('now-5', now), null); // no unit
  });

  await t.test('an inverted or empty range does not resolve', () => {
    assert.equal(resolveRange({ from: 'now', to: 'now-1h' }, now), null);
    assert.equal(resolveRange({ from: 'now', to: 'now' }, now), null);
  });

  await t.test('a relative range keeps meaning the same as time passes', () => {
    const range = { from: 'now-6h', to: 'now' };
    const early = resolveRange(range, now);
    const later = resolveRange(range, now + 60 * MINUTE);
    assert.equal(later.to - later.from, early.to - early.from);
    assert.ok(later.from > early.from, 'the window moved with the clock');
  });
});

test('picking a start point and a duration', async (t) => {
  const now = Date.parse('2026-08-20T14:30:00+03:00');

  await t.test('the offered ranges all fit what one fetch can read', () => {
    for (const quick of QUICK_RANGES) {
      const span = resolveRange({ from: quick.from, to: 'now' }, now);
      assert.ok(span.to - span.from <= MAX_RANGE_MS, `${quick.label} is longer than the cap`);
    }
    assert.equal(DURATIONS.at(-1).ms, MAX_RANGE_MS);
  });

  await t.test('an anchor is local midnight, that many days back', () => {
    const yesterday = new Date(anchorStart(1, now));
    assert.equal(yesterday.getHours(), 0);
    assert.equal(yesterday.getMinutes(), 0);
    assert.equal(new Date(now).getDate() - yesterday.getDate(), 1);
  });

  await t.test('start plus duration is an absolute pair, so it cannot drift', () => {
    const range = rangeFromStart(anchorStart(1, now), 6 * 3600_000);
    assert.match(range.from, /^\d{4}-\d{2}-\d{2} 00:00$/);
    assert.match(range.to, /^\d{4}-\d{2}-\d{2} 06:00$/);

    // Resolved an hour later it still means the same six hours.
    const early = resolveRange(range, now);
    const later = resolveRange(range, now + 3600_000);
    assert.deepEqual(early, later);
  });

  await t.test('"until now" from a start keeps running on, and caps if it has to', () => {
    // Today 00:00 onwards is under a day, so it stays open-ended and keeps up with `now`.
    const today = { from: formatLocal(anchorStart(0, now)), to: 'now' };
    assert.equal(capRange(today, now), today);
    assert.ok(resolveRange(today, now).to > resolveRange(today, now - 3600_000).to,
      'the end follows the clock rather than being pinned');

    // From three days ago it cannot: the cap keeps the recent day and says so in the label.
    const older = { from: formatLocal(anchorStart(3, now)), to: 'now' };
    assert.deepEqual(capRange(older, now), { from: 'now-24h', to: 'now' });
  });

  await t.test('a duration longer than the cap is shortened, not refused', () => {
    const range = rangeFromStart(anchorStart(2, now), 48 * 3600_000);
    const resolved = resolveRange(range, now);
    assert.equal(resolved.to - resolved.from, MAX_RANGE_MS);
  });
});

test('the picker button label', async (t) => {
  await t.test('a quick range is named', () => {
    assert.equal(describeRange({ from: 'now-6h', to: 'now' }), 'Last 6 hours');
  });

  await t.test('a day and two clock times, not two full timestamps', () => {
    assert.equal(describeRange({ from: '2026-08-19 00:00', to: '2026-08-19 06:00' }),
      'Aug 19 00:00-06:00'.replace('-', '\u2013'));
  });

  await t.test('crossing midnight names both days', () => {
    assert.equal(describeRange({ from: '2026-08-19 18:00', to: '2026-08-20 06:00' }),
      'Aug 19 18:00 \u2192 Aug 20 06:00');
  });
});

test('two of start, end and duration make a range', async (t) => {
  const hour = 3600_000;
  const day = 24 * hour;
  const at = (text) => Date.parse(text);

  await t.test('moving the start keeps the span, not the old end', () => {
    // The reported bug: looking at 7 days ago for 6h, then clicking "3 days ago". The end
    // field still held 08-13 06:00, which is four days *before* the new start.
    const range = composeRange({
      startMs: at('2026-08-17T00:00'),
      endMs: at('2026-08-13T06:00'),
      durationMs: 6 * hour,
      spanMs: 6 * hour,
    });
    assert.deepEqual(range, { from: '2026-08-17 00:00', to: '2026-08-17 06:00' });
    assert.ok(resolveRange(range), 'and it is not inverted');
  });

  await t.test('a start and an end with no duration are taken as typed', () => {
    assert.deepEqual(
      composeRange({ startMs: at('2026-08-19T09:00'), endMs: at('2026-08-19T15:00'), spanMs: hour }),
      { from: '2026-08-19 09:00', to: '2026-08-19 15:00' },
    );
  });

  await t.test('an inverted typed pair is left inverted for the caller to report', () => {
    const range = composeRange({
      startMs: at('2026-08-19T15:00'), endMs: at('2026-08-19T09:00'), spanMs: hour,
    });
    assert.equal(resolveRange(range), null, 'a mistake worth reporting, not reinterpreting');
  });

  await t.test('a duration reaches back from an end when there is no start', () => {
    assert.deepEqual(composeRange({ endMs: at('2026-08-19T15:00'), durationMs: 3 * hour, spanMs: hour }),
      { from: '2026-08-19 12:00', to: '2026-08-19 15:00' });
  });

  await t.test('a duration alone is still "the last N", ending now', () => {
    assert.deepEqual(composeRange({ durationMs: 6 * hour, spanMs: 6 * hour }),
      { from: 'now-6h', to: 'now' });
  });

  await t.test('a span with no matching quick range falls back to the default', () => {
    assert.deepEqual(composeRange({ durationMs: 7 * hour, spanMs: 7 * hour }), DEFAULT_RANGE);
  });

  await t.test('a start with no duration uses the span being looked at', () => {
    assert.deepEqual(composeRange({ startMs: at('2026-08-19T00:00'), spanMs: 12 * hour }),
      { from: '2026-08-19 00:00', to: '2026-08-19 12:00' });
  });

  await t.test('a span longer than the cap is shortened where it is built', () => {
    const range = composeRange({ startMs: at('2026-08-19T00:00'), durationMs: 3 * day, spanMs: hour });
    assert.equal(resolveRange(range).to - resolveRange(range).from, MAX_RANGE_MS);
  });
});

test('a range longer than a day is capped', async (t) => {
  const now = Date.parse('2026-08-20T14:30:00+03:00');

  await t.test('a relative one keeps its shape, so the button label stays honest', () => {
    assert.deepEqual(capRange({ from: 'now-7d', to: 'now' }, now), { from: 'now-24h', to: 'now' });
  });

  await t.test('an absolute one keeps its end: the recent part is the useful part', () => {
    const capped = capRange({ from: '2026-08-10 00:00', to: '2026-08-19 00:00' }, now);
    assert.equal(capped.to, '2026-08-19 00:00');
    assert.equal(capped.from, '2026-08-18 00:00');
  });

  await t.test('editing the end keeps the end and moves the start back a day', () => {
    const capped = capRange({ from: '2026-08-10 00:00', to: '2026-08-19 08:00' }, now, 'end');
    assert.deepEqual(capped, { from: '2026-08-18 08:00', to: '2026-08-19 08:00' });
  });

  await t.test('editing the start keeps the start and moves the end on a day', () => {
    // The other way round: the end just typed is the one thing the reader was sure about,
    // so whichever they set last is the one that survives.
    const capped = capRange({ from: '2026-08-10 00:00', to: '2026-08-19 08:00' }, now, 'start');
    assert.deepEqual(capped, { from: '2026-08-10 00:00', to: '2026-08-11 00:00' });
  });

  await t.test('a duration reaching back from an end', () => {
    assert.deepEqual(rangeToEnd(Date.parse('2026-08-19T08:00'), 6 * 3600_000),
      { from: '2026-08-19 02:00', to: '2026-08-19 08:00' });
  });

  await t.test('a range that already fits is left exactly as it was', () => {
    const range = { from: 'now-6h', to: 'now' };
    assert.equal(capRange(range, now), range);
  });

  await t.test('resolving caps too, since a hand-edited URL never sees the picker', () => {
    const resolved = resolveRange({ from: 'now-7d', to: 'now' }, now);
    assert.equal(resolved.to - resolved.from, MAX_RANGE_MS);
  });

  await t.test('unreadable input is still rejected rather than capped into shape', () => {
    assert.equal(resolveRange({ from: 'now-5', to: 'now' }, now), null);
    assert.equal(resolveRange({ from: 'now', to: 'now-1h' }, now), null);
  });
});

test('the view round-trips through the URL', async (t) => {
  await t.test('a pasted link restores range, mode and filter', () => {
    const view = viewFromQuery('?from=now-24h&to=now&mode=agg&states=Failed,Crashed');
    assert.deepEqual(view.range, { from: 'now-24h', to: 'now' });
    assert.equal(view.mode, 'agg');
    assert.deepEqual(view.states, ['Failed', 'Crashed']);
  });

  await t.test('an empty query gives the default range', () => {
    assert.deepEqual(viewFromQuery('').range, DEFAULT_RANGE);
  });

  await t.test('a half-specified range is ignored rather than half-applied', () => {
    assert.deepEqual(viewFromQuery('?from=now-24h').range, DEFAULT_RANGE);
  });

  await t.test('writing then reading is lossless', () => {
    // Both ends absolute and 6h apart, so this says the same thing whenever it is run.
    // It used to pin a fixed date against `now`, and started failing the day that pair
    // aged past the 24h cap — which is the cap working, not a round-trip losing anything.
    const view = {
      range: { from: '2026-08-19 09:44', to: '2026-08-19 15:44' },
      mode: 'graph',
      states: ['Failed'],
      flow: 'planetiq',
    };
    const back = viewFromQuery(viewToQuery(view));
    assert.deepEqual(back.range, view.range);
    assert.equal(back.mode, 'graph');
    assert.deepEqual(back.states, ['Failed']);
    assert.equal(back.flow, 'planetiq');
  });

  await t.test('a link whose range has since aged past the cap comes back capped', () => {
    // `from` fixed, `to` relative: the span grows with the clock, so it is only a matter
    // of time before the link asks for more than a day.
    const stale = viewToQuery({ range: { from: '2020-01-01 00:00', to: 'now' }, states: [] });
    assert.deepEqual(viewFromQuery(stale).range, { from: 'now-24h', to: 'now' });
  });

  await t.test('a quick range is recognised so the button reads as a name', () => {
    assert.equal(describeRange({ from: 'now-6h', to: 'now' }), 'Last 6 hours');
    assert.match(describeRange({ from: 'now-6h', to: 'now-1h' }), /→/);
  });
});

test('a run mark squares off the end the window cut', async (t) => {
  const arcs = (path) => (path.match(/A /g) ?? []).length;

  await t.test('a run wholly inside the window is rounded at both ends', () => {
    assert.equal(arcs(markPath(10, 0, 100, 14)), 4);
  });

  await t.test('one squared end drops that end\'s two corners', () => {
    assert.equal(arcs(markPath(10, 0, 100, 14, { squareLeft: true })), 2);
    assert.equal(arcs(markPath(10, 0, 100, 14, { squareRight: true })), 2);
  });

  await t.test('a run cut at both ends is a plain rectangle', () => {
    assert.equal(arcs(markPath(10, 0, 100, 14, { squareLeft: true, squareRight: true })), 0);
  });

  await t.test('the path spans exactly the given box', () => {
    const path = markPath(10, 5, 100, 14, { squareLeft: true, squareRight: true });
    const xs = [...path.matchAll(/[MHV] ([\d.]+)/g)].map((m) => Number(m[1]));
    assert.ok(xs.includes(10) && xs.includes(110), `got ${path}`);
  });

  await t.test('a sliver narrower than the corner radius stays valid', () => {
    // Runs clamp to a 4px minimum width, so the radius has to shrink, not overflow.
    const path = markPath(0, 0, 4, 14);
    assert.ok(!/NaN|-\d/.test(path), path);
  });
});

test('reading the page in UTC instead of the local clock', async (t) => {
  // 23:30 UTC — a different day in any positive offset, which is what makes this worth
  // testing rather than eyeballing.
  const evening = Date.parse('2026-08-24T23:30:00Z');
  t.after(() => setZone('local'));

  await t.test('the same instant, named by two clocks', () => {
    setZone('utc');
    assert.equal(formatStamp(evening), '2026-08-24 23:30');
    assert.equal(zoneLabel(), 'UTC');

    setZone('local');
    assert.notEqual(getZone(), 'utc');
    // Whatever the runner's offset, the stamp is a real rendering of the same instant.
    assert.match(formatStamp(evening), /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/);
  });

  await t.test('a stamp round-trips through the zone it was written in', () => {
    // Rendered in UTC and parsed as local, every trip through the field would shift the
    // range by the offset.
    for (const zone of ['utc', 'local']) {
      setZone(zone);
      assert.equal(parseTimeExpression(formatStamp(evening)), evening, zone);
    }
  });

  await t.test('an explicit offset is still honoured, whatever the setting', () => {
    for (const zone of ['utc', 'local']) {
      setZone(zone);
      assert.equal(parseTimeExpression('2026-08-24T23:30:00Z'), evening);
      assert.equal(parseTimeExpression('2026-08-25T02:30:00+03:00'), evening);
    }
  });

  await t.test('midnight means midnight on the clock being read', () => {
    setZone('utc');
    assert.equal(new Date(anchorStart(0, evening)).toISOString(), '2026-08-24T00:00:00.000Z');
    assert.equal(new Date(anchorStart(1, evening)).toISOString(), '2026-08-23T00:00:00.000Z');
  });

  await t.test('the calendar shows the UTC day, not the local one', () => {
    setZone('utc');
    assert.equal(partsOf(evening).day, 24);
    assert.equal(partsOf(evening).hours, 23);
  });

  await t.test('switching clocks keeps the window on the same runs', () => {
    // A fixed 11:00 in +03:00 is 08:00 in UTC. Left as the bare stamp it was stored as, it
    // would be re-read as 11:00 UTC and the window would jump by the offset.
    setZone('local');
    const fixed = { from: '2026-08-24 11:00', to: '2026-08-24 17:00' };
    const before = resolveRange(fixed);

    setZone('utc');
    const after = resolveRange(restampRange(fixed, before));
    assert.deepEqual(after, before, 'the same instants, named by the other clock');
  });

  await t.test('a relative range needs no rewriting', () => {
    setZone('utc');
    const relative = { from: 'now-6h', to: 'now' };
    assert.deepEqual(restampRange(relative, { from: 1, to: 2 }), relative,
      'six hours is six hours on any clock');
  });

  await t.test('a half-relative range keeps the half that is relative', () => {
    setZone('utc');
    const mixed = restampRange({ from: '2026-08-24 11:00', to: 'now' },
      { from: Date.parse('2026-08-24T08:00:00Z'), to: Date.now() });
    assert.equal(mixed.from, '2026-08-24 08:00');
    assert.equal(mixed.to, 'now');
  });

  await t.test('it is part of the view, so a pasted link reads the same for anyone', () => {
    const query = viewToQuery({ range: DEFAULT_RANGE, states: [], zone: 'utc' });
    assert.match(query, /tz=utc/);
    assert.equal(viewFromQuery(query).zone, 'utc');
    assert.equal(viewFromQuery('?from=now-6h&to=now').zone, null, 'local needs no parameter');
  });
});

test('the month grid behind the date picker', async (t) => {
  await t.test('weeks are whole, and start on Monday', () => {
    assert.deepEqual(WEEKDAYS[0], 'Mo');
    for (const week of monthGrid(2026, 7)) assert.equal(week.length, 7);
  });

  await t.test('a month starting on a Saturday is padded, not shifted', () => {
    // 1 August 2026 is a Saturday: five blanks before it, Monday-first.
    const [first] = monthGrid(2026, 7);
    assert.deepEqual(first, [null, null, null, null, null, 1, 2]);
  });

  await t.test('a month starting on a Monday has no padding', () => {
    // 1 June 2026 is a Monday.
    assert.deepEqual(monthGrid(2026, 5)[0], [1, 2, 3, 4, 5, 6, 7]);
  });

  await t.test('every day appears exactly once', () => {
    for (const [year, month] of [[2026, 7], [2024, 1], [2026, 1], [2026, 11]]) {
      const days = monthGrid(year, month).flat().filter((day) => day !== null);
      assert.deepEqual(days, Array.from({ length: daysInMonth(year, month) }, (u, i) => i + 1),
        `${monthLabel(year, month)}`);
    }
  });

  await t.test('February knows about leap years', () => {
    assert.equal(daysInMonth(2024, 1), 29);
    assert.equal(daysInMonth(2026, 1), 28);
    assert.equal(daysInMonth(2100, 1), 28, 'a century that is not a leap year');
  });

  await t.test('stepping months carries the year', () => {
    assert.deepEqual(shiftMonth(2026, 0, -1), { year: 2025, month: 11 });
    assert.deepEqual(shiftMonth(2026, 11, 1), { year: 2027, month: 0 });
    assert.equal(monthLabel(2026, 7), 'August 2026');
  });

  await t.test('a day and a time round-trip through an instant', () => {
    const parts = { year: 2026, month: 7, day: 19, hours: 9, minutes: 44 };
    assert.deepEqual(partsOf(instantOf(parts)), parts);
  });

  await t.test('a mistyped time stays on the day that was clicked', () => {
    // instantOf would roll 99 hours into four days' time, silently moving the range off
    // the day just picked.
    assert.deepEqual(clampTime(99, 200), { hours: 23, minutes: 59 });
    assert.deepEqual(clampTime(-3, -1), { hours: 0, minutes: 0 });
    assert.deepEqual(clampTime(NaN, undefined), { hours: 0, minutes: 0 }, 'an emptied field');
    assert.deepEqual(clampTime(9.7, 44), { hours: 9, minutes: 44 });
  });

  await t.test('a day that does not exist resolves rather than parsing as garbage', () => {
    // The 31st of a 30-day month: the platform rolls it, which is a real date either way.
    const rolled = partsOf(instantOf({ year: 2026, month: 8, day: 31, hours: 12 }));
    assert.equal(rolled.month, 9);
    assert.equal(rolled.day, 1);
  });
});

test('the range panel stays inside the window', async (t) => {
  const panelWidth = 520;
  const viewportWidth = 1400;

  await t.test('aligned with the button when there is room', () => {
    assert.equal(popoverOffset({ pickerLeft: 400, panelWidth, viewportWidth }), 0);
  });

  await t.test('pulled left when it would run off the right edge', () => {
    // Button at 1000: 1000 + 520 = 1520 > 1400, so it shifts back by the overflow plus margin.
    const offset = popoverOffset({ pickerLeft: 1000, panelWidth, viewportWidth });
    assert.equal(1000 + offset + panelWidth, viewportWidth - 8);
    assert.ok(offset < 0);
  });

  await t.test('shifted only as far as it has to be', () => {
    // Not flush to an edge: a right-anchored panel jumped the whole way, which is how the
    // first column ended up off-screen once the header got shorter.
    const offset = popoverOffset({ pickerLeft: 300, panelWidth: 520, viewportWidth: 700 });
    assert.equal(300 + offset, 172);
    assert.equal(300 + offset + 520, 692, 'right edge inside the margin');
  });

  await t.test('never past the left edge, whatever the numbers', () => {
    for (const pickerLeft of [0, 4, 120, 700, 1390]) {
      for (const width of [200, 520, 900, 2000]) {
        const left = pickerLeft + popoverOffset({ pickerLeft, panelWidth: width, viewportWidth });
        assert.ok(left >= 8, `left edge ${left} for button at ${pickerLeft}, width ${width}`);
        if (width <= viewportWidth - 16) {
          assert.ok(left + width <= viewportWidth - 8, `right edge for width ${width}`);
        }
      }
    }
  });
});

test('the tooltip stays inside the viewport', async (t) => {
  const box = { width: 340, height: 90, viewportWidth: 1200, viewportHeight: 800 };

  await t.test('sits down-and-right of the cursor when there is room', () => {
    const { x, y } = tooltipPosition({ cursorX: 100, cursorY: 100, ...box });
    assert.deepEqual([x, y], [114, 114]);
  });

  await t.test('flips left rather than overflowing the right edge', () => {
    const { x } = tooltipPosition({ cursorX: 1150, cursorY: 100, ...box });
    assert.ok(x + box.width <= box.viewportWidth, `right edge at ${x + box.width}`);
    assert.ok(x < 1150, 'flipped to the left of the cursor');
  });

  await t.test('flips up rather than overflowing the bottom', () => {
    const { y } = tooltipPosition({ cursorX: 100, cursorY: 780, ...box });
    assert.ok(y + box.height <= box.viewportHeight, `bottom edge at ${y + box.height}`);
    assert.ok(y < 780, 'flipped above the cursor');
  });

  await t.test('never clips against the left or top edge', () => {
    // This is what the old fixed 380px guess got wrong.
    const { x, y } = tooltipPosition({ cursorX: 2, cursorY: 2, ...box });
    assert.ok(x >= 8 && y >= 8, `got ${x},${y}`);
  });

  await t.test('a tooltip larger than the viewport still starts on screen', () => {
    const { x, y } = tooltipPosition({
      cursorX: 500, cursorY: 400, width: 2000, height: 2000,
      viewportWidth: 800, viewportHeight: 600,
    });
    assert.ok(x >= 8 && y >= 8, `got ${x},${y}`);
  });
});

test('a run only belongs in the window if it occupies it', async (t) => {
  const from = 10 * MINUTE;
  const to = 20 * MINUTE;

  await t.test('a run inside the window overlaps it', () => {
    assert.ok(overlapsWindow(run('a', 12 * MINUTE, 15 * MINUTE), from, to));
  });

  await t.test('a run straddling either edge overlaps it', () => {
    assert.ok(overlapsWindow(run('early', 5 * MINUTE, 12 * MINUTE), from, to));
    assert.ok(overlapsWindow(run('late', 18 * MINUTE, 25 * MINUTE), from, to));
    assert.ok(overlapsWindow(run('spanning', 0, 60 * MINUTE), from, to));
  });

  // The bug: fetched on expected_start_time, so a run scheduled at 09:54 that only
  // started at 09:55:43 matched a window ending 09:55 and drew as a 4px sliver.
  await t.test('a run that started after the window does not', () => {
    const late = { id: 'late', expected_start_time: iso(19 * MINUTE),
      start_time: iso(21 * MINUTE), end_time: iso(24 * MINUTE) };
    assert.equal(overlapsWindow(late, from, to), false);
  });

  await t.test('a run that finished before the window does not', () => {
    assert.equal(overlapsWindow(run('old', 0, 5 * MINUTE), from, to), false);
  });

  await t.test('a run touching an edge exactly still counts', () => {
    assert.ok(overlapsWindow(run('touch', 5 * MINUTE, from), from, to));
    assert.ok(overlapsWindow(run('touch2', to, 30 * MINUTE), from, to));
  });
});

test('sub-flows link to the sibling that finished before them', async (t) => {
  const sub = (id, at, parent) => ({ ...run(id, at, at + 30_000), parent });
  const runs = [
    sub('b', 2 * MINUTE), sub('a', 1 * MINUTE), sub('c', 3 * MINUTE), // one parent, out of order
    sub('x', 1 * MINUTE), sub('y', 2 * MINUTE), // a second parent
  ];
  const parentByRun = new Map([
    ['a', 'P1'], ['b', 'P1'], ['c', 'P1'], ['x', 'P2'], ['y', 'P2'],
  ]);
  const links = sequentialChainLinks(runs, parentByRun);

  await t.test('consecutive siblings are chained in start order', () => {
    const p1 = links.filter((l) => 'abc'.includes(l.upstreamRunId));
    assert.deepEqual(p1.map((l) => `${l.upstreamRunId}->${l.downstreamRunId}`).sort(), ['a->b', 'b->c']);
  });

  await t.test('chains never cross between parents', () => {
    assert.ok(!links.some((l) => 'abc'.includes(l.upstreamRunId) && 'xy'.includes(l.downstreamRunId)));
    assert.equal(links.length, 3); // a->b, b->c, x->y
  });

  await t.test('a run with no known parent is left unlinked', () => {
    const orphan = [...runs, sub('orphan', 9 * MINUTE)];
    assert.equal(sequentialChainLinks(orphan, parentByRun).length, 3);
  });

  await t.test('a lone sub-flow produces no link', () => {
    assert.deepEqual(sequentialChainLinks([sub('solo', 0)], new Map([['solo', 'P9']])), []);
  });
});

test('overlapping siblings are not a sequence', async (t) => {
  const parentByRun = new Map([['first', 'P'], ['second', 'P']]);

  await t.test('a hand-off is linked', () => {
    const runs = [run('first', 0, 60_000), run('second', 90_000, 120_000)];
    assert.equal(sequentialChainLinks(runs, parentByRun).length, 1);
  });

  // Drawing this end-to-start put the curve in reverse, which read as a wrong link.
  await t.test('siblings that overlap are left unlinked', () => {
    const runs = [run('first', 0, 120_000), run('second', 60_000, 90_000)];
    assert.deepEqual(sequentialChainLinks(runs, parentByRun), []);
  });
});

test('sub-flows that fan out both get a link', async (t) => {
  // A dispatcher sub-flow ends, then two sub-flows start together on the back of it.
  const parentByRun = new Map([['dispatch', 'P'], ['delta', 'P'], ['post', 'P']]);
  const runs = [
    run('dispatch', 0, 60_000),
    run('delta', 61_000, 300_000),
    run('post', 61_000, 90_000),
  ];
  const links = sequentialChainLinks(runs, parentByRun);

  await t.test('both branches link back to the run they followed', () => {
    assert.deepEqual(
      links.map((l) => `${l.upstreamRunId}->${l.downstreamRunId}`).sort(),
      ['dispatch->delta', 'dispatch->post'],
    );
  });

  await t.test('the long branch does not become the predecessor of the other', () => {
    // delta is still running when post starts, so it cannot precede it.
    assert.ok(!links.some((l) => l.upstreamRunId === 'delta'));
  });

  await t.test('every link points forward in time', () => {
    const byId = new Map(runs.map((r) => [r.id, r]));
    for (const { upstreamRunId, downstreamRunId } of links) {
      const end = Date.parse(byId.get(upstreamRunId).end_time);
      const start = Date.parse(byId.get(downstreamRunId).start_time);
      assert.ok(end <= start, `${upstreamRunId} ends after ${downstreamRunId} starts`);
    }
  });
});

test('the run cache decides what still needs fetching', async (t) => {
  const now = Date.parse('2026-08-19T12:00:00Z');
  const hour = 3600_000;
  const cached = (over) => ({
    from: now - 6 * hour, to: now - 30_000, fetchedAt: now - 30_000,
    runs: [run('r', now - hour, now - hour + 60_000)], links: [], ...over,
  });

  await t.test('no cache means a full fetch', () => {
    assert.deepEqual(planFetch(null, now - 6 * hour, now, now),
      { mode: 'full', since: null, drawCached: false });
  });

  await t.test('a recent cache covering the window answers outright', () => {
    const plan = planFetch(cached({ to: now, fetchedAt: now - 2000 }), now - 6 * hour, now, now);
    assert.equal(plan.mode, 'fresh');
    assert.ok(plan.drawCached);
  });

  await t.test('a cache that stops short is extended, not refetched', () => {
    const plan = planFetch(cached(), now - 6 * hour, now, now);
    assert.equal(plan.mode, 'incremental');
    assert.ok(plan.since < now - 30_000, 'refetches a little overlap at the leading edge');
    assert.ok(plan.drawCached, 'and paints what it has meanwhile');
  });

  await t.test('asking for an earlier start is a different span', () => {
    // The cache can only be extended forwards.
    assert.equal(planFetch(cached(), now - 24 * hour, now, now).mode, 'full');
  });

  await t.test('a long-abandoned tab refetches from scratch', () => {
    const old = cached({ fetchedAt: now - 2 * hour, to: now });
    assert.equal(planFetch(old, now - 6 * hour, now, now).mode, 'full');
  });

  await t.test('the freshness window is the caller\'s, not a fixed ten seconds', () => {
    const cached8s = cached({ to: now, fetchedAt: now - 8_000 });

    assert.equal(planFetch(cached8s, now - 6 * hour, now, now).mode, 'fresh',
      'the default still shields the server from a mashed Refresh button');
    assert.equal(planFetch(cached8s, now - 6 * hour, now, now, 5_000).mode, 'incremental',
      'a 5s auto-refresh means 5s, not every other tick served from the cache');
    assert.equal(planFetch(cached8s, now - 6 * hour, now, now, 0).mode, 'incremental',
      'and an explicit Refresh always goes to the server');
  });

  await t.test('a window too big to read in full is cached as the part that was read', () => {
    // `from` here is the fetch's coveredFrom, not the 7d that was asked for. Cached as
    // the 7d it would claim coverage it does not have, and the next narrower window —
    // which planFetch would then serve incrementally — would be drawn from the hole.
    const truncated = cached({ from: now - 25 * hour, to: now, fetchedAt: now - 60_000 });

    assert.equal(planFetch(truncated, now - 168 * hour, now, now).mode, 'full',
      'the same wide window has to be read again; the cache does not reach back that far');
    assert.equal(planFetch(truncated, now - 30 * 60_000, now, now).mode, 'incremental',
      'but a narrower window sits inside what was actually read');
  });
});

test('a window with more runs than one fetch can read', async (t) => {
  const now = Date.parse('2026-08-19T12:00:00Z');
  const from = now - 168 * 3600_000;
  let sorts = [];

  /** Answers /flow_runs/filter with `total` runs, newest at `now`, one per minute. */
  const stubFetch = (total) => {
    globalThis.fetch = async (url, init) => {
      const body = JSON.parse(init.body);
      if (!String(url).includes('/flow_runs/filter')) return json([]);

      sorts.push(body.sort);
      const started = body.flow_runs.expected_start_time?.after_;
      // Only the started-inside query is asked for in bulk; the look-back ones are small.
      const count = Date.parse(started) >= from ? total : 1;
      const page = [];
      for (let i = body.offset; i < Math.min(body.offset + body.limit, count); i++) {
        page.push({
          id: `r${i}`,
          // Descending, as the sort asks: index 0 is the newest.
          start_time: new Date(now - i * 60_000).toISOString(),
          end_time: new Date(now - i * 60_000 + 30_000).toISOString(),
          state_type: 'COMPLETED',
        });
      }
      return json(page);
    };
  };
  const json = (value) => ({ ok: true, json: async () => value });
  const realFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = realFetch; });

  await t.test('reads the recent end of it, and says where that ends', async () => {
    stubFetch(9000);
    const { runs, coveredFrom } = await new PrefectApi('http://p/api')
      .fetchRunsOverlapping(from, now);

    assert.ok(runs.length >= 2200 && runs.length < 2400, `capped, got ${runs.length}`);
    assert.ok(coveredFrom > from, 'the gap is reported rather than passing for "no runs"');
    // Newest-first, so the oldest run read is one page-budget of minutes back.
    assert.equal(coveredFrom, now - 2199 * 60_000);
    assert.ok(sorts.every((sort) => sort === 'EXPECTED_START_TIME_DESC'),
      'ascending would spend the whole budget on the oldest runs and drop the recent ones');
  });

  await t.test('a window that fits is covered from its start', async () => {
    stubFetch(40);
    const { runs, coveredFrom } = await new PrefectApi('http://p/api')
      .fetchRunsOverlapping(from, now);

    assert.equal(runs.length, 40); // the look-back queries return the same run, deduped
    assert.equal(coveredFrom, from);
  });
});

test('merging cached and fetched runs', async (t) => {
  const settled = (id, at, state) => run(id, at, at + 60_000,
    { state_type: state ?? 'COMPLETED', state_name: state ?? 'Completed' });

  await t.test('a refetched run replaces its cached copy', () => {
    const before = [settled('a', 0, 'RUNNING')];
    const after = mergeRuns(before, [settled('a', 0, 'COMPLETED')], 0);
    assert.equal(after.length, 1);
    assert.equal(after[0].state_type, 'COMPLETED');
  });

  await t.test('runs that fell out of the window are dropped', () => {
    const merged = mergeRuns([settled('old', 0)], [settled('new', 10 * MINUTE)], 5 * MINUTE);
    assert.deepEqual(merged.map((r) => r.id), ['new']);
  });

  await t.test('only runs that could still change are asked about again', () => {
    const cache = { runs: [settled('done', 0), settled('going', 0, 'RUNNING')] };
    assert.deepEqual(unsettledRunIds(cache, 0), ['going']);
  });

  await t.test('links are immutable, so merging never duplicates them', () => {
    const link = { upstreamRunId: 'u', downstreamRunId: 'd' };
    assert.equal(mergeLinks([link], [link, { upstreamRunId: 'u2', downstreamRunId: 'd2' }]).length, 2);
  });
});

test('the open pop-up is part of the shareable view', async (t) => {
  const base = { range: { from: 'now-24h', to: 'now' }, mode: 'graph', states: [], refresh: 'Off' };

  await t.test('sub-flows of one run round-trip', () => {
    const url = viewToQuery({ ...base, modal: { kind: 'subflows', id: 'run-1' } });
    assert.deepEqual(viewFromQuery(url).modal, { kind: 'subflows', id: 'run-1' });
  });

  await t.test('sub-flows of a whole flow round-trip', () => {
    const url = viewToQuery({ ...base, modal: { kind: 'subflowsOf', id: 'flow-9' } });
    assert.deepEqual(viewFromQuery(url).modal, { kind: 'subflowsOf', id: 'flow-9' });
  });

  await t.test('a chain window round-trips', () => {
    const url = viewToQuery({ ...base, modal: { kind: 'chain', id: 'run-2' } });
    assert.deepEqual(viewFromQuery(url).modal, { kind: 'chain', id: 'run-2' });
  });

  await t.test('no pop-up leaves the URL clean', () => {
    const url = viewToQuery({ ...base, modal: null });
    assert.equal(viewFromQuery(url).modal, null);
    assert.ok(!url.includes('subflows') && !url.includes('chain'));
  });

  await t.test('an unknown kind is not written', () => {
    const url = viewToQuery({ ...base, modal: { kind: 'nonsense', id: 'x' } });
    assert.equal(viewFromQuery(url).modal, null);
  });

  await t.test('the rest of the view survives alongside it', () => {
    const url = viewToQuery({
      range: { from: 'now-6h', to: 'now' }, mode: 'agg', states: ['Failed'], refresh: '30s',
      modal: { kind: 'chain', id: 'run-3' },
    });
    const view = viewFromQuery(url);
    assert.equal(view.mode, 'agg');
    assert.deepEqual(view.states, ['Failed']);
    assert.equal(view.refresh, '30s');
    assert.deepEqual(view.modal, { kind: 'chain', id: 'run-3' });
  });
});

/**
 * `src/app.js` is all DOM wiring and has no unit tests, so a function deleted out from
 * under its callers gets as far as the browser — which is how removing the link backfill
 * took the helper below it with it, leaving every draw to throw.
 *
 * A snapshot of the file's functions rather than a call-graph check: it catches the same
 * mistake, and matching calls against declarations needs a real tokenizer to stop prose
 * inside a template literal from reading as a call. Deleting one deliberately fails this
 * too — update the list, having looked at whether anything still calls it.
 */
test('dragging out a smaller time range', async (t) => {
  const now = Date.parse('2026-08-19T12:00:00Z');
  const hour = 3600_000;
  // A 6h range across a 1000px plot starting after a 215px label gutter.
  const plot = { left: 215, width: 1000, from: now - 6 * hour, to: now };

  await t.test('a drag reads as the span it covers', () => {
    const zoom = zoomFromDrag(plot, 215 + 500, 215 + 600);
    assert.equal(zoom.from, now - 3 * hour);
    assert.equal(zoom.to, now - 2.4 * hour);
  });

  await t.test('right to left is the same range', () => {
    assert.deepEqual(zoomFromDrag(plot, 215 + 600, 215 + 500),
      zoomFromDrag(plot, 215 + 500, 215 + 600));
  });

  await t.test('a drag outside the plot is clamped to it, not extrapolated', () => {
    const zoom = zoomFromDrag(plot, -400, 215 + 4000);
    assert.equal(zoom.from, plot.from);
    assert.equal(zoom.to, plot.to);
  });

  await t.test('a twitch is refused rather than zoomed to nothing', () => {
    assert.equal(zoomFromDrag(plot, 500, 500), null);
  });
});

test('the zoom bar', async (t) => {
  const now = Date.parse('2026-08-19T12:00:00Z');
  const hour = 3600_000;
  const range = { from: now - 6 * hour, to: now };

  await t.test('the thumb shows which slice of the range is on screen', () => {
    const { left, width } = thumbGeometry(range, { from: now - 3 * hour, to: now - 2 * hour }, 600);
    assert.equal(left, 300); // half way along
    assert.equal(width, 100); // one sixth of it
  });

  await t.test('a deep zoom still leaves something to grab', () => {
    const { width } = thumbGeometry(range, { from: now - 1000, to: now }, 600);
    assert.equal(width, MIN_THUMB_PX);
  });

  await t.test('the thumb never overhangs the end of the track', () => {
    const { left, width } = thumbGeometry(range, { from: now - 1000, to: now }, 600);
    assert.equal(left + width, 600);
  });

  await t.test('dragging it pans the window, keeping its span', () => {
    const zoom = { from: now - 3 * hour, to: now - 2 * hour };
    const panned = zoomFromThumb(range, zoom, 600, 400);

    assert.equal(panned.to - panned.from, hour, 'same span');
    assert.equal(panned.from, now - 2 * hour, 'moved along by the pixels dragged');
  });

  await t.test('panning stops at the ends of the range', () => {
    const zoom = { from: now - 3 * hour, to: now - 2 * hour };
    assert.equal(zoomFromThumb(range, zoom, 600, 5000).to, range.to);
    assert.equal(zoomFromThumb(range, zoom, 600, -5000).from, range.from);
  });
});

test('a zoom survives the range moving under it', async (t) => {
  const now = Date.parse('2026-08-19T12:00:00Z');
  const hour = 3600_000;

  await t.test('a relative range slides, and the zoom keeps its span', () => {
    // "now-6h → now", one refresh tick later.
    const zoom = { from: now - 6 * hour, to: now - 5 * hour };
    const clamped = clampZoom(zoom, now - 6 * hour + 30_000, now + 30_000);

    assert.equal(clamped.to - clamped.from, hour,
      'clamping each edge on its own would shave the span every tick');
    assert.equal(clamped.from, now - 6 * hour + 30_000, 'slid to the new start');
  });

  await t.test('a zoom wider than what is loaded becomes all of it', () => {
    const clamped = clampZoom({ from: now - 24 * hour, to: now }, now - 6 * hour, now);
    assert.deepEqual(clamped, { from: now - 6 * hour, to: now });
  });

  await t.test('nesting keeps the range it was dragged out of', () => {
    // What makes the bar's reset a single click however deep you go: zooming never
    // touches the range, only the window inside it.
    const range = { from: now - 6 * hour, to: now };
    const plot = { left: 0, width: 1000, ...range };

    const first = zoomFromDrag(plot, 0, 500);           // 3h
    const inner = zoomFromDrag({ ...plot, ...first }, 0, 100); // 18m of that 3h

    assert.equal(inner.to - inner.from, 0.3 * hour);
    assert.deepEqual(clampZoom(inner, range.from, range.to), inner, 'still inside the range');
    assert.equal(thumbGeometry(range, inner, 600).width, 30, 'the thumb just gets smaller');
  });
});

test('app.js still defines the functions it is wired from', () => {
  const source = readFileSync(new URL('./src/app.js', import.meta.url), 'utf8');
  const defined = [...source.matchAll(/^(?:async )?function (\w+)/gm)].map((m) => m[1]).sort();

  assert.deepEqual(defined, [
    'applyCalendar', 'applyRange', 'asOfNotice', 'batchOfChain', 'buildCalendar',
    'buildRangePicker', 'buildRefreshControl', 'clearIsolation', 'closeCalendar',
    'closeModal', 'closeRangePanel', 'draw', 'drawCalendar', 'drawZoomBar', 'drawnFrom',
    'drawnWindow', 'fieldInstant', 'highlightLinks', 'init', 'isolateChain',
    'linkNotice', 'load', 'openCalendar', 'openModal', 'openRangePanel',
    'openRunInPrefect', 'prefectLink', 'rangeSpan', 'readUrl', 'restoreModalFromUrl',
    'runNotice', 'setMode', 'setRange', 'setRefresh', 'setZoom', 'showChainWindow',
    'showError', 'showRangeError', 'showSubflows', 'syncControls', 'toggleStateFilter',
    'trackWidth', 'typedTime', 'withAssumedLinks', 'writeUrl',
  ]);
});

test('a cached run keeps the labels the fallback matches on', () => {
  // Dropping tags and parameters from the projection left a reloaded window with no labels
  // at all, so every link fell through to the weaker timing guess — 238 dashed lines on a
  // window the API answers with 178 label matches. The derived labels are kept instead,
  // which is a third of the cost of the fields they come from.
  const raw = {
    id: 'r1', flow_id: 'f', tags: ['2026-08-20T11:00:00+00:00', 'auto-scheduled'],
    parameters: { run_partition: '2026-08-20T11:00:00+00:00' },
    start_time: iso(0), end_time: iso(1000),
  };
  const [stored] = projectForStorage([raw], ['run_partition']);

  assert.deepEqual(stored.batchLabels, ['2026-08-20T11:00:00+00:00']);
  assert.equal(stored.tags, undefined, 'the raw fields are not what is kept');
  assert.equal(stored.parameters, undefined);

  // And read back, it answers the same as the run it came from.
  assert.deepEqual(batchLabelsOf(JSON.parse(JSON.stringify(stored))), batchLabelsOf(raw, ['run_partition']));
});

test('the cache stores every field the next load reads back', () => {
  const store = new Map();
  globalThis.sessionStorage = {
    getItem: (key) => store.get(key) ?? null,
    setItem: (key, value) => store.set(key, value),
    removeItem: (key) => store.delete(key),
  };
  const now = Date.parse('2026-08-19T12:00:00Z');
  const entry = {
    from: now - 6 * 3600_000,
    to: now,
    fetchedAt: now,
    runs: [run('r', now - 60_000, now, { state_type: 'COMPLETED' })],
    links: [{ upstreamRunId: 'a', downstreamRunId: 'b' }],
    flows: [{ id: 'f', name: 'INGEST' }],
    edges: [{ src: 'f', dst: 'g' }],
    linksFrom: now - 6 * 3600_000,
  };

  assert.ok(writeCache('http://p/api', entry));
  const back = readCache('http://p/api');

  // A field silently dropped here is not a lost optimisation: `linksFrom` went missing,
  // and the next refresh took the minute of events it had just read for the whole of the
  // link coverage, warning that a perfectly linked 6h chart had no old links.
  assert.deepEqual(Object.keys(back).sort(), Object.keys(entry).sort());
  assert.equal(back.linksFrom, entry.linksFrom);
  assert.equal(back.runs.length, 1);
});
