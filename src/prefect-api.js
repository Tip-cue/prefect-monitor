/** Client for the Prefect 3 REST API. */

import { resolveFlowSlug } from './links.js';
import { runStart } from './time.js';

const PAGE_SIZE = 200;

/**
 * Pagination stops here — 11 pages, a couple of seconds, and about as much as
 * sessionStorage will hold once projected.
 *
 * A busy week can hold ten times that. Runs are therefore fetched newest-first, so what
 * survives the cap is the recent end of the window, and the fetch reports how far back it
 * actually reached rather than letting the gap pass for "no runs".
 */
const MAX_RUNS_PER_WINDOW = 2200;

/** Event pages to walk before giving up. */
const MAX_EVENT_PAGES = 30;

/**
 * How far before the window to look for a run that is still going or that ended
 * inside it.
 *
 * Unbounded, this drags in zombies: a server accumulates runs stuck in RUNNING for weeks,
 * and every one would draw as a bar spanning the whole chart. A day is well past any real
 * run in most deployments while still excluding those.
 */
const OVERLAP_LOOKBACK_MS = 24 * 60 * 60 * 1000;

/**
 * Payload keys read as "which batch was this firing about", in order.
 *
 * A run-to-run link is drawn from the event that triggered it, and that event usually
 * carries the partition it was about — the timestamp of the data, not of the run. Shown as
 * the title of a chain window, it is what tells two otherwise identical chains apart.
 *
 * There is no standard key for it, so these are the conventional names. If your events use
 * another, add it here: any scalar under the triggering event's `payload` will do. A payload
 * with none of them gets no label rather than a guess, and the chain window simply has no
 * batch line — better than falling back to when the event fired, which reads as a batch key
 * without being one.
 */
export const BATCH_PAYLOAD_KEYS = [
  'batch',
  'partition',
  'logical_date',
  'interval_start',
  'datetime',
];

/**
 * The batch a triggering event was about, or null if its payload does not say.
 *
 * @param {object|undefined} payload the triggering event's payload
 * @param {string[]} keys which keys to read, in order of preference
 */
export function batchFromPayload(payload, keys = BATCH_PAYLOAD_KEYS) {
  for (const key of keys) {
    const value = payload?.[key];
    // Scalars only: an object or an array is structure, not a label.
    if (value !== null && value !== undefined && typeof value !== 'object') return String(value);
  }
  return null;
}

/** Prefect's own UI is served next to the API, so a run id is enough to deep-link. */
export function runUrlFromApiBase(apiBaseUrl, runId) {
  return `${apiBaseUrl.replace(/\/api\/?$/, '')}/runs/flow-run/${runId}`;
}

export class PrefectApi {
  /**
   * @param {string} baseUrl e.g. "https://prefect.example.com/api"
   * @param {{batchKeys?: string[]}} options `batchKeys` overrides which payload keys a
   *   batch label is read from, for a server that names it something of its own.
   */
  constructor(baseUrl, { batchKeys = BATCH_PAYLOAD_KEYS } = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.batchKeys = batchKeys;
    this.flowEdgesCache = null;
    this.rootFlowCache = new Map();
    this.automationsPromise = null;
  }

  /** Fetched once per client: both the edge derivation and the event scoping need them. */
  #automations() {
    this.automationsPromise ??= this.request('/automations/filter', { body: { limit: PAGE_SIZE } });
    return this.automationsPromise;
  }

  runUrl(runId) {
    return runUrlFromApiBase(this.baseUrl, runId);
  }

  /**
   * One API call, retried once on a network error: a keep-alive socket dropped by
   * the load balancer between requests is routine and not worth failing a refresh over.
   */
  async request(path, { method = 'POST', body } = {}) {
    let response;

    for (let attempt = 0; ; attempt++) {
      try {
        response = await fetch(this.baseUrl + path, {
          method,
          headers: { 'Content-Type': 'application/json' },
          body: method === 'POST' ? JSON.stringify(body ?? {}) : undefined,
        });
        break;
      } catch (error) {
        if (attempt > 0) throw new Error(`${method} ${path} → ${error.message}`);
        await new Promise((resolve) => setTimeout(resolve, 400));
      }
    }

    if (!response.ok) {
      const detail = (await response.text()).slice(0, 200);
      throw new Error(`${method} ${path} → HTTP ${response.status} ${detail}`);
    }
    return response.json();
  }

  /** Every page of a paginated POST /filter endpoint, up to `maxItems`. */
  async #fetchAllPages(path, body, maxItems) {
    const items = [];

    for (let offset = 0; offset < maxItems; offset += PAGE_SIZE) {
      const page = await this.request(path, { body: { ...body, limit: PAGE_SIZE, offset } });
      items.push(...page);
      if (page.length < PAGE_SIZE) break;
    }
    return items;
  }

  /**
   * Newest first, because the cap truncates: ascending order spent the whole budget on
   * the oldest runs in the window and dropped everything since, which drew a 7d chart
   * with two days of bars on the left and nothing after them.
   *
   * @param {object} flowRunsFilter the Prefect `flow_runs` filter object
   */
  async fetchRuns(flowRunsFilter) {
    return this.#fetchAllPages(
      '/flow_runs/filter',
      { flow_runs: flowRunsFilter, sort: 'EXPECTED_START_TIME_DESC' },
      MAX_RUNS_PER_WINDOW,
    );
  }

  /**
   * Top-level runs that **overlap** the window, not just those that started inside it.
   *
   * A long run — ours take ~16 minutes — means that on a 15m window the one you care
   * about usually began before it. Filtering on start time alone dropped it, and its
   * links with it.
   * Prefect's filters are AND-only and a running run has no end_time, so this is three
   * queries unioned rather than one:
   *
   *   started inside the window
   *   started earlier, ended inside      (end_time after `from`)
   *   started earlier, still going       (no end_time, so matched on state)
   *
   * @returns {Promise<{runs: object[], coveredFrom: number}>} `coveredFrom` is the
   *   earliest instant the answer is complete from: `from` normally, later than it when
   *   the window holds more runs than the cap. The caller has to keep that instant, not
   *   `from`, or a cache of a truncated window would claim to cover a span it does not
   *   have and serve a narrower window from the hole.
   */
  async fetchRunsOverlapping(from, to) {
    const iso = (ms) => new Date(ms).toISOString();
    const topLevel = { parent_task_run_id: { is_null_: true } };
    const earliest = iso(from - OVERLAP_LOOKBACK_MS);

    const [inside, endedInside, stillRunning] = await Promise.all([
      this.fetchRuns({
        ...topLevel,
        expected_start_time: { after_: iso(from), before_: iso(to) },
      }),
      this.fetchRuns({
        ...topLevel,
        expected_start_time: { after_: earliest, before_: iso(from) },
        end_time: { after_: iso(from) },
      }),
      this.fetchRuns({
        ...topLevel,
        expected_start_time: { after_: earliest, before_: iso(from) },
        state: { type: { any_: ['RUNNING', 'PAUSED', 'CANCELLING'] } },
      }),
    ]);

    const byId = new Map();
    for (const run of [...inside, ...endedInside, ...stillRunning]) byId.set(run.id, run);

    // Only the started-inside query can plausibly hit the cap; the other two are bounded
    // by the look-back and are a handful of runs. Newest-first, so the last one returned
    // is the oldest, and nothing before it was read.
    const truncated = inside.length >= MAX_RUNS_PER_WINDOW;
    return {
      runs: [...byId.values()],
      coveredFrom: truncated ? runStart(inside[inside.length - 1]) : from,
    };
  }

  /**
   * Specific runs by id — used to refresh the ones that were still in flight when
   * they were cached, since those are the only cached runs that can have changed.
   */
  async fetchRunsByIds(runIds) {
    if (runIds.length === 0) return [];
    return this.fetchRuns({ id: { any_: runIds.slice(0, MAX_RUNS_PER_WINDOW) } });
  }

  /**
   * Sub-flow runs of the given parents, together with which parent each belongs to.
   *
   * A sub-flow run does not carry its parent flow run's id — only the id of the task
   * run that called it — so the mapping takes one extra batched request rather than
   * one per sub-flow.
   *
   * @returns {Promise<{runs: object[], parentByRun: Map<string, string>}>}
   */
  async fetchSubflowRuns(parentRunIds) {
    const runs = await this.fetchRuns({ parent_flow_run_id: { any_: parentRunIds } });
    const parentByRun = new Map();

    const taskRunIds = [...new Set(runs.map((run) => run.parent_task_run_id).filter(Boolean))];
    if (taskRunIds.length > 0) {
      try {
        const taskRuns = await this.request('/task_runs/filter', {
          body: { task_runs: { id: { any_: taskRunIds } }, limit: 200 },
        });
        const flowRunByTask = new Map(taskRuns.map((task) => [task.id, task.flow_run_id]));
        for (const run of runs) {
          const parent = flowRunByTask.get(run.parent_task_run_id);
          if (parent) parentByRun.set(run.id, parent);
        }
      } catch (error) {
        console.warn('could not resolve sub-flow parents', error);
      }
    }
    return { runs, parentByRun };
  }

  async fetchFlows() {
    return this.#fetchAllPages('/flows/filter', { sort: 'NAME_ASC' }, 1000);
  }

  /**
   * Flow-to-flow edges, derived from the server's enabled automations.
   *
   * Note these pipelines do not trigger on `prefect.flow-run.Completed`; they emit
   * their own events (`INGEST.CHUNKS_COLLECTED`). So the source comes from the
   * trigger's match resource id and the targets from its run-deployment actions —
   * one automation can fan out to several (one ingest event starting two pipelines).
   *
   * Never throws: without edges the chart is still a useful timeline.
   *
   * @returns {Promise<{src: string, dst: string, name: string}[]>}
   */
  async fetchFlowEdges(flows) {
    if (this.flowEdgesCache) return this.flowEdgesCache;

    try {
      const [automations, deployments] = await Promise.all([
        this.#automations(),
        this.request('/deployments/filter', { body: { limit: PAGE_SIZE } }),
      ]);

      const flowIdByDeployment = new Map(deployments.map((d) => [d.id, d.flow_id]));
      const edges = [];

      for (const automation of automations) {
        if (automation.enabled === false) continue;

        const targets = (automation.actions ?? [])
          .filter((action) => action.type === 'run-deployment')
          .map((action) => flowIdByDeployment.get(action.deployment_id))
          .filter(Boolean);

        if (targets.length === 0) continue;

        for (const trigger of eventTriggers(automation.trigger)) {
          const slug = trigger.match?.['prefect.resource.id'];
          const source = await this.resolveRootFlow(resolveFlowSlug(slug, flows));

          if (!source) {
            console.warn('unresolved automation source', automation.name, slug);
            continue;
          }
          for (const target of targets) {
            if (source !== target) edges.push({ src: source, dst: target, name: automation.name });
          }
        }
      }

      this.flowEdgesCache = edges;
    } catch (error) {
      console.warn('automations unavailable:', error);
      this.flowEdgesCache = [];
    }

    return this.flowEdgesCache;
  }

  /**
   * The **exact** run-to-run links, straight from the server's own automation events.
   *
   * No matching or guessing is involved. Prefect emits, per automation firing:
   *   - `prefect.automation.triggered`, whose payload embeds the whole triggering
   *     event — including the flow run that emitted it;
   *   - `prefect.automation.action.executed`, whose related resources name both the
   *     flow run the action *created* and the triggering event it came from.
   *
   * Joining the two on the triggering-event id gives upstream → downstream directly.
   * Both streams are small — a hundred or so of each per 6h — because there is one per
   * firing, not one per run.
   *
   * An emitter can be a sub-flow (a delta writer firing the export that follows it),
   * which is never a lane, so those are walked up to their top-level ancestor.
   *
   * @returns {Promise<{links: object[], coveredFrom: number}>} `coveredFrom` is how far
   *   back the answer actually reaches. A wide window exceeds the page budget, so it
   *   can be later than `from`; the caller extends coverage from there rather than
   *   assuming the whole span was read.
   */
  async fetchAutomationRunLinks(from, to) {
    // Scope to the automations that actually start runs. The zombie and
    // stranded-retry automations fire on flow-run events and dominate the stream
    // otherwise — 24k events over 7d against 8k for these four.
    const automations = await this.#automations();
    const creators = automations
      .filter((automation) => automation.enabled !== false)
      .filter((automation) => (automation.actions ?? []).some((a) => a.type === 'run-deployment'))
      .map((automation) => `prefect.automation.${automation.id}`);

    if (creators.length === 0) return { links: [], coveredFrom: from };

    const { events, oldestOccurred } = await this.#fetchEvents(
      ['prefect.automation.triggered', 'prefect.automation.action.executed'],
      from,
      to,
      creators,
    );

    const triggerById = new Map();
    const executions = [];

    for (const event of events) {
      if (event.event === 'prefect.automation.triggered') {
        const trigger = event.payload?.triggering_event;
        const emitter = relatedId(trigger?.related, 'flow-run');
        if (trigger?.id && emitter) {
          triggerById.set(trigger.id, {
            emitter,
            // The batch the firing was about, straight from the event that fired it.
            batch: batchFromPayload(trigger.payload, this.batchKeys),
            event: trigger.event,
          });
        }
        continue;
      }

      // Only actions that actually started a run, and actually succeeded.
      if (event.payload?.action_type !== 'run-deployment') continue;
      if (event.payload?.status_code >= 300) continue;

      const created = relatedId(event.related, 'flow-run');
      const triggerEventId = relatedId(event.related, 'triggering-event');
      if (created && triggerEventId) {
        executions.push({
          downstreamRunId: created,
          triggerEventId,
          automation: event.resource?.['prefect.resource.name'] ?? '',
        });
      }
      continue;
    }

    const links = [];
    for (const execution of executions) {
      const trigger = triggerById.get(execution.triggerEventId);
      if (!trigger) continue; // its triggered event fell outside the window
      links.push({
        upstreamRunId: trigger.emitter,
        downstreamRunId: execution.downstreamRunId,
        automation: execution.automation,
        batch: trigger.batch,
        triggeringEvent: trigger.event,
      });
    }

    return {
      links: await this.#liftSubflowUpstreams(links),
      coveredFrom: oldestOccurred ?? from,
    };
  }

  /**
   * Whether the server still holds *any* event in this window.
   *
   * Links come only from events, and Prefect prunes events long before it prunes runs
   * (`PREFECT_EVENTS_RETENTION_PERIOD`, days, against weeks for runs). So a window of old
   * runs draws no links at all, which is indistinguishable from "no automation fired"
   * unless you ask. One request, and only when there were no links to draw.
   */
  async hasEventsIn(from, to) {
    const page = await this.request('/events/filter', {
      body: {
        filter: { occurred: { since: new Date(from).toISOString(), until: new Date(to).toISOString() } },
        limit: 1,
      },
    });
    return (page.total ?? page.events?.length ?? 0) > 0;
  }

  /**
   * Events are capped at 50 per page and paginated by an absolute `next_page` URL.
   *
   * Pages come newest-first, so stopping early keeps recent history exact and leaves
   * older runs to the label fallback — which is the right way round. A 7d window is
   * ~167 pages, well past the budget; 6h is 5 and 24h is 19.
   */
  async #fetchEvents(names, from, to, resourceIds, maxPages = MAX_EVENT_PAGES) {
    const filter = {
      occurred: { since: new Date(from).toISOString(), until: new Date(to).toISOString() },
      event: { name: names },
    };
    if (resourceIds?.length) filter.resource = { id: resourceIds };

    let page = await this.request('/events/filter', { body: { filter, limit: 50 } });
    const events = [...(page.events ?? [])];

    let pages = 1;
    while (page.next_page && pages < maxPages) {
      // Only the token is taken from the server's `next_page`, never the URL itself.
      // Prefect reports it as `http://<host>/api/...` because it cannot see the TLS
      // in front of it, and following that cross-origin redirect (to `…:443/`) is
      // refused by the browser — which is why anything past one page of events failed
      // while a single page was fine.
      const token = new URL(page.next_page, 'http://placeholder').searchParams.get('page-token');
      if (!token) break;

      page = await this.request(
        `/events/filter/next?page-token=${encodeURIComponent(token)}`,
        { method: 'GET' },
      );
      events.push(...(page.events ?? []));
      pages++;
    }

    // Pages come newest-first, so the last event seen is the oldest reached. When the
    // budget ran out that is short of `from`, and the caller resumes from there.
    const oldest = events.at(-1)?.occurred;
    if (page.next_page) {
      console.info(
        `automation events: read ${events.length} of ${page.total}; links reach back to `
        + `${oldest}, so anything earlier is drawn unlinked`,
      );
    }
    return {
      events,
      oldestOccurred: page.next_page && oldest ? Date.parse(oldest) : null,
    };
  }

  /**
   * Replaces any upstream that is a sub-flow run with its top-level ancestor, in two
   * batched calls rather than one per link.
   */
  async #liftSubflowUpstreams(links) {
    const ids = [...new Set(links.map((link) => link.upstreamRunId))];
    if (ids.length === 0) return links;

    try {
      const runs = await this.request('/flow_runs/filter', {
        body: { flow_runs: { id: { any_: ids } }, limit: 200 },
      });
      const parentTaskByRun = new Map(
        runs.filter((run) => run.parent_task_run_id).map((run) => [run.id, run.parent_task_run_id]),
      );
      if (parentTaskByRun.size === 0) return links;

      const taskRuns = await this.request('/task_runs/filter', {
        body: { task_runs: { id: { any_: [...parentTaskByRun.values()] } }, limit: 200 },
      });
      const parentRunByTask = new Map(taskRuns.map((task) => [task.id, task.flow_run_id]));

      const lifted = new Map();
      for (const [runId, taskId] of parentTaskByRun) {
        const parent = parentRunByTask.get(taskId);
        if (parent) lifted.set(runId, parent);
      }
      return links.map((link) => ({
        ...link,
        upstreamRunId: lifted.get(link.upstreamRunId) ?? link.upstreamRunId,
      }));
    } catch (error) {
      console.warn('could not lift sub-flow upstreams', error);
      return links;
    }
  }

  /**
   * Walks a sub-flow up to its top-level ancestor.
   *
   * Some automations fire on an event emitted by a sub-flow — a pipeline's
   * delta writer emits the coverage export's trigger. Sub-flows never appear as
   * top-level lanes, so an edge starting at one would have nowhere to attach; it is
   * hung off the pipeline that contains it instead.
   */
  async resolveRootFlow(flowId) {
    if (!flowId) return flowId;
    if (this.rootFlowCache.has(flowId)) return this.rootFlowCache.get(flowId);

    let rootFlowId = flowId;
    try {
      const [sample] = await this.request('/flow_runs/filter', {
        body: { flows: { id: { any_: [flowId] } }, limit: 1, sort: 'START_TIME_DESC' },
      });

      let run = sample;
      for (let hop = 0; run?.parent_task_run_id && hop < 5; hop++) {
        const parentTask = await this.request(`/task_runs/${run.parent_task_run_id}`, { method: 'GET' });
        run = await this.request(`/flow_runs/${parentTask.flow_run_id}`, { method: 'GET' });
      }
      if (run?.flow_id) rootFlowId = run.flow_id;
    } catch (error) {
      console.warn('could not resolve root flow for', flowId, error);
    }

    this.rootFlowCache.set(flowId, rootFlowId);
    return rootFlowId;
  }
}

/** The id behind a related resource of the given role, e.g. "flow-run" → its uuid. */
function relatedId(related, role) {
  const match = (related ?? []).find((resource) => resource['prefect.resource.role'] === role);
  if (!match) return null;
  return String(match['prefect.resource.id']).replace(/^prefect\.[a-z-]+\./, '');
}

/** Flattens a compound automation trigger down to its event triggers. */
function eventTriggers(trigger) {
  if (!trigger) return [];
  if (trigger.type === 'event') return [trigger];
  return (trigger.triggers ?? []).flatMap(eventTriggers);
}
