# prefect-monitor

Grafana-style monitoring frontend for a Prefect 3 server. Plain ES modules, no build
step and no dependencies — it talks to the Prefect REST API straight from the browser.

## Run

```bash
npm start                       # a no-store static server on :8080
open http://localhost:8080
```

Requires Node 20+ for `npm test` (`node --test`); the app itself is served as static
files and runs anywhere.

It must be served over HTTP rather than opened as a `file://` path, because browsers
refuse to load ES modules from the filesystem.

## Deploying it

There is nothing to build and no backend of its own: copy `index.html`, `styles.css` and
`src/` behind any static file server. Serving it from the **same host as the Prefect
server** is worth the trouble — the API is then same-origin at `/api` (no CORS to
configure), and clicking a run navigates to the Prefect UI in the same tab, which is what
makes it feel like part of it. An nginx container with the files mounted, behind the same
ingress as Prefect on a `/monitor/` path, is all it takes.

## Layout

| File | What lives there |
|---|---|
| `index.html`, `styles.css` | markup and styling, no logic |
| `src/app.js` | DOM wiring: controls, pointer handling, modal, load cycle |
| `src/prefect-api.js` | the REST client, edge derivation, deep-link URLs |
| `src/render.js` | builds the timeline SVG, legend, tooltip and state table |
| `src/layout.js` | pipeline grouping, lane ordering, sub-row packing |
| `src/zoom.js` | drag-to-zoom geometry and the scroll bar's thumb |
| `src/links.js` | linking runs to what triggered them, chains, chain filtering |
| `src/states.js`, `src/time.js` | state colour/severity, timestamps and ticks |
| `test.js` | `npm test` — covers everything under `src/` that isn't drawing |

`src/render.js` builds markup from data and never touches app state; `src/layout.js`,
`src/links.js`, `src/states.js` and `src/time.js` are pure and directly unit-tested.

## Configuring it

A static page cannot read environment variables, so anything a deployment needs to set lives
in **`config.js`**, loaded before the app:

```js
window.PREFECT_MONITOR = {
  apiUrl: 'https://prefect.example.com/api',  // default: /api, or localhost:4200
  apiUrlEditable: false,                      // default: only when not same-origin
  extraBatchKeys: ['run_partition'],          // payload keys your events use
};
```

The committed file is empty, because served next to a Prefect server the defaults are right.
`npm start` renders it from the environment instead of reading it, so a local run needs no
edit to a tracked file — and a container image can write the same file at startup:

| variable | effect |
|---|---|
| `PREFECT_MONITOR_API_URL` | pins the API base URL, and hides the header field with it |
| `PREFECT_MONITOR_API_URL_EDITABLE` | `false` hides the field, `true` forces it on |
| `PREFECT_MONITOR_BATCH_KEYS` | comma-separated extra payload keys for batch labels |

```bash
PREFECT_MONITOR_API_URL=https://prefect.example.com/api npm start
```

The **Prefect API URL** field appears only when there is something to choose: the API is on
another host and no deployment pinned it. It defaults to `http://localhost:4200/api`, the
address `prefect server start` listens on, and whatever you type is remembered in
localStorage.

Pointing it at a remote server needs that server to allow this origin, via
`PREFECT_SERVER_CORS_ALLOWED_ORIGINS`. For a server in Kubernetes, a port-forward avoids the
question entirely:

```bash
kubectl -n prefect port-forward svc/prefect-server 4200:4200
```

Note that `kubectl port-forward` drops silently after a while — the process stays alive
while the tunnel is dead.

Served *from* the Prefect host the API is same-origin at `/api`, so there is nothing to
point it at and the field is hidden. A stored override is ignored there too, rather than
letting one left behind by a dev session on the same hostname quietly redirect the page.

## The view lives in the URL

Everything that defines what you are looking at — range, mode, state filter, refresh
interval, and any open pop-up — is in the query string, so a refresh keeps it and a
pasted link reproduces it for someone else:

```
/?from=now-6h&to=now&mode=agg&states=Failed&refresh=30s
/?from=now-24h&to=now&subflows=<run-id>     a sub-flow pop-up
/?from=now-24h&to=now&chain=<run-id>        a chain window
```

The pop-up matters for **Back**: clicking a run navigates to Prefect in this tab, so
opening a pop-up pushes a history entry and coming back reopens it rather than landing on
a bare chart. Everything else replaces, so changing a range does not fill the history. A
run that is no longer in the window is fetched by id rather than given up on.

The range is stored as **expressions, not timestamps**, the way Grafana and Kibana do
it: `now-6h` → `now` still means "the last six hours" tomorrow, where a resolved pair
would silently freeze. The refresh control is a Grafana-style Refresh button plus an
interval menu (Off through 1h).

## Picking a range

**At most 24 hours**, offered and enforced. One fetch reads ~2200 runs, which on a busy
server is about a day; past that the chart showed a fraction of what was asked for and had
to say so. A longer period is more useful as a *shifted* day than as a truncated week.

Three ways in, and no need to fill in more than one of them:

- **Ending now** — the last 15m, 30m, 1h, 3h, 6h, 12h or 24h.
- **Starting at** — midnight today, yesterday, 2, 3 or 7 days ago, or a time typed in
  as `2026-08-19 09:44`.
- **For** — the same seven durations, an exact **end** timestamp, or **Until now**, which
  runs the window on from the start instead of ending it a fixed span later. That last one
  is what you want for today: the chart keeps up as runs come in. From a start more than a
  day ago it cannot, so the cap takes over and the panel says so rather than quietly
  dropping the start that was just clicked.

Any one of start, end and duration can be set at a time, and which two are present decides
the third — start+end is exactly that, start+duration runs on from the start, end+duration
reaches back from the end, a duration on its own is still "the last N".

Picking a **start point keeps the span** rather than the end that happened to be in the
field: going from "7 days ago" to "3 days ago" while looking at 6h gives that day's first six
hours. It used to keep the old end, which was then four days before the new start — an
inverted range and an error where the intent was obvious. A start and an end you *typed* are
still taken as the pair you typed, and an inverted one is reported rather than reinterpreted.

**The 24h fallback follows the end you just set.** Type an end more than a day after the
start and the start moves to 24h before it; edit the start instead and the end moves to 24h
after it. The one you just typed is the one thing you were sure about, so it is the one that
survives — the panel says as much next to the fields.

A start point makes both ends absolute, since a fixed start
that drifted on the next refresh would defeat the point of choosing one; "ending now" stays
relative. A pasted link asking for longer than a day is shortened — keeping its end, since
the recent part is the useful part — in the expressions as well as in the resolved pair, so
the button never labels the chart with a week it is not showing. Anything unreadable is
rejected rather than guessed at: `Date.parse` is lenient enough to read `now-5` as
2001-05-01, which would have drawn an empty chart instead of an error.

## Modes

Both share the time-range picker and the refresh control.

**Flow graph** — one lane per flow, every run drawn as a block on the time axis,
coloured by Prefect state. A block with a **square end** continues past the edge of
the window — it started before it, or has not finished — the way a Gantt chart marks
a clipped bar; rounded ends begin and end inside what you are looking at. Curved links connect the specific upstream run to the
downstream run it triggered. Hovering a run fades every link that doesn't touch it, so
one chain stays traceable in a busy window. Only the 150 most recent links are drawn,
and the chart says so if it ever truncates — the cap is 2500, a guard rather than a
working limit. It was 150, from when links came from fuzzy matching and a 6h window
produced ~1000; with exact links a 24h window has 486, and 150 of them silently cut the
view down to its last 7 hours. Clicking a run opens its sub-flow runs.

**Aggregate** — the same lanes in the same order, each binned into pixel columns where
**the worst state in a bin wins**. This matters: a 6h window holds ~500 runs per lane,
so per-run blocks abut into one solid bar and a 2px failure disappears under the next
success. Each lane also carries an `N/M not completed` count, red when non-zero. The
per-run links collapse to one connector per flow pair, drawn in the left gutter, so the
pipeline shape is still legible without the runs.

Marks behave identically in both modes — click for sub-flows, shift-click to isolate the
chain, right-click to open it — the only difference being that a bin covers several runs
where a graph mark covers one.

**Aggregate mode's gutter is the counts table**, in the chart's own box rather than a
panel: one column per state present, worst first, then a row total, separated by rules
and always visible. Cells carry the count and its share of the row (`180 (99%)`) on a
tint of the state's colour with a stripe down the side; a zero is a dot, so the eye
lands on the counts that exist. They are read against the rows they describe, so they
belong on the same line as the flow.

The flow graph has no counts column — each run's state is drawn directly — and neither
do the pop-ups, where every row is a single run.

Clicking a **flow name** in either mode opens that flow's sub-flow runs as a pop-up
timeline, with the sub-flows linked in the order their parent ran them and the lanes
ordered by that sequence rather than alphabetically. Inside the
pop-up, clicking any run opens it in the Prefect UI (`/runs/flow-run/<id>`), and the
pop-up header links to the parent run there too.

## Narrowing down

A **chain** is one batch end to end — an ingest run, the transform and enrich runs it
triggered, and the exports those triggered. Both of these work on chains rather than on flows,
because a run only means something alongside what it triggered.

**Legend chips filter.** Clicking one keeps only the chains containing a run in that
state and removes everything else: not dimmed, gone, along with any lane or pipeline
box left empty. On one measured 6h window, selecting "Failed" went from 513 runs to 3 —
the failures plus the runs they are linked to — so the healthy batches of the *same*
pipeline disappear too. Multi-select, with a clear affordance.

**Shift-click a run** to isolate its chain in place: everything reachable through the
links stays lit and the rest drops to 15%. Use it to follow one batch without losing
its surroundings; use the filter to throw the surroundings away. Escape or a
background click clears it.

An open pop-up refreshes with everything else: it used to be fetched once and then left
alone, so a running sub-flow stayed running on screen however long the refresh ran behind
it. It redraws in place rather than blanking to "loading…", and a slow fetch that has been
superseded by the next tick is dropped rather than painted over the newer one.

The auto-refresh interval is honoured as chosen — the cache answers outright only for
shorter than the interval, so 5s means 5s rather than every other tick — and the Refresh
button always goes to the server.

Loading is shown rather than implied: an indeterminate bar under the header, a
spinning refresh icon, and a dimmed chart. A 24h window takes several seconds, which
otherwise reads as a freeze.

**Drag across the plot** to zoom into the span you dragged, the way Grafana's charts do.
It costs nothing: the runs and links for the whole range are already loaded, so the axis
just narrows. Under a few pixels of movement it stays a click and opens the run instead.

Zooming never touches the range — the picker still says "Last 6 hours" — so a CloudWatch-
style scroll bar appears under the plot showing which slice you are in: drag the thumb to
move through the range, ✕ (or Escape) to come back out. Zoom again inside a zoom and the
thumb just gets smaller; the way back is still one click, not one per level. The zoom is
absolute in the URL (`zoomFrom`/`zoomTo`), so a link reproduces exactly what you are
looking at, and it keeps its span as a relative range slides under it on each refresh.

### Batch labels

A chain window is titled with the **batch** it is about — `batch 2026-08-19T06:00` — which is
what tells two otherwise identical chains apart. It is read from the payload of the event
that triggered the chain, never inferred: `BATCH_PAYLOAD_KEYS` in `src/prefect-api.js` lists
the keys tried, in order (`batch`, `partition`, `logical_date`, `interval_start`,
`datetime`).

If your events already carry it under another name, add that name rather than changing them:
`extraBatchKeys: ['run_partition']` in `config.js`, or `PREFECT_MONITOR_BATCH_KEYS`. Extra
keys are tried *after* the conventional ones, so a payload carrying both still prefers the
conventional key.

Otherwise, emit the partition your run is about in the event that triggers the next one:

```python
from prefect.events import emit_event

emit_event(
    event="INGEST.CHUNKS_COLLECTED",
    resource={"prefect.resource.id": "ingest_main_flow"},
    payload={"batch": "2026-08-19T06:00"},   # any scalar; a key from the list above
)
```

If none of those keys is present the window simply has no batch line. That is deliberate:
the obvious fallback — when the event fired — reads as a batch key without being one, and
would make two chains a second apart look like different batches.

**Right-click a run** to open that chain as its own window — the same runs on their
own axis, titled with the batch label they agree on (`batch 2026-08-19T06:00`), with
the rest of the window gone rather than dimmed. Clicking a run inside it opens that
run in Prefect. (No modifier: Firefox forces its own menu on shift-right-click and
never delivers the event to the page.)

## The map: pipelines, lanes, sub-rows

**Pipelines.** Flows are grouped into connected components — a whole pipeline, its first
flow and everything that flow triggers — and each group is drawn as one rounded area
captioned `<root> pipeline · N flows`. On the server this was built against that is three
areas: a five-flow pipeline, a two-flow one, and a lone unconnected flow.

**Lane order.** Within a group, lanes are laid out depth-first from each root, so a
chain occupies consecutive lanes and its links stay short and un-crossed; children are
visited leaves-first so a large subtree lands last and doesn't push its parent's other
edges across the chart. Larger pipelines come first, isolated flows sink to the bottom.
This gives `ingest → enrich → transform → export → score` with **zero edge crossings**. A
plain topological sort interleaved unrelated flows between a parent and its child,
which is what produced the long crossing curves.

**Sub-rows.** Concurrent runs of the same flow used to be painted on top of each other,
so only the last one was visible — a 16-minute transform run overlapping the next one looked
like a single block. Each lane now packs its runs into sub-rows by interval
partitioning (each run takes the first row free at its start time), and the lane grows
to fit. Capped at 8 sub-rows; beyond that the earliest-freeing row is reused.

## How flow-to-flow edges are derived

Not from `prefect.flow-run.Completed` events — the pipelines here trigger on their own
custom events (`INGEST.CHUNKS_COLLECTED`, `TRANSFORM.WRITE_COMPLETE`, …).
For each **enabled** automation:

- source flow = `trigger.match["prefect.resource.id"]`, a flow slug matched against
  flow names (exact, else unique suffix). Some events are emitted by a *sub*-flow —
  `transform_sub_flow_delta_writer` fires the export — and sub-flows are
  never top-level lanes, so the edge would vanish. Resolution walks up
  `parent_task_run_id → task run → parent flow run` to the top-level ancestor, which is
  how the exports correctly hang off `TRANSFORM_MAIN_FLOW` rather than floating
  disconnected;
- target flow(s) = the `run-deployment` actions' deployments, resolved via
  `/deployments/filter` in one call. One automation can fan out to several
  (one `on-ingest-complete` automation starts both transform and enrich).

### Linking the runs

The runs themselves are linked from the server's own record of what happened, not by
inference. Every automation firing leaves two events behind:

- `prefect.automation.triggered` — its payload embeds the entire triggering event,
  including the flow run that emitted it;
- `prefect.automation.action.executed` — its related resources name both the flow run
  the action **created** and the triggering event it came from.

Joining those on the triggering-event id gives upstream → downstream exactly. Both
streams are small because there is one per firing rather than one per run, and they
are scoped to the automations that actually have a `run-deployment` action — the
zombie and stranded-retry automations fire on flow-run events and would otherwise
dominate the stream (24k events over 7d against 8k for the four that matter).

An emitter can be a sub-flow — the transform pipeline's delta writer fires the export — which
is never a lane, so those upstreams are walked up to their top-level ancestor in two
batched calls rather than one per link.

Because links come only from these events, "no links" is ambiguous — so the legend
row says which it is: the events failed to load, or none fired in this window.

Pagination takes only the page **token** from the server's `next_page`, never the URL.
Prefect reports it as `http://<host>/api/...` because it cannot see the TLS in front of
it, and the browser refuses that cross-origin redirect to `…:443/` — which made any
window past a single page of events (roughly anything over an hour) fail while shorter
ones worked.

The event stream is paginated 50 at a time with a page budget per request, so a wide range
reaches only part way back — on the order of a day. Each fetch reports how far it got, and the legend
says so when that is short of the chart, rather than leaving links silently missing.

In practice it rarely is: the run cap usually bites first, and the chart is drawn only
as far back as the runs reach. An earlier version crawled backwards a 6h chunk per refresh
to close the gap; once the axis followed the runs there was no gap left to close, so it is
gone. If a server emits enough events per run to invert that, the legend will say so.

Measured against a live server this yields **every** link exactly: 122 of 122 over 6h,
495 of 505 over 24h,
the remainder being runs whose triggering event falls outside the window.

**Sub-flows** are the one exception, and a narrower claim. Inside a single parent run,
the order it called its sub-flows is a fact about that parent, so each sub-flow run is
linked to the sibling that **finished most recently before it started**.

Predecessor rather than a linear chain, because sub-flows fan out: a transform run's
datadispatcher is followed by *both* the delta writer and post process, which start
together a second after it ends. Chaining consecutive siblings in start order kept only
one of those branches and left the other looking unrelated. It also means a link can
never point backwards in time, which chaining by start order could — an overlapping
sibling produced a curve running right to left.

A sub-flow run does not carry its parent flow run's id, only the calling task run's, so
the mapping costs one extra batched request.

**Runs outlive their events.** Prefect prunes events in days
(`PREFECT_EVENTS_RETENTION_PERIOD`) and flow runs in weeks, so a window old enough draws runs
with nothing left to explain them — no links, and none possible however often you refresh.
That is indistinguishable from "no automation fired" unless you ask, so when a window has no
links the legend asks: one request for any event at all in that span, and it then says which
of the two it is.

**The fallback, for runs the events no longer explain.** Where an event is missing, a run is
matched to the upstream run that shares its **batch label** — read from run parameters under
the configured keys, and from tags by *shape* (a bare ISO timestamp, or the value half of
`SOMETHING:<timestamp>`, since a flow tags its runs however it likes).

Where a run publishes no label at all — some flows don't — the fallback drops to the
**nearest preceding run along the same automation edge**, within 15 minutes. Only for a run
with no label: one that *has* a label and finds no candidate sharing it is telling you which
batch it belongs to, and that no run from that batch is here — evidence against the nearest
preceding run rather than for it. Guessing anyway put 931 dashed lines on a 6h window whose
events explained 975. That bound is
measured: across 122 event-confirmed links the gap from an upstream finishing to its
downstream starting ran from a few seconds to 96, so fifteen minutes absorbs a queue or a
retry while refusing to pair a run with something from an hour earlier. A confirmed pair can
even *overlap* by a few seconds — the trigger fires on a state change written after the run
ends — so a little tolerance is allowed on "finished before it started".

Inference never overrides an event, only fills gaps, and is confined to the flow pairs the
automations define. A **label match draws solid**, like an event link — the two runs say they
are about the same thing, which is evidence. Only a **timing guess is dashed**, and the
legend names both (`94 matched by batch label · 24 assumed from timing, drawn dashed`). The shape says "probably",
which a solid line would not.

Measured against 6h of real events, with the events withheld: **122 of 122 agreed, none
disagreed** — 98 by label, 24 by timing.

The subtle part is *which* upstream run. A first attempt at this disagreed with the events on
**68 of 119** links, because it took the first run carrying the label. A batch usually has
many: an ingest flow running every minute tags every one of its runs with the quarter-hour it
belongs to, and the automation fires on the **last** of them. Matching the upstream that
finished last before the downstream started is what makes it agree — measured against 6h of
real events afterwards: **96 agreed, 0 disagreed**.

Events themselves are fetched newest-first and capped at 30 pages, and that truncation is
reported rather than passed off as complete.

Deriving edges from time proximity alone was worse still: it invented a
`TRANSFORM → ENRICH` edge (both are really children of `INGEST`) and an
`EXPORT ↔ SCORE` cycle.

## Colours

Prefect's own state colours. Their red/green pair is only ΔE ≈ 5.5 under
deuteranopia, so failure states (failed / crashed / cancelled) also carry a diagonal
hatch, and the legend and tooltip always name the state — identity is never
colour-alone.

## Self-check

```bash
npm test        # node --test, no browser and no Prefect server needed
```

Covers tick spacing, batch-key vs recency precedence, automation scoping, slug
resolution, pipeline grouping, lane ordering, sub-row packing and its cap,
worst-state-wins, and state colouring. Rendering is left to the eye, but the
decisions behind it are not.

## Local caching

The projection keeps each run's **derived batch labels** rather than the tags and parameters
they come from — a third of the cost, and nothing else reads those fields. Dropping them
entirely, as the first version did, left a reloaded window with no labels at all: every link
fell through to the weaker timing guess, so a window the API explains with 178 label matches
came back as 238 dashed lines.


A 24h window is ~2000 runs across a dozen pages of runs and a dozen of events — about
six seconds. Almost none of it changes between one refresh and the next: a completed run
never changes again, and a relative range only extends at the `now` end.

So runs and links are cached per API base in `sessionStorage`, along with the span they
cover. On a reload or a refresh tick the cached window is **drawn immediately**, then only
the tail is fetched — the new slice plus the handful of runs that were still in flight,
which are the only cached runs that can have changed. Links are immutable once emitted,
so cached ones are always still valid.

Measured: a 6h refresh goes from 2.6s to 1.0s, a 24h refresh from 5.9s to
1.0s, storing 212KB and 848KB respectively (the limit is around 5MB). A/B'd against a
full fetch of the same window: identical run sets, identical states, identical links.

The legend row shows `as of Ns ago` whenever what you are looking at predates the last
few seconds, so cached data is never mistaken for live. `sessionStorage` rather than
`localStorage` deliberately — this is a speed-up for the tab you are working in, and
going stale across days would be a liability. A cache older than 30 minutes, or one that
does not cover the requested start, is discarded and the window fetched in full.

Everything about it is best-effort: storage disabled, an unreadable entry, or a quota
refusal on a wide window all degrade to "no cache", never to a broken page.

## Which runs are in the window

Runs that **overlap** the window, not just those that started inside it. A transform run
takes ~16 minutes, so on a 15m window the run you care about usually began before it;
filtering on start time alone dropped it, and its links with it. Prefect's filters are
AND-only and a running run has no `end_time`, so that is three queries unioned: started
inside, started earlier and ended inside, started earlier and still going.

The look-back is capped at 24h. Unbounded it drags in zombies — a server accumulates runs
stuck in `Running` for weeks, and each would draw as a bar spanning the whole chart.

### More runs than fit

A fetch reads at most 2200 runs — 11 pages, and about as much as `sessionStorage` will
hold. A wide range exceeds that: a busy week can be ten times as many runs. Three things
follow from it.

Runs are read **newest first**, so the cap keeps the recent end of the range. Ascending
spent the whole budget on the oldest runs and dropped everything after them, which drew
7d as two days of bars on the left and nothing since.

The chart is then drawn **from where the runs actually reach**, not from the start of the
range, with a warning saying how much was left out. Against the full 7d axis the six
unread days read as an outage rather than as the gap in the data that they are.

And the cache records that instant rather than the range asked for. Cached as the range,
it would claim coverage it does not have — and since a narrower range sits inside a wider
one, the next 30m view was served incrementally out of the hole and showed a minute of
runs.

## Known gaps
- No auth header field — assumes an unauthenticated API or a port-forward.
- Sub-flow pop-ups query at most 200 parent runs at a time.
