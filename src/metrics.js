/**
 * The metrics page: the shared header, and nothing else yet.
 *
 * A second document rather than a route, because the timeline's chart, zoom bar and pop-up
 * stack are a page's worth of machinery this one would load only to hide — and because the
 * server already serves any file at the root, so a page costs nothing but the file.
 */

import { mountHeader } from './header.js';
import { resolveRange } from './time-range.js';
import { formatFull } from './zone.js';

const $ = (selector) => document.querySelector(selector);

let header = null;

/**
 * Stands in for the metrics, and says what the header currently resolves to.
 *
 * Deliberately the resolved instants and a drawn-at stamp rather than "coming soon": it is
 * the only thing on the page that tells you the picker, the clock and the refresh timer are
 * really wired, rather than a bar that merely looks right.
 */
function draw() {
  const resolved = resolveRange(header.range);
  $('#scaffoldNotice').textContent = resolved
    ? `${formatFull(resolved.from)} → ${formatFull(resolved.to)} · drawn ${formatFull(Date.now())}`
    : 'that range does not resolve';
}

/** Replace, never push: nothing here navigates away, so there is no pop-up to come back to. */
function writeUrl() {
  window.history.replaceState(null, '', header.query());
}

function init() {
  header = mountHeader({
    page: 'metrics',
    onViewChange: () => {
      writeUrl();
      draw();
    },
    onReload: () => draw(),
    // Nothing is fetched yet; the metrics client lands here when there is something to read.
    onApiUrlChange: () => draw(),
  });

  window.addEventListener('popstate', () => {
    header.readParams(window.location.search); // the clock, before anything is labelled
    header.sync();
    draw();
  });

  writeUrl();
  draw();
}

init();
