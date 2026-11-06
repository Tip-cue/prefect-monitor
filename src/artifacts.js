/**
 * A run's artifacts as HTML. Prefect stores markdown as text and tables as a JSON string
 * of row objects; anything else (link, result, progress) is shown as its raw data.
 *
 * The markdown is the small subset flows actually write: headings, bold, `code`, links,
 * bullets, paragraphs. Everything is escaped first, so artifact text can't inject markup.
 */

import { escapeHtml } from './render.js';

function inline(text) {
  return escapeHtml(text)
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<b>$1</b>')
    .replace(/\[([^\]]+)\]\((https?:[^)\s]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');
}

export function markdownHtml(markdown) {
  const out = [];
  let bullets = false;

  for (const line of String(markdown).split('\n')) {
    const item = /^\s*[-*]\s+(.*)/.exec(line);
    if (bullets && !item) { out.push('</ul>'); bullets = false; }
    if (item) {
      if (!bullets) { out.push('<ul>'); bullets = true; }
      out.push(`<li>${inline(item[1])}</li>`);
      continue;
    }
    const heading = /^(#{1,6})\s+(.*)/.exec(line);
    if (heading) out.push(`<h${heading[1].length}>${inline(heading[2])}</h${heading[1].length}>`);
    else if (line.trim()) out.push(`<p>${inline(line)}</p>`);
  }
  if (bullets) out.push('</ul>');
  return out.join('');
}

function tableHtml(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return '<p class="muted">empty table</p>';
  const columns = Object.keys(rows[0]);
  const head = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join('');
  const body = rows.map((row) => `<tr>${columns.map((column) => (
    `<td>${escapeHtml(row[column] ?? '')}</td>`)).join('')}</tr>`).join('');
  return `<div class="artifact-table"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

export function artifactHtml(artifact) {
  const { type, data, description } = artifact;
  let body;

  if (type === 'markdown') body = markdownHtml(data ?? '');
  else if (type === 'table') {
    try { body = tableHtml(JSON.parse(data)); } catch { body = `<pre>${escapeHtml(data)}</pre>`; }
  } else body = `<pre>${escapeHtml(typeof data === 'string' ? data : JSON.stringify(data, null, 2))}</pre>`;

  // The description is markdown too: flows put the run's summary there, above the table.
  const summary = description ? markdownHtml(description) : '';
  return `<section class="artifact">${summary}${body}</section>`;
}

/** Newest first in, oldest first out; a key updated several times shows only its latest. */
export function latestPerKey(artifacts) {
  const seen = new Set();
  return artifacts.filter((a) => {
    const id = a.key ?? a.id;
    return seen.has(id) ? false : seen.add(id);
  }).reverse();
}

const LEVELS = { 10: 'DEBUG', 20: 'INFO', 30: 'WARNING', 40: 'ERROR', 50: 'CRITICAL' };

/**
 * Logs framed per run, like the artifacts: one line each, time, level and message.
 *
 * @param {{title: string, logs: object[]}[]} groups `title` is already HTML
 * @param {(ms: number) => string} formatTime the pop-up's clock, in the chosen zone
 */
export function logGroupsHtml(groups, formatTime) {
  const shown = groups.filter((group) => group.logs.length > 0);
  if (shown.length === 0) return '<p class="muted">no logs for this run or its sub-flows</p>';
  return shown.map(({ title, logs }) => `<div class="artifact-run">${title}</div><div class="artifact-frame log-frame">${
    logs.map(({ timestamp, level, message }) => {
      const name = LEVELS[level] ?? String(level);
      return `<div class="log-line log-${name.toLowerCase()}"><span class="log-time">${
        formatTime(Date.parse(timestamp))}</span><span class="log-level">${name}</span><span>${
        escapeHtml(message)}</span></div>`;
    }).join('')}</div>`).join('');
}

/**
 * Artifacts framed by the run that published them: a flow's artifacts mostly live in its
 * sub-flows, and without the frame there is no telling which sub-flow said what.
 *
 * @param {{title: string, artifacts: object[]}[]} groups `title` is already HTML
 */
export function artifactGroupsHtml(groups) {
  const shown = groups.filter((group) => group.artifacts.length > 0);
  if (shown.length === 0) return '<p class="muted">no artifacts for this run or its sub-flows</p>';
  return shown.map(({ title, artifacts }) => `<div class="artifact-run">${title}</div><div class="artifact-frame">${
    latestPerKey(artifacts).map(artifactHtml).join('')}</div>`).join('');
}
