/**
 * Dev server: static files with caching switched off.
 *
 * `python3 -m http.server` sends no `Cache-Control`, so browsers happily reuse ES
 * modules and an edit looks like it never applied — which is exactly how a stale
 * `src/links.js` once looked like a bug in the app. The deployed nginx sends
 * `no-cache` for the same reason.
 */

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, resolve, sep } from 'node:path';

const PORT = Number(process.env.PORT ?? 8080);
const ROOT = resolve(process.cwd());

/**
 * `config.js` is rendered from the environment rather than read off disk, so a local run can
 * point at any server without editing a tracked file:
 *
 *   PREFECT_MONITOR_API_URL=https://prefect.example.com/api npm start
 *   PREFECT_MONITOR_BATCH_KEYS=run_partition,cycle npm start
 *   PREFECT_MONITOR_API_URL_EDITABLE=false npm start
 *
 * A container image does the same thing at startup — write this file, then serve.
 */
function renderConfig() {
  const config = {};
  const { PREFECT_MONITOR_API_URL, PREFECT_MONITOR_API_URL_EDITABLE, PREFECT_MONITOR_BATCH_KEYS } = process.env;

  if (PREFECT_MONITOR_API_URL) config.apiUrl = PREFECT_MONITOR_API_URL;
  if (PREFECT_MONITOR_API_URL_EDITABLE) {
    config.apiUrlEditable = PREFECT_MONITOR_API_URL_EDITABLE !== 'false';
  }
  if (PREFECT_MONITOR_BATCH_KEYS) {
    config.extraBatchKeys = PREFECT_MONITOR_BATCH_KEYS.split(',').map((key) => key.trim()).filter(Boolean);
  }
  return `window.PREFECT_MONITOR = ${JSON.stringify(config)};\n`;
}

const CONTENT_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.png': 'image/png',
};

/**
 * Resolves a request path inside ROOT, or null if it escapes.
 *
 * Even a local server is a trust boundary: without this, `GET /../../.ssh/id_rsa`
 * would be served to anything that can reach the port.
 */
function resolveInsideRoot(urlPath) {
  const relative = urlPath === '/' ? 'index.html' : decodeURIComponent(urlPath).replace(/^\/+/, '');
  const target = resolve(join(ROOT, relative));
  return target === ROOT || target.startsWith(ROOT + sep) ? target : null;
}

createServer(async (request, response) => {
  const { pathname } = new URL(request.url, `http://localhost:${PORT}`);

  // Rendered, not read: the file on disk is the empty default, and the environment is how
  // a run points the page somewhere without editing a tracked file.
  if (pathname === '/config.js') {
    response.writeHead(200, { 'Content-Type': CONTENT_TYPES['.js'], 'Cache-Control': 'no-store' });
    response.end(renderConfig());
    return;
  }

  const file = resolveInsideRoot(pathname);

  if (!file) {
    response.writeHead(403, { 'Content-Type': 'text/plain' });
    response.end('forbidden\n');
    return;
  }

  try {
    const body = await readFile(file);
    response.writeHead(200, {
      'Content-Type': CONTENT_TYPES[extname(file)] ?? 'application/octet-stream',
      'Cache-Control': 'no-store',
    });
    response.end(body);
  } catch {
    response.writeHead(404, { 'Content-Type': 'text/plain' });
    response.end(`not found: ${pathname}\n`);
  }
}).listen(PORT, () => {
  console.log(`prefect-monitor on http://localhost:${PORT} (no-store, so reloads are honest)`);
});
