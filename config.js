/**
 * Deployment configuration, read before the app starts.
 *
 * Empty on purpose: served next to a Prefect server the defaults are right. Overwrite this
 * file to point the page at an API elsewhere, or to take the header field away.
 *
 *   window.PREFECT_MONITOR = {
 *     apiUrl: 'https://prefect.example.com/api',  // default: /api, or localhost:4200
 *     apiUrlEditable: false,                      // default: only when not same-origin
 *     extraBatchKeys: ['run_partition'],          // payload keys your events use
 *   };
 *
 * `npm start` renders this file from the environment, so nothing on disk has to change for
 * local use, and a container can write it the same way at startup:
 *
 *   PREFECT_MONITOR_API_URL
 *   PREFECT_MONITOR_API_URL_EDITABLE
 *   PREFECT_MONITOR_BATCH_KEYS      comma-separated
 */
window.PREFECT_MONITOR = {};
