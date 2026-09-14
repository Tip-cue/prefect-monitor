/**
 * Zooming into part of the loaded range by dragging across the chart, and the scroll bar
 * that shows where you are and gets you back out.
 *
 * A zoom is presentation only: the runs and links for the whole range are already loaded,
 * so narrowing the axis is instant and needs no fetch. The picker's range stays what it
 * was — the zoom sits inside it — which is what makes "back to the original" a single
 * click no matter how many times you have zoomed in.
 *
 * Pure geometry: no DOM, no state. `src/app.js` wires the pointer events to it.
 */

/** Pointer movement below this is a click, not a selection. */
export const DRAG_THRESHOLD_PX = 4;

/** A selection narrower than this is a mis-drag; refuse it rather than zoom to nothing. */
export const MIN_ZOOM_MS = 1000;

/** So a deep zoom still leaves something to grab and drag. */
export const MIN_THUMB_PX = 14;

/**
 * The instant at an x pixel, clamped to the plot.
 *
 * @param {{left: number, width: number, from: number, to: number}} plot
 * @param {number} x pixels from the left edge of the chart SVG
 */
export function timeAt(plot, x) {
  const fraction = (x - plot.left) / plot.width;
  return plot.from + Math.min(1, Math.max(0, fraction)) * (plot.to - plot.from);
}

/**
 * The zoom a drag from `xA` to `xB` asks for, in either direction.
 *
 * @returns {{from: number, to: number}|null} null if it is too narrow to mean anything
 */
export function zoomFromDrag(plot, xA, xB) {
  const [from, to] = [timeAt(plot, xA), timeAt(plot, xB)].sort((a, b) => a - b);
  return to - from < MIN_ZOOM_MS ? null : { from, to };
}

/**
 * Holds a zoom inside the loaded range, keeping its span.
 *
 * The range moves under it: `now-6h → now` slides on every refresh, and a truncated fetch
 * moves the start. Keeping the span and sliding the window is what makes a zoom survive
 * auto-refresh — clamping each edge separately would shrink it a little every tick.
 */
export function clampZoom(zoom, from, to) {
  if (!zoom) return null;

  const span = Math.min(zoom.to - zoom.from, to - from);
  const start = Math.min(Math.max(zoom.from, from), to - span);
  return { from: start, to: start + span };
}

/**
 * How fast the wheel zooms: the span is scaled by e^(deltaY × this). One notch of a mouse
 * wheel (|deltaY| ≈ 100) changes it by ~20%; a trackpad's small deltas make it continuous.
 * Negative deltaY (scrolling up) zooms in, as maps do — flip the sign here to change that.
 */
export const WHEEL_ZOOM_RATE = 0.002;

/** No single wheel event scales the span by more than this, however large its delta. */
const MAX_WHEEL_FACTOR = 2;

/**
 * The zoom a wheel movement asks for: the shown span scaled about the instant under the
 * pointer, so what the cursor points at stays where it is.
 *
 * Widening past the loaded range means all the way out — null, the same as the ✕ — so
 * scrolling out keeps going until there is no zoom left, rather than stopping just short.
 *
 * @param {{left: number, width: number, from: number, to: number}} plot what is shown
 * @param {{from: number, to: number}} loaded the range the zoom sits inside
 * @param {number} x pointer position, pixels from the left edge of the chart SVG
 * @param {number} deltaY the wheel event's vertical delta
 * @returns {{from: number, to: number}|null} null for "no zoom"
 */
export function zoomFromWheel(plot, loaded, x, deltaY) {
  const shown = plot.to - plot.from;
  const factor = Math.min(MAX_WHEEL_FACTOR, Math.max(1 / MAX_WHEEL_FACTOR, Math.exp(deltaY * WHEEL_ZOOM_RATE)));
  const span = shown * factor;

  if (span >= loaded.to - loaded.from) return null;
  if (span < MIN_ZOOM_MS) return { from: plot.from, to: plot.to }; // as deep as it goes

  const anchor = timeAt(plot, x);
  const from = anchor - ((anchor - plot.from) / shown) * span;
  return clampZoom({ from, to: from + span }, loaded.from, loaded.to);
}

/**
 * Where the scroll bar's thumb sits: the zoom drawn against the whole range, the way
 * CloudWatch shows which slice of the period you are looking at.
 *
 * @returns {{left: number, width: number}} pixels within a `trackWidth`-wide track
 */
export function thumbGeometry(range, zoom, trackWidth) {
  const scale = trackWidth / (range.to - range.from);
  const width = Math.min(trackWidth, Math.max(MIN_THUMB_PX, (zoom.to - zoom.from) * scale));
  const left = Math.min((zoom.from - range.from) * scale, trackWidth - width);

  return { left: Math.max(0, left), width };
}

/** The zoom that dragging the thumb to `left` pixels asks for. */
export function zoomFromThumb(range, zoom, trackWidth, left) {
  const span = zoom.to - zoom.from;
  const start = range.from + (left / trackWidth) * (range.to - range.from);

  return clampZoom({ from: start, to: start + span }, range.from, range.to);
}
