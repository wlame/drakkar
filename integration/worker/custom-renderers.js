// Deployment-owned custom cell renderers for the ripgrep integration
// worker — see docs/ui-enrichment.md#custom-cell-renderers. Served
// byte-for-byte at GET /api/v1/ui/renderers.js and dynamically imported
// by the debug UI when ui.custom_renderers_path is set.
//
// Each function has signature (value, row, cell) => HTMLElement and must
// build a real DOM node synchronously — no dependencies, plain DOM APIs.

// matchBar: a proportional inline bar for a match-count column. `value`
// is the row's raw match count; MATCH_BAR_MAX caps the bar at a plausible
// "a lot of matches" ceiling so a handful of huge outliers don't flatten
// every other row's bar to a sliver.
const MATCH_BAR_MAX = 100;

function matchBar(value, row, cell) {
  const count = typeof value === 'number' ? value : Number(value) || 0;
  const pct = Math.max(0, Math.min(1, count / MATCH_BAR_MAX)) * 100;

  const wrapper = document.createElement('div');
  wrapper.className = 'match-bar';
  wrapper.style.display = 'flex';
  wrapper.style.alignItems = 'center';
  wrapper.style.gap = '6px';

  const track = document.createElement('div');
  track.style.flex = '1 1 auto';
  track.style.height = '8px';
  track.style.borderRadius = '4px';
  track.style.background = 'rgba(127, 127, 127, 0.25)';
  track.style.overflow = 'hidden';

  const fill = document.createElement('div');
  fill.style.height = '100%';
  fill.style.width = pct + '%';
  fill.style.background = count > 0 ? '#3b82f6' : 'rgba(127, 127, 127, 0.4)';
  track.appendChild(fill);

  const label = document.createElement('span');
  label.textContent = String(count);
  label.style.fontVariantNumeric = 'tabular-nums';
  label.style.minWidth = '2ch';
  label.style.textAlign = 'right';

  wrapper.appendChild(track);
  wrapper.appendChild(label);
  return wrapper;
}

// patternChip: a styled span showing the {pattern, matches} JSON payload
// written to RipgrepProbeDetails.top_pattern_chip. `value` is that whole
// object (view='custom' scalars receive the raw decoded field value).
function patternChip(value, row, cell) {
  const chip = document.createElement('span');
  chip.className = 'pattern-chip';
  chip.style.display = 'inline-flex';
  chip.style.alignItems = 'center';
  chip.style.gap = '4px';
  chip.style.padding = '2px 8px';
  chip.style.borderRadius = '999px';
  chip.style.background = 'rgba(59, 130, 246, 0.15)';
  chip.style.border = '1px solid rgba(59, 130, 246, 0.4)';
  chip.style.fontSize = '0.85em';

  const pattern = value && typeof value === 'object' ? value.pattern : undefined;
  const matches = value && typeof value === 'object' ? value.matches : undefined;

  if (!pattern) {
    chip.textContent = 'no pattern';
    return chip;
  }

  const patternText = document.createElement('strong');
  patternText.textContent = pattern;
  chip.appendChild(patternText);

  if (typeof matches === 'number') {
    const countText = document.createElement('span');
    countText.textContent = `× ${matches}`;
    countText.style.opacity = '0.75';
    chip.appendChild(countText);
  }

  return chip;
}

export default {
  matchBar,
  patternChip,
};
