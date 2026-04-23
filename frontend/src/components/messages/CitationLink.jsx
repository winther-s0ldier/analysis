import React from 'react';

// Matches patterns like [A1], [C3], [A12: session_detection] — the conventions
// the synthesis prompt enforces. Capture group 1 = node id (A1/C3/etc.).
const CITATION_RE = /\[([AC]\d+)(?::\s*[^\]]+)?\]/g;

/**
 * Brief flash + smooth scroll to the chart with matching analysis_id.
 * ChartCard renders `id="chart-<analysis_id>"` on its root element.
 */
function scrollToChart(analysisId) {
  if (!analysisId) return;
  const el = document.getElementById(`chart-${analysisId}`);
  if (!el) return;
  el.scrollIntoView({ behavior: 'smooth', block: 'center' });
  const prev = el.style.boxShadow;
  el.style.transition = 'box-shadow 0.4s ease';
  el.style.boxShadow = '0 0 0 3px #6366F1AA, 0 4px 12px rgba(99,102,241,0.25)';
  window.setTimeout(() => { el.style.boxShadow = prev; }, 1400);
}

export function CitationLink({ nodeId, label }) {
  const onClick = (e) => {
    e.stopPropagation();
    e.preventDefault();
    scrollToChart(nodeId);
  };
  return (
    <button
      type="button"
      onClick={onClick}
      title={`Jump to chart for ${nodeId}`}
      className="inline-flex items-center font-mono text-[11px] font-semibold px-1 py-0 rounded transition-colors hover:bg-accent/10 hover:text-accent text-text-secondary bg-border-subtle/60 mx-0.5 align-baseline cursor-pointer"
      style={{ lineHeight: '1.4' }}
    >
      {label || `[${nodeId}]`}
    </button>
  );
}

/**
 * Turn a plain string into a React fragment where every [A1]/[C3]/[A1: type]
 * citation is a clickable CitationLink. Non-string input is passed through.
 */
export function renderWithCitations(text) {
  if (typeof text !== 'string' || !text) return text;
  const out = [];
  let lastIdx = 0;
  let keyCounter = 0;
  for (const m of text.matchAll(CITATION_RE)) {
    const start = m.index;
    const end   = start + m[0].length;
    if (start > lastIdx) {
      out.push(text.slice(lastIdx, start));
    }
    out.push(
      <CitationLink
        key={`c-${keyCounter++}`}
        nodeId={m[1]}
        label={m[0]}
      />
    );
    lastIdx = end;
  }
  if (lastIdx < text.length) out.push(text.slice(lastIdx));
  return <>{out}</>;
}
