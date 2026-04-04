import React from 'react';
import { ClipboardList, Network, ArrowRight } from 'lucide-react';
import { AIMessage } from './AIMessage';
import { marked } from 'marked';

// ── ExecutiveSummaryCard ───────────────────────────────────────────────────────
export function ExecutiveSummaryCard({ text }) {
  if (!text) return null;

  // text is either the full executive_summary object or a plain string (fallback)
  const isObject = text && typeof text === 'object';
  const exec = isObject ? text : null;

  // Structured view when we have the full executive_summary object
  if (exec) {
    const priorities = Array.isArray(exec.top_priorities) ? exec.top_priorities : [];
    const timelineStr = exec.timeline || '';

    // Parse timeline into Quick Win / Medium Fix / Strategic buckets
    const buckets = _parseTimeline(timelineStr);

    return (
      <div className="w-full mt-2 mb-4 bg-bg-surface border border-border-default rounded-xl shadow-sm overflow-hidden">
        {/* Header */}
        <div className="flex items-center gap-2 px-5 py-4 border-b border-border-subtle bg-bg-elevated">
          <ClipboardList size={18} strokeWidth={2} className="text-status-success shrink-0" />
          <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">Analysis Summary</h3>
        </div>

        <div className="p-5 flex flex-col gap-5">
          {/* Health Assessment */}
          {exec.overall_health && (
            <div>
              <SectionLabel>Health Assessment</SectionLabel>
              <p className="text-[13.5px] text-text-secondary leading-relaxed">{exec.overall_health}</p>
            </div>
          )}

          {/* Top Priorities */}
          {priorities.length > 0 && (
            <div>
              <SectionLabel>Top Priorities</SectionLabel>
              <ol className="flex flex-col gap-2 mt-1">
                {priorities.map((p, i) => (
                  <li key={i} className="flex items-start gap-2.5">
                    <span
                      className="shrink-0 w-5 h-5 rounded-full flex items-center justify-center text-[11px] font-bold mt-0.5"
                      style={{ background: i === 0 ? '#FEE2E2' : i === 1 ? '#FEF3C7' : '#EFF6FF', color: i === 0 ? '#DC2626' : i === 1 ? '#D97706' : '#2563EB' }}
                    >
                      {i + 1}
                    </span>
                    <span className="text-[13px] text-text-secondary leading-snug">{p}</span>
                  </li>
                ))}
              </ol>
            </div>
          )}

          {/* Business Impact */}
          {exec.business_impact && (
            <div
              className="px-4 py-3 rounded-lg border-l-4"
              style={{ background: '#FFFBEB', borderColor: '#F59E0B' }}
            >
              <div className="text-[10px] font-bold uppercase tracking-wider mb-1" style={{ color: '#B45309' }}>Business Impact</div>
              <p className="text-[13px] leading-relaxed" style={{ color: '#78350F' }}>{exec.business_impact}</p>
            </div>
          )}

          {/* Resource Allocation */}
          {exec.resource_allocation && (
            <div>
              <SectionLabel>Resource Allocation</SectionLabel>
              <p className="text-[13px] text-text-secondary leading-relaxed">{exec.resource_allocation}</p>
            </div>
          )}

          {/* Timeline */}
          {(buckets.quickWin.length > 0 || buckets.mediumFix.length > 0 || buckets.strategic.length > 0 || timelineStr) && (
            <div>
              <SectionLabel>Action Timeline</SectionLabel>
              {buckets.quickWin.length || buckets.mediumFix.length || buckets.strategic.length ? (
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 mt-1">
                  <TimelineBucket label="Quick Win" sub="1–2 weeks" color="#059669" bg="#ECFDF5" items={buckets.quickWin} />
                  <TimelineBucket label="Medium Fix" sub="1 month" color="#D97706" bg="#FFFBEB" items={buckets.mediumFix} />
                  <TimelineBucket label="Strategic" sub="3+ months" color="#6366F1" bg="#EEF2FF" items={buckets.strategic} />
                </div>
              ) : (
                <p className="text-[13px] text-text-secondary leading-relaxed">{timelineStr}</p>
              )}
            </div>
          )}
        </div>
      </div>
    );
  }

  // Fallback: plain string (backwards compat)
  const textStr = typeof text === 'string' ? text : JSON.stringify(text, null, 2);
  return (
    <div className="w-full mt-2 mb-4 bg-bg-surface border border-border-default rounded-lg p-5 shadow-sm">
      <div className="flex items-center gap-2 mb-3 text-status-success">
        <ClipboardList size={20} />
        <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">Executive Summary</h3>
      </div>
      <div className="text-[14px] text-text-secondary leading-relaxed whitespace-pre-wrap">{textStr}</div>
    </div>
  );
}

function SectionLabel({ children }) {
  return (
    <div className="text-[10px] font-bold uppercase tracking-widest text-text-muted mb-1.5">{children}</div>
  );
}

function TimelineBucket({ label, sub, color, bg, items }) {
  return (
    <div className="rounded-lg p-3" style={{ background: bg, border: `1px solid ${color}22` }}>
      <div className="text-[11px] font-bold uppercase tracking-wider mb-0.5" style={{ color }}>{label}</div>
      <div className="text-[10px] mb-2" style={{ color: color + '99' }}>{sub}</div>
      {items.length > 0 ? (
        <ul className="flex flex-col gap-1">
          {items.map((item, i) => (
            <li key={i} className="flex items-start gap-1.5 text-[12px]" style={{ color: '#374151' }}>
              <span className="mt-1.5 w-1 h-1 rounded-full shrink-0" style={{ background: color }} />
              {item}
            </li>
          ))}
        </ul>
      ) : (
        <div className="text-[11px]" style={{ color: '#9CA3AF' }}>—</div>
      )}
    </div>
  );
}

// Parse "Quick Wins (1-2 weeks): ... | Medium Fixes: ... | Strategic: ..." into buckets
function _parseTimeline(str) {
  const buckets = { quickWin: [], mediumFix: [], strategic: [] };
  if (!str) return buckets;

  // Split on common delimiters: newlines, pipes, or explicit bucket labels
  const linesRaw = str.split(/\n|(?<=[^\|])\|(?=[^\|])/);
  let currentBucket = null;

  for (const line of linesRaw) {
    const l = line.trim();
    if (!l || l === '|') continue;

    if (/quick\s*win/i.test(l)) {
      currentBucket = 'quickWin';
      const rest = l.replace(/quick\s*wins?\s*\([^)]*\)\s*:?/i, '').replace(/quick\s*wins?\s*:?/i, '').trim();
      if (rest) buckets.quickWin.push(...rest.split(/[•\-,·]/).map(s => s.trim()).filter(Boolean));
    } else if (/medium\s*fix|medium\s*term|1\s*month/i.test(l)) {
      currentBucket = 'mediumFix';
      const rest = l.replace(/medium\s*(fix(es)?|term)\s*\([^)]*\)\s*:?/i, '').trim();
      if (rest) buckets.mediumFix.push(...rest.split(/[•\-,·]/).map(s => s.trim()).filter(Boolean));
    } else if (/strategic|long[- ]term|3\s*months/i.test(l)) {
      currentBucket = 'strategic';
      const rest = l.replace(/strategic\s*\([^)]*\)\s*:?/i, '').trim();
      if (rest) buckets.strategic.push(...rest.split(/[•\-,·]/).map(s => s.trim()).filter(Boolean));
    } else if (currentBucket) {
      const items = l.split(/[•\-·]/).map(s => s.trim()).filter(Boolean);
      buckets[currentBucket].push(...items);
    }
  }

  return buckets;
}

// ── CrossConnectionsCard ──────────────────────────────────────────────────────
export function CrossConnectionsCard({ connections = [] }) {
  if (!connections.length) return null;

  return (
    <div className="w-full mt-2 mb-4">
      <div className="flex items-center gap-2 mb-3 text-accent">
        <Network size={20} />
        <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">Cross-Metric Connections</h3>
      </div>
      <div className="flex flex-col gap-2">
        {connections.map((conn, idx) => {
          const fa = conn.finding_a || conn.link || conn.description || (typeof conn === 'string' ? conn : '');
          const fb = conn.finding_b || '';
          const meaning = conn.synthesized_meaning || '';

          // If only a plain string (old format), fall back to simple display
          if (!fb && !meaning) {
            return (
              <div key={idx} className="p-3 bg-bg-elevated border border-border-subtle rounded-lg text-[13px] text-text-secondary">
                {fa}
              </div>
            );
          }

          return (
            <div key={idx} className="rounded-lg border border-border-subtle bg-bg-surface overflow-hidden">
              {/* Connection pair */}
              <div className="flex items-center gap-2 px-4 py-3 bg-bg-elevated flex-wrap">
                <span className="text-[12px] font-medium text-text-primary bg-accent-dim px-2 py-0.5 rounded">{fa}</span>
                <ArrowRight size={14} className="text-text-muted shrink-0" />
                <span className="text-[12px] font-medium text-text-primary bg-accent-dim px-2 py-0.5 rounded">{fb}</span>
              </div>
              {/* Meaning */}
              {meaning && (
                <div className="px-4 py-2.5 text-[12.5px] text-text-secondary leading-relaxed border-t border-border-subtle">
                  {meaning}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── NarrativeCard ─────────────────────────────────────────────────────────────
// Renders the conversational_report which is markdown, not HTML.
// Converts markdown → HTML via marked before injecting.
export function NarrativeCard({ htmlContent }) {
  if (!htmlContent) return null;
  const raw = typeof htmlContent === 'string' ? htmlContent : '';
  // Strip any injected scripts/handlers, then parse as markdown
  const sanitized = raw
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/on\w+\s*=\s*["'][^"']*["']/gi, '');
  const rendered = _mdToHtml(sanitized);
  return (
    <AIMessage>
      <div
        className="w-full prose prose-sm prose-slate max-w-none text-[14px] leading-relaxed text-text-secondary narrative-md"
        dangerouslySetInnerHTML={{ __html: rendered }}
      />
    </AIMessage>
  );
}

function _mdToHtml(md) {
  try {
    return marked.parse(md, { breaks: true, gfm: true });
  } catch {
    return `<pre style="white-space:pre-wrap">${md}</pre>`;
  }
}
