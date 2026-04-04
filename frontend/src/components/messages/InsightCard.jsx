import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Lightbulb, ChevronDown, ChevronUp, Clock, AlertTriangle, GitBranch, ArrowRight } from 'lucide-react';
import { Badge } from '../ui/Badge';

const PRIORITY_VARIANT = { critical: 'error', high: 'warning', medium: 'info', low: 'default' };
const PRIORITY_COLOR   = { critical: '#DC2626', high: '#D97706', medium: '#2563EB', low: '#6B7280' };
const PRIORITY_BG      = { critical: '#FEF2F2', high: '#FFFBEB', medium: '#EFF6FF', low: '#F9FAFB' };

// ── ImpactBar: thin progress bar showing impact_score / 10 ───────────────────
function ImpactBar({ score, priority }) {
  if (score == null) return null;
  const pct = Math.min(100, Math.max(0, (score / 10) * 100));
  const color = PRIORITY_COLOR[priority] || '#6B7280';
  return (
    <div className="flex items-center gap-2 mt-1.5">
      <div className="text-[10px] font-semibold text-text-muted uppercase tracking-wider w-[68px] shrink-0">Impact</div>
      <div className="flex-1 h-1.5 rounded-full bg-border-subtle overflow-hidden">
        <div className="h-full rounded-full transition-all duration-500" style={{ width: `${pct}%`, background: color }} />
      </div>
      <div className="text-[11px] font-mono font-semibold shrink-0" style={{ color }}>{score.toFixed(1)}</div>
    </div>
  );
}

// ── CriticAnnotation: inline peer-review warning for this insight ─────────────
function CriticAnnotation({ challenges }) {
  const [open, setOpen] = useState(false);
  if (!challenges?.length) return null;

  const highCount = challenges.filter(c => c.severity === 'high').length;
  const severityColor = highCount > 0 ? '#D97706' : '#6B7280';

  return (
    <div
      className="mt-3 rounded-md border overflow-hidden"
      style={{ borderColor: severityColor + '55', background: '#FFFBEB' }}
    >
      <button
        className="w-full flex items-center gap-2 px-3 py-2 text-left"
        onClick={() => setOpen(o => !o)}
      >
        <AlertTriangle size={12} strokeWidth={2.5} style={{ color: severityColor, shrink: 0 }} />
        <span className="text-[11px] font-semibold flex-1" style={{ color: severityColor }}>
          Peer Review — {challenges.length} concern{challenges.length !== 1 ? 's' : ''}
          {highCount > 0 ? ` (${highCount} high)` : ''}
        </span>
        {open ? <ChevronUp size={12} style={{ color: severityColor }} /> : <ChevronDown size={12} style={{ color: severityColor }} />}
      </button>
      <AnimatePresence initial={false}>
        {open && (
          <motion.ul
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="px-3 pb-2 flex flex-col gap-1.5 border-t"
            style={{ borderColor: severityColor + '33' }}
          >
            {challenges.map((c, i) => {
              const sc = PRIORITY_COLOR[c.severity] || '#6B7280';
              return (
                <li key={i} className="text-[12px] text-text-secondary pt-1.5">
                  <span className="font-semibold uppercase text-[10px] mr-1.5 px-1 rounded" style={{ background: PRIORITY_BG[c.severity] || '#F9FAFB', color: sc }}>{c.severity}</span>
                  {c.issue || c.claim || String(c)}
                </li>
              );
            })}
          </motion.ul>
        )}
      </AnimatePresence>
    </div>
  );
}

// ── SingleInsight ─────────────────────────────────────────────────────────────
function SingleInsight({ insight, idx, criticChallenges = [] }) {
  const [expanded, setExpanded] = useState(false);
  const priority = (insight.fix_priority || 'medium').toLowerCase();

  const hasDeepDive = insight.root_cause_hypothesis || insight.downstream_implications || insight.ux_implications || (insight.possible_causes?.length > 0);
  const fixSteps = insight.how_to_fix || [];
  const hasCriticNote = criticChallenges.length > 0;

  return (
    <motion.div
      className="bg-bg-surface border border-border-default rounded-xl shadow-sm overflow-hidden"
      variants={{ hidden: { opacity: 0, y: 12 }, visible: { opacity: 1, y: 0 } }}
    >
      {/* ── Severity accent strip ── */}
      <div className="h-1 w-full" style={{ background: PRIORITY_COLOR[priority] || '#E5E7EB' }} />

      <div className="p-4">
        {/* ── Header row ── */}
        <div className="flex items-start justify-between gap-3 mb-2">
          <div className="flex items-start gap-2.5">
            <span
              className="flex items-center justify-center w-6 h-6 rounded-full text-[12px] font-bold shrink-0 mt-0.5"
              style={{ background: PRIORITY_BG[priority] || '#F9FAFB', color: PRIORITY_COLOR[priority] || '#6B7280' }}
            >
              {idx + 1}
            </span>
            <h4 className="text-[14px] font-semibold text-text-primary leading-snug">
              {insight.title || insight.headline || 'Insight'}
            </h4>
          </div>
          <Badge variant={PRIORITY_VARIANT[priority] || 'info'} className="shrink-0 text-[10px]">
            {priority.toUpperCase()}
          </Badge>
        </div>

        {/* ── Impact bar + timeline bucket ── */}
        <div className="ml-8 flex flex-col gap-1 mb-3">
          <ImpactBar score={insight.impact_score} priority={priority} />
          {insight.timeline_bucket && (
            <div className="flex items-center gap-1.5 mt-0.5">
              <Clock size={11} strokeWidth={2} className="text-text-muted" />
              <span className="text-[11px] text-text-muted">{insight.timeline_bucket}</span>
            </div>
          )}
        </div>

        {/* ── Summary ── */}
        <p className="text-[13px] text-text-secondary leading-relaxed ml-8 mb-3">
          {insight.ai_summary || insight.description || ''}
        </p>

        {/* ── Deep dive toggle (root cause, downstream, ux) ── */}
        {hasDeepDive && (
          <button
            className="ml-8 flex items-center gap-1.5 text-[12px] text-accent font-medium mb-2 hover:opacity-80 transition-opacity"
            onClick={() => setExpanded(o => !o)}
          >
            <GitBranch size={12} strokeWidth={2.5} />
            {expanded ? 'Hide analysis' : 'Show root cause & downstream impact'}
            {expanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
          </button>
        )}

        <AnimatePresence initial={false}>
          {expanded && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.18 }}
              className="ml-8 flex flex-col gap-3 overflow-hidden"
            >
              {insight.root_cause_hypothesis && (
                <DetailSection label="Root Cause">
                  {insight.root_cause_hypothesis}
                </DetailSection>
              )}
              {insight.possible_causes?.length > 0 && (
                <DetailSection label="Possible Causes">
                  <ul className="mt-0.5 space-y-0.5 list-disc list-inside text-[12px] text-text-secondary">
                    {insight.possible_causes.map((c, i) => <li key={i}>{c}</li>)}
                  </ul>
                </DetailSection>
              )}
              {insight.downstream_implications && (
                <DetailSection label="Downstream Impact">
                  {insight.downstream_implications}
                </DetailSection>
              )}
              {insight.ux_implications && (
                <DetailSection label="UX Implications">
                  {insight.ux_implications}
                </DetailSection>
              )}
            </motion.div>
          )}
        </AnimatePresence>

        {/* ── Action steps ── */}
        {fixSteps.length > 0 && (
          <div className="ml-8 mt-3 pt-3 border-t border-border-subtle">
            <div className="text-[10px] font-bold uppercase tracking-wider text-text-muted mb-2">Action Steps</div>
            <ol className="flex flex-col gap-1.5">
              {fixSteps.map((step, i) => (
                <li key={i} className="flex items-start gap-2 text-[12.5px] text-text-secondary">
                  <span
                    className="shrink-0 w-4 h-4 rounded-full flex items-center justify-center text-[10px] font-bold mt-0.5"
                    style={{ background: PRIORITY_BG[priority] || '#F9FAFB', color: PRIORITY_COLOR[priority] || '#9CA3AF' }}
                  >
                    {i + 1}
                  </span>
                  {step}
                </li>
              ))}
            </ol>
          </div>
        )}

        {/* ── Inline critic annotation ── */}
        {hasCriticNote && <CriticAnnotation challenges={criticChallenges} />}
      </div>
    </motion.div>
  );
}

function DetailSection({ label, children }) {
  return (
    <div className="mb-1">
      <div className="text-[10px] font-bold uppercase tracking-wider text-text-muted mb-1">{label}</div>
      {typeof children === 'string'
        ? <p className="text-[12.5px] text-text-secondary leading-relaxed">{children}</p>
        : children}
    </div>
  );
}

// ── InsightCard (public) ──────────────────────────────────────────────────────
export function InsightCard({ insights = [], criticChallenges = [] }) {
  // Sort by impact_score DESC (critical first), then insertion order
  const sorted = [...insights].sort((a, b) => {
    const sa = a.impact_score ?? _priorityToScore(a.fix_priority);
    const sb = b.impact_score ?? _priorityToScore(b.fix_priority);
    return sb - sa;
  });

  if (!sorted.length) return null;

  return (
    <div className="w-full mt-2 mb-4">
      <div className="flex items-center gap-2 mb-3 text-accent">
        <Lightbulb size={20} />
        <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">Key Insights</h3>
        <span className="text-[12px] text-text-muted font-mono">{sorted.length}</span>
      </div>
      <motion.div
        className="flex flex-col gap-3"
        initial="hidden"
        animate="visible"
        variants={{ hidden: { opacity: 0 }, visible: { opacity: 1, transition: { staggerChildren: 0.07 } } }}
      >
        {sorted.map((insight, idx) => {
          // Match critic challenges to this insight by related_insight_index or if no index set (show on all)
          const insightIdx = insights.indexOf(insight);
          const relevantChallenges = criticChallenges.filter(c =>
            c.related_insight_index == null || c.related_insight_index === insightIdx
          );
          return (
            <SingleInsight
              key={idx}
              insight={insight}
              idx={idx}
              criticChallenges={idx === 0 ? relevantChallenges : criticChallenges.filter(c => c.related_insight_index === insightIdx)}
            />
          );
        })}
      </motion.div>
    </div>
  );
}

function _priorityToScore(priority) {
  return { critical: 9, high: 7, medium: 5, low: 2 }[(priority || 'medium').toLowerCase()] ?? 5;
}
