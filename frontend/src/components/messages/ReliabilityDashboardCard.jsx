import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Gauge, Info, ChevronDown, ChevronUp } from 'lucide-react';

// Reliability buckets — matches backend labels exactly so the badge, bar,
// and aggregate color all read consistently across the report.
const LABEL_STYLE = {
  strong:     { color: '#047857', bg: '#ECFDF5', text: 'Strong' },
  suggestive: { color: '#B45309', bg: '#FFFBEB', text: 'Suggestive' },
  tentative:  { color: '#B91C1C', bg: '#FEF2F2', text: 'Tentative' },
  none:       { color: '#6B7280', bg: '#F3F4F6', text: 'No signal' },
};

function CountPill({ label, n, color, bg }) {
  return (
    <div
      className="flex flex-col items-center rounded-md px-3 py-2 min-w-[64px]"
      style={{ background: bg, border: `1px solid ${color}33` }}
    >
      <div className="text-[18px] font-semibold font-mono leading-none" style={{ color }}>
        {n}
      </div>
      <div
        className="text-[9px] font-bold tracking-wider uppercase mt-1"
        style={{ color }}
      >
        {label}
      </div>
    </div>
  );
}

export function ReliabilityDashboardCard({ dashboard }) {
  const [open, setOpen] = useState(false);
  if (!dashboard || typeof dashboard !== 'object') return null;

  const pct     = dashboard.aggregate_pct ?? 0;
  const label   = String(dashboard.label || 'none').toLowerCase();
  const style   = LABEL_STYLE[label] || LABEL_STYLE.none;

  const strong     = dashboard.strong_nodes     ?? 0;
  const suggestive = dashboard.suggestive_nodes ?? 0;
  const tentative  = dashboard.tentative_nodes  ?? 0;
  const failed     = dashboard.failed_nodes     ?? 0;

  const p25        = dashboard.weak_link_confidence_p25 ?? 0;
  const mean       = dashboard.mean_node_confidence ?? 0;
  const mcPenalty  = dashboard.multiple_comparisons_penalty ?? 1.0;
  const sigTests   = dashboard.significance_tests_run ?? 0;
  const mcActive   = mcPenalty < 0.999;

  return (
    <div className="w-full mt-2 mb-3 bg-bg-surface border border-border-default rounded-xl shadow-sm overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 pt-3">
        <Gauge size={18} className="shrink-0" style={{ color: style.color }} />
        <h3 className="text-[14px] font-semibold text-text-primary flex-1">
          Report Reliability
        </h3>
        <span
          className="text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded"
          style={{ color: style.color, background: style.bg }}
        >
          {style.text}
        </span>
      </div>

      {/* Score + bar */}
      <div className="px-4 py-3 flex items-center gap-3">
        <div
          className="text-[28px] font-semibold font-mono leading-none shrink-0"
          style={{ color: style.color }}
        >
          {pct}%
        </div>
        <div className="flex-1 h-2 rounded-full bg-border-subtle overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-700"
            style={{ width: `${pct}%`, background: style.color }}
          />
        </div>
      </div>

      {/* Node-count pills */}
      <div className="px-4 pb-3 flex gap-2 flex-wrap">
        <CountPill label="Strong"     n={strong}     color="#047857" bg="#ECFDF5" />
        <CountPill label="Suggestive" n={suggestive} color="#B45309" bg="#FFFBEB" />
        <CountPill label="Tentative"  n={tentative}  color="#B91C1C" bg="#FEF2F2" />
        <CountPill label="Failed"     n={failed}     color="#6B7280" bg="#F3F4F6" />
      </div>

      {/* MC-penalty callout (only if it actually reduced the score) */}
      {mcActive && (
        <div className="mx-4 mb-3 rounded-md px-3 py-2 text-[11.5px] text-text-secondary leading-snug"
             style={{ background: '#FEF3C7', border: '1px solid #FCD34D' }}>
          <strong>Multiple-comparisons penalty applied:</strong>{' '}
          {sigTests} significance tests ran, so the aggregate was multiplied by {mcPenalty.toFixed(2)}.
        </div>
      )}

      {/* Details toggle */}
      <button
        className="w-full flex items-center gap-1.5 px-4 py-2 border-t border-border-subtle text-[11.5px] text-text-muted hover:bg-bg-elevated transition-colors"
        onClick={() => setOpen(o => !o)}
      >
        <Info size={11} />
        {open ? 'Hide formula' : 'How this is calculated'}
        {open ? <ChevronUp size={11} className="ml-auto" /> : <ChevronDown size={11} className="ml-auto" />}
      </button>
      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.18 }}
            className="overflow-hidden"
          >
            <div className="px-4 pb-4 pt-2 text-[12px] text-text-secondary leading-relaxed bg-bg-elevated border-t border-border-subtle space-y-2">
              <div>{dashboard.formula_note}</div>
              <div className="font-mono text-[11px] text-text-muted">
                weak-link p25 = {p25.toFixed(2)} · mean = {mean.toFixed(2)} · MC penalty = {mcPenalty.toFixed(2)}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
