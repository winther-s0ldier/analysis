import React from 'react';
import { motion } from 'framer-motion';
import { CheckSquare } from 'lucide-react';
import { Badge } from '../ui/Badge';

const PRIORITY_VARIANT = { critical: 'error', high: 'warning', medium: 'info', low: 'default' };
const BUCKET_COLOR = {
  'Quick Win (1–2 weeks)': '#059669',
  'Medium Fix (1 month)':  '#D97706',
  'Strategic (3 months)':  '#6366F1',
  'Strategic (3+ months)': '#6366F1',
};

/**
 * ActionPlanCard — compact table linking insight → first action → timeline bucket.
 * Dispatched after synthesis so the user has a single scannable "what to do next" view.
 */
export function ActionPlanCard({ insights = [] }) {
  // Only show insights that have at least one action step
  const actionable = insights.filter(ins => ins.how_to_fix?.length > 0);
  if (!actionable.length) return (
    <div className="w-full flex items-center gap-2 px-4 py-3 rounded-xl border text-[13px]"
      style={{ background: '#F9FAFB', borderColor: '#E5E7EB', color: '#6B7280' }}>
      <CheckSquare size={15} className="shrink-0 opacity-50" />
      <span>No immediate action steps identified for these insights.</span>
    </div>
  );

  // Sort: critical first, then by impact_score
  const sorted = [...actionable].sort((a, b) => {
    const sa = a.impact_score ?? _ps(a.fix_priority);
    const sb = b.impact_score ?? _ps(b.fix_priority);
    return sb - sa;
  });

  // Group by timeline bucket
  const quickWins   = sorted.filter(i => (i.timeline_bucket || '').includes('Quick'));
  const mediumFixes = sorted.filter(i => (i.timeline_bucket || '').includes('Medium'));
  const strategic   = sorted.filter(i => (i.timeline_bucket || '').includes('Strategic') || (!i.timeline_bucket && _ps(i.fix_priority) <= 3));

  const groups = [
    { label: 'Quick Wins', sub: '1–2 weeks', color: '#059669', bg: '#ECFDF5', items: quickWins },
    { label: 'Medium Fixes', sub: '1 month',  color: '#D97706', bg: '#FFFBEB', items: mediumFixes },
    { label: 'Strategic',   sub: '3+ months', color: '#6366F1', bg: '#EEF2FF', items: strategic },
  ].filter(g => g.items.length > 0);

  if (!groups.length) return (
    <div className="w-full flex items-center gap-2 px-4 py-3 rounded-xl border text-[13px]"
      style={{ background: '#F9FAFB', borderColor: '#E5E7EB', color: '#6B7280' }}>
      <CheckSquare size={15} className="shrink-0 opacity-50" />
      <span>Actions identified but timeline buckets could not be determined.</span>
    </div>
  );

  return (
    <div className="w-full mt-2 mb-4">
      <div className="flex items-center gap-2 mb-3 text-text-primary">
        <CheckSquare size={18} strokeWidth={2} className="text-accent" />
        <h3 className="text-[15px] font-semibold tracking-tight">Action Plan</h3>
      </div>
      <motion.div
        className="flex flex-col gap-3"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.4 }}
      >
        {groups.map((group) => (
          <div key={group.label} className="rounded-xl border overflow-hidden" style={{ borderColor: group.color + '44' }}>
            {/* Group header */}
            <div className="px-4 py-2.5 flex items-center gap-2" style={{ background: group.bg }}>
              <div className="w-2 h-2 rounded-full shrink-0" style={{ background: group.color }} />
              <span className="text-[12px] font-bold" style={{ color: group.color }}>{group.label}</span>
              <span className="text-[11px]" style={{ color: group.color + '99' }}>{group.sub}</span>
              <span className="ml-auto text-[11px] font-mono" style={{ color: group.color + '99' }}>{group.items.length} item{group.items.length !== 1 ? 's' : ''}</span>
            </div>
            {/* Rows */}
            <div className="divide-y divide-border-subtle bg-bg-surface">
              {group.items.map((ins, i) => {
                const priority = (ins.fix_priority || 'medium').toLowerCase();
                return (
                  <div key={i} className="px-4 py-3 flex items-start gap-3">
                    <Badge variant={PRIORITY_VARIANT[priority] || 'info'} className="shrink-0 text-[10px] mt-0.5">
                      {priority.toUpperCase()}
                    </Badge>
                    <div className="flex-1 min-w-0">
                      <div className="text-[13px] font-medium text-text-primary leading-snug mb-1 truncate">
                        {ins.title || ins.headline || 'Insight'}
                      </div>
                      <div className="text-[12px] text-text-secondary leading-relaxed">
                        {ins.how_to_fix[0]}
                      </div>
                    </div>
                    {ins.impact_score != null && (
                      <div className="shrink-0 text-right">
                        <div className="text-[10px] text-text-muted">Impact</div>
                        <div className="text-[13px] font-semibold font-mono" style={{ color: group.color }}>
                          {ins.impact_score.toFixed(1)}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </motion.div>
    </div>
  );
}

function _ps(priority) {
  return { critical: 9, high: 7, medium: 5, low: 2 }[(priority || 'medium').toLowerCase()] ?? 5;
}
