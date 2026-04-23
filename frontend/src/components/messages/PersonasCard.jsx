import React from 'react';
import { motion } from 'framer-motion';
import { Users, UserCheck, AlertOctagon, Target } from 'lucide-react';
import { Badge } from '../ui/Badge';
import { renderWithCitations } from './CitationLink';

const PRIORITY_VARIANT = { critical: 'error', high: 'warning', medium: 'info', low: 'default' };
const PRIORITY_COLOR   = { critical: '#DC2626', high: '#D97706', medium: '#2563EB', low: '#6B7280' };
const PRIORITY_BG      = { critical: '#FEF2F2', high: '#FFFBEB', medium: '#EFF6FF', low: '#F9FAFB' };

// Pull a percentage out of strings like "12,043 users (32%) [A1]" — synthesis
// writes segment.size in this free-form shape.
function extractPercent(sizeStr) {
  if (!sizeStr || typeof sizeStr !== 'string') return null;
  const m = sizeStr.match(/(\d+(?:\.\d+)?)\s*%/);
  return m ? Math.min(100, Math.max(0, parseFloat(m[1]))) : null;
}

function SizeBar({ pct, priority }) {
  if (pct == null) return null;
  const color = PRIORITY_COLOR[priority] || '#6366F1';
  return (
    <div className="flex items-center gap-2 mt-1">
      <div className="flex-1 h-1.5 rounded-full bg-border-subtle overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{ width: `${pct}%`, background: color }}
        />
      </div>
      <span className="text-[10.5px] font-mono font-semibold shrink-0" style={{ color }}>
        {pct.toFixed(0)}%
      </span>
    </div>
  );
}

export function PersonasCard({ personas = [] }) {
  if (!personas.length) return null;

  return (
    <div className="w-full mt-2 mb-4">
      <div className="flex items-center gap-2 mb-3 text-status-info">
        <Users size={20} />
        <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">
          Key Segments
        </h3>
        <span className="text-[12px] text-text-muted font-mono">{personas.length}</span>
      </div>
      <motion.div
        className="grid grid-cols-1 sm:grid-cols-2 gap-3"
        initial="hidden"
        animate="visible"
        variants={{
          hidden: { opacity: 0 },
          visible: { opacity: 1, transition: { staggerChildren: 0.06 } },
        }}
      >
        {personas.map((segment, idx) => {
          const priority = (segment.priority_level || 'medium').toLowerCase();
          const priorityVariant = PRIORITY_VARIANT[priority] || 'info';
          const pct = extractPercent(segment.size);

          return (
            <motion.div
              key={idx}
              className="p-4 bg-bg-surface border border-border-default rounded-lg shadow-sm flex flex-col h-full"
              variants={{
                hidden:  { opacity: 0, scale: 0.96 },
                visible: { opacity: 1, scale: 1 },
              }}
              style={{ borderLeft: `3px solid ${PRIORITY_COLOR[priority] || '#E5E7EB'}` }}
            >
              {/* ── Header ── */}
              <div className="flex items-start justify-between gap-2 mb-2">
                <div className="flex items-start gap-2 min-w-0">
                  <UserCheck size={14} className="shrink-0 mt-0.5" style={{ color: PRIORITY_COLOR[priority] }} />
                  <h4 className="text-[14px] font-semibold text-text-primary leading-snug">
                    {segment.name}
                  </h4>
                </div>
                <Badge variant={priorityVariant} className="shrink-0 text-[10px]">
                  {priority.toUpperCase()}
                </Badge>
              </div>

              {/* ── Size row with bar ── */}
              {segment.size && (
                <div className="mb-2">
                  <div className="text-[11px] font-mono text-text-muted leading-tight">
                    {renderWithCitations(segment.size)}
                  </div>
                  <SizeBar pct={pct} priority={priority} />
                </div>
              )}

              {/* ── Profile / description ── */}
              {segment.profile && (
                <div className="text-[13px] text-text-secondary leading-relaxed mb-3">
                  {renderWithCitations(segment.profile)}
                </div>
              )}

              {/* ── Pain points ── */}
              {segment.pain_points?.length > 0 && (
                <div className="mt-auto pt-2 border-t border-border-subtle">
                  <div className="flex items-center gap-1.5 mb-1.5">
                    <AlertOctagon size={11} className="text-status-error shrink-0" />
                    <div className="text-[10px] font-bold text-text-muted uppercase tracking-wider">
                      Pain Points
                    </div>
                  </div>
                  <ul className="text-[12px] text-text-secondary space-y-0.5 list-disc list-inside marker:text-status-error/60">
                    {segment.pain_points.map((p, i) => <li key={i}>{renderWithCitations(p)}</li>)}
                  </ul>
                </div>
              )}

              {/* ── Opportunities ── */}
              {segment.opportunities?.length > 0 && (
                <div className="mt-2 pt-2 border-t border-border-subtle">
                  <div className="flex items-center gap-1.5 mb-1.5">
                    <Target size={11} className="text-status-success shrink-0" />
                    <div className="text-[10px] font-bold text-status-success uppercase tracking-wider">
                      Opportunities
                    </div>
                  </div>
                  <ul className="text-[12px] text-text-secondary space-y-0.5 list-disc list-inside marker:text-status-success/60">
                    {segment.opportunities.map((o, i) => <li key={i}>{renderWithCitations(o)}</li>)}
                  </ul>
                </div>
              )}
            </motion.div>
          );
        })}
      </motion.div>
    </div>
  );
}
