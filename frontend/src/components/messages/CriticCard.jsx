import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ShieldAlert, ShieldCheck, ChevronDown, ChevronUp } from 'lucide-react';
import { cn } from '../ui/Badge';

const SEV_COLOR = { high: '#DC2626', medium: '#D97706', low: '#6B7280' };
const SEV_BG    = { high: '#FEF2F2', medium: '#FFFBEB', low: '#F9FAFB' };

export function CriticCard({ critic = {} }) {
  // Auto-expand by default when there is anything of substance to dissent about.
  // Collapsing critic feedback buries the "what could be wrong" signal — keep it
  // visible and let the user explicitly collapse if they want to dismiss it.
  const _initialChallenges = (critic.challenges || []);
  const _anyHigh = _initialChallenges.some(c =>
    typeof c !== 'string' && (c.severity || '').toLowerCase() === 'high'
  );
  const [expanded, setExpanded] = useState(
    _anyHigh || _initialChallenges.length >= 2
  );

  // Handle both raw format: { approved, confidence_adjustment, challenges, overall_verdict }
  // and any partially-mapped format. approved is a bool; confidence_adjustment is 0.0–1.0.
  const approved = critic.approved !== undefined
    ? critic.approved
    : (critic.verdict?.toLowerCase() === 'approved');

  const confidenceRaw = critic.confidence_adjustment ?? critic.confidence;
  // confidence_adjustment is 0.0–1.0; critic.confidence might already be 0–100
  const confidence = confidenceRaw != null
    ? (confidenceRaw <= 1.0 ? Math.round(confidenceRaw * 100) : Math.round(confidenceRaw))
    : null;

  const verdict = critic.overall_verdict || critic.verdict || (approved ? 'Approved' : 'Issues Found');

  // Challenges may be objects { claim, issue, severity } or plain strings
  const challenges = (critic.challenges || []).map(c =>
    typeof c === 'string' ? { issue: c, severity: 'medium' } : c
  );

  const highCount = challenges.filter(c => c.severity === 'high').length;
  const hasChallenges = challenges.length > 0;

  return (
    <div className={cn(
      "w-full mt-4 mb-2 border rounded-xl shadow-sm overflow-hidden flex flex-col",
      approved ? "border-status-success bg-status-success-dim/30" : "border-status-warning bg-status-warning-dim/30"
    )}>
      {/* ── Header ── */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 p-4 bg-bg-surface">
        <div className="flex items-center gap-3">
          {approved
            ? <ShieldCheck className="text-status-success shrink-0" size={22} />
            : <ShieldAlert className="text-status-warning shrink-0" size={22} />
          }
          <div>
            <h3 className="font-semibold text-[15px] text-text-primary">Adversarial Review</h3>
            <div className="text-[12px] text-text-tertiary">
              <strong className={approved ? "text-status-success" : "text-status-warning"}>
                {approved ? 'Approved' : 'Issues Found'}
              </strong>
              {highCount > 0 && (
                <span className="ml-2 text-status-error">{highCount} high-severity concern{highCount !== 1 ? 's' : ''}</span>
              )}
            </div>
          </div>
        </div>

        <div className="flex items-center gap-4 shrink-0">
          {/* Confidence bar */}
          {confidence != null && (
            <div className="flex flex-col items-end gap-0.5">
              <span className="text-[10px] font-mono text-text-faint uppercase tracking-wider">Reliability</span>
              <div className="flex items-center gap-2">
                <div className="w-20 h-1.5 rounded-full bg-border-subtle overflow-hidden">
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${confidence}%`,
                      background: confidence >= 80 ? '#10B981' : confidence >= 60 ? '#F59E0B' : '#EF4444'
                    }}
                  />
                </div>
                <span className="text-[12px] font-semibold font-mono text-text-secondary">{confidence}%</span>
              </div>
            </div>
          )}

          {hasChallenges && (
            <button
              className="flex items-center justify-center w-8 h-8 rounded hover:bg-bg-elevated transition-colors text-text-muted"
              onClick={() => setExpanded(e => !e)}
              title={expanded ? 'Collapse' : 'View concerns'}
            >
              {expanded ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
            </button>
          )}
        </div>
      </div>

      {/* ── Overall verdict ── */}
      {verdict && (
        <div className="px-4 pb-3 text-[13px] text-text-secondary leading-relaxed bg-bg-surface border-t border-border-subtle pt-3">
          {verdict}
        </div>
      )}

      {/* ── Challenges list ── */}
      <AnimatePresence initial={false}>
        {expanded && hasChallenges && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="border-t border-border-subtle bg-bg-elevated px-4 py-3"
          >
            <h4 className="text-[11px] font-bold text-text-secondary mb-2.5 uppercase tracking-widest">
              Challenges Found ({challenges.length})
            </h4>
            <ul className="flex flex-col gap-2.5">
              {challenges.map((c, i) => {
                const sev = (c.severity || 'medium').toLowerCase();
                const sevColor = SEV_COLOR[sev] || '#6B7280';
                const sevBg    = SEV_BG[sev] || '#F9FAFB';
                return (
                  <li key={i} className="rounded-lg p-3" style={{ background: sevBg, border: `1px solid ${sevColor}22` }}>
                    <div className="flex items-center gap-2 mb-1">
                      <span
                        className="text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded"
                        style={{ background: sevColor + '22', color: sevColor }}
                      >
                        {sev}
                      </span>
                    </div>
                    <p className="text-[12.5px] text-text-secondary leading-relaxed">{c.issue || c.claim || String(c)}</p>
                    {c.claim && c.issue && (
                      <p className="text-[11px] text-text-muted mt-1 italic">
                        Claim: "{c.claim.length > 120 ? c.claim.slice(0, 120) + '…' : c.claim}"
                      </p>
                    )}
                  </li>
                );
              })}
            </ul>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
