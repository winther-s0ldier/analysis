import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Activity, ShieldAlert, ShieldCheck, ChevronDown, ChevronUp } from 'lucide-react';
import { cn } from '../ui/Badge';

export function CriticCard({ critic = {} }) {
  const [expanded, setExpanded] = useState(false);
  const isApproved = critic.verdict?.toLowerCase() === 'approved';

  return (
    <div className={cn(
      "w-full mt-4 mb-2 border rounded-xl shadow-sm overflow-hidden flex flex-col",
      isApproved ? "border-status-success bg-status-success-dim/30" : "border-status-warning bg-status-warning-dim/30"
    )}>
      <div className="flex flex-col sm:flex-row sm:items-center justify-between p-4 bg-bg-surface">
        <div className="flex items-center gap-3">
          {isApproved ? <ShieldCheck className="text-status-success" size={24} /> : <ShieldAlert className="text-status-warning" size={24} />}
          <div>
            <h3 className="font-semibold text-[15px] text-text-primary">Adversarial Review</h3>
            <div className="text-[13px] text-text-tertiary">
              Verdict: <strong className={isApproved ? "text-status-success" : "text-status-warning"}>{critic.verdict || 'Reviewed'}</strong>
            </div>
          </div>
        </div>
        
        <div className="mt-3 sm:mt-0 flex items-center gap-4">
          {critic.confidence !== undefined && (
            <div className="flex flex-col items-end">
              <span className="text-[11px] font-mono text-text-faint uppercase">Confidence</span>
              <span className="font-semibold text-text-secondary">{critic.confidence}%</span>
            </div>
          )}
          {critic.challenges?.length > 0 && (
            <button 
              className="flex items-center justify-center w-8 h-8 rounded hover:bg-bg-elevated transition-colors text-text-muted"
              onClick={() => setExpanded(!expanded)}
            >
              {expanded ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
            </button>
          )}
        </div>
      </div>

      <AnimatePresence initial={false}>
        {expanded && critic.challenges?.length > 0 && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="border-t border-border-subtle bg-bg-elevated px-4 py-3"
          >
            <h4 className="text-[12px] font-semibold text-text-secondary mb-2 uppercase tracking-wider">Challenges Found</h4>
            <ul className="space-y-2">
              {critic.challenges.map((c, i) => (
                <li key={i} className="text-[13px] text-text-secondary flex gap-2">
                  <span className="text-status-warning mt-0.5">•</span>
                  <span>{c}</span>
                </li>
              ))}
            </ul>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
