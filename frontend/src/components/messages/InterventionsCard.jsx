import React from 'react';
import { motion } from 'framer-motion';
import { Target } from 'lucide-react';
import { Badge } from '../ui/Badge';

export function InterventionsCard({ interventions = [] }) {
  if (!interventions.length) return null;

  return (
    <div className="w-full mt-2 mb-4">
      <div className="flex items-center gap-2 mb-3 text-status-warning">
        <Target size={20} />
        <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">Recommendations</h3>
      </div>
      <motion.div
        className="flex flex-col gap-3"
        initial="hidden"
        animate="visible"
        variants={{
          hidden: { opacity: 0 },
          visible: { opacity: 1, transition: { staggerChildren: 0.06 } }
        }}
      >
        {interventions.map((item, idx) => {
          const sev = (item.severity || 'medium').toLowerCase();
          const sevVariant = { critical: 'error', high: 'warning', medium: 'info', low: 'default' }[sev] || 'info';
          const validSev = sev;
          const realtime = item.realtime_interventions || [];
          const proactive = item.proactive_outreach || [];
          return (
            <motion.div
              key={idx}
              className="border border-border-default bg-bg-surface rounded-lg overflow-hidden shadow-sm"
              variants={{
                hidden: { opacity: 0, x: -10 },
                visible: { opacity: 1, x: 0 }
              }}
            >
              <div className="flex items-center gap-2 px-4 py-3 bg-bg-elevated border-b border-border-subtle">
                <Badge variant={sevVariant} className="text-[10px]">{validSev.toUpperCase()}</Badge>
                <h4 className="text-[13px] font-semibold text-text-primary">{item.title || item.strategy}</h4>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 divide-y sm:divide-y-0 sm:divide-x divide-border-subtle">
                {realtime.length > 0 && (
                  <div className="p-3">
                    <div className="text-[10px] font-semibold text-text-muted uppercase tracking-wider mb-1.5">Immediate Actions</div>
                    <ul className="text-[12px] text-text-secondary space-y-1 list-disc list-inside">
                      {realtime.map((r, i) => <li key={i}>{r}</li>)}
                    </ul>
                  </div>
                )}
                {proactive.length > 0 && (
                  <div className="p-3">
                    <div className="text-[10px] font-semibold text-text-muted uppercase tracking-wider mb-1.5">Proactive Outreach</div>
                    <ul className="text-[12px] text-text-secondary space-y-1 list-disc list-inside">
                      {proactive.map((p, i) => <li key={i}>{p}</li>)}
                    </ul>
                  </div>
                )}
              </div>
            </motion.div>
          );
        })}
      </motion.div>
    </div>
  );
}
