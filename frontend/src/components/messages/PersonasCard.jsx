import React from 'react';
import { motion } from 'framer-motion';
import { Users } from 'lucide-react';
import { Badge } from '../ui/Badge';

export function PersonasCard({ personas = [] }) {
  if (!personas.length) return null;

  return (
    <div className="w-full mt-2 mb-4">
      <div className="flex items-center gap-2 mb-3 text-status-info">
        <Users size={20} />
        <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">Key Segments</h3>
      </div>
      <motion.div
        className="grid grid-cols-1 sm:grid-cols-2 gap-3"
        initial="hidden"
        animate="visible"
        variants={{
          hidden: { opacity: 0 },
          visible: { opacity: 1, transition: { staggerChildren: 0.06 } }
        }}
      >
        {personas.map((segment, idx) => {
          const priority = (segment.priority_level || 'medium').toLowerCase();
          const priorityVariant = { critical: 'error', high: 'warning', medium: 'info', low: 'default' }[priority] || 'info';
          const validPriority = priority;
          return (
            <motion.div
              key={idx}
              className="p-4 bg-bg-surface border border-border-default rounded-lg shadow-sm flex flex-col h-full"
              variants={{
                hidden: { opacity: 0, scale: 0.95 },
                visible: { opacity: 1, scale: 1 }
              }}
            >
              <div className="flex items-start justify-between gap-2 mb-2">
                <h4 className="text-[14px] font-semibold text-text-primary leading-snug">{segment.name}</h4>
                <Badge variant={priorityVariant} className="shrink-0 text-[10px]">{validPriority.toUpperCase()}</Badge>
              </div>
              {segment.profile && (
                <div className="text-[13px] text-text-secondary leading-relaxed mb-2">{segment.profile}</div>
              )}
              {segment.size && (
                <div className="text-[11px] font-mono text-text-muted uppercase tracking-wider mb-2">{segment.size}</div>
              )}
              {segment.pain_points?.length > 0 && (
                <div className="mt-auto pt-2 border-t border-border-subtle">
                  <div className="text-[11px] font-semibold text-text-muted uppercase tracking-wider mb-1">Pain Points</div>
                  <ul className="text-[12px] text-text-secondary space-y-0.5 list-disc list-inside">
                    {segment.pain_points.slice(0, 2).map((p, i) => <li key={i}>{p}</li>)}
                  </ul>
                </div>
              )}
              {segment.opportunities?.length > 0 && (
                <div className="mt-2">
                  <div className="text-[11px] font-semibold text-status-success uppercase tracking-wider mb-1">Opportunities</div>
                  <ul className="text-[12px] text-text-secondary space-y-0.5 list-disc list-inside">
                    {segment.opportunities.slice(0, 2).map((o, i) => <li key={i}>{o}</li>)}
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
