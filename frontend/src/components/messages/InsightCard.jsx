import React from 'react';
import { motion } from 'framer-motion';
import { Lightbulb } from 'lucide-react';
import { Badge } from '../ui/Badge';

export function InsightCard({ insights = [] }) {
  if (!insights.length) return null;

  const priorityVariant = { critical: 'error', high: 'warning', medium: 'info', low: 'default' };

  return (
    <div className="w-full mt-2 mb-4">
      <div className="flex items-center gap-2 mb-3 text-accent">
        <Lightbulb size={20} />
        <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">Key Insights</h3>
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
        {insights.map((insight, idx) => {
          const priority = (insight.fix_priority || 'medium').toLowerCase();
          return (
            <motion.div
              key={idx}
              className="p-4 bg-bg-surface border border-border-default rounded-lg shadow-sm"
              variants={{
                hidden: { opacity: 0, y: 10 },
                visible: { opacity: 1, y: 0 }
              }}
            >
              <div className="flex items-start justify-between gap-2 mb-2">
                <div className="flex items-start gap-2">
                  <span className="flex items-center justify-center w-6 h-6 rounded-full bg-accent-dim text-accent text-[12px] font-bold shrink-0 mt-0.5">
                    {idx + 1}
                  </span>
                  <h4 className="text-[14px] font-semibold text-text-primary leading-snug">
                    {insight.title || insight.headline || 'Insight'}
                  </h4>
                </div>
                {priority && (
                  <Badge variant={priorityVariant[priority] || 'info'} className="shrink-0 text-[10px]">
                    {priority.toUpperCase()}
                  </Badge>
                )}
              </div>
              <p className="text-[13px] text-text-secondary leading-relaxed ml-8">
                {insight.ai_summary || insight.description || ''}
              </p>
              {insight.how_to_fix?.length > 0 && (
                <div className="mt-3 ml-8 pt-2 border-t border-border-subtle">
                  <div className="text-[11px] font-semibold text-text-muted uppercase tracking-wider mb-1">How to Fix</div>
                  <ul className="text-[12px] text-text-secondary space-y-0.5 list-disc list-inside">
                    {insight.how_to_fix.slice(0, 2).map((f, i) => <li key={i}>{f}</li>)}
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
