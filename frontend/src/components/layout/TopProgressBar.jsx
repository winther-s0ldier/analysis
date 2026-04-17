import React, { useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { usePipelineStore } from '../../store/pipelineStore';

const STAGE_PCT = {
  idle: 0,
  uploading: 10,
  profiling: 25,
  discovering: 45,
  analyzing: 65,
  synthesizing: 80,
  building_report: 90,
  complete: 100,
  error: 100,
};

export function TopProgressBar() {
  const currentSession = usePipelineStore((s) => s.sessions[s.currentSessionId]);
  const phase = currentSession?.phase ?? 'idle';
  const nodes = currentSession?.nodes ?? [];
  const [pct, setPct] = useState(0);

  useEffect(() => {
    let currentPct = STAGE_PCT[phase] ?? 0;
    
    // Smooth transition during analyzing phase based on nodes
    if (phase === 'analyzing' && nodes.length > 0) {
      const completed = nodes.filter(n => n.status === 'complete' || n.status === 'failed').length;
      const fraction = completed / nodes.length;
      currentPct = 45 + fraction * 20; // from 45% (discover end) to 65% (analyzing end)
    }

    setPct(currentPct);
  }, [phase, nodes]);

  // Hide the bar if we are idle or fully completed
  const show = pct > 0 && pct < 100;

  return (
    <AnimatePresence>
      <motion.div
        className="fixed top-0 left-0 right-0 h-[2px] z-[100] bg-transparent pointer-events-none"
        initial={{ opacity: 0 }}
        animate={{ opacity: show ? 1 : 0 }}
        transition={{ duration: 0.2 }}
      >
        <motion.div
          className="h-full bg-accent rounded-r-full"
          initial={{ width: '0%' }}
          animate={{ width: `${pct}%` }}
          transition={{ duration: 0.4, ease: [0.4, 0, 0.2, 1] }}
        />
      </motion.div>
    </AnimatePresence>
  );
}
