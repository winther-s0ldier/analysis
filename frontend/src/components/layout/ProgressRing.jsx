import React, { useEffect, useState, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { usePipelineStore } from '../../store/pipelineStore';
import { cn } from '../ui/Badge';
import {
  FileUp,
  Search,
  Activity,
  Cpu,
  Check,
  Loader2,
  FileSearch
} from 'lucide-react';

export function ProgressRing() {
  const { phase, nodes } = usePipelineStore();
  const [hidden, setHidden] = useState(false);
  const completed = nodes.filter(n => n.status === 'complete' || n.status === 'failed').length;
  const total = nodes.length || 1;
  const radius = 22;
  const circumference = 2 * Math.PI * radius;

  // Fade out 2s after pipeline completes
  useEffect(() => {
    if (phase === 'complete') {
      const t = setTimeout(() => setHidden(true), 2000);
      return () => clearTimeout(t);
    } else {
      setHidden(false);
    }
  }, [phase]);

  const PHASE_ICONS = {
    uploading: <FileUp size={20} />,
    profiling: <Loader2 size={20} className="animate-spin" />,
    discovering: <Search size={20} />,
    analyzing: <Activity size={20} />,
    synthesizing: <Cpu size={20} />,
    building_report: <FileSearch size={20} />,
    complete: <Check size={20} />
  };

  const isVisible = !hidden && ['uploading', 'profiling', 'discovering', 'analyzing', 'synthesizing', 'building_report', 'complete'].includes(phase);
  const isComplete = phase === 'complete';

  // Calculate percentage fill
  const fillPct = useMemo(() => {
    if (phase === 'uploading') return 0.05;
    if (phase === 'profiling') return 0.15;
    if (phase === 'discovering') return 0.30;
    if (phase === 'analyzing' || phase === 'synthesizing') return 0.30 + (completed / total) * 0.45;
    if (phase === 'building_report') return 0.90;
    if (phase === 'complete') return 1;
    return 0;
  }, [phase, completed, total]);

  const strokeDashoffset = Math.max(0, circumference - fillPct * circumference);

  return (
    <AnimatePresence>
      {isVisible && (
        <motion.div
          className="absolute top-4 right-5 flex items-center gap-3 z-10 pointer-events-none"
          initial={{ opacity: 0, x: 10 }}
          animate={{ opacity: 1, x: 0 }}
          exit={{ opacity: 0, scale: 0.95 }}
          transition={{ duration: 0.4, ease: "easeOut" }}
        >
          <div className="relative w-[52px] h-[52px]">
            {/* Background Glow */}
            <AnimatePresence>
              {isComplete && (
                <motion.div 
                  className="absolute inset-0 rounded-full bg-status-success !bg-opacity-10 blur-md pointer-events-none"
                  initial={{ opacity: 0, scale: 0.8 }}
                  animate={{ opacity: 1, scale: 1.2 }}
                  transition={{ duration: 1.5, repeat: Infinity, repeatType: "reverse" }}
                />
              )}
            </AnimatePresence>

            {/* SVG Ring */}
            <div className="relative w-full h-full -rotate-90">
              <svg width="52" height="52" viewBox="0 0 52 52">
                <circle
                  cx="26" cy="26" r={radius}
                  className="fill-none stroke-border-default/30 stroke-[2.5px]"
                />
                <motion.circle
                  cx="26" cy="26" r={radius}
                  className={cn(
                    "fill-none stroke-accent stroke-[2.5px] stroke-linecap-round",
                    isComplete && "!stroke-status-success"
                  )}
                  strokeDasharray={circumference}
                  animate={{ strokeDashoffset }}
                  transition={{ type: "spring", stiffness: 40, damping: 15 }}
                />
              </svg>
            </div>

            {/* Inner Icon Overlay */}
            <div className="absolute inset-0 flex items-center justify-center text-text-tertiary">
              <motion.div
                key={phase}
                initial={{ opacity: 0, scale: 0.5 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.5 }}
                transition={{ duration: 0.3 }}
                className={cn(isComplete && "text-status-success")}
              >
                {PHASE_ICONS[phase] || <Loader2 size={18} className="animate-spin" />}
              </motion.div>
            </div>
          </div>

          <div className="flex flex-col">
            <span className={cn(
               "font-mono text-[11px] font-bold tracking-tight text-text-secondary transition-colors duration-500",
               isComplete && "!text-status-success"
            )}>
              {phase === 'analyzing' ? `${completed}/${total}` : Math.round(fillPct * 100) + '%'}
            </span>
            <span className="text-[9px] uppercase tracking-widest text-text-muted font-bold -mt-0.5">
              {phase.replace('_', ' ')}
            </span>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
