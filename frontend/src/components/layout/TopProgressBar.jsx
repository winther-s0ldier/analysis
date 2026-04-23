import React, { useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { usePipelineStore } from '../../store/pipelineStore';
import {
  Database, Search, Activity, Cpu, Check,
} from 'lucide-react';

// Phases mapped to their segment index. Upload is folded in implicitly:
// the bar appears the moment we leave 'idle'.
const SEGMENTS = [
  { key: 'profiling',    label: 'Profile',    Icon: Database },
  { key: 'discovering',  label: 'Discover',   Icon: Search },
  { key: 'analyzing',    label: 'Analyze',    Icon: Activity },
  { key: 'synthesizing', label: 'Synthesize', Icon: Cpu },
];

const PHASE_ORDER = [
  'idle', 'uploading', 'profiling', 'discovering',
  'analyzing', 'synthesizing', 'building_report', 'complete', 'error',
];

function getSegmentStatus(segmentKey, currentPhase) {
  const segIdx = PHASE_ORDER.indexOf(segmentKey);
  const curIdx = PHASE_ORDER.indexOf(currentPhase);
  if (currentPhase === 'idle' || curIdx === -1) return 'pending';
  if (currentPhase === 'complete') return 'complete';
  if (currentPhase === 'error') return curIdx > segIdx ? 'complete' : 'pending';
  if (curIdx > segIdx) return 'complete';
  if (curIdx === segIdx) return 'active';
  // 'uploading' counts as before-Profile
  return 'pending';
}

export function TopProgressBar() {
  const currentSession = usePipelineStore((s) => s.sessions[s.currentSessionId]);
  const phase = currentSession?.phase ?? 'idle';
  const nodes = currentSession?.nodes ?? [];
  const isMobile = typeof window !== 'undefined' && window.innerWidth <= 768;

  // Show the bar for the duration of an active run (and briefly while complete).
  const show = phase !== 'idle' && phase !== 'complete' && phase !== 'error';

  // Sub-progress within the Analyze segment, driven by per-node fraction
  const analyzeFraction = useMemo(() => {
    if (phase !== 'analyzing' || nodes.length === 0) return 0;
    const done = nodes.filter(n => n.status === 'complete' || n.status === 'failed').length;
    return done / nodes.length;
  }, [phase, nodes]);

  return (
    <AnimatePresence>
      {show && (
        <motion.div
          key="top-progress"
          className="fixed top-0 left-0 right-0 z-[100] pointer-events-none"
          initial={{ y: -36, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: -36, opacity: 0 }}
          transition={{ duration: 0.25, ease: [0.165, 0.84, 0.44, 1] }}
        >
          <div
            className="mx-auto flex items-center"
            style={{
              maxWidth: 720,
              padding: isMobile ? '6px 12px' : '8px 16px',
              gap: isMobile ? 4 : 8,
            }}
          >
            {SEGMENTS.map((seg, i) => {
              const status = getSegmentStatus(seg.key, phase);
              const isActive = status === 'active';
              const isComplete = status === 'complete';
              const segPct = isComplete
                ? 1
                : (isActive
                  ? (seg.key === 'analyzing' ? Math.max(0.1, analyzeFraction) : 0.5)
                  : 0);

              const dotColor = isComplete
                ? '#10B981'
                : isActive
                  ? '#FB7185'
                  : '#CBD5E1';

              return (
                <React.Fragment key={seg.key}>
                  {/* Step dot + label */}
                  <div className="flex items-center gap-1.5 shrink-0">
                    <motion.div
                      className="rounded-full flex items-center justify-center"
                      style={{
                        width: 18,
                        height: 18,
                        background: isComplete
                          ? '#10B981'
                          : isActive
                            ? 'linear-gradient(135deg, #FB7185 0%, #FB923C 100%)'
                            : '#F1F5F9',
                        border: isActive ? '2px solid rgba(251,113,133,0.25)' : '1px solid #E2E8F0',
                        color: isComplete || isActive ? '#FFFFFF' : '#94A3B8',
                        transition: 'background 0.3s, border-color 0.3s',
                      }}
                    >
                      {isComplete ? <Check size={10} strokeWidth={3} /> : <seg.Icon size={9} strokeWidth={2.5} />}
                    </motion.div>
                    {!isMobile && (
                      <span
                        className="text-[10.5px] font-semibold tracking-tight"
                        style={{ color: isComplete || isActive ? '#0F172A' : '#94A3B8' }}
                      >
                        {seg.label}
                      </span>
                    )}
                  </div>

                  {/* Connector — fills based on segPct (or completion of next stage) */}
                  {i < SEGMENTS.length - 1 && (
                    <div
                      className="flex-1 rounded-full overflow-hidden relative"
                      style={{
                        height: 2,
                        background: '#E2E8F0',
                        minWidth: 12,
                      }}
                    >
                      <motion.div
                        className="absolute inset-y-0 left-0 rounded-full"
                        style={{
                          background: isComplete
                            ? '#10B981'
                            : 'linear-gradient(90deg, #FB7185 0%, #FB923C 100%)',
                        }}
                        initial={{ width: '0%' }}
                        animate={{ width: `${segPct * 100}%` }}
                        transition={{ duration: 0.4, ease: 'easeOut' }}
                      />
                    </div>
                  )}
                </React.Fragment>
              );
            })}
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
