import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { usePipelineStore } from '../../store/pipelineStore';
import {
  PanelLeftClose, PanelLeft,
  Database, Search, Activity, Cpu, FileText, Upload,
} from 'lucide-react';
import { cn } from '../ui/Badge';

const STAGES = [
  { id: 1, key: 'uploading',      label: 'Upload',     Icon: Upload   },
  { id: 2, key: 'profiling',      label: 'Profile',    Icon: Database },
  { id: 3, key: 'discovering',    label: 'Discover',   Icon: Search   },
  { id: 4, key: 'analyzing',      label: 'Analyze',    Icon: Activity },
  { id: 5, key: 'synthesizing',   label: 'Synthesize', Icon: Cpu      },
  { id: 6, key: 'building_report',label: 'Report',     Icon: FileText },
];

const PHASE_ORDER = [
  'idle', 'uploading', 'profiling', 'discovering',
  'analyzing', 'synthesizing', 'building_report', 'complete', 'error',
];

function getStageStatus(stageId, currentPhase) {
  const currentIndex = PHASE_ORDER.indexOf(currentPhase);
  if (currentIndex === -1 || currentPhase === 'idle') return 'pending';
  if (currentPhase === 'error') return 'error';
  if (currentIndex > stageId) return 'complete';
  if (currentIndex === stageId) return 'active';
  return 'pending';
}

export function Sidebar() {
  const { phase } = usePipelineStore();
  const [collapsed, setCollapsed] = useState(false);

  return (
    <>
      <AnimatePresence initial={false}>
        {!collapsed && (
          <motion.aside
            key="sidebar"
            className="flex flex-col shrink-0 z-10 overflow-hidden"
            style={{ backgroundColor: '#3D2B1A', borderRight: '1px solid rgba(255,255,255,0.07)' }}
            initial={{ width: 0, opacity: 0 }}
            animate={{ width: 220, opacity: 1 }}
            exit={{ width: 0, opacity: 0 }}
            transition={{ duration: 0.35, ease: [0.165, 0.84, 0.44, 1] }}
          >
            {/* Logo row */}
            <div className="px-4 pt-5 pb-4 flex items-center justify-between shrink-0">
              <div className="flex items-center gap-2.5">
                <img
                  src="/adhopsun.jpeg"
                  alt="ADHOPSUN"
                  className="w-7 h-7 rounded-lg shrink-0 object-cover"
                />
                <span className="text-[13px] font-bold tracking-tight" style={{ color: '#F0EBE3' }}>
                  ADHOPSUN
                </span>
              </div>

              <button
                onClick={() => setCollapsed(true)}
                className="w-7 h-7 flex items-center justify-center rounded-md transition-colors duration-200"
                style={{ color: '#6B5F58' }}
                onMouseEnter={e => e.currentTarget.style.color = '#A89890'}
                onMouseLeave={e => e.currentTarget.style.color = '#6B5F58'}
                title="Collapse sidebar"
              >
                <PanelLeftClose size={15} />
              </button>
            </div>

            {/* Divider */}
            <div style={{ height: 1, background: 'rgba(255,255,255,0.05)', marginBottom: 8 }} />

            {/* Stage list */}
            <nav className="flex-1 px-2 pb-4 overflow-y-auto space-y-0.5">
              {STAGES.map((s) => {
                const status = getStageStatus(s.id, phase);
                const isActive   = status === 'active';
                const isComplete = status === 'complete';
                const isError    = status === 'error';
                const isPending  = status === 'pending';

                return (
                  <div
                    key={s.id}
                    className={cn(
                      'relative flex items-center gap-2.5 px-3 py-2 rounded-lg cursor-default transition-all duration-300',
                    )}
                    style={{
                      backgroundColor: isActive ? '#4E3522' : 'transparent',
                    }}
                    onMouseEnter={e => {
                      if (!isActive) e.currentTarget.style.backgroundColor = 'rgba(255,255,255,0.05)';
                    }}
                    onMouseLeave={e => {
                      if (!isActive) e.currentTarget.style.backgroundColor = 'transparent';
                    }}
                  >
                    {/* Active left-border indicator */}
                    {isActive && (
                      <span
                        className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 rounded-full"
                        style={{ height: '60%', background: '#6366F1' }}
                      />
                    )}

                    {/* Icon */}
                    <div
                      className="w-[18px] h-[18px] flex items-center justify-center shrink-0"
                      style={{
                        color: isActive   ? '#6366F1'
                             : isComplete ? '#10B981'
                             : isError    ? '#EF4444'
                             : 'rgba(255,255,255,0.22)',
                      }}
                    >
                      <s.Icon size={15} strokeWidth={isActive ? 2.5 : 2} />
                    </div>

                    {/* Label */}
                    <span
                      className="text-[12.5px] font-medium leading-none tracking-tight transition-all duration-300"
                      style={{
                        color: isActive   ? '#F9FAFB'
                             : isComplete ? 'rgba(249,250,251,0.65)'
                             : isError    ? '#EF4444'
                             : 'rgba(249,250,251,0.28)',
                      }}
                    >
                      {s.label}
                    </span>

                    {/* Active pulse dot */}
                    {isActive && (
                      <motion.span
                        className="ml-auto w-1.5 h-1.5 rounded-full shrink-0"
                        style={{ background: '#6366F1' }}
                        animate={{ opacity: [1, 0.3, 1] }}
                        transition={{ duration: 1.6, repeat: Infinity, ease: 'easeInOut' }}
                      />
                    )}

                    {/* Complete check */}
                    {isComplete && (
                      <span className="ml-auto w-1.5 h-1.5 rounded-full shrink-0" style={{ background: '#10B981' }} />
                    )}

                    {/* Error dot */}
                    {isError && (
                      <span className="ml-auto w-1.5 h-1.5 rounded-full shrink-0" style={{ background: '#EF4444' }} />
                    )}
                  </div>
                );
              })}
            </nav>
          </motion.aside>
        )}
      </AnimatePresence>

      {/* Collapsed expand button */}
      <AnimatePresence>
        {collapsed && (
          <motion.div
            key="collapsed-rail"
            className="flex flex-col items-center pt-5 shrink-0 z-10"
            style={{
              width: 52,
              backgroundColor: '#3D2B1A',
              borderRight: '1px solid rgba(255,255,255,0.07)',
            }}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
          >
            <button
              onClick={() => setCollapsed(false)}
              className="w-8 h-8 flex items-center justify-center rounded-lg transition-colors duration-200"
              style={{ color: '#6B5F58' }}
              onMouseEnter={e => e.currentTarget.style.color = '#A89890'}
              onMouseLeave={e => e.currentTarget.style.color = '#6B5F58'}
              title="Expand sidebar"
            >
              <PanelLeft size={16} />
            </button>

            <div style={{ height: 1, background: 'rgba(255,255,255,0.05)', width: '100%', marginTop: 12, marginBottom: 8 }} />

            <div className="flex flex-col gap-1 px-1.5 w-full">
              {STAGES.map((s) => {
                const status = getStageStatus(s.id, phase);
                const isActive   = status === 'active';
                const isComplete = status === 'complete';
                const isError    = status === 'error';

                return (
                  <div
                    key={s.id}
                    className="flex items-center justify-center w-full h-8 rounded-md transition-colors duration-200"
                    style={{
                      backgroundColor: isActive ? '#161922' : 'transparent',
                      color: isActive   ? '#6366F1'
                           : isComplete ? '#10B981'
                           : isError    ? '#EF4444'
                           : 'rgba(255,255,255,0.22)',
                    }}
                    title={s.label}
                  >
                    <s.Icon size={15} strokeWidth={isActive ? 2.5 : 2} />
                  </div>
                );
              })}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}
