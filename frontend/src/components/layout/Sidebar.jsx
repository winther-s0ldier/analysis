import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';
import {
  PanelLeftClose, PanelLeft,
  Database, Search, Activity, Cpu, FileText, Upload, BarChart2, Clock, Plus,
} from 'lucide-react';
import { cn } from '../ui/Badge';

const STAGES = [
  { id: 1, key: 'uploading', label: 'Upload', Icon: Upload },
  { id: 2, key: 'profiling', label: 'Profile', Icon: Database },
  { id: 3, key: 'discovering', label: 'Discover', Icon: Search },
  { id: 4, key: 'analyzing', label: 'Analyze', Icon: Activity },
  { id: 5, key: 'synthesizing', label: 'Synthesize', Icon: Cpu },
  { id: 6, key: 'building_report', label: 'Report', Icon: FileText },
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
  const currentSessionId = usePipelineStore((s) => s.currentSessionId);
  const lastStartedPipelineSessionId = usePipelineStore((s) => s.lastStartedPipelineSessionId);
  const currentSession = usePipelineStore((s) => s.sessions[s.currentSessionId]);
  const phase = currentSession?.phase ?? 'idle';
  const hasReport = currentSession?.hasReport ?? false;
  const collapsed = usePipelineStore((s) => s.sidebarCollapsed);
  const setSidebarCollapsed = usePipelineStore((s) => s.setSidebarCollapsed);
  const historyOpen = usePipelineStore((s) => s.historyOpen);
  const setHistoryOpen = usePipelineStore((s) => s.setHistoryOpen);
  const reset = usePipelineStore((s) => s.reset);
  const setCanvasOpen = usePipelineStore((s) => s.setCanvasOpen);

  // Determine if user is viewing a different session while a pipeline runs
  const livePipelinePhase = usePipelineStore(
    (s) => s.sessions[s.lastStartedPipelineSessionId]?.phase
  );
  const isLiveRunning = livePipelinePhase && !['idle', 'complete', 'error'].includes(livePipelinePhase);
  const isViewingHistory = isLiveRunning && currentSessionId !== lastStartedPipelineSessionId;

  const handleNewSession = () => { reset(); useChatStore.getState().clearMessages(); };
  const handleReturnToLive = () => {
    usePipelineStore.getState().switchSession(lastStartedPipelineSessionId);
  };
  const setCollapsed = (v) => setSidebarCollapsed(v);
  const isMobile = typeof window !== 'undefined' && window.innerWidth <= 768;

  return (
    <>
      <AnimatePresence initial={false}>
        {!collapsed && (
          <motion.aside
            key="sidebar"
            className="flex flex-col shrink-0 z-10"
            style={{
              backgroundColor: '#3D2B1A',
              borderRight: '1px solid rgba(255,255,255,0.07)',
              overflow: isMobile ? 'auto' : 'hidden',
              // On mobile: fixed overlay drawer; on desktop: inline
              ...(isMobile ? {
                position: 'fixed',
                top: 0,
                left: 0,
                height: '100%',
                zIndex: 9999,
              } : {}),
            }}
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
                  alt="ADOPSHUN"
                  className="w-7 h-7 rounded-lg shrink-0 object-cover"
                />
                <span className="text-[13px] font-bold tracking-tight" style={{ color: '#F0EBE3' }}>
                  ADOPSHUN
                </span>
              </div>

              <div className="flex items-center gap-1">
                <button
                  onClick={handleNewSession}
                  className="w-7 h-7 flex items-center justify-center rounded-md transition-colors duration-200"
                  style={{ color: '#6B5F58' }}
                  onMouseEnter={e => { e.currentTarget.style.color = '#A5B4FC'; e.currentTarget.style.background = 'rgba(99,102,241,0.15)'; }}
                  onMouseLeave={e => { e.currentTarget.style.color = '#6B5F58'; e.currentTarget.style.background = 'transparent'; }}
                  title="New session"
                >
                  <Plus size={15} />
                </button>
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
            </div>

            {/* Back to Live Session banner — only shows when viewing history while a live analysis runs */}
            {isViewingHistory && (
              <div className="px-3 pb-2 animate-in fade-in slide-in-from-top-2 duration-300">
                <button
                  onClick={handleReturnToLive}
                  className="w-full flex items-center justify-center gap-2 py-2.5 rounded-xl text-[11.5px] font-bold transition-all duration-300"
                  style={{
                    background: 'linear-gradient(135deg, #6366F1 0%, #4F46E5 100%)',
                    boxShadow: '0 4px 12px rgba(99, 102, 241, 0.3)',
                    color: 'white',
                    border: 'none',
                    cursor: 'pointer',
                  }}
                  onMouseEnter={e => { e.currentTarget.style.transform = 'translateY(-1px)'; e.currentTarget.style.boxShadow = '0 6px 16px rgba(99, 102, 241, 0.4)'; }}
                  onMouseLeave={e => { e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = '0 4px 12px rgba(99, 102, 241, 0.3)'; }}
                >
                  <motion.div
                    animate={{ scale: [1, 1.2, 1] }}
                    transition={{ duration: 2, repeat: Infinity }}
                    style={{ width: 6, height: 6, borderRadius: '50%', background: '#10B981' }}
                  />
                  RETURN TO LIVE SESSION
                </button>
              </div>
            )}

            {/* Divider */}
            <div style={{ height: 1, background: 'rgba(255,255,255,0.05)', marginBottom: 8 }} />

            {/* Stage list */}
            <nav className="flex-1 px-2 overflow-y-auto space-y-0.5">
              {STAGES.map((s) => {
                const status = getStageStatus(s.id, phase);
                const isActive = status === 'active';
                const isComplete = status === 'complete';
                const isError = status === 'error';
                const isPending = status === 'pending';

                return (
                  <div
                    key={s.id}
                    onClick={() => {
                      if (s.key === 'building_report' && (isComplete || isActive) && hasReport) {
                        setCanvasOpen(true);
                      }
                    }}
                    className={cn(
                      'relative flex items-center gap-2.5 px-3 py-2 rounded-lg transition-all duration-300',
                      s.key === 'building_report' && hasReport ? 'cursor-pointer' : 'cursor-default'
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
                        color: isActive ? '#6366F1'
                          : isComplete ? '#10B981'
                            : isError ? '#EF4444'
                              : 'rgba(255,255,255,0.22)',
                      }}
                    >
                      <s.Icon size={15} strokeWidth={isActive ? 2.5 : 2} />
                    </div>

                    {/* Label */}
                    <span
                      className="text-[12.5px] font-medium leading-none tracking-tight transition-all duration-300"
                      style={{
                        color: isActive ? '#F9FAFB'
                          : isComplete ? 'rgba(249,250,251,0.65)'
                            : isError ? '#EF4444'
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


            {/* Divider */}
            <div style={{ height: 1, background: 'rgba(255,255,255,0.05)', margin: '8px 0' }} />

            {/* History button */}
            <div className="px-2 shrink-0">
              <button
                onClick={() => setHistoryOpen(!historyOpen)}
                className="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg transition-all duration-200"
                style={{
                  background: historyOpen ? 'rgba(99,102,241,0.18)' : 'transparent',
                  border: historyOpen ? '1px solid rgba(99,102,241,0.35)' : '1px solid transparent',
                  cursor: 'pointer',
                }}
                onMouseEnter={e => { if (!historyOpen) { e.currentTarget.style.background = 'rgba(255,255,255,0.05)'; e.currentTarget.style.borderColor = 'transparent'; } }}
                onMouseLeave={e => { if (!historyOpen) { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.borderColor = 'transparent'; } }}
                title="Analysis History"
              >
                <Clock size={15} style={{ color: historyOpen ? '#6366F1' : 'rgba(255,255,255,0.45)', flexShrink: 0 }} />
                <span className="text-[12.5px] font-medium leading-none tracking-tight" style={{ color: historyOpen ? '#F9FAFB' : 'rgba(249,250,251,0.45)' }}>
                  History
                </span>
                {historyOpen && (
                  <span className="ml-auto w-1.5 h-1.5 rounded-full shrink-0" style={{ background: '#6366F1' }} />
                )}
              </button>
            </div>

            {/* Divider */}
            <div style={{ height: 1, background: 'rgba(255,255,255,0.05)', margin: '8px 0 0' }} />

            {/* User Activity button — bottom of expanded sidebar */}
            <div
              className="px-2 shrink-0"
              style={{
                paddingBottom: isMobile
                  ? 'max(16px, env(safe-area-inset-bottom, 16px))'
                  : '16px',
              }}
            >
              <button
                onClick={() => window.location.href = '/user-activity/'}
                className="w-full flex items-center gap-2.5 px-3 rounded-xl transition-all duration-200"
                style={{
                  background: 'rgba(99,102,241,0.18)',
                  border: '1px solid rgba(99,102,241,0.35)',
                  cursor: 'pointer',
                  paddingTop: isMobile ? 12 : 10,
                  paddingBottom: isMobile ? 12 : 10,
                  minHeight: isMobile ? 48 : 'auto',
                }}
                onMouseEnter={e => { e.currentTarget.style.background = 'rgba(99,102,241,0.30)'; e.currentTarget.style.borderColor = 'rgba(99,102,241,0.6)'; }}
                onMouseLeave={e => { e.currentTarget.style.background = 'rgba(99,102,241,0.18)'; e.currentTarget.style.borderColor = 'rgba(99,102,241,0.35)'; }}
                title="Open User Activity Dashboard"
              >
                {/* Pulse dot */}
                <span style={{
                  width: 7, height: 7, borderRadius: '50%',
                  background: '#22C55E', flexShrink: 0,
                  animation: 'uaPulse 2s ease-in-out infinite',
                }} />
                <style>{`@keyframes uaPulse {
                  0%   { box-shadow: 0 0 0 0 rgba(34,197,94,0.5); }
                  70%  { box-shadow: 0 0 0 5px rgba(34,197,94,0); }
                  100% { box-shadow: 0 0 0 0 rgba(34,197,94,0); }
                }`}</style>
                <span className="text-[12.5px] font-semibold leading-none tracking-tight" style={{ color: '#C4C6FF' }}>
                  User Activity
                </span>
              </button>
            </div>
          </motion.aside>
        )}
      </AnimatePresence>

      {/* Collapsed expand button — hidden on mobile (hamburger in App.jsx handles it) */}
      <AnimatePresence>
        {collapsed && !isMobile && (
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

            <div className="flex flex-col gap-1 px-1.5 w-full flex-1">
              {STAGES.map((s) => {
                const status = getStageStatus(s.id, phase);
                const isActive = status === 'active';
                const isComplete = status === 'complete';
                const isError = status === 'error';

                return (
                  <div
                    key={s.id}
                    className="flex items-center justify-center w-full h-8 rounded-md transition-colors duration-200"
                    style={{
                      backgroundColor: isActive ? '#161922' : 'transparent',
                      color: isActive ? '#6366F1'
                        : isComplete ? '#10B981'
                          : isError ? '#EF4444'
                            : 'rgba(255,255,255,0.22)',
                    }}
                    title={s.label}
                  >
                    <s.Icon size={15} strokeWidth={isActive ? 2.5 : 2} />
                  </div>
                );
              })}
            </div>

            {/* New session + History icons — collapsed rail */}
            <div style={{ height: 1, background: 'rgba(255,255,255,0.05)', width: '100%', marginBottom: 4 }} />
            {isViewingHistory && (
              <button
                onClick={handleReturnToLive}
                className="w-8 h-8 flex items-center justify-center rounded-lg mb-1 transition-all duration-200 relative"
                style={{
                  background: 'linear-gradient(135deg, #6366F1 0%, #4F46E5 100%)',
                  border: 'none', cursor: 'pointer',
                  boxShadow: '0 2px 8px rgba(99,102,241,0.4)',
                }}
                onMouseEnter={e => { e.currentTarget.style.opacity = '0.85'; e.currentTarget.style.transform = 'translateY(-1px)'; }}
                onMouseLeave={e => { e.currentTarget.style.opacity = '1'; e.currentTarget.style.transform = 'translateY(0)'; }}
                title="Return to live session"
              >
                <motion.div
                  animate={{ scale: [1, 1.2, 1] }}
                  transition={{ duration: 2, repeat: Infinity }}
                  style={{ width: 6, height: 6, borderRadius: '50%', background: '#10B981' }}
                />
              </button>
            )}
            <button
              onClick={handleNewSession}
              className="w-8 h-8 flex items-center justify-center rounded-lg mb-1 transition-colors duration-200"
              style={{ color: 'rgba(255,255,255,0.35)', background: 'transparent', border: 'none', cursor: 'pointer' }}
              onMouseEnter={e => { e.currentTarget.style.color = '#A5B4FC'; e.currentTarget.style.background = 'rgba(99,102,241,0.15)'; }}
              onMouseLeave={e => { e.currentTarget.style.color = 'rgba(255,255,255,0.35)'; e.currentTarget.style.background = 'transparent'; }}
              title="New session"
            >
              <Plus size={15} />
            </button>
            <button
              onClick={() => setHistoryOpen(!historyOpen)}
              className="w-8 h-8 flex items-center justify-center rounded-lg mb-1 transition-colors duration-200 relative"
              style={{
                color: historyOpen ? '#6366F1' : 'rgba(255,255,255,0.35)',
                background: historyOpen ? 'rgba(99,102,241,0.18)' : 'transparent',
                border: 'none', cursor: 'pointer',
              }}
              onMouseEnter={e => { if (!historyOpen) { e.currentTarget.style.color = '#A5B4FC'; e.currentTarget.style.background = 'rgba(99,102,241,0.1)'; } }}
              onMouseLeave={e => { if (!historyOpen) { e.currentTarget.style.color = 'rgba(255,255,255,0.35)'; e.currentTarget.style.background = 'transparent'; } }}
              title="Analysis History"
            >
              <Clock size={15} />
            </button>

            {/* User Activity icon — bottom of collapsed rail */}
            <div style={{ height: 1, background: 'rgba(255,255,255,0.05)', width: '100%', marginBottom: 8 }} />
            <button
              onClick={() => window.location.href = '/user-activity/'}
              className="w-8 h-8 flex items-center justify-center rounded-lg mb-4 transition-colors duration-200 relative"
              style={{ color: 'rgba(255,255,255,0.35)', background: 'transparent', border: 'none', cursor: 'pointer' }}
              onMouseEnter={e => { e.currentTarget.style.color = '#22C55E'; e.currentTarget.style.background = 'rgba(34,197,94,0.1)'; }}
              onMouseLeave={e => { e.currentTarget.style.color = 'rgba(255,255,255,0.35)'; e.currentTarget.style.background = 'transparent'; }}
              title="User Activity Dashboard"
            >
              <BarChart2 size={15} />
              {/* Live pulse indicator */}
              <span style={{
                position: 'absolute', top: 6, right: 6,
                width: 5, height: 5, borderRadius: '50%',
                background: '#22C55E',
                animation: 'uaPulse 2s ease-in-out infinite',
              }} />
            </button>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}
