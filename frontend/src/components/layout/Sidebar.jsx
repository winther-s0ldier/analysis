import React, { useEffect, useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';
import { getHistory, restoreSession } from '../../api';
import {
  PanelLeftClose, PanelLeft,
  Plus, Clock, BarChart2, ChevronRight, RotateCcw, AlertCircle, FileText,
} from 'lucide-react';

// ── Utilities ──────────────────────────────────────────────────────────────
function relativeTime(ts) {
  if (!ts) return '';
  const diff = (Date.now() / 1000) - ts;
  if (diff < 60) return 'just now';
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`;
  return new Date(ts * 1000).toLocaleDateString();
}

const RECENT_LIMIT = 12;

// ── Sidebar ────────────────────────────────────────────────────────────────
export function Sidebar() {
  const currentSessionId = usePipelineStore((s) => s.currentSessionId);
  const lastStartedPipelineSessionId = usePipelineStore((s) => s.lastStartedPipelineSessionId);
  const currentSession = usePipelineStore((s) => s.sessions[s.currentSessionId]);
  const phase = currentSession?.phase ?? 'idle';
  const collapsed = usePipelineStore((s) => s.sidebarCollapsed);
  const setSidebarCollapsed = usePipelineStore((s) => s.setSidebarCollapsed);
  const reset = usePipelineStore((s) => s.reset);
  const restoreSessionState = usePipelineStore((s) => s.restoreSessionState);

  const livePipelinePhase = usePipelineStore(
    (s) => s.sessions[s.lastStartedPipelineSessionId]?.phase
  );
  const isLiveRunning = livePipelinePhase && !['idle', 'complete', 'error'].includes(livePipelinePhase);
  const isViewingHistory = isLiveRunning && currentSessionId !== lastStartedPipelineSessionId;

  const [entries, setEntries] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [restoring, setRestoring] = useState(null);

  const historyVersion = usePipelineStore((s) => s.historyVersion);

  // Fetch history on mount and whenever a run completes or a chat message lands
  const fetchHistory = useCallback(() => {
    setLoading(true);
    setError('');
    getHistory()
      .then(data => setEntries(Array.isArray(data) ? data : []))
      .catch(() => setError('Could not load history.'))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { fetchHistory(); }, [fetchHistory]);

  // Re-fetch when pipeline completes — delay so backend _save_to_history finishes first
  useEffect(() => {
    if (phase === 'complete') {
      const t = setTimeout(() => fetchHistory(), 1500);
      return () => clearTimeout(t);
    }
  }, [phase, fetchHistory]);

  useEffect(() => {
    if (historyVersion > 0) fetchHistory();
  }, [historyVersion, fetchHistory]);

  const handleNewSession = useCallback(() => {
    reset();
    useChatStore.getState().clearMessages();
  }, [reset]);

  const handleReturnToLive = useCallback(() => {
    if (lastStartedPipelineSessionId) {
      usePipelineStore.getState().switchSession(lastStartedPipelineSessionId);
    }
  }, [lastStartedPipelineSessionId]);

  const handleRestore = useCallback(async (sessionId) => {
    if (sessionId === currentSessionId) return;
    setRestoring(sessionId);
    setError('');
    try {
      const data = await restoreSession(sessionId);
      restoreSessionState(data.session_id, {
        output_folder: data.output_folder,
        phase: data.phase || 'complete',
        nodes: data.nodes || [],
        has_report: data.has_report || false,
        canvas_narrative: data.canvas_narrative || null,
      });
      useChatStore.getState().restoreMessages(data.messages || [], data.session_id);
    } catch {
      setError('Failed to restore session.');
    } finally {
      setRestoring(null);
    }
  }, [restoreSessionState, currentSessionId]);

  const setCollapsed = (v) => setSidebarCollapsed(v);
  const isMobile = typeof window !== 'undefined' && window.innerWidth <= 768;

  return (
    <>
      <AnimatePresence initial={false}>
        {!collapsed && (
          <motion.aside
            key="sidebar"
            className="flex flex-col shrink-0 z-10 bg-bg-sidebar"
            style={{
              borderRight: '1px solid #E2E8F0',
              overflow: isMobile ? 'auto' : 'hidden',
              ...(isMobile ? {
                position: 'fixed',
                top: 0,
                left: 0,
                height: '100%',
                zIndex: 9999,
              } : {}),
            }}
            initial={{ width: 0, opacity: 0 }}
            animate={{ width: 240, opacity: 1 }}
            exit={{ width: 0, opacity: 0 }}
            transition={{ duration: 0.3, ease: [0.165, 0.84, 0.44, 1] }}
          >
            {/* Logo row */}
            <div className="px-4 pt-5 pb-3 flex items-center justify-between shrink-0">
              <div className="flex items-center gap-2.5 min-w-0">
                <img
                  src="/adhopsun.jpeg"
                  alt="ADOPSHUN"
                  className="w-7 h-7 rounded-lg shrink-0 object-cover"
                />
                <span className="text-[13px] font-bold tracking-tight text-text-primary truncate">
                  ADOPSHUN
                </span>
              </div>

              <button
                onClick={() => setCollapsed(true)}
                className="w-7 h-7 flex items-center justify-center rounded-md text-text-muted hover:text-text-primary hover:bg-bg-elevated transition-colors duration-150"
                title="Collapse sidebar"
              >
                <PanelLeftClose size={15} />
              </button>
            </div>

            {/* New Analysis CTA */}
            <div className="px-3 pb-3 shrink-0">
              <button
                onClick={handleNewSession}
                className="w-full flex items-center justify-center gap-2 py-2.5 rounded-xl text-[13px] font-semibold text-white tracking-tight transition-all duration-200 bg-gradient-brand hover:bg-gradient-brand-hover shadow-brand"
                style={{ border: 'none', cursor: 'pointer' }}
              >
                <Plus size={15} strokeWidth={2.5} />
                New Analysis
              </button>
            </div>

            {/* Return to live session banner */}
            {isViewingHistory && (
              <div className="px-3 pb-2 animate-in fade-in slide-in-from-top-2 duration-300 shrink-0">
                <button
                  onClick={handleReturnToLive}
                  className="w-full flex items-center justify-center gap-2 py-2 rounded-lg text-[11.5px] font-bold text-accent-navy bg-accent/10 border border-accent/30 transition-all duration-200 hover:bg-accent/20"
                  style={{ cursor: 'pointer' }}
                >
                  <span style={{
                    width: 6, height: 6, borderRadius: '50%',
                    background: '#10B981', flexShrink: 0,
                  }} />
                  RETURN TO LIVE SESSION
                </button>
              </div>
            )}

            {/* Recent header */}
            <div className="px-4 pt-2 pb-1.5 flex items-center justify-between shrink-0">
              <span className="text-[10px] font-bold uppercase tracking-[0.08em] text-text-muted">
                Recent
              </span>
              <button
                onClick={fetchHistory}
                title="Refresh"
                className="text-text-muted hover:text-text-primary transition-colors"
                style={{ background: 'transparent', border: 'none', cursor: 'pointer', padding: 2 }}
              >
                <Clock size={11} />
              </button>
            </div>

            {/* Inline session list */}
            <div className="flex-1 overflow-y-auto px-2 pb-2 min-h-0">
              {loading && (
                <div className="px-3 py-4 text-center text-[11.5px] text-text-faint">
                  Loading…
                </div>
              )}

              {error && (
                <div className="mx-2 my-2 p-2 rounded-lg bg-status-error/10 border border-status-error/25 flex items-start gap-2">
                  <AlertCircle size={12} className="text-status-error shrink-0 mt-0.5" />
                  <span className="text-[11.5px] text-status-error">{error}</span>
                </div>
              )}

              {!loading && !error && entries.length === 0 && (
                <div className="px-3 py-6 text-center">
                  <FileText size={22} className="text-text-faint mx-auto mb-2" />
                  <div className="text-[11.5px] text-text-faint leading-snug">
                    No past analyses yet.<br />Complete a run to see it here.
                  </div>
                </div>
              )}

              {!loading && entries.slice(0, RECENT_LIMIT).map((entry) => {
                const isRestoring = restoring === entry.session_id;
                const isCurrent = entry.session_id === currentSessionId;
                return (
                  <button
                    key={entry.session_id}
                    onClick={() => handleRestore(entry.session_id)}
                    disabled={isRestoring}
                    className={`w-full text-left px-3 py-2 rounded-lg mb-0.5 transition-colors duration-150 group ${
                      isCurrent
                        ? 'bg-bg-elevated'
                        : 'hover:bg-bg-surface'
                    }`}
                    style={{
                      background: isCurrent ? undefined : undefined,
                      border: 'none',
                      cursor: isRestoring ? 'wait' : 'pointer',
                      opacity: isRestoring ? 0.65 : 1,
                    }}
                  >
                    <div className="flex items-center gap-2">
                      <span className="flex-1 text-[12.5px] font-semibold text-text-primary truncate tracking-tight">
                        {entry.title || entry.csv_filename || 'Untitled session'}
                      </span>
                      {isRestoring
                        ? <RotateCcw size={11} className="text-accent shrink-0" style={{ animation: 'spin 1s linear infinite' }} />
                        : <ChevronRight size={11} className="text-text-faint shrink-0 opacity-0 group-hover:opacity-100 transition-opacity" />
                      }
                    </div>
                    <div className="flex items-center gap-1.5 mt-0.5 text-[10.5px] text-text-muted">
                      <span>{relativeTime(entry.completed_at)}</span>
                      {entry.node_count ? <span>·</span> : null}
                      {entry.node_count ? <span>{entry.node_count} analyses</span> : null}
                    </div>
                  </button>
                );
              })}
            </div>

            {/* Bottom: User Activity */}
            <div
              className="px-3 pt-2 shrink-0 border-t border-border-subtle"
              style={{
                paddingBottom: isMobile
                  ? 'max(16px, env(safe-area-inset-bottom, 16px))'
                  : '14px',
                paddingTop: 12,
              }}
            >
              <button
                onClick={() => window.location.href = '/user-activity/'}
                className="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg bg-bg-surface hover:bg-bg-elevated border border-border-subtle transition-colors duration-150"
                style={{ cursor: 'pointer' }}
                title="Open User Activity Dashboard"
              >
                <span style={{
                  width: 7, height: 7, borderRadius: '50%',
                  background: '#10B981', flexShrink: 0,
                }} />
                <BarChart2 size={13} className="text-text-tertiary" />
                <span className="text-[12px] font-semibold text-text-primary tracking-tight">
                  User Activity
                </span>
              </button>
            </div>

            <style>{`
              @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
            `}</style>
          </motion.aside>
        )}
      </AnimatePresence>

      {/* Collapsed rail */}
      <AnimatePresence>
        {collapsed && !isMobile && (
          <motion.div
            key="collapsed-rail"
            className="flex flex-col items-center pt-5 shrink-0 z-10 bg-bg-sidebar"
            style={{
              width: 52,
              borderRight: '1px solid #E2E8F0',
            }}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
          >
            <button
              onClick={() => setCollapsed(false)}
              className="w-8 h-8 flex items-center justify-center rounded-lg text-text-muted hover:text-text-primary hover:bg-bg-elevated transition-colors duration-150"
              title="Expand sidebar"
            >
              <PanelLeft size={16} />
            </button>

            <div style={{ height: 1, background: '#E2E8F0', width: '70%', margin: '12px 0 8px' }} />

            <button
              onClick={handleNewSession}
              className="w-9 h-9 flex items-center justify-center rounded-lg mb-2 text-white bg-gradient-brand shadow-brand transition-transform duration-150 hover:-translate-y-0.5"
              style={{ border: 'none', cursor: 'pointer' }}
              title="New analysis"
            >
              <Plus size={15} strokeWidth={2.5} />
            </button>

            {isViewingHistory && (
              <button
                onClick={handleReturnToLive}
                className="w-8 h-8 flex items-center justify-center rounded-lg mb-1 transition-colors duration-200"
                style={{
                  background: 'rgba(251,113,133,0.12)',
                  border: '1px solid rgba(251,113,133,0.32)',
                  cursor: 'pointer',
                }}
                title="Return to live session"
              >
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#10B981' }} />
              </button>
            )}

            <div className="flex-1" />

            <button
              onClick={() => window.location.href = '/user-activity/'}
              className="w-8 h-8 flex items-center justify-center rounded-lg mb-4 text-text-tertiary hover:text-text-primary hover:bg-bg-elevated transition-colors duration-150 relative"
              style={{ background: 'transparent', border: 'none', cursor: 'pointer' }}
              title="User Activity Dashboard"
            >
              <BarChart2 size={15} />
              <span style={{
                position: 'absolute', top: 6, right: 6,
                width: 5, height: 5, borderRadius: '50%',
                background: '#10B981',
              }} />
            </button>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}
