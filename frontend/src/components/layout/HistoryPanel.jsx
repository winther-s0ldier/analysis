import React, { useEffect, useState, useCallback, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Clock, X, RotateCcw, FileText, ChevronRight, AlertCircle, ArrowLeft } from 'lucide-react';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';
import { getHistory, restoreSession } from '../../api';

function relativeTime(ts) {
  if (!ts) return '';
  const diff = (Date.now() / 1000) - ts;
  if (diff < 60) return 'just now';
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`;
  return new Date(ts * 1000).toLocaleDateString();
}

function DatasetBadge({ type }) {
  if (!type) return null;
  const label = type.replace(/_/g, ' ');
  return (
    <span style={{
      fontSize: 10,
      fontWeight: 600,
      letterSpacing: '0.04em',
      textTransform: 'uppercase',
      padding: '2px 6px',
      borderRadius: 4,
      background: 'rgba(99,102,241,0.18)',
      color: '#A5B4FC',
      border: '1px solid rgba(99,102,241,0.3)',
      flexShrink: 0,
    }}>
      {label}
    </span>
  );
}

export function HistoryPanel() {
  const historyOpen = usePipelineStore((s) => s.historyOpen);
  const setHistoryOpen = usePipelineStore((s) => s.setHistoryOpen);
  const lastStartedPipelineSessionId = usePipelineStore((s) => s.lastStartedPipelineSessionId);
  const restoreSessionState = usePipelineStore((s) => s.restoreSessionState);
  const reset = usePipelineStore((s) => s.reset);

  const [entries, setEntries] = useState([]);
  const [loading, setLoading] = useState(false);
  const [restoring, setRestoring] = useState(null);
  const [error, setError] = useState('');

  // Fetch history entries when panel opens.
  useEffect(() => {
    if (!historyOpen) return;
    setLoading(true);
    setError('');
    getHistory()
      .then(data => setEntries(Array.isArray(data) ? data : []))
      .catch(() => setError('Could not load history.'))
      .finally(() => setLoading(false));
  }, [historyOpen]);

  // Back to the live pipeline session — just switch currentSessionId.
  // No snapshots needed: each session's state lives in its own Map slot.
  const handleBack = useCallback(() => {
    if (lastStartedPipelineSessionId) {
      usePipelineStore.getState().switchSession(lastStartedPipelineSessionId);
    }
    setHistoryOpen(false);
  }, [lastStartedPipelineSessionId, setHistoryOpen]);

  const handleNewAnalysis = useCallback(() => {
    reset();
    useChatStore.getState().clearMessages();
    setHistoryOpen(false);
  }, [reset, setHistoryOpen]);

  const handleRestore = useCallback(async (sessionId) => {
    setRestoring(sessionId);
    setError('');
    try {
      const data = await restoreSession(sessionId);
      // Populate the session slot and switch to it
      restoreSessionState(data.session_id, {
        output_folder: data.output_folder,
        phase: data.phase || 'complete',
        nodes: data.nodes || [],
        has_report: data.has_report || false,
        canvas_narrative: data.canvas_narrative || null,
      });
      // Restore messages into the chat store for this session
      useChatStore.getState().restoreMessages(data.messages || [], data.session_id);
      setHistoryOpen(false);
    } catch (e) {
      setError('Failed to restore session.');
    } finally {
      setRestoring(null);
    }
  }, [restoreSessionState, setHistoryOpen]);

  return (
    <AnimatePresence>
      {historyOpen && (
        <>
          {/* Backdrop (mobile) */}
          <motion.div
            key="history-backdrop"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            onClick={() => setHistoryOpen(false)}
            style={{
              position: 'fixed', inset: 0, zIndex: 9998,
              background: 'rgba(0,0,0,0.35)',
              display: typeof window !== 'undefined' && window.innerWidth <= 768 ? 'block' : 'none',
            }}
          />

          {/* Panel */}
          <motion.div
            key="history-panel"
            initial={{ x: -20, opacity: 0 }}
            animate={{ x: 0, opacity: 1 }}
            exit={{ x: -20, opacity: 0 }}
            transition={{ duration: 0.25, ease: [0.165, 0.84, 0.44, 1] }}
            style={{
              position: typeof window !== 'undefined' && window.innerWidth <= 768 ? 'fixed' : 'relative',
              top: 0, left: typeof window !== 'undefined' && window.innerWidth <= 768 ? 220 : 0,
              height: typeof window !== 'undefined' && window.innerWidth <= 768 ? '100%' : '100vh',
              width: 280,
              backgroundColor: '#2E1F10',
              borderRight: '1px solid rgba(255,255,255,0.07)',
              display: 'flex',
              flexDirection: 'column',
              zIndex: 9999,
              flexShrink: 0,
            }}
          >
            {/* Header */}
            <div style={{
              padding: '14px 16px 12px',
              borderBottom: '1px solid rgba(255,255,255,0.06)',
              display: 'flex', flexDirection: 'column', gap: 10,
            }}>
              {/* Back button row */}
              <button
                onClick={handleBack}
                style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 7,
                  width: '100%',
                  background: 'linear-gradient(135deg, #6366F1 0%, #4F46E5 100%)',
                  border: 'none',
                  borderRadius: 10, padding: '10px 14px', cursor: 'pointer',
                  color: '#fff', fontSize: 13, fontWeight: 700,
                  boxShadow: '0 2px 8px rgba(99,102,241,0.4)',
                  letterSpacing: '-0.01em',
                  transition: 'opacity 0.15s, transform 0.1s',
                }}
                onMouseEnter={e => { e.currentTarget.style.opacity = '0.88'; e.currentTarget.style.transform = 'translateY(-1px)'; }}
                onMouseLeave={e => { e.currentTarget.style.opacity = '1'; e.currentTarget.style.transform = 'translateY(0)'; }}
              >
                <ArrowLeft size={14} />
                Back to Session
              </button>

              {/* Title + New button row */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Clock size={14} style={{ color: '#6366F1', flexShrink: 0 }} />
                <span style={{ color: '#F0EBE3', fontSize: 13, fontWeight: 700, letterSpacing: '-0.01em', flex: 1 }}>
                  Analysis History
                </span>
                <button
                  onClick={handleNewAnalysis}
                  title="Clear current session and start fresh"
                  style={{
                    fontSize: 11, fontWeight: 600, padding: '3px 10px', borderRadius: 6,
                    background: 'rgba(99,102,241,0.18)', border: '1px solid rgba(99,102,241,0.35)',
                    color: '#A5B4FC', cursor: 'pointer', whiteSpace: 'nowrap',
                  }}
                  onMouseEnter={e => { e.currentTarget.style.background = 'rgba(99,102,241,0.3)'; }}
                  onMouseLeave={e => { e.currentTarget.style.background = 'rgba(99,102,241,0.18)'; }}
                >
                  + New
                </button>
              </div>
            </div>

            {/* Body */}
            <div style={{ flex: 1, overflowY: 'auto', padding: '8px 0' }}>
              {loading && (
                <div style={{ padding: '24px 16px', textAlign: 'center', color: 'rgba(255,255,255,0.35)', fontSize: 12 }}>
                  Loading…
                </div>
              )}

              {error && (
                <div style={{ margin: '12px 12px 0', padding: '10px 12px', background: 'rgba(239,68,68,0.12)', border: '1px solid rgba(239,68,68,0.25)', borderRadius: 8, display: 'flex', gap: 8, alignItems: 'flex-start' }}>
                  <AlertCircle size={13} style={{ color: '#EF4444', flexShrink: 0, marginTop: 1 }} />
                  <span style={{ color: '#FCA5A5', fontSize: 12 }}>{error}</span>
                </div>
              )}

              {!loading && !error && entries.length === 0 && (
                <div style={{ padding: '32px 16px', textAlign: 'center' }}>
                  <FileText size={28} style={{ color: 'rgba(255,255,255,0.15)', margin: '0 auto 10px' }} />
                  <div style={{ color: 'rgba(255,255,255,0.3)', fontSize: 12, lineHeight: 1.5 }}>
                    No past analyses yet.<br />Complete a pipeline run to see it here.
                  </div>
                </div>
              )}

              {!loading && entries.map((entry) => {
                const isRestoring = restoring === entry.session_id;
                return (
                  <button
                    key={entry.session_id}
                    onClick={() => handleRestore(entry.session_id)}
                    disabled={isRestoring}
                    style={{
                      width: '100%', textAlign: 'left', background: 'transparent',
                      border: 'none', cursor: isRestoring ? 'wait' : 'pointer',
                      padding: '10px 14px', display: 'flex', flexDirection: 'column',
                      gap: 5, borderBottom: '1px solid rgba(255,255,255,0.04)',
                      opacity: isRestoring ? 0.6 : 1,
                      transition: 'background 0.15s',
                    }}
                    onMouseEnter={e => { if (!isRestoring) e.currentTarget.style.background = 'rgba(255,255,255,0.04)'; }}
                    onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}
                  >
                    {/* Row 1: title + time */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, width: '100%' }}>
                      <span style={{
                        color: '#F0EBE3', fontSize: 12.5, fontWeight: 600,
                        flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                        letterSpacing: '-0.01em',
                      }}>
                        {entry.title || entry.csv_filename || 'Unknown file'}
                      </span>
                      {isRestoring
                        ? <RotateCcw size={12} style={{ color: '#6366F1', flexShrink: 0, animation: 'spin 1s linear infinite' }} />
                        : <ChevronRight size={12} style={{ color: 'rgba(255,255,255,0.2)', flexShrink: 0 }} />
                      }
                    </div>

                    {/* Row 2: badge + stats + conversation turns */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                      <DatasetBadge type={entry.dataset_type} />
                      <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 11 }}>
                        {entry.row_count ? `${entry.row_count.toLocaleString()} rows` : ''}
                        {entry.node_count ? ` · ${entry.node_count} analyses` : ''}
                      </span>
                      {entry.conversation_turns > 0 && (
                        <span style={{
                          fontSize: 10, fontWeight: 600, padding: '1px 5px', borderRadius: 4,
                          background: 'rgba(16,185,129,0.15)', color: '#6EE7B7',
                          border: '1px solid rgba(16,185,129,0.25)', flexShrink: 0,
                        }}>
                          {entry.conversation_turns} Q&amp;A
                        </span>
                      )}
                    </div>

                    {/* Row 3: top priority */}
                    {entry.top_priority && (
                      <div style={{
                        color: 'rgba(255,255,255,0.45)', fontSize: 11,
                        overflow: 'hidden', textOverflow: 'ellipsis',
                        display: '-webkit-box', WebkitLineClamp: 2,
                        WebkitBoxOrient: 'vertical', lineHeight: 1.4,
                      }}>
                        {entry.top_priority}
                      </div>
                    )}

                    {/* Row 4: timestamp */}
                    <div style={{ color: 'rgba(255,255,255,0.22)', fontSize: 10.5 }}>
                      {relativeTime(entry.completed_at)}
                    </div>
                  </button>
                );
              })}
            </div>

            <style>{`
              @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
            `}</style>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}
