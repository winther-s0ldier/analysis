import React, { useEffect, useRef, useCallback, useState, Component } from 'react';
import { Sidebar } from './components/layout/Sidebar';
import { HistoryPanel } from './components/layout/HistoryPanel';
import { TopProgressBar } from './components/layout/TopProgressBar';
import { ProgressRing } from './components/layout/ProgressRing';
import { ProcessingStatus } from './components/layout/ProcessingStatus';
import { InputArea } from './components/layout/InputArea';
import { CanvasPanel, SelectionPopover } from './components/layout/CanvasPanel';
import { ChatMessage } from './components/messages/ChatMessage';
import { ThinkingBubble } from './components/messages/AIMessage';
import { useLenis } from './hooks/useLenis';
import { usePipelineStore } from './store/pipelineStore';
import { useChatStore } from './store/chatStore';
import { fetchSynthesis, sendChatMessage } from './api';
import { useSSEStream } from './hooks/useSSEStream';
import { Toaster } from 'sonner';
import { useAutoAnimate } from '@formkit/auto-animate/react';
import { motion } from 'framer-motion';
import { Menu, FileText, MessageSquare } from 'lucide-react';

// ── Resizable Split Handle ─────────────────────────────────────────────────
function ResizeHandle({ onDrag, onDragEnd }) {
  const [hovered, setHovered] = useState(false);
  const [dragging, setDragging] = useState(false);

  const handlePointerDown = useCallback((e) => {
    e.preventDefault();
    setDragging(true);
    const onMove = (ev) => onDrag(ev.clientX);
    const onUp = () => {
      setDragging(false);
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', onUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      onDragEnd?.();
    };
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp);
  }, [onDrag, onDragEnd]);

  return (
    <div
      onPointerDown={handlePointerDown}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        width: 8,
        cursor: 'col-resize',
        position: 'relative',
        zIndex: 30,
        flexShrink: 0,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        borderLeft: '1px solid #DDD7CC',
        background: hovered || dragging ? 'rgba(99,102,241,0.04)' : 'transparent',
        transition: dragging ? 'none' : 'background 0.2s ease',
      }}
    >
      {/* Visible drag indicator */}
      <div
        style={{
          width: dragging ? 3 : 2,
          height: 40,
          borderRadius: 4,
          background: dragging ? '#6366F1' : hovered ? '#A5B4FC' : '#D1D5DB',
          transition: dragging ? 'none' : 'all 0.2s ease',
          opacity: dragging || hovered ? 1 : 0.6,
        }}
      />
    </div>
  );
}

function App() {
  const { wrapperRef, contentRef } = useLenis();
  const { sessionId, phase, hasReport, canvasOpen, sidebarCollapsed, setSidebarCollapsed } = usePipelineStore();
  const { messages, addMessage, insertAfterMessage, thinking, setThinking } = useChatStore();
  const messagesEndRef = useRef(null);
  const [pipelineParent] = useAutoAnimate();
  const [conversationParent] = useAutoAnimate();
  const [mobileView, setMobileView] = useState('chat'); // 'chat' or 'canvas'

  // Detect mobile viewport
  const [isMobile, setIsMobile] = useState(() => window.innerWidth <= 768);
  useEffect(() => {
    const handler = () => setIsMobile(window.innerWidth <= 768);
    window.addEventListener('resize', handler);
    // On mobile, collapse sidebar by default
    if (window.innerWidth <= 768) setSidebarCollapsed(true);
    return () => window.removeEventListener('resize', handler);
  }, [setSidebarCollapsed]);

  // Auto-switch mobile view to canvas when it opens
  useEffect(() => {
    if (isMobile && canvasOpen) {
      setMobileView('canvas');
    } else if (isMobile && !canvasOpen) {
      setMobileView('chat');
    }
  }, [isMobile, canvasOpen]);

  // Ask AI handler — used by SelectionPopover in the chat column and by CanvasPanel
  const handleChatAsk = useCallback(async (selectedText, question) => {
    if (!sessionId) return;
    const userMsgId = addMessage('user', 'canvas_question', { selectedText, question });
    setThinking(true);
    const context = selectedText.trim()
      ? `Regarding this section:\n> "${selectedText.slice(0, 400)}"\n\n${question}`
      : question;
    try {
      const res = await sendChatMessage(sessionId, context);
      if (res?.response) insertAfterMessage(userMsgId, 'ai', 'text', res.response);
      else insertAfterMessage(userMsgId, 'ai', 'text', 'No response received from the AI.');
    } catch {
      insertAfterMessage(userMsgId, 'ai', 'text', 'Sorry, could not process your question right now.');
    } finally {
      setThinking(false);
    }
  }, [sessionId, addMessage, insertAfterMessage, setThinking]);

  // Initialize SSE connection for the session
  useSSEStream(sessionId);

  // ── Resizable panel split ──────────────────────────────────────────────
  // chatPct = percentage width of chat column (canvas gets the rest).
  // Default: 45% chat / 55% canvas. User can drag the edge to resize.
  const DEFAULT_CHAT_PCT = 45;
  const [chatPct, setChatPct] = useState(DEFAULT_CHAT_PCT);
  const [isDragging, setIsDragging] = useState(false);
  const mainRef = useRef(null);

  // Reset to default when canvas closes then reopens
  const prevCanvasOpen = useRef(false);
  useEffect(() => {
    if (canvasOpen && !prevCanvasOpen.current) {
      setChatPct(DEFAULT_CHAT_PCT);
    }
    prevCanvasOpen.current = canvasOpen;
  }, [canvasOpen]);

  const handleResizeDrag = useCallback((clientX) => {
    setIsDragging(true);
    if (!mainRef.current) return;
    const rect = mainRef.current.getBoundingClientRect();
    const pct = ((clientX - rect.left) / rect.width) * 100;
    // Clamp: chat min 25%, max 75%
    setChatPct(Math.min(75, Math.max(25, pct)));
  }, []);

  const handleResizeDragEnd = useCallback(() => {
    setIsDragging(false);
  }, []);

  // Auto-scroll only when conversation messages change or thinking indicator appears.
  // Pipeline chart messages arriving mid-analysis should NOT yank the scroll position.
  const conversationCount = messages.filter(m => m.category === 'conversation').length;
  useEffect(() => {
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [conversationCount, thinking]);

  // Fallback: if SSE missed synthesis_complete (e.g. page reload mid-pipeline),
  // fetch synthesis once when pipeline reaches complete phase.
  const synthesisFallbackRef = useRef(false);
  useEffect(() => {
    if (phase === 'complete' && sessionId && !synthesisFallbackRef.current) {
      synthesisFallbackRef.current = true;
      fetchSynthesis(sessionId)
        .then(synthesis => {
          if (!synthesis || Object.keys(synthesis).length === 0) return;
          const insights = Array.isArray(synthesis.detailed_insights)
            ? synthesis.detailed_insights
            : synthesis.detailed_insights?.insights || [];
          if (insights.length) addMessage('ai', 'insights', insights);

          // Synthesis agent writes "personas" OR "key_segments" depending on dataset type
          const _rawPersonas = synthesis.key_segments ?? synthesis.personas;
          const segments = Array.isArray(_rawPersonas)
            ? _rawPersonas
            : _rawPersonas?.segments ?? _rawPersonas?.personas ?? [];
          if (segments.length) addMessage('ai', 'personas', segments);

          // Synthesis agent writes "intervention_strategies" OR "recommendations"
          const _rawStrat = synthesis.recommendations ?? synthesis.intervention_strategies;
          const strategies = Array.isArray(_rawStrat)
            ? _rawStrat
            : _rawStrat?.strategies ?? [];
          if (strategies.length) addMessage('ai', 'interventions', strategies);

          const _rawConns = synthesis.cross_metric_connections;
          const connections = Array.isArray(_rawConns)
            ? _rawConns
            : _rawConns?.connections ?? [];
          if (connections.length) addMessage('ai', 'connections', connections);
          if (synthesis.executive_summary) addMessage('ai', 'summary', synthesis.executive_summary);
          if (synthesis.conversational_report) addMessage('ai', 'narrative', synthesis.conversational_report);
          if (synthesis._critic_review) addMessage('ai', 'critic', synthesis._critic_review);
        })
        .catch(() => { });
    }
  }, [phase, sessionId, addMessage]);


  return (
    <>
      <Toaster position="bottom-right" richColors theme="light" closeButton />

      {/* Mobile hamburger — only visible when sidebar is collapsed on mobile AND in chat view */}
      {isMobile && sidebarCollapsed && mobileView === 'chat' && (
        <button
          onClick={() => setSidebarCollapsed(false)}
          style={{
            position: 'fixed',
            top: 12,
            left: 12,
            zIndex: 10000,
            width: 36,
            height: 36,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: '#3D2B1A',
            color: '#F0EBE3',
            border: 'none',
            borderRadius: 10,
            boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
            cursor: 'pointer',
          }}
          aria-label="Open sidebar"
        >
          <Menu size={18} />
        </button>
      )}

      {/* Mobile view toggle — only visible when canvas is open on mobile */}
      {isMobile && canvasOpen && (
        <button
          onClick={() => setMobileView(mobileView === 'chat' ? 'canvas' : 'chat')}
          style={{
            position: 'fixed',
            bottom: 84, // Above the User Activity button area
            right: 16,
            zIndex: 10000,
            padding: '10px 16px',
            background: '#111827',
            color: '#F9FAFB',
            border: 'none',
            borderRadius: 24,
            fontSize: 13,
            fontWeight: 600,
            boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            cursor: 'pointer',
          }}
        >
          {mobileView === 'chat' ? (
            <>
              <FileText size={16} />
              View Report
            </>
          ) : (
            <>
              <MessageSquare size={16} />
              Back to Chat
            </>
          )}
        </button>
      )}

      {/* Mobile sidebar overlay backdrop */}
      {isMobile && !sidebarCollapsed && (
        <div
          onClick={() => setSidebarCollapsed(true)}
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(0,0,0,0.45)',
            zIndex: 9998,
          }}
        />
      )}

      <TopProgressBar />
      <Sidebar />
      <HistoryPanel />
      <main
        ref={mainRef}
        className="flex-1 flex overflow-hidden bg-bg-page relative font-sans"
        style={isMobile ? { flexDirection: 'column' } : {}}
      >
        {/* Chat column */}
        <div
          className="flex flex-col overflow-hidden"
          style={{
            width: isMobile ? '100%' : (canvasOpen ? `${chatPct}%` : '100%'),
            display: isMobile && mobileView === 'canvas' ? 'none' : 'flex',
            minWidth: 0,
            flexShrink: 0,
            transition: isDragging ? 'none' : 'width 0.3s ease',
          }}
        >
          <ProgressRing />

          <div
            ref={wrapperRef}
            className="flex-1 overflow-y-auto flex flex-col relative"
          >
            {/* Ask AI bubble — works anywhere in the chat column */}
            {sessionId && <SelectionPopover containerRef={wrapperRef} onSubmit={handleChatAsk} />}
            <div
              ref={contentRef}
              className="w-full mx-auto flex flex-col gap-[18px] flex-1"
              style={{ padding: isMobile ? '64px 16px 80px' : '48px 24px 16px', maxWidth: canvasOpen ? '100%' : 720 }}
            >
              {(() => {
                const pipelineMessages = messages.filter(m => m.category === 'pipeline');
                const conversationMessages = messages.filter(m => m.category === 'conversation');
                const isEmpty = pipelineMessages.length === 0 && conversationMessages.length === 0 && !thinking;

                if (isEmpty) {
                  return (
                    <div className="flex-1 flex flex-col items-center justify-center text-center p-[48px_24px] min-h-[320px]">
                      <div className="text-accent mb-6 opacity-60">
                        <motion.div
                          animate={{ rotate: [0, 10, -10, 0], scale: [1, 1.05, 1] }}
                          transition={{ duration: 4, repeat: Infinity, ease: "easeInOut" }}
                        >
                          <svg width="48" height="48" viewBox="0 0 20 20" fill="none">
                            <rect x="2" y="2" width="7" height="7" rx="2" fill="currentColor" opacity="0.8" />
                            <rect x="11" y="2" width="7" height="7" rx="2" fill="currentColor" opacity="0.4" />
                            <rect x="2" y="11" width="7" height="7" rx="2" fill="currentColor" opacity="0.4" />
                            <rect x="11" y="11" width="7" height="7" rx="2" fill="currentColor" opacity="0.7" />
                          </svg>
                        </motion.div>
                      </div>
                      <h1 className="text-[28px] font-semibold text-text-primary mb-3 tracking-tight">What would you like to analyze?</h1>
                      <p className="text-[16px] text-text-tertiary max-w-[400px] leading-relaxed mb-6 font-medium">Upload a data file to start a full multi-agent analysis pipeline.</p>
                      <div className="flex gap-2 flex-wrap justify-center opacity-80">
                        {['CSV', 'XLSX', 'JSON', 'JSONL', 'Parquet'].map(ext => (
                          <span key={ext} className="font-mono text-[11px] font-bold text-text-muted bg-bg-surface border border-border-subtle px-2.5 py-1 rounded-md shadow-xs">{ext}</span>
                        ))}
                      </div>
                    </div>
                  );
                }

                return (
                  <div className="flex flex-col gap-[18px]">
                    {/* ── Pipeline results block — all graphs/cards always here ── */}
                    {pipelineMessages.length > 0 && (
                      <div ref={pipelineParent} className="flex flex-col gap-[18px]">
                        {pipelineMessages.map(msg => <ChatMessage key={msg.id} message={msg} />)}
                      </div>
                    )}

                    {/* ── Separator — only when both zones have content ── */}
                    {pipelineMessages.length > 0 && (conversationMessages.length > 0 || thinking) && (
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10, margin: '2px 0' }}>
                        <div style={{ flex: 1, height: 1, background: '#DDD7CC' }} />
                        <span style={{
                          fontSize: 10, fontWeight: 700, color: '#9C9590',
                          letterSpacing: '0.08em', textTransform: 'uppercase',
                        }}>Chat</span>
                        <div style={{ flex: 1, height: 1, background: '#DDD7CC' }} />
                      </div>
                    )}

                    {/* ── Conversation block — Q&A always at the bottom ── */}
                    <div ref={conversationParent} className="flex flex-col gap-[18px]">
                      {conversationMessages.map(msg => <ChatMessage key={msg.id} message={msg} />)}
                      {/* Pulsing dots shown while Ask AI / chat is awaiting a response */}
                      {thinking && <ThinkingBubble />}
                    </div>
                  </div>
                );
              })()}
              <div ref={messagesEndRef} />
            </div>
          </div>

          <ProcessingStatus />
          <InputArea />
        </div>

        {/* Drag handle — visible only when canvas is open */}
        {canvasOpen && (
          <ResizeHandle
            onDrag={handleResizeDrag}
            onDragEnd={handleResizeDragEnd}
          />
        )}

        {/* Canvas panel — fills remaining space via flex:1 */}
        <div
          className="flex-1 flex overflow-hidden"
          style={{
            display: isMobile && mobileView === 'chat' ? 'none' : 'flex',
            minWidth: 0,
          }}
        >
          <CanvasPanel onAsk={handleChatAsk} />
        </div>
      </main>
    </>
  );
}

class AppErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }
  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }
  render() {
    if (this.state.hasError) {
      return (
        <div className="flex-1 flex flex-col items-center justify-center p-12 text-center gap-4">
          <div className="text-[32px]">⚠</div>
          <h2 className="text-[18px] font-semibold text-text-primary">Something went wrong</h2>
          <p className="text-[13px] text-text-tertiary max-w-[360px]">
            {this.state.error?.message || 'An unexpected error occurred in the UI.'}
          </p>
          <button
            onClick={() => this.setState({ hasError: false, error: null })}
            className="px-4 py-2 rounded-lg text-[13px] font-medium bg-accent text-white hover:bg-accent-dark transition-colors"
          >
            Try again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

export default function WrappedApp() {
  return (
    <AppErrorBoundary>
      <App />
    </AppErrorBoundary>
  );
}
