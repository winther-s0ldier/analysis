import React, { useEffect, useRef, useCallback, useState } from 'react';
import { Sidebar } from './components/layout/Sidebar';
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
        borderLeft: '1px solid #E5E7EB',
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
  const { sessionId, phase, hasReport, canvasOpen } = usePipelineStore();
  const { messages, addMessage, thinking, setThinking } = useChatStore();
  const messagesEndRef = useRef(null);
  const [parent] = useAutoAnimate();

  // Ask AI handler — used by SelectionPopover in the chat column and by CanvasPanel
  const handleChatAsk = useCallback(async (selectedText, question) => {
    if (!sessionId) return;
    addMessage('user', 'canvas_question', { selectedText, question });
    setThinking(true);
    // When there is no selected text (e.g. from ReportAskBar) send the plain
    // question; otherwise wrap with the quoted selection for context.
    const context = selectedText.trim()
      ? `Regarding this section:\n> "${selectedText.slice(0, 400)}"\n\n${question}`
      : question;
    try {
      const res = await sendChatMessage(sessionId, context);
      if (res?.response) addMessage('ai', 'text', res.response);
      else addMessage('ai', 'text', 'No response received from the AI.');
    } catch {
      addMessage('ai', 'text', 'Sorry, could not process your question right now.');
    } finally {
      setThinking(false);
    }
  }, [sessionId, addMessage, setThinking]);

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

  // Auto-scroll chat on new messages or when thinking state changes
  useEffect(() => {
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages, thinking]);

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
        .catch(() => {});
    }
  }, [phase, sessionId, addMessage]);

  return (
    <>
      <Toaster position="bottom-right" richColors theme="light" closeButton />
      <TopProgressBar />
      <Sidebar />
      <main ref={mainRef} className="flex-1 flex overflow-hidden bg-bg-page relative font-sans">
        {/* Chat column — shrinks when canvas is open */}
        <div
          className="flex flex-col overflow-hidden"
          style={{
            width: canvasOpen ? `${chatPct}%` : '100%',
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
              style={{ padding: '48px 24px 16px', maxWidth: canvasOpen ? '100%' : 720 }}
            >
              {messages.length === 0 && !thinking ? (
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
              ) : (
                <div ref={parent} className="flex flex-col gap-[18px]">
                  {messages.map(msg => <ChatMessage key={msg.id} message={msg} />)}
                  {/* Pulsing dots shown while Ask AI / chat is awaiting a response */}
                  {thinking && <ThinkingBubble />}
                </div>
              )}
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
        <CanvasPanel onAsk={handleChatAsk} />
      </main>
    </>
  );
}

export default App;
