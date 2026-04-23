import React, { useState, useRef, useEffect, useMemo, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { X, Download, MessageSquare, Loader2, FileText, ArrowUp, List, ExternalLink, Link, Check } from 'lucide-react';
import { marked } from 'marked';
import DOMPurify from 'dompurify';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';
import { sendChatMessage } from '../../api';

// ── SelectionPopover ──────────────────────────────────────────────────────────
// Floating "Ask AI" bubble that appears on text selection.
// Exported so App.jsx can use it in the chat column too.
export function SelectionPopover({ containerRef, onSubmit }) {
  const [pos, setPos] = useState(null);
  const [selectedText, setSelectedText] = useState('');
  const [expanded, setExpanded] = useState(false);
  const [question, setQuestion] = useState('');
  const inputRef = useRef(null);

  useEffect(() => {
    const handleMouseUp = (e) => {
      // Don't reset when the user clicks inside the popover itself
      if (e?.target?.closest?.('[data-ask-popover]')) return;

      const sel = window.getSelection();
      if (!sel || sel.isCollapsed || !sel.toString().trim()) return;
      if (!containerRef.current) return;

      const anchor = sel.anchorNode?.parentElement;
      if (!containerRef.current.contains(anchor)) return;
      // Don't trigger on text selected inside form inputs
      if (anchor?.closest?.('input, textarea, [contenteditable]')) return;

      const range = sel.getRangeAt(0);
      const rect = range.getBoundingClientRect();
      const containerRect = containerRef.current.getBoundingClientRect();

      setPos({
        // Account for the container's own scroll offset so the pill sits above the selection
        top:  rect.top  - containerRect.top  - 44 + (containerRef.current.scrollTop || 0),
        left: rect.left - containerRect.left + rect.width / 2,
      });
      setSelectedText(sel.toString().trim());
      setExpanded(false);
      setQuestion('');
    };

    const handleMouseDown = (e) => {
      if (!e.target.closest('[data-ask-popover]')) {
        setPos(null);
        setExpanded(false);
        setQuestion('');
      }
    };

    document.addEventListener('mouseup', handleMouseUp);
    document.addEventListener('mousedown', handleMouseDown);
    return () => {
      document.removeEventListener('mouseup', handleMouseUp);
      document.removeEventListener('mousedown', handleMouseDown);
    };
  }, [containerRef]);

  const handleSend = () => {
    if (!question.trim()) return;
    onSubmit(selectedText, question.trim());
    setPos(null);
    setExpanded(false);
    setQuestion('');
  };

  if (!pos) return null;

  return (
    <div
      data-ask-popover
      style={{ position: 'absolute', top: pos.top, left: pos.left, transform: 'translateX(-50%)', zIndex: 30 }}
    >
      {!expanded ? (
        <button
          onClick={() => { setExpanded(true); setTimeout(() => inputRef.current?.focus(), 0); }}
          style={{
            background: '#1F2937', color: '#fff', borderRadius: 8,
            padding: '6px 10px', fontSize: 12, fontWeight: 600,
            display: 'flex', alignItems: 'center', gap: 4,
            boxShadow: '0 4px 16px rgba(0,0,0,0.25)', whiteSpace: 'nowrap',
            border: 'none', cursor: 'pointer',
          }}
        >
          <MessageSquare size={12} strokeWidth={2.5} />
          Ask AI
        </button>
      ) : (
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 4 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          transition={{ duration: 0.15, ease: [0.165, 0.84, 0.44, 1] }}
          style={{
            background: '#fff', borderRadius: 12, padding: '10px 12px',
            width: 290, boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
            border: '1px solid #E5E7EB',
          }}
        >
          {/* Selected text preview */}
          <div style={{
            fontSize: 11, color: '#6B7280', fontStyle: 'italic',
            borderLeft: '2px solid #6366F1', paddingLeft: 8,
            marginBottom: 8, lineHeight: 1.5,
            maxHeight: 56, overflow: 'hidden',
          }}>
            "{selectedText.length > 120 ? selectedText.slice(0, 120) + '…' : selectedText}"
          </div>
          {/* Input row */}
          <div style={{ display: 'flex', gap: 6, alignItems: 'flex-end' }}>
            <textarea
              ref={inputRef}
              value={question}
              onChange={e => setQuestion(e.target.value)}
              onKeyDown={e => {
                if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend(); }
                if (e.key === 'Escape') { setExpanded(false); setQuestion(''); }
              }}
              placeholder="Ask anything about this…"
              rows={1}
              style={{
                flex: 1, fontSize: 13, border: 'none', outline: 'none',
                resize: 'none', background: 'transparent', color: '#111827',
                minHeight: 32, maxHeight: 100, lineHeight: 1.5,
                fontFamily: 'inherit',
              }}
            />
            <button
              onClick={handleSend}
              disabled={!question.trim()}
              style={{
                width: 28, height: 28, borderRadius: '50%',
                background: question.trim() ? '#6366F1' : '#E5E7EB',
                color: question.trim() ? '#fff' : '#9CA3AF',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                flexShrink: 0, border: 'none', cursor: question.trim() ? 'pointer' : 'default',
                transition: 'background 0.15s',
              }}
            >
              <ArrowUp size={13} strokeWidth={2.5} />
            </button>
          </div>
        </motion.div>
      )}
    </div>
  );
}

// ── IframeAskPopover ──────────────────────────────────────────────────────────
// Same Ask AI popover but bridges text selection from inside an iframe.
// Since the report is same-origin we can attach to contentDocument directly.
// pos is calculated relative to the panel container so the popover overlays correctly.
function IframeAskPopover({ iframeRef, containerRef, onSubmit }) {
  const [pos, setPos] = useState(null);
  const [selectedText, setSelectedText] = useState('');
  const [expanded, setExpanded] = useState(false);
  const [question, setQuestion] = useState(null); // null = not yet typed
  const [inputVal, setInputVal] = useState('');
  const inputRef = useRef(null);

  // Attach to iframe document once it loads (called from parent's onLoad)
  const attach = useCallback(() => {
    const doc = iframeRef.current?.contentDocument;
    if (!doc) return;

    const onMouseUp = () => {
      const sel = iframeRef.current?.contentWindow?.getSelection();
      if (!sel || sel.isCollapsed || !sel.toString().trim()) return;

      const range = sel.getRangeAt(0);
      const selRect = range.getBoundingClientRect();         // relative to iframe viewport
      const iframeEl = iframeRef.current;
      const iframeRect = iframeEl.getBoundingClientRect();  // iframe relative to page
      const containerRect = containerRef.current?.getBoundingClientRect(); // panel relative to page

      if (!containerRect) return;

      // Convert to panel-relative coords
      const top  = (iframeRect.top - containerRect.top) + selRect.top - 44;
      const left = (iframeRect.left - containerRect.left) + selRect.left + selRect.width / 2;

      setPos({ top, left });
      setSelectedText(sel.toString().trim());
      setExpanded(false);
      setInputVal('');
    };

    const onMouseDown = () => {
      // Small delay so the click-inside-popover case doesn't fire first
      setTimeout(() => {
        const sel = iframeRef.current?.contentWindow?.getSelection();
        if (!sel || sel.isCollapsed) {
          setPos(null);
          setExpanded(false);
          setInputVal('');
        }
      }, 60);
    };

    doc.addEventListener('mouseup', onMouseUp);
    doc.addEventListener('mousedown', onMouseDown);
    return () => {
      doc.removeEventListener('mouseup', onMouseUp);
      doc.removeEventListener('mousedown', onMouseDown);
    };
  }, [iframeRef, containerRef]);

  // Also dismiss when clicking in the parent page (outside popover)
  useEffect(() => {
    const onParentDown = (e) => {
      if (!e.target.closest('[data-iframe-ask-popover]')) {
        setPos(null);
        setExpanded(false);
        setInputVal('');
      }
    };
    document.addEventListener('mousedown', onParentDown);
    return () => document.removeEventListener('mousedown', onParentDown);
  }, []);

  const handleSend = () => {
    if (!inputVal.trim()) return;
    onSubmit(selectedText, inputVal.trim());
    setPos(null);
    setExpanded(false);
    setInputVal('');
  };

  if (!pos) return null;

  return (
    <div
      data-iframe-ask-popover
      style={{
        position: 'absolute',
        top: pos.top,
        left: pos.left,
        transform: 'translateX(-50%)',
        zIndex: 40,
        pointerEvents: 'auto',
      }}
    >
      {!expanded ? (
        <button
          onClick={() => { setExpanded(true); setTimeout(() => inputRef.current?.focus(), 0); }}
          style={{
            background: '#1F2937', color: '#fff', borderRadius: 8,
            padding: '6px 10px', fontSize: 12, fontWeight: 600,
            display: 'flex', alignItems: 'center', gap: 4,
            boxShadow: '0 4px 16px rgba(0,0,0,0.25)', whiteSpace: 'nowrap',
            border: 'none', cursor: 'pointer',
          }}
        >
          <MessageSquare size={12} strokeWidth={2.5} />
          Ask AI
        </button>
      ) : (
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 4 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          transition={{ duration: 0.15, ease: [0.165, 0.84, 0.44, 1] }}
          style={{
            background: '#fff', borderRadius: 12, padding: '10px 12px',
            width: 290, boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
            border: '1px solid #E5E7EB',
          }}
        >
          <div style={{
            fontSize: 11, color: '#6B7280', fontStyle: 'italic',
            borderLeft: '2px solid #6366F1', paddingLeft: 8,
            marginBottom: 8, lineHeight: 1.5,
            maxHeight: 56, overflow: 'hidden',
          }}>
            "{selectedText.length > 120 ? selectedText.slice(0, 120) + '…' : selectedText}"
          </div>
          <div style={{ display: 'flex', gap: 6, alignItems: 'flex-end' }}>
            <textarea
              ref={inputRef}
              value={inputVal}
              onChange={e => setInputVal(e.target.value)}
              onKeyDown={e => {
                if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend(); }
                if (e.key === 'Escape') { setExpanded(false); setInputVal(''); }
              }}
              placeholder="Ask anything about this…"
              rows={1}
              style={{
                flex: 1, fontSize: 13, border: 'none', outline: 'none',
                resize: 'none', background: 'transparent', color: '#111827',
                minHeight: 32, maxHeight: 100, lineHeight: 1.5,
                fontFamily: 'inherit',
              }}
            />
            <button
              onClick={handleSend}
              disabled={!inputVal.trim()}
              style={{
                width: 28, height: 28, borderRadius: '50%',
                background: inputVal.trim() ? '#6366F1' : '#E5E7EB',
                color: inputVal.trim() ? '#fff' : '#9CA3AF',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                flexShrink: 0, border: 'none', cursor: inputVal.trim() ? 'pointer' : 'default',
                transition: 'background 0.15s',
              }}
            >
              <ArrowUp size={13} strokeWidth={2.5} />
            </button>
          </div>
        </motion.div>
      )}
    </div>
  );
}

// ── PhaseStatusBar ────────────────────────────────────────────────────────────
// Live header showing *what* the backend is doing right now. Replaces the old
// generic "Building report…" pulse so a 60-second wait feels like a process,
// not a freeze.
//   analyzing     → "Running analyses — X / Y complete" with progress bar
//   synthesizing  → "Synthesizing insights…" (once chunks land) / "Critic reviewing…"
//   building_report → "Polishing report…"
function PhaseStatusBar({ phase, nodes, hasNarrative }) {
  const total = nodes?.length || 0;
  const done  = nodes?.filter(n => n.status === 'complete' || n.status === 'failed').length || 0;
  const pct   = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;

  let label, sub;
  if (phase === 'analyzing') {
    label = 'Running analyses';
    sub   = total > 0 ? `${done} of ${total} complete` : 'preparing…';
  } else if (phase === 'synthesizing') {
    label = hasNarrative ? 'Writing insights' : 'Synthesizing insights';
    sub   = hasNarrative ? 'narrative streaming…' : 'LLM is reasoning over results…';
  } else if (phase === 'building_report') {
    label = 'Polishing report';
    sub   = 'rendering final HTML…';
  } else {
    label = 'Working';
    sub   = '';
  }

  return (
    <div style={{
      padding: '18px 24px',
      background: '#FAFAFA',
      borderBottom: '1px solid #F3F4F6',
      display: 'flex',
      alignItems: 'center',
      gap: 14,
    }}>
      <Loader2 size={16} className="animate-spin" style={{ color: '#6366F1', flexShrink: 0 }} />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: '#111827' }}>
          {label}{sub ? <span style={{ fontWeight: 500, color: '#6B7280' }}> — {sub}</span> : null}
        </div>
        {phase === 'analyzing' && total > 0 && (
          <div style={{
            marginTop: 8,
            height: 4,
            background: '#F3F4F6',
            borderRadius: 9999,
            overflow: 'hidden',
          }}>
            <div style={{
              height: 4,
              width: `${pct}%`,
              background: '#6366F1',
              borderRadius: 9999,
              transition: 'width 0.4s ease',
            }} />
          </div>
        )}
      </div>
    </div>
  );
}

// ── InlineStatCards ───────────────────────────────────────────────────────────
// Renders the "headline" numbers from the synthesis JSON — reliability tier +
// caveat count — the moment synthesis JSON lands, WITHOUT waiting for the
// full HTML report to finish building. This is what the user sees 5-10s
// before the iframe swap.
function InlineStatCards({ synthesisData }) {
  if (!synthesisData || typeof synthesisData !== 'object') return null;
  const rd = synthesisData.reliability_dashboard;
  if (!rd || typeof rd !== 'object') return null;

  const pct       = rd.aggregate_pct ?? 0;
  const label     = (rd.label || '').toLowerCase();
  const strong    = rd.strong_nodes ?? 0;
  const suggest   = rd.suggestive_nodes ?? 0;
  const tentative = rd.tentative_nodes ?? 0;
  const failed    = rd.failed_nodes ?? 0;

  const colMap = {
    strong:     '#059669',
    suggestive: '#D97706',
    tentative:  '#DC2626',
    none:       '#9CA3AF',
  };
  const col = colMap[label] || '#6B7280';
  const labelTxt = label ? label.charAt(0).toUpperCase() + label.slice(1) : '—';

  // Caveat count
  const caveatsRaw = synthesisData.caveats;
  let caveatCount = 0;
  if (Array.isArray(caveatsRaw)) caveatCount = caveatsRaw.length;
  else if (caveatsRaw && typeof caveatsRaw === 'object' && Array.isArray(caveatsRaw.items)) caveatCount = caveatsRaw.items.length;

  return (
    <div style={{ padding: '20px 24px', borderBottom: '1px solid #F3F4F6', background: '#fff' }}>
      <div style={{
        fontSize: 10,
        fontWeight: 700,
        textTransform: 'uppercase',
        letterSpacing: '0.08em',
        color: '#9CA3AF',
        marginBottom: 10,
      }}>
        Headline
      </div>
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        <div style={{
          flex: 1, minWidth: 140,
          border: '1px solid #E5E7EB', borderTop: `3px solid ${col}`,
          borderRadius: 10, padding: '12px 14px', background: '#fff',
        }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#9CA3AF', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Reliability</div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginTop: 4 }}>
            <span style={{ fontSize: 24, fontWeight: 800, color: col, lineHeight: 1 }}>{pct}%</span>
            <span style={{ fontSize: 11, fontWeight: 700, color: col, textTransform: 'uppercase', letterSpacing: '0.06em' }}>{labelTxt}</span>
          </div>
        </div>
        <div style={{
          flex: 1, minWidth: 140,
          border: '1px solid #E5E7EB', borderRadius: 10, padding: '12px 14px', background: '#fff',
        }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: '#9CA3AF', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Node Tiers</div>
          <div style={{ display: 'flex', gap: 10, marginTop: 6, fontSize: 12, color: '#374151' }}>
            <span><strong style={{ color: '#059669' }}>{strong}</strong> strong</span>
            <span><strong style={{ color: '#D97706' }}>{suggest}</strong> sugg.</span>
            <span><strong style={{ color: '#DC2626' }}>{tentative}</strong> tent.</span>
            {failed > 0 && <span><strong style={{ color: '#7C2D12' }}>{failed}</strong> failed</span>}
          </div>
        </div>
        {caveatCount > 0 && (
          <div style={{
            flex: 1, minWidth: 140,
            border: '1px solid #FDE68A', borderRadius: 10, padding: '12px 14px', background: '#FFFBEB',
          }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: '#92400E', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Limitations</div>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginTop: 4 }}>
              <span style={{ fontSize: 24, fontWeight: 800, color: '#D97706', lineHeight: 1 }}>{caveatCount}</span>
              <span style={{ fontSize: 11, color: '#92400E' }}>caveat{caveatCount !== 1 ? 's' : ''}</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ── LiveChartGallery ──────────────────────────────────────────────────────────
// Mini-tile grid of completed analyses shown while the narrative hasn't started
// streaming yet. Keeps the canvas visually alive during the analyze phase:
// each tile animates in as its node_complete event arrives. Deliberately
// text-only (no chart images) so it doesn't compete with the chat column —
// think of it as a "progress mini-map" rather than a second chart view.
function LiveChartGallery({ chartMessages }) {
  if (!chartMessages || chartMessages.length === 0) return null;

  const sevCol = { critical: '#BE123C', high: '#B45309', medium: '#1E40AF', low: '#065F46', info: '#475569' };

  return (
    <div style={{ padding: '20px 24px' }}>
      <div style={{
        fontSize: 10,
        fontWeight: 700,
        textTransform: 'uppercase',
        letterSpacing: '0.08em',
        color: '#9CA3AF',
        marginBottom: 10,
      }}>
        Completed so far ({chartMessages.length})
      </div>
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
        gap: 10,
      }}>
        {chartMessages.map((m) => {
          const p = m.payload || {};
          const sev = (p.severity || 'info').toLowerCase();
          const col = sevCol[sev] || '#475569';
          const conf = typeof p.confidence === 'number' ? Math.round(p.confidence * 100) : null;
          const typeLabel = (p.analysisType || p.id || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
          return (
            <div
              key={m.id}
              style={{
                border: '1px solid #E5E7EB',
                borderLeft: `3px solid ${col}`,
                borderRadius: 8,
                padding: '10px 12px',
                background: '#fff',
                animation: 'narrativeFadeIn 0.45s ease forwards',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                <span style={{ fontSize: 12, fontWeight: 700, color: '#111827', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {typeLabel || 'Analysis'}
                </span>
                {conf !== null && (
                  <span style={{
                    fontSize: 10, fontWeight: 700, color: col,
                    background: '#F3F4F6', padding: '2px 6px', borderRadius: 9999, flexShrink: 0,
                  }}>{conf}%</span>
                )}
              </div>
              {p.finding && (
                <div style={{ fontSize: 11.5, color: '#6B7280', marginTop: 6, lineHeight: 1.5, display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
                  {p.finding}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Narrative renderer ────────────────────────────────────────────────────────
// The streaming narrative is markdown. Convert to HTML then sanitize with DOMPurify.
function _renderNarrative(raw) {
  if (!raw || typeof raw !== 'string') return '';
  try {
    const html = marked.parse(raw, { breaks: true, gfm: true });
    return DOMPurify.sanitize(html, { USE_PROFILES: { html: true } });
  } catch {
    return `<pre style="white-space:pre-wrap">${String(raw).replace(/[<>&"]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;'}[c]))}</pre>`;
  }
}

// ── CanvasPanel ───────────────────────────────────────────────────────────────
// onAsk: (selectedText, question) => void  — provided by App.jsx so answers
// always appear in the left chat column regardless of which panel triggered them.
export function CanvasPanel({ onAsk }) {
  const sessionId = usePipelineStore((s) => s.currentSessionId);
  const currentSession = usePipelineStore((s) => s.sessions[s.currentSessionId]);
  const canvasOpen = currentSession?.canvasOpen ?? false;
  const canvasNarrative = currentSession?.canvasNarrative ?? null;
  const hasReport = currentSession?.hasReport ?? false;
  const phase = currentSession?.phase ?? 'idle';
  const nodes = currentSession?.nodes ?? [];
  const synthesisData = currentSession?.synthesisData ?? null;
  const setCanvasOpen = usePipelineStore((s) => s.setCanvasOpen);
  const addMessage = useChatStore((s) => s.addMessage);
  const setThinking = useChatStore((s) => s.setThinking);

  // Charts from chat for the live gallery. Subscribe to the raw messages
  // array (its ref only changes when chat state mutates) and filter via
  // useMemo — NEVER filter inside the selector itself, because that returns
  // a new array every render and triggers an infinite re-render loop.
  const allMessages = useChatStore((s) => s.sessions[sessionId]?.messages);
  const chartMessages = useMemo(
    () => (allMessages || []).filter(m => m.type === 'chart'),
    [allMessages]
  );
  const contentRef = useRef(null);
  const narrativeRef = useRef(null);
  const iframeRef = useRef(null);
  const iframeCleanupRef = useRef(null); // ref for iframe listener cleanup (avoids stale closure)

  // Fade-in animation on each new streaming chunk
  useEffect(() => {
    if (!narrativeRef.current) return;
    const last = narrativeRef.current.lastElementChild;
    if (!last) return;
    last.style.animation = 'none';
    requestAnimationFrame(() => {
      last.style.animation = 'narrativeFadeIn 0.45s ease forwards';
    });
  }, [canvasNarrative]);

  // Narrative SelectionPopover handler — wraps the shared onAsk with setThinking
  const handleSubmitQuestion = useCallback(async (selectedText, question) => {
    if (!sessionId) return;
    if (onAsk) {
      await onAsk(selectedText, question);
      return;
    }
    // Fallback: direct call (should not happen in normal usage)
    addMessage('user', 'canvas_question', { selectedText, question });
    setThinking(true);
    const context = selectedText
      ? `Regarding this section from the report:\n> "${selectedText.slice(0, 400)}"\n\n${question}`
      : question;
    try {
      const res = await sendChatMessage(sessionId, context);
      if (res?.response) addMessage('ai', 'text', res.response);
    } catch {
      addMessage('ai', 'text', 'Sorry, could not process your question right now.');
    } finally {
      setThinking(false);
    }
  }, [sessionId, addMessage, setThinking, onAsk]);

  const handleDownload = useCallback(async () => {
    if (!sessionId) return;
    try {
      const res = await fetch(`/report/${sessionId}`);
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `report_${sessionId}.html`;
      link.click();
      setTimeout(() => URL.revokeObjectURL(url), 10000);
    } catch {
      // silently fail — user can use browser save-as
    }
  }, [sessionId]);

  const [copied, setCopied] = useState(false);

  const handleOpenInTab = useCallback(() => {
    if (!sessionId) return;
    window.open(`/report/${sessionId}`, '_blank', 'noopener,noreferrer');
  }, [sessionId]);

  const handleCopyLink = useCallback(() => {
    if (!sessionId) return;
    const url = `${window.location.origin}/report/${sessionId}`;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(url).then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      }).catch(() => {});
    } else {
      // Fallback for HTTP (no secure context)
      const el = document.createElement('textarea');
      el.value = url;
      el.style.position = 'fixed';
      el.style.opacity = '0';
      document.body.appendChild(el);
      el.select();
      document.execCommand('copy');
      document.body.removeChild(el);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }, [sessionId]);

  // Table of contents extracted from report iframe headings
  const [tocItems, setTocItems] = useState([]);
  const [tocOpen, setTocOpen] = useState(false);

  // iframe load: auto-fit height + attach iframe selection bridge
  const [iframeLoaded, setIframeLoaded] = useState(false);
  const [iframeHeight, setIframeHeight] = useState(500);

  const handleIframeLoad = useCallback(() => {
    setIframeLoaded(true);
    try {
      const doc = iframeRef.current?.contentDocument;
      if (doc) {
        const h = doc.body?.scrollHeight || doc.documentElement?.scrollHeight || 500;
        setIframeHeight(h + 8);

        // Extract headings for TOC
        const headings = Array.from(doc.querySelectorAll('h1, h2, h3'));
        const items = headings
          .filter(el => el.textContent?.trim())
          .map((el, i) => {
            // Assign an id if missing so we can scroll to it
            if (!el.id) el.id = `_toc_${i}`;
            return { id: el.id, text: el.textContent.trim().slice(0, 60), tag: el.tagName.toLowerCase() };
          });
        setTocItems(items);

        // Bridge text selection events from the iframe document to this panel
        const iframeEl = iframeRef.current;
        const panelEl  = contentRef.current;

        const onMouseUp = () => {
          const sel = iframeRef.current?.contentWindow?.getSelection();
          if (!sel || sel.isCollapsed || !sel.toString().trim()) return;

          const range = sel.getRangeAt(0);
          const selRect    = range.getBoundingClientRect();
          const iframeRect = iframeEl.getBoundingClientRect();
          const panelRect  = panelEl?.getBoundingClientRect();

          if (!panelRect) return;

          const top  = (iframeRect.top - panelRect.top)  + selRect.top  - 44 + (panelEl?.scrollTop || 0);
          const left = (iframeRect.left - panelRect.left) + selRect.left + selRect.width / 2;

          setIframeSelPos({ top, left });
          setIframeSelText(sel.toString().trim());
          setIframeExpanded(false);
          setIframeInputVal('');
        };

        const onMouseDown = () => {
          setTimeout(() => {
            const sel = iframeRef.current?.contentWindow?.getSelection();
            if (!sel || sel.isCollapsed) {
              setIframeSelPos(null);
              setIframeExpanded(false);
              setIframeInputVal('');
            }
          }, 60);
        };

        // Clean up previous listeners before attaching new ones (prevents accumulation on iframe reload)
        if (iframeCleanupRef.current) iframeCleanupRef.current();
        doc.addEventListener('mouseup', onMouseUp);
        doc.addEventListener('mousedown', onMouseDown);
        iframeCleanupRef.current = () => {
          doc.removeEventListener('mouseup', onMouseUp);
          doc.removeEventListener('mousedown', onMouseDown);
        };
      }
    } catch {
      // Cross-origin safety — ignore
    }
  }, []);

  useEffect(() => {
    return () => { if (iframeCleanupRef.current) iframeCleanupRef.current(); };
  }, []);

  // Iframe Ask AI state (managed here so it overlays the panel correctly)
  const [iframeSelPos, setIframeSelPos]     = useState(null);
  const [iframeSelText, setIframeSelText]   = useState('');
  const [iframeExpanded, setIframeExpanded] = useState(false);
  const [iframeInputVal, setIframeInputVal] = useState('');
  const iframeInputRef = useRef(null);

  // Dismiss iframe popover when clicking outside it in the parent doc
  useEffect(() => {
    const onParentDown = (e) => {
      if (!e.target.closest('[data-iframe-ask-popover]')) {
        setIframeSelPos(null);
        setIframeExpanded(false);
        setIframeInputVal('');
      }
    };
    document.addEventListener('mousedown', onParentDown);
    return () => document.removeEventListener('mousedown', onParentDown);
  }, []);

  const handleIframeAskSend = () => {
    if (!iframeInputVal.trim()) return;
    handleSubmitQuestion(iframeSelText, iframeInputVal.trim());
    setIframeSelPos(null);
    setIframeExpanded(false);
    setIframeInputVal('');
  };

  const showReport    = hasReport && sessionId;
  // Progressive view: while the final HTML report isn't ready yet, render
  // (top-down) the phase bar, the inline stat cards (if synthesis JSON is in),
  // the streaming narrative (if any), and a chart gallery mini-map (if no
  // narrative yet but analyses have produced charts). This replaces the
  // old "empty canvas until narrative streams" behavior.
  const showProgressive = !hasReport && (
    phase === 'analyzing' || phase === 'synthesizing' || phase === 'building_report' ||
    canvasNarrative || (chartMessages && chartMessages.length > 0)
  );
  const showNarrative  = !hasReport && canvasNarrative;
  const showStatCards  = !hasReport && synthesisData && synthesisData.reliability_dashboard;
  const showGallery    = !hasReport && !canvasNarrative && chartMessages && chartMessages.length > 0;
  const showPhaseBar   = !hasReport && (phase === 'analyzing' || phase === 'synthesizing' || phase === 'building_report');

  return (
    <AnimatePresence>
      {canvasOpen && (
        <motion.div
          initial={{ opacity: 0, flex: 0 }}
          animate={{ opacity: 1, flex: 1 }}
          exit={{ opacity: 0, flex: 0 }}
          transition={{ duration: 0.3, ease: [0.165, 0.84, 0.44, 1] }}
          className="flex flex-col overflow-hidden"
          style={{ background: '#FFFFFF', minWidth: 0 }}
        >
          {/* ── Toolbar ── */}
          <div
            className="flex items-center gap-2 px-4 py-3 shrink-0"
            style={{ borderBottom: '1px solid #F3F4F6', background: '#FAFAFA' }}
          >
            <FileText size={13} strokeWidth={2} style={{ color: '#9CA3AF' }} />
            <span className="text-[13px] font-semibold flex-1 truncate" style={{ color: '#374151' }}>
              {showReport ? 'Analysis Report' : 'Narrative Summary'}
            </span>

            {/* TOC toggle — only when report has headings */}
            {showReport && tocItems.length > 0 && (
              <button
                onClick={() => setTocOpen(o => !o)}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[12px] font-medium border transition-all"
                style={{
                  background: tocOpen ? '#EEF2FF' : '#F9FAFB',
                  borderColor: tocOpen ? '#6366F1' : '#E5E7EB',
                  color: tocOpen ? '#4F46E5' : '#374151',
                }}
                title="Table of contents"
              >
                <List size={13} strokeWidth={2} />
                Contents
              </button>
            )}

            {/* Open in tab */}
            {showReport && (
              <button
                onClick={handleOpenInTab}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[12px] font-medium border transition-all"
                style={{ background: '#F9FAFB', borderColor: '#E5E7EB', color: '#374151' }}
                onMouseEnter={e => { e.currentTarget.style.background = '#F3F4F6'; }}
                onMouseLeave={e => { e.currentTarget.style.background = '#F9FAFB'; }}
                title="Open report in new tab"
              >
                <ExternalLink size={13} strokeWidth={2} />
                Open
              </button>
            )}

            {/* Copy link */}
            {showReport && (
              <button
                onClick={handleCopyLink}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[12px] font-medium border transition-all"
                style={{
                  background: copied ? '#F0FDF4' : '#F9FAFB',
                  borderColor: copied ? '#86EFAC' : '#E5E7EB',
                  color: copied ? '#16A34A' : '#374151',
                }}
                onMouseEnter={e => { if (!copied) { e.currentTarget.style.background = '#F3F4F6'; } }}
                onMouseLeave={e => { if (!copied) { e.currentTarget.style.background = '#F9FAFB'; } }}
                title="Copy report link"
              >
                {copied ? <Check size={13} strokeWidth={2.5} /> : <Link size={13} strokeWidth={2} />}
                {copied ? 'Copied!' : 'Copy link'}
              </button>
            )}

            {/* Download */}
            {(showReport || showNarrative) && (
              <button
                onClick={handleDownload}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[12px] font-medium border transition-all"
                style={{ background: '#F9FAFB', borderColor: '#E5E7EB', color: '#374151' }}
                onMouseEnter={e => { e.currentTarget.style.background = '#F3F4F6'; }}
                onMouseLeave={e => { e.currentTarget.style.background = '#F9FAFB'; }}
                title="Download report"
              >
                <Download size={13} strokeWidth={2} />
                Download
              </button>
            )}

            {/* Close */}
            <button
              onClick={() => setCanvasOpen(false)}
              className="w-7 h-7 flex items-center justify-center rounded-lg transition-colors"
              style={{ color: '#9CA3AF' }}
              onMouseEnter={e => { e.currentTarget.style.background = '#F3F4F6'; e.currentTarget.style.color = '#374151'; }}
              onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = '#9CA3AF'; }}
            >
              <X size={15} strokeWidth={2} />
            </button>
          </div>

          {/* ── Content ── */}
          {/* position: relative so iframe Ask AI popover can be absolute-positioned */}
          <div ref={contentRef} className="flex-1 overflow-y-auto relative" style={{ overscrollBehavior: 'contain' }}>

            {/* TOC panel — floats at top above iframe when open */}
            <AnimatePresence initial={false}>
              {showReport && tocOpen && tocItems.length > 0 && (
                <motion.div
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: 'auto', opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.2 }}
                  style={{ background: '#FAFAFA', borderBottom: '1px solid #E5E7EB', overflow: 'hidden' }}
                >
                  <nav style={{ padding: '12px 20px', display: 'flex', flexWrap: 'wrap', gap: '4px 12px' }}>
                    {tocItems.map((item, i) => (
                      <button
                        key={i}
                        onClick={() => {
                          try {
                            const el = iframeRef.current?.contentDocument?.getElementById(item.id);
                            if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
                          } catch {}
                        }}
                        style={{
                          fontSize: item.tag === 'h1' ? 12 : 11,
                          fontWeight: item.tag === 'h1' ? 700 : item.tag === 'h2' ? 600 : 500,
                          color: '#374151',
                          paddingLeft: item.tag === 'h2' ? 8 : item.tag === 'h3' ? 16 : 0,
                          background: 'none', border: 'none', cursor: 'pointer',
                          borderRadius: 4, padding: '3px 6px',
                          whiteSpace: 'nowrap', overflow: 'hidden', maxWidth: 200,
                          textOverflow: 'ellipsis',
                          textAlign: 'left',
                        }}
                        title={item.text}
                        onMouseEnter={e => { e.currentTarget.style.background = '#EEF2FF'; e.currentTarget.style.color = '#4F46E5'; }}
                        onMouseLeave={e => { e.currentTarget.style.background = 'none'; e.currentTarget.style.color = '#374151'; }}
                      >
                        {item.text}
                      </button>
                    ))}
                  </nav>
                </motion.div>
              )}
            </AnimatePresence>

            {/* Report: iframe that fills content area */}
            {showReport && (
              <>
                {!iframeLoaded && (
                  <div className="absolute inset-0 flex items-center justify-center bg-white/80 z-10">
                    <Loader2 size={22} className="animate-spin" style={{ color: '#6366F1' }} />
                  </div>
                )}
                <iframe
                  ref={iframeRef}
                  src={`/report/${sessionId}`}
                  style={{
                    width: '100%',
                    height: iframeHeight,
                    border: 'none',
                    display: 'block',
                    opacity: iframeLoaded ? 1 : 0,
                    transition: 'opacity 0.3s',
                  }}
                  title="Analysis Report"
                  onLoad={handleIframeLoad}
                />

                {/* Iframe Ask AI popover — positioned relative to contentRef */}
                {iframeSelPos && (
                  <div
                    data-iframe-ask-popover
                    style={{
                      position: 'absolute',
                      top: iframeSelPos.top,
                      left: iframeSelPos.left,
                      transform: 'translateX(-50%)',
                      zIndex: 40,
                    }}
                  >
                    {!iframeExpanded ? (
                      <button
                        onClick={() => {
                          setIframeExpanded(true);
                          setTimeout(() => iframeInputRef.current?.focus(), 0);
                        }}
                        style={{
                          background: '#1F2937', color: '#fff', borderRadius: 8,
                          padding: '6px 10px', fontSize: 12, fontWeight: 600,
                          display: 'flex', alignItems: 'center', gap: 4,
                          boxShadow: '0 4px 16px rgba(0,0,0,0.25)', whiteSpace: 'nowrap',
                          border: 'none', cursor: 'pointer',
                        }}
                      >
                        <MessageSquare size={12} strokeWidth={2.5} />
                        Ask AI
                      </button>
                    ) : (
                      <motion.div
                        initial={{ opacity: 0, scale: 0.95, y: 4 }}
                        animate={{ opacity: 1, scale: 1, y: 0 }}
                        transition={{ duration: 0.15, ease: [0.165, 0.84, 0.44, 1] }}
                        style={{
                          background: '#fff', borderRadius: 12, padding: '10px 12px',
                          width: 290, boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
                          border: '1px solid #E5E7EB',
                        }}
                      >
                        <div style={{
                          fontSize: 11, color: '#6B7280', fontStyle: 'italic',
                          borderLeft: '2px solid #6366F1', paddingLeft: 8,
                          marginBottom: 8, lineHeight: 1.5,
                          maxHeight: 56, overflow: 'hidden',
                        }}>
                          "{iframeSelText.length > 120 ? iframeSelText.slice(0, 120) + '…' : iframeSelText}"
                        </div>
                        <div style={{ display: 'flex', gap: 6, alignItems: 'flex-end' }}>
                          <textarea
                            ref={iframeInputRef}
                            value={iframeInputVal}
                            onChange={e => setIframeInputVal(e.target.value)}
                            onKeyDown={e => {
                              if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleIframeAskSend(); }
                              if (e.key === 'Escape') { setIframeExpanded(false); setIframeInputVal(''); }
                            }}
                            placeholder="Ask anything about this…"
                            rows={1}
                            style={{
                              flex: 1, fontSize: 13, border: 'none', outline: 'none',
                              resize: 'none', background: 'transparent', color: '#111827',
                              minHeight: 32, maxHeight: 100, lineHeight: 1.5,
                              fontFamily: 'inherit',
                            }}
                          />
                          <button
                            onClick={handleIframeAskSend}
                            disabled={!iframeInputVal.trim()}
                            style={{
                              width: 28, height: 28, borderRadius: '50%',
                              background: iframeInputVal.trim() ? '#6366F1' : '#E5E7EB',
                              color: iframeInputVal.trim() ? '#fff' : '#9CA3AF',
                              display: 'flex', alignItems: 'center', justifyContent: 'center',
                              flexShrink: 0, border: 'none',
                              cursor: iframeInputVal.trim() ? 'pointer' : 'default',
                              transition: 'background 0.15s',
                            }}
                          >
                            <ArrowUp size={13} strokeWidth={2.5} />
                          </button>
                        </div>
                      </motion.div>
                    )}
                  </div>
                )}
              </>
            )}

            {/* Progressive view — phase bar, stat cards, narrative, gallery */}
            {showProgressive && !showReport && (
              <div className="relative w-full">
                {showPhaseBar && (
                  <PhaseStatusBar phase={phase} nodes={nodes} hasNarrative={!!canvasNarrative} />
                )}
                {showStatCards && <InlineStatCards synthesisData={synthesisData} />}
                {showNarrative && (
                  <div style={{ padding: '32px 40px', position: 'relative' }}>
                    <SelectionPopover containerRef={contentRef} onSubmit={handleSubmitQuestion} />
                    <div
                      ref={narrativeRef}
                      className="prose prose-sm max-w-none narrative-stream narrative-md"
                      style={{ fontSize: 14, lineHeight: 1.75, color: '#374151' }}
                      dangerouslySetInnerHTML={{ __html: _renderNarrative(canvasNarrative) }}
                    />
                  </div>
                )}
                {showGallery && <LiveChartGallery chartMessages={chartMessages} />}
              </div>
            )}
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
