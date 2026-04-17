import React, { useMemo } from 'react';
import Terminal, { TerminalOutput, ColorMode } from 'react-terminal-ui';
import { usePipelineStore } from '../../store/pipelineStore';
import { motion } from 'framer-motion';

// Tighten up react-terminal-ui's default spacing to fit the chat bubble
const terminalOverrides = `
  .react-terminal-wrapper { padding: 36px 16px 16px !important; font-size: 13px !important; border-radius: 0 !important; }
  .react-terminal-wrapper:after { font-size: 11px !important; top: 8px !important; letter-spacing: 0.06em; color: #6B7280 !important; }
  .react-terminal-window-buttons { top: 10px !important; left: 12px !important; gap: 6px !important; }
  .react-terminal-window-buttons button { width: 10px !important; height: 10px !important; }
  .react-terminal-line { line-height: 1.7 !important; white-space: pre-wrap !important; }
`;

export function TerminalCard({ totalNodes = 0 }) {
  const nodes = usePipelineStore((s) => s.sessions[s.currentSessionId]?.nodes ?? []);

  const completedNodes = nodes.filter(n => n.status === 'complete' || n.status === 'failed').length;
  const total = Math.max(totalNodes, nodes.length) || 1;
  const progressPct = Math.round((completedNodes / total) * 100);
  const isDone = nodes.length > 0 && completedNodes === nodes.length;

  const lines = useMemo(() => {
    if (nodes.length === 0) {
      return [
        <TerminalOutput key="init">
          <span style={{ color: '#6B7280', fontStyle: 'italic' }}>Initializing agents…</span>
        </TerminalOutput>,
      ];
    }

    const out = nodes.map((n) => {
      const isOk   = n.status === 'complete';
      const isRun  = n.status === 'running';
      const isFail = n.status === 'failed';

      const icon       = isOk ? '✓' : isRun ? '›' : isFail ? '✗' : '·';
      const label      = (n.name || n.type || n.id || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
      const tag        = n.id ? `[${n.id}]` : '';
      const statusText = isOk ? 'done' : isRun ? 'running…' : isFail ? 'failed' : 'waiting';

      const iconColor   = isOk ? '#10B981' : isRun ? '#818CF8' : isFail ? '#F87171' : '#4B5563';
      const labelColor  = isOk ? '#D1FAE5' : isRun ? '#E0E7FF' : isFail ? '#FEE2E2' : '#9CA3AF';
      const statusColor = isOk ? '#10B981' : isRun ? '#818CF8' : isFail ? '#F87171' : '#4B5563';

      return (
        <TerminalOutput key={n.id}>
          <span style={{ color: '#4B5563', fontSize: 11 }}>{tag} </span>
          <span style={{ color: iconColor, fontWeight: 'bold' }}>{icon} </span>
          <span style={{ color: labelColor }}>{label}</span>
          <span style={{ color: '#374151' }}>{' … '}</span>
          <span style={{ color: statusColor, fontWeight: 600, fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{statusText}</span>
        </TerminalOutput>
      );
    });

    if (isDone) {
      out.push(
        <TerminalOutput key="sep"><span style={{ color: '#374151' }}>{'─'.repeat(36)}</span></TerminalOutput>,
        <TerminalOutput key="done"><span style={{ color: '#10B981', fontWeight: 'bold' }}>✓ All {completedNodes} nodes complete</span></TerminalOutput>
      );
    }

    return out;
  }, [nodes, isDone, completedNodes]);

  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, ease: [0.165, 0.84, 0.44, 1] }}
      className="w-full mt-1 mb-1"
    >
      {/* Inject compact overrides once */}
      <style>{terminalOverrides}</style>

      <div className="rounded-2xl overflow-hidden" style={{ border: '1px solid #1F2937' }}>
        <Terminal
          name="Pipeline Execution"
          colorMode={ColorMode.Dark}
          height="auto"
        >
          {lines}
        </Terminal>

        {/* Progress bar */}
        <div
          className="px-4 py-2.5 flex items-center gap-3"
          style={{ background: '#111827', borderTop: '1px solid #1F2937' }}
        >
          <div className="flex-1 h-1 rounded-full overflow-hidden" style={{ background: '#374151' }}>
            <motion.div
              className="h-full rounded-full"
              style={{
                background: isDone ? '#10B981' : '#FB7185',
                boxShadow: isDone ? '0 0 6px rgba(16,185,129,0.4)' : '0 0 6px rgba(251,113,133,0.4)',
              }}
              initial={{ width: 0 }}
              animate={{ width: `${progressPct}%` }}
              transition={{ duration: 0.5, ease: 'easeOut' }}
            />
          </div>
          <span className="text-[11px] font-mono font-semibold tabular-nums" style={{ color: '#6B7280' }}>
            {completedNodes}/{total}
          </span>
          <span
            className="text-[11px] font-mono font-semibold tabular-nums"
            style={{ color: isDone ? '#10B981' : '#FB7185' }}
          >
            {progressPct}%
          </span>
        </div>
      </div>
    </motion.div>
  );
}
