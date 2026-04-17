import React from 'react';
import { cn } from '../ui/Badge';
import { usePipelineStore } from '../../store/pipelineStore';

const PRIORITY_STYLES = {
  critical: { bg: 'rgba(239,68,68,0.08)',   text: '#EF4444', border: 'rgba(239,68,68,0.2)'  , label: 'Critical' },
  high:     { bg: 'rgba(245,158,11,0.08)',  text: '#F59E0B', border: 'rgba(245,158,11,0.2)' , label: 'High'     },
  medium:   { bg: 'rgba(59,130,246,0.08)',  text: '#3B82F6', border: 'rgba(59,130,246,0.2)' , label: 'Medium'   },
  low:      { bg: 'rgba(107,114,128,0.08)', text: '#6B7280', border: 'rgba(107,114,128,0.2)', label: 'Low'      },
};

function PriorityBadge({ priority }) {
  if (!priority) return null;
  const key = String(priority).toLowerCase();
  const s = PRIORITY_STYLES[key] || PRIORITY_STYLES.low;
  return (
    <span
      className="inline-flex items-center px-2 py-0.5 rounded-full text-[10.5px] font-semibold border shrink-0"
      style={{ background: s.bg, color: s.text, borderColor: s.border }}
    >
      {s.label}
    </span>
  );
}

function NodeRow({ node, index = 0 }) {
  const nodes = usePipelineStore((s) => s.sessions[s.currentSessionId]?.nodes ?? []);
  const liveNode = nodes.find(n => n.id === node.id);
  const status = liveNode?.status || 'pending';

  const isComplete = status === 'complete';
  const isRunning  = status === 'running';
  const isFailed   = status === 'failed';

  const rawName = node.name || node.analysis_type || '';
  const displayName = rawName.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

  const idBg     = isComplete ? 'rgba(16,185,129,0.1)'  : isFailed ? 'rgba(239,68,68,0.1)'  : isRunning ? 'rgba(251,113,133,0.1)'  : '#F3F4F6';
  const idColor  = isComplete ? '#10B981'                : isFailed ? '#EF4444'               : isRunning ? '#FB7185'               : '#9CA3AF';
  const idBorder = isComplete ? 'rgba(16,185,129,0.3)'  : isFailed ? 'rgba(239,68,68,0.3)'   : isRunning ? 'rgba(251,113,133,0.3)'  : '#E5E7EB';

  const rowBg = isComplete ? 'rgba(16,185,129,0.06)' : isFailed ? 'rgba(239,68,68,0.06)' : isRunning ? 'rgba(251,113,133,0.06)' : 'transparent';

  return (
    <div
      className="flex items-start gap-3 px-3 py-2.5 rounded-lg transition-all duration-500 group"
      style={{ cursor: 'default', backgroundColor: rowBg, transitionDelay: `${index * 80}ms` }}
      onMouseEnter={e => { if (!isComplete && !isFailed && !isRunning) e.currentTarget.style.backgroundColor = '#F3F4F6'; }}
      onMouseLeave={e => e.currentTarget.style.backgroundColor = rowBg}
    >
      {/* ID tag — color reflects live status */}
      <span
        className="shrink-0 mt-[2px] font-mono text-[10px] font-semibold rounded px-1.5 py-0.5 leading-none transition-colors duration-300"
        style={{ background: idBg, color: idColor, border: `1px solid ${idBorder}` }}
      >
        {node.id}
      </span>

      {/* Text */}
      <div className="flex-1 min-w-0">
        <div className="text-[13px] font-semibold text-text-primary leading-tight mb-0.5">
          {displayName}
        </div>
        {node.description && (
          <div
            className="text-[12px] text-text-tertiary leading-relaxed"
            style={{
              display:         '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
              overflow:        'hidden',
            }}
          >
            {node.description}
          </div>
        )}
      </div>

      {/* Priority badge */}
      <PriorityBadge priority={node.priority} />
    </div>
  );
}

export function DiscoveryCard({ data }) {
  const customMetricNodes = usePipelineStore((s) => s.sessions[s.currentSessionId]?.customMetricNodes ?? []);
  // Merge AI-discovered nodes with any user-added custom metric nodes
  const dag = [...(data?.dag || []), ...customMetricNodes];

  return (
    <div className="w-full mt-1 mb-2 bg-bg-surface border border-border-default rounded-2xl shadow-sm overflow-hidden text-[14px]">
      {/* Header */}
      <div className="px-4 pt-4 pb-3 border-b border-border-subtle">
        <div className="flex items-center gap-2.5 mb-1.5">
          <h4 className="text-[14px] font-semibold text-text-primary tracking-tight">
            Analysis Plan
          </h4>
          {dag.length > 0 && (
            <span
              className="inline-flex items-center px-2 py-0.5 rounded-full text-[10.5px] font-semibold border"
              style={{
                background: 'rgba(251,113,133,0.08)',
                color: '#FB7185',
                borderColor: 'rgba(251,113,133,0.2)',
              }}
            >
              {dag.length} nodes
            </span>
          )}
        </div>
        {data?.data_summary && (
          <p className="text-[12.5px] text-text-tertiary leading-relaxed">
            {data.data_summary}
          </p>
        )}
      </div>

      {/* Node list */}
      <div className="p-2">
        {dag.map((node, i) => (
          <NodeRow key={node.id ?? i} node={node} index={i} />
        ))}
        {dag.length === 0 && (
          <p className="px-3 py-4 text-[12px] text-text-muted text-center">No analysis nodes found.</p>
        )}
      </div>
    </div>
  );
}
