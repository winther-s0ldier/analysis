import React, { useRef, useState } from 'react';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';
import { motion } from 'framer-motion';
import { AlertTriangle, Info, AlertCircle, TrendingUp, Lightbulb, Wrench, BarChart2, Activity, MessageCircle } from 'lucide-react';

const MIN_HEIGHT = 360;
const MAX_HEIGHT = 680;

// ── Severity badge ────────────────────────────────────────────────────────────
const SEVERITY_CONFIG = {
  critical: { label: 'Critical',  bg: '#FEF2F2', border: '#FECACA', text: '#DC2626', Icon: AlertCircle   },
  warning:  { label: 'Warning',   bg: '#FFFBEB', border: '#FDE68A', text: '#D97706', Icon: AlertTriangle  },
  info:     { label: 'Info',      bg: '#EFF6FF', border: '#BFDBFE', text: '#2563EB', Icon: Info           },
};

function SeverityBadge({ severity }) {
  const cfg = SEVERITY_CONFIG[severity] || SEVERITY_CONFIG.info;
  const { Icon, label, bg, border, text } = cfg;
  return (
    <span
      style={{
        display: 'inline-flex', alignItems: 'center', gap: 4,
        background: bg, border: `1px solid ${border}`, color: text,
        borderRadius: 6, padding: '2px 8px', fontSize: 11, fontWeight: 600,
        lineHeight: 1.5, flexShrink: 0,
      }}
    >
      <Icon size={10} strokeWidth={2.5} />
      {label}
    </span>
  );
}

// ── Metadata chip row ─────────────────────────────────────────────────────────
function MetaChip({ icon: Icon, label, value, accent }) {
  if (!value) return null;
  return (
    <div
      style={{
        display: 'flex', gap: 8, alignItems: 'flex-start',
        padding: '8px 10px',
        background: accent ? '#F5F3FF' : '#F9FAFB',
        border: `1px solid ${accent ? '#DDD6FE' : '#F3F4F6'}`,
        borderRadius: 8,
        flex: '1 1 0', minWidth: 0,
      }}
    >
      <Icon
        size={13} strokeWidth={2}
        style={{ color: accent ? '#7C3AED' : '#6B7280', flexShrink: 0, marginTop: 1 }}
      />
      <div style={{ minWidth: 0 }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: accent ? '#7C3AED' : '#9CA3AF', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 2 }}>
          {label}
        </div>
        <div style={{ fontSize: 12, color: '#374151', lineHeight: 1.5 }}>
          {value}
        </div>
      </div>
    </div>
  );
}

// ── ConfidenceBar ─────────────────────────────────────────────────────────────
function ConfidenceBar({ confidence }) {
  if (confidence == null) return null;
  const pct = Math.round(confidence * 100);
  const color = pct >= 90 ? '#10B981' : pct >= 75 ? '#F59E0B' : '#EF4444';
  const label = pct < 75 ? 'Directional — treat as indicative' : null;
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
      <span style={{ fontSize: 10, fontWeight: 700, color: '#9CA3AF', textTransform: 'uppercase', letterSpacing: '0.06em', width: 70, flexShrink: 0 }}>Confidence</span>
      <div style={{ flex: 1, height: 4, borderRadius: 2, background: '#F3F4F6', overflow: 'hidden' }}>
        <div style={{ height: '100%', width: `${pct}%`, background: color, borderRadius: 2, transition: 'width 0.5s' }} />
      </div>
      <span style={{ fontSize: 11, fontWeight: 600, fontFamily: 'monospace', color, width: 34, textAlign: 'right' }}>{pct}%</span>
      {label && <span style={{ fontSize: 10, color: '#9CA3AF', fontStyle: 'italic' }}>{label}</span>}
    </div>
  );
}

// ── ChartCard ─────────────────────────────────────────────────────────────────
export function ChartCard({ id, analysisType, finding, hasChart = true, severity, confidence, decisionMakerTakeaway, keyFinding, topValues, anomalies, whatItMeans, recommendation, proposedFix }) {
  const { sessionId } = usePipelineStore();
  const { setPendingMessage } = useChatStore();
  const [loaded, setLoaded] = useState(false);
  const [iframeError, setIframeError] = useState(false);
  const [iframeHeight, setIframeHeight] = useState(MIN_HEIGHT);
  const iframeRef = useRef(null);

  if (!sessionId) return null;

  const handleLoad = () => {
    setLoaded(true);
    // Same-origin: read the rendered chart body height and auto-fit the container.
    try {
      const doc = iframeRef.current?.contentDocument;
      if (doc) {
        const h =
          doc.body?.scrollHeight ||
          doc.documentElement?.scrollHeight ||
          MIN_HEIGHT;
        setIframeHeight(Math.min(Math.max(h + 8, MIN_HEIGHT), MAX_HEIGHT));
      }
    } catch {
      // Cross-origin or permission error: keep the default height
    }
  };

  // Determine which meta chips to show
  const hasMeta = decisionMakerTakeaway || keyFinding || topValues || anomalies || whatItMeans || recommendation || proposedFix;

  return (
    <motion.div
      className="w-full border border-border-default rounded-lg bg-bg-surface shadow-[0_4px_12px_rgba(0,0,0,0.06)] overflow-hidden mt-4 mb-2 flex flex-col"
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      {/* Chart iframe — only when a chart file was generated */}
      {hasChart && (
        <div className="w-full bg-white relative" style={{ height: iframeError ? 'auto' : iframeHeight }}>
          {!loaded && !iframeError && (
            <div className="absolute inset-0 flex items-center justify-center bg-bg-elevated/50">
              <div className="w-6 h-6 border-2 border-border-strong border-t-accent rounded-full animate-spin" />
            </div>
          )}
          {iframeError ? (
            <div className="flex items-center justify-center p-6 text-text-faint text-[13px]">
              Chart could not be loaded.
            </div>
          ) : (
            <iframe
              ref={iframeRef}
              src={`/chart/${sessionId}/${id}`}
              style={{ width: '100%', height: '100%', border: 'none' }}
              className={`transition-opacity duration-300 ${loaded ? 'opacity-100' : 'opacity-0'}`}
              onLoad={handleLoad}
              onError={() => setIframeError(true)}
              title={`Chart for ${analysisType}`}
            />
          )}
        </div>
      )}

      {/* Footer: type label + severity + finding + meta chips */}
      <div className={`p-4 bg-bg-elevated text-[13px] ${hasChart ? 'border-t border-border-subtle' : ''}`}>

        {/* Top row: analysis type label + severity badge */}
        <div className="flex items-center gap-2 mb-1 flex-wrap">
          {(analysisType || id) && (
            <span className="font-mono text-[11px] text-text-faint uppercase font-semibold tracking-wider">
              {(analysisType || id).replace(/_/g, ' ')}
            </span>
          )}
          {severity && severity !== 'info' && <SeverityBadge severity={severity} />}
        </div>

        {/* Top finding */}
        {finding && (
          <div className="text-text-secondary leading-relaxed mb-3">
            {finding}
          </div>
        )}

        {/* Confidence bar */}
        <ConfidenceBar confidence={confidence} />

        {/* Contextual meta chips */}
        {hasMeta && (
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <MetaChip
              icon={Lightbulb}
              label="Key Insight"
              value={decisionMakerTakeaway}
              accent={true}
            />
            <MetaChip
              icon={TrendingUp}
              label="Key Finding"
              value={keyFinding}
              accent={false}
            />
            <MetaChip
              icon={BarChart2}
              label="Notable Values"
              value={topValues}
              accent={false}
            />
            <MetaChip
              icon={AlertTriangle}
              label="Anomalies"
              value={anomalies}
              accent={false}
            />
            <MetaChip
              icon={Activity}
              label="What it means"
              value={whatItMeans}
              accent={false}
            />
            <MetaChip
              icon={Lightbulb}
              label="Recommendation"
              value={recommendation}
              accent={true}
            />
            <MetaChip
              icon={Wrench}
              label="Proposed fix"
              value={proposedFix}
              accent={false}
            />
          </div>
        )}

        {/* Ask about this */}
        <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid #F3F4F6' }}>
          <button
            onClick={() => {
              const label = (analysisType || id || '').replace(/_/g, ' ');
              setPendingMessage(`Tell me more about the ${label} analysis`);
            }}
            style={{
              display: 'inline-flex', alignItems: 'center', gap: 6,
              fontSize: 12, fontWeight: 500, color: '#6366F1',
              background: '#EEF2FF', border: '1px solid #C7D2FE',
              borderRadius: 8, padding: '5px 12px', cursor: 'pointer',
              transition: 'background 0.15s',
            }}
            onMouseEnter={e => e.currentTarget.style.background = '#E0E7FF'}
            onMouseLeave={e => e.currentTarget.style.background = '#EEF2FF'}
          >
            <MessageCircle size={12} strokeWidth={2} />
            Ask about this
          </button>
        </div>
      </div>
    </motion.div>
  );
}
