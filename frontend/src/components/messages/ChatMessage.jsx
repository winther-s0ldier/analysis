import React from 'react';
import { motion } from 'framer-motion';
import { UserBubble } from './UserBubble';
import { AITextMessage } from './AIMessage';
import { AIMessage } from './AIMessage';
import { ProfileCard } from './ProfileCard';
import { DiscoveryCard } from './DiscoveryCard';
import { TerminalCard } from './TerminalCard';
import { ChartCard } from './ChartCard';
import { InsightCard } from './InsightCard';
import { ActionPlanCard } from './ActionPlanCard';
import { PersonasCard } from './PersonasCard';
import { InterventionsCard } from './InterventionsCard';
import { CriticCard } from './CriticCard';
import { ExecutiveSummaryCard, CrossConnectionsCard, NarrativeCard } from './ExecutiveSummaryCard';
import { ReportEmbedCard } from './ReportEmbedCard';
import { RerunSynthesisCard } from './RerunSynthesisCard';
import { RunAnalysisCard } from './RunAnalysisCard';
import { ClarificationCard } from './ClarificationCard';
import { usePipelineStore } from '../../store/pipelineStore';

// ── Skeleton card shown while a node is executing ─────────────────────────────
function SkeletonChartCard({ analysisType }) {
  const label = (analysisType || '').replace(/_/g, ' ');
  return (
    <motion.div
      className="w-full rounded-xl overflow-hidden mt-4 mb-2"
      style={{ border: '1px solid #E5E7EB', background: '#fff' }}
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.25 }}
    >
      {/* Shimmer chart area */}
      <div style={{ height: 220, background: '#F9FAFB', overflow: 'hidden', position: 'relative' }}>
        <div style={{
          position: 'absolute', inset: 0,
          background: 'linear-gradient(90deg, #F3F4F6 25%, #E9EAEC 50%, #F3F4F6 75%)',
          backgroundSize: '200% 100%',
          animation: 'shimmer 1.4s infinite',
        }} />
        {/* Fake bar chart silhouette */}
        <div style={{ position: 'absolute', bottom: 24, left: 32, right: 32, display: 'flex', alignItems: 'flex-end', gap: 10 }}>
          {[60, 85, 45, 70, 55, 90, 40].map((h, i) => (
            <div key={i} style={{ flex: 1, height: h, background: '#E5E7EB', borderRadius: '4px 4px 0 0', opacity: 0.6 }} />
          ))}
        </div>
      </div>
      {/* Footer */}
      <div style={{ padding: '12px 16px', borderTop: '1px solid #F3F4F6' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
          <div style={{ height: 9, width: 90, background: '#F3F4F6', borderRadius: 4 }} />
          <div style={{ height: 9, width: 48, background: '#F3F4F6', borderRadius: 9999 }} />
        </div>
        <div style={{ height: 8, width: '65%', background: '#F3F4F6', borderRadius: 4, marginBottom: 5 }} />
        <div style={{ height: 8, width: '45%', background: '#F3F4F6', borderRadius: 4 }} />
        {label && (
          <div style={{ marginTop: 10, fontSize: 11, color: '#9CA3AF', fontStyle: 'italic' }}>
            Running {label}…
          </div>
        )}
      </div>
      <style>{`@keyframes shimmer { 0%{background-position:200% 0} 100%{background-position:-200% 0} }`}</style>
    </motion.div>
  );
}

export function ChatMessage({ message }) {
  const canvasOpen = usePipelineStore((s) => s.sessions[s.currentSessionId]?.canvasOpen ?? false);

  if (message.role === 'user') {
    if (message.type === 'canvas_question') {
      return (
        <motion.div
          className="flex justify-end w-full mb-4"
          initial={{ opacity: 0, y: 10, scale: 0.97 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          transition={{ duration: 0.3, ease: [0.165, 0.84, 0.44, 1] }}
        >
          <div style={{ display:'flex', flexDirection:'column', gap:5, maxWidth:'85%' }}>
            <div style={{
              fontSize:12, color:'#4B5563', fontStyle:'italic',
              borderLeft:'3px solid #6366F1', paddingLeft:10, lineHeight:1.5,
              background:'rgba(99,102,241,0.06)', borderRadius:'0 6px 6px 0',
              padding:'6px 10px',
            }}>
              "{message.payload.selectedText.length > 200
                ? message.payload.selectedText.slice(0,200)+'…'
                : message.payload.selectedText}"
            </div>
            <div style={{
              alignSelf:'flex-end',
              background:'#111827', color:'#F9FAFB',
              padding:'8px 14px', borderRadius:16, borderTopRightRadius:4,
              fontSize:14, fontWeight:500, lineHeight:1.5,
              boxShadow:'0 2px 8px rgba(0,0,0,0.12)',
            }}>
              {message.payload.question}
            </div>
          </div>
        </motion.div>
      );
    }
    return <UserBubble message={message} />;
  }

  switch(message.type) {
    case 'text':
      return <AITextMessage payload={message.payload} />;
    case 'terminal':
      return <AIMessage><TerminalCard totalNodes={message.payload?.length || 0} /></AIMessage>;
    case 'profile':
      return <AIMessage><ProfileCard data={message.payload} /></AIMessage>;
    case 'discovery':
      return <AIMessage><DiscoveryCard data={message.payload} /></AIMessage>;
    case 'chart':
      return (
        <ChartCard
          id={message.payload.id}
          analysisType={message.payload.analysisType}
          finding={message.payload.finding}
          hasChart={message.payload.hasChart}
          severity={message.payload.severity}
          confidence={message.payload.confidence}
          decisionMakerTakeaway={message.payload.decisionMakerTakeaway}
          keyFinding={message.payload.keyFinding}
          topValues={message.payload.topValues}
          anomalies={message.payload.anomalies}
          whatItMeans={message.payload.whatItMeans}
          recommendation={message.payload.recommendation}
          proposedFix={message.payload.proposedFix}
        />
      );
    case 'insights': {
      // payload is either an array (old) or { insights, criticChallenges } (new)
      const insightArr = Array.isArray(message.payload) ? message.payload : (message.payload?.insights || []);
      const challenges = Array.isArray(message.payload) ? [] : (message.payload?.criticChallenges || []);
      return <InsightCard insights={insightArr} criticChallenges={challenges} />;
    }
    case 'action_plan':
      return <ActionPlanCard insights={Array.isArray(message.payload) ? message.payload : []} />;
    case 'personas':
      return <PersonasCard personas={message.payload} />;
    case 'interventions':
      return <InterventionsCard interventions={message.payload} />;
    case 'connections':
      return <CrossConnectionsCard connections={message.payload} />;
    case 'summary':
      return <ExecutiveSummaryCard text={message.payload} />;
    case 'narrative':
      if (canvasOpen) return null;
      return <NarrativeCard htmlContent={message.payload} />;
    case 'critic':
      return <CriticCard critic={message.payload} />;
    case 'report':
      if (canvasOpen) return null;
      return <ReportEmbedCard sessionId={message.payload.sessionId} />;
    case 'rerun':
      return <RerunSynthesisCard />;
    case 'run_analysis':
      return <AIMessage><RunAnalysisCard sessionId={message.payload?.sessionId} /></AIMessage>;
    case 'clarification':
      return (
        <ClarificationCard
          message={message.payload.message}
          ambiguousNodes={message.payload.ambiguousNodes}
          columns={message.payload.columns}
        />
      );
    case 'skeleton':
      return <SkeletonChartCard analysisType={message.payload?.analysisType} />;
    case 'date_separator':
      return (
        <div style={{
          display: 'flex', alignItems: 'center', gap: 10,
          padding: '16px 0 8px', userSelect: 'none',
        }}>
          <div style={{ flex: 1, height: 1, background: 'rgba(0,0,0,0.08)' }} />
          <span style={{
            fontSize: 11, fontWeight: 600, color: 'rgba(0,0,0,0.35)',
            letterSpacing: '0.04em', whiteSpace: 'nowrap',
          }}>
            {message.payload?.date || ''}
          </span>
          <div style={{ flex: 1, height: 1, background: 'rgba(0,0,0,0.08)' }} />
        </div>
      );
    default:
       return null;
  }
}
