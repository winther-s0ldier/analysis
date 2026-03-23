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
import { PersonasCard } from './PersonasCard';
import { InterventionsCard } from './InterventionsCard';
import { CriticCard } from './CriticCard';
import { ExecutiveSummaryCard, CrossConnectionsCard, NarrativeCard } from './ExecutiveSummaryCard';
import { ReportEmbedCard } from './ReportEmbedCard';
import { RerunSynthesisCard } from './RerunSynthesisCard';
import { RunAnalysisCard } from './RunAnalysisCard';
import { ClarificationCard } from './ClarificationCard';
import { usePipelineStore } from '../../store/pipelineStore';

export function ChatMessage({ message }) {
  const { canvasOpen } = usePipelineStore();

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
          decisionMakerTakeaway={message.payload.decisionMakerTakeaway}
          keyFinding={message.payload.keyFinding}
          topValues={message.payload.topValues}
          anomalies={message.payload.anomalies}
          whatItMeans={message.payload.whatItMeans}
          recommendation={message.payload.recommendation}
          proposedFix={message.payload.proposedFix}
        />
      );
    case 'insights':
      return <InsightCard insights={message.payload} />;
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
    default:
       return null;
  }
}
