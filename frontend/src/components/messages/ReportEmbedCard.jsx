import React from 'react';
import { BookOpen, ExternalLink } from 'lucide-react';
import { usePipelineStore } from '../../store/pipelineStore';

export function ReportEmbedCard({ sessionId }) {
  const setCanvasOpen = usePipelineStore((s) => s.setCanvasOpen);

  return (
    <button
      className="w-full mt-4 mb-2 border border-border-default rounded-xl bg-bg-surface shadow-sm overflow-hidden flex items-center justify-between p-4 hover:bg-bg-elevated transition-colors text-left group"
      onClick={() => setCanvasOpen(true)}
    >
      <div className="flex items-center gap-3 text-accent">
        <BookOpen size={20} />
        <span className="font-semibold text-[15px] text-text-primary tracking-tight">Full Analysis Report</span>
      </div>
      <div className="text-text-tertiary group-hover:text-text-secondary transition-colors">
        <ExternalLink size={16} />
      </div>
    </button>
  );
}
