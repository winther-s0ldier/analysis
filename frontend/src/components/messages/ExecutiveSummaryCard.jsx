import React from 'react';
import { ClipboardList, Network } from 'lucide-react';
import { AIMessage } from './AIMessage';

export function ExecutiveSummaryCard({ text }) {
  if (!text) return null;
  return (
    <div className="w-full mt-2 mb-4 bg-bg-surface border border-border-default rounded-lg p-5 shadow-sm">
      <div className="flex items-center gap-2 mb-3 text-status-success">
        <ClipboardList size={20} />
        <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">Executive Summary</h3>
      </div>
      <div className="text-[14px] text-text-secondary leading-relaxed whitespace-pre-wrap">{text}</div>
    </div>
  );
}

export function CrossConnectionsCard({ connections = [] }) {
  if (!connections.length) return null;
  return (
    <div className="w-full mt-2 mb-4">
      <div className="flex items-center gap-2 mb-3 text-accent">
        <Network size={20} />
        <h3 className="text-[15px] font-semibold text-text-primary tracking-tight">Cross-Metric Connections</h3>
      </div>
      <div className="grid grid-cols-1 gap-2">
        {connections.map((conn, idx) => (
          <div key={idx} className="p-3 bg-bg-elevated border border-border-subtle rounded-md text-[13px] text-text-secondary shadow-xs">
            {conn.link || conn.description || conn}
          </div>
        ))}
      </div>
    </div>
  );
}

export function NarrativeCard({ htmlContent }) {
  if (!htmlContent) return null;
  return (
    <AIMessage>
      <div className="w-full prose prose-sm prose-slate max-w-none text-[14px] leading-relaxed text-text-secondary whitespace-pre-wrap" dangerouslySetInnerHTML={{ __html: typeof htmlContent === 'string' ? htmlContent.replace(/<script[\s\S]*?<\/script>/gi, '').replace(/on\w+\s*=\s*["'][^"']*["']/gi, '') : '' }} />
    </AIMessage>
  );
}
