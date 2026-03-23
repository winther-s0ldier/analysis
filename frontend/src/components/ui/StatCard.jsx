import React from 'react';

export function StatCard({ label, value, subtext }) {
  return (
    <div className="flex flex-col p-3 bg-bg-surface border border-border-subtle rounded-md shadow-xs">
      <span className="text-[11px] font-mono text-text-faint uppercase tracking-wider mb-1">{label}</span>
      <span className="text-lg font-semibold text-text-primary mb-0.5 leading-tight">{value}</span>
      {subtext && <span className="text-xs text-text-muted">{subtext}</span>}
    </div>
  );
}
