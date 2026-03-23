import React from 'react';
import { cn } from './Badge';

export function Pill({ children, className }) {
  return (
    <span className={cn(
      "inline-flex items-center px-2 py-0.5 rounded-sm bg-bg-surface border border-border-subtle text-[11px] font-mono font-medium text-text-muted",
      className
    )}>
      {children}
    </span>
  );
}
