import React from 'react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs) {
  return twMerge(clsx(inputs));
}

export function Badge({ children, variant = 'default', className }) {
  const variants = {
    default: 'bg-bg-surface border-border-subtle text-text-muted',
    success: 'bg-status-success-dim border-[rgba(16,185,129,0.2)] text-status-success',
    warning: 'bg-status-warning-dim border-[rgba(245,158,11,0.2)] text-status-warning',
    error:   'bg-status-error-dim border-[rgba(239,68,68,0.2)] text-status-error',
    info:    'bg-status-info-dim border-[rgba(59,130,246,0.2)] text-status-info',
    accent: 'bg-accent-dim border-[rgba(251,113,133,0.2)] text-accent',
  };

  return (
    <span className={cn(
      "inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border",
      variants[variant],
      className
    )}>
      {children}
    </span>
  );
}
