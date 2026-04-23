import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ShieldCheck, AlertTriangle, ChevronDown, ChevronUp } from 'lucide-react';

// ── Colors mirror InsightCard's RELIABILITY_STYLE so the UI reads consistently
const BUCKET_STYLE = {
  strong:     { color: '#047857', bg: '#ECFDF5', border: '#6EE7B7', label: 'Strong' },
  suggestive: { color: '#B45309', bg: '#FFFBEB', border: '#FCD34D', label: 'Suggestive' },
  tentative:  { color: '#B91C1C', bg: '#FEF2F2', border: '#FCA5A5', label: 'Tentative' },
};

function ChecksBucket({ bucketKey, items }) {
  if (!items?.length) return null;
  const style = BUCKET_STYLE[bucketKey];
  return (
    <div
      className="rounded-md border px-3 py-2"
      style={{ borderColor: style.border, background: style.bg }}
    >
      <div
        className="text-[10px] font-bold uppercase tracking-wider mb-1.5"
        style={{ color: style.color }}
      >
        {style.label} · {items.length}
      </div>
      <ul className="flex flex-col gap-1">
        {items.map((it, i) => (
          <li key={i} className="text-[12px] text-text-secondary leading-snug">
            <span className="font-mono text-[11px] text-text-muted mr-1.5">
              [{it.node_id || '?'}
              {it.analysis_type ? `: ${it.analysis_type}` : ''}]
            </span>
            {it.one_line || it.description || ''}
          </li>
        ))}
      </ul>
    </div>
  );
}

export function ChecksThatPassedCard({ checks }) {
  const [open, setOpen] = useState(false);
  if (!checks || typeof checks !== 'object') return null;
  const strong = checks.strong || [];
  const suggestive = checks.suggestive || [];
  const tentative = checks.tentative || [];
  const total = strong.length + suggestive.length + tentative.length;
  if (total === 0) return null;

  return (
    <div className="w-full mt-2 mb-3 bg-bg-surface border border-border-default rounded-xl overflow-hidden">
      <button
        className="w-full flex items-center gap-2 px-4 py-3 text-left"
        onClick={() => setOpen(o => !o)}
      >
        <ShieldCheck size={16} className="text-accent shrink-0" />
        <span className="text-[14px] font-semibold text-text-primary flex-1">
          Checks that passed
        </span>
        <span className="text-[11px] font-mono text-text-muted">
          {strong.length}/{suggestive.length}/{tentative.length}
        </span>
        {open
          ? <ChevronUp size={14} className="text-text-muted" />
          : <ChevronDown size={14} className="text-text-muted" />}
      </button>
      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.18 }}
            className="overflow-hidden"
          >
            <div className="px-4 pb-4 pt-1 flex flex-col gap-2 border-t border-border-subtle">
              <ChecksBucket bucketKey="strong"     items={strong} />
              <ChecksBucket bucketKey="suggestive" items={suggestive} />
              <ChecksBucket bucketKey="tentative"  items={tentative} />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export function CaveatsCard({ items }) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return null;

  return (
    <div
      className="w-full mt-2 mb-3 rounded-xl border overflow-hidden"
      style={{ borderColor: '#FCA5A5', background: '#FEF2F2' }}
    >
      <div className="flex items-center gap-2 px-4 py-3 border-b" style={{ borderColor: '#FCA5A5' }}>
        <AlertTriangle size={16} style={{ color: '#B91C1C' }} className="shrink-0" />
        <span className="text-[14px] font-semibold" style={{ color: '#7F1D1D' }}>
          Caveats — what could be wrong
        </span>
        <span className="text-[11px] font-mono ml-auto" style={{ color: '#B91C1C' }}>
          {list.length}
        </span>
      </div>
      <ul className="px-4 py-3 flex flex-col gap-2">
        {list.map((item, i) => {
          const text = typeof item === 'string' ? item : (item.text || item.caveat || JSON.stringify(item));
          return (
            <li key={i} className="flex items-start gap-2 text-[12.5px] text-text-secondary leading-snug">
              <span
                className="shrink-0 w-4 h-4 rounded-full flex items-center justify-center text-[10px] font-bold mt-0.5"
                style={{ background: '#FEE2E2', color: '#B91C1C' }}
              >
                !
              </span>
              {text}
            </li>
          );
        })}
      </ul>
    </div>
  );
}
