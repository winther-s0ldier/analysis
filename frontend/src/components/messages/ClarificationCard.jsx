import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';

const COLUMN_ROLES = ['entity_col', 'time_col', 'event_col', 'value_col', 'outcome_col'];

export function ClarificationCard({ message, ambiguousNodes = [], columns = [] }) {
  const { sessionId } = usePipelineStore();
  const { addMessage } = useChatStore();
  const [selections, setSelections] = useState({});
  const [submitting, setSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);

  const handleChange = (role, value) => {
    setSelections(prev => ({
      ...prev,
      [role]: value === '(none)' ? undefined : value,
    }));
  };

  const handleSubmit = async () => {
    const columnRoles = {};
    for (const [role, val] of Object.entries(selections)) {
      if (val) columnRoles[role] = val;
    }
    if (Object.keys(columnRoles).length === 0) return;

    setSubmitting(true);
    try {
      const res = await fetch(`/clarify/${sessionId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ column_roles: columnRoles }),
      });
      if (!res.ok) throw new Error(await res.text());
      setSubmitted(true);
      addMessage('ai', 'text', 'Column roles confirmed. Re-running discovery...');
    } catch (err) {
      addMessage('ai', 'text', `Clarification submission failed: ${String(err)}`);
      setSubmitting(false);
    }
  };

  if (submitted) {
    return (
      <motion.div
        className="w-full border border-green-200 rounded-lg bg-green-50 p-4 mt-4 mb-2"
        initial={{ opacity: 0 }} animate={{ opacity: 1 }}
      >
        <span className="text-green-700 text-sm font-medium">Column roles confirmed.</span>
      </motion.div>
    );
  }

  return (
    <motion.div
      className="w-full border border-border-default rounded-lg bg-bg-surface shadow-sm overflow-hidden mt-4 mb-2"
      initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}
    >
      <div className="p-4 border-b border-border-subtle bg-bg-elevated">
        <div className="font-semibold text-sm text-text-primary">Column Role Confirmation Required</div>
        <div className="text-xs text-text-secondary mt-1">{message}</div>
        {ambiguousNodes.length > 0 && (
          <div className="text-xs text-text-muted mt-1">
            Affected analyses: {ambiguousNodes.map(n => n.analysis_type || n).join(', ')}
          </div>
        )}
      </div>

      <div className="p-4 space-y-3">
        {COLUMN_ROLES.map(role => (
          <div key={role} className="flex items-center gap-3">
            <label className="text-xs font-mono text-text-secondary w-28 shrink-0">{role}</label>
            <select
              className="flex-1 text-sm border border-border-default rounded px-2 py-1 bg-bg-surface text-text-primary"
              value={selections[role] || '(none)'}
              onChange={e => handleChange(role, e.target.value)}
            >
              <option value="(none)">(none)</option>
              {columns.map(col => (
                <option key={col} value={col}>{col}</option>
              ))}
            </select>
          </div>
        ))}
      </div>

      <div className="p-4 pt-0">
        <button
          onClick={handleSubmit}
          disabled={submitting || Object.values(selections).filter(Boolean).length === 0}
          className="px-4 py-2 text-sm font-medium rounded-lg bg-accent text-white hover:bg-accent/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {submitting ? 'Submitting...' : 'Confirm & Continue'}
        </button>
      </div>
    </motion.div>
  );
}
