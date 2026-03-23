import React, { useState } from 'react';
import { Play, Plus, X, Loader2, CheckCircle, AlertCircle } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';
import { analyzeMetrics, validateMetric } from '../../api';
import { toast } from 'sonner';

export function RunAnalysisCard({ sessionId }) {
  const { setPhase, customMetricNodes, addCustomMetricNode, removeCustomMetricNode, startPipelineRun } = usePipelineStore();
  const { addMessage } = useChatStore();
  const [inputVal, setInputVal] = useState('');
  const [validating, setValidating] = useState(false);
  // rejection: null | { reason: string, missing: string[] }
  const [rejection, setRejection] = useState(null);
  const [running, setRunning] = useState(false);
  const [done, setDone] = useState(false);

  const addMetric = async () => {
    const v = inputVal.trim();
    if (!v || validating) return;
    setRejection(null);
    setValidating(true);
    try {
      const res = await validateMetric(sessionId, v);
      const val = res.validation;
      if (val?.valid) {
        // Assign a stable ID based on current count so it matches what backend will use
        const newId = `C${customMetricNodes.length + 1}`;
        addCustomMetricNode({
          id:            newId,
          name:          val.metric_name || v,
          description:   val.description || '',
          analysis_type: val.analysis_type || 'custom',
          column_roles:  val.column_roles || {},
          priority:      'medium',
        });
        setInputVal('');
      } else {
        setRejection({
          reason:  val?.reason || 'This metric cannot be computed from the available data.',
          missing: val?.missing_requirements || [],
        });
      }
    } catch {
      toast.error('Could not validate metric — check connection.');
    } finally {
      setValidating(false);
    }
  };

  const removeMetric = (id) => removeCustomMetricNode(id);

  const handleRun = async () => {
    if (running || done) return;
    setRunning(true);
    try {
      startPipelineRun(); // increment pipelineRunId → triggers SSE reconnection
      addMessage('ai', 'terminal', []);
      // Pass full structured node specs — backend appends them to state.dag
      await analyzeMetrics(
        sessionId,
        'Analyze all discovered metrics.',
        customMetricNodes.map(n => ({
          id:            n.id,
          name:          n.name,
          analysis_type: n.analysis_type,
          description:   n.description,
          column_roles:  n.column_roles || {},
          priority:      n.priority || 'medium',
        }))
      );
      setPhase('analyzing');
      toast.success('Analysis started');
      setDone(true);
    } catch (err) {
      toast.error(`Failed to start analysis: ${err.message}`);
      setRunning(false);
    }
  };

  if (done) {
    return (
      <div className="text-[12px] text-text-muted font-mono py-1 opacity-60">Analysis running…</div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, ease: [0.165, 0.84, 0.44, 1] }}
      className="w-full mt-1"
    >
      <div
        className="rounded-2xl border overflow-hidden"
        style={{ background: '#FFFFFF', borderColor: '#E5E7EB', boxShadow: '0 1px 4px rgba(0,0,0,0.04)' }}
      >
        {/* Header */}
        <div className="px-4 pt-4 pb-3 border-b" style={{ borderColor: '#F3F4F6' }}>
          <div className="text-[13.5px] font-semibold text-text-primary mb-0.5">Ready to run analysis</div>
          <div className="text-[12px] text-text-tertiary">
            Add custom metrics below, or run the discovered plan as-is.
          </div>
        </div>

        {/* Custom metric input */}
        <div className="px-4 py-3">
          <div className="flex gap-2">
            <input
              type="text"
              value={inputVal}
              onChange={e => { setInputVal(e.target.value); setRejection(null); }}
              onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); addMetric(); } }}
              placeholder="e.g. revenue by region, churn by cohort…"
              disabled={validating}
              className="flex-1 px-3 py-2 text-[13px] rounded-lg border outline-none transition-all"
              style={{ background: '#F9FAFB', borderColor: '#E5E7EB', color: '#111827' }}
              onFocus={e => { e.target.style.borderColor = 'rgba(99,102,241,0.5)'; e.target.style.boxShadow = '0 0 0 3px rgba(99,102,241,0.08)'; }}
              onBlur={e => { e.target.style.borderColor = '#E5E7EB'; e.target.style.boxShadow = 'none'; }}
            />
            <button
              onClick={addMetric}
              disabled={validating || !inputVal.trim()}
              className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-[12.5px] font-medium border transition-all"
              style={{
                background: validating ? '#F9FAFB' : '#F3F4F6',
                borderColor: '#E5E7EB',
                color: '#374151',
                opacity: !inputVal.trim() ? 0.5 : 1,
              }}
              onMouseEnter={e => { if (!validating && inputVal.trim()) e.currentTarget.style.background = '#E5E7EB'; }}
              onMouseLeave={e => { e.currentTarget.style.background = validating ? '#F9FAFB' : '#F3F4F6'; }}
            >
              {validating
                ? <Loader2 size={14} strokeWidth={2.5} className="animate-spin" />
                : <Plus size={14} strokeWidth={2.5} />
              }
              {validating ? 'Checking…' : 'Add'}
            </button>
          </div>

          {/* Rejection message — shows reason + what data is missing */}
          <AnimatePresence>
            {rejection && (
              <motion.div
                initial={{ opacity: 0, y: -4 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -4 }}
                transition={{ duration: 0.2 }}
                className="flex items-start gap-2 mt-2 px-3 py-2.5 rounded-lg text-[12px] leading-relaxed"
                style={{ background: 'rgba(239,68,68,0.06)', color: '#B91C1C', border: '1px solid rgba(239,68,68,0.2)' }}
              >
                <AlertCircle size={13} strokeWidth={2} className="shrink-0 mt-0.5" />
                <div className="flex-1">
                  <div>{rejection.reason}</div>
                  {rejection.missing?.length > 0 && (
                    <div className="mt-1.5">
                      <div className="text-[11px] font-semibold mb-1 opacity-70">Missing data required:</div>
                      <ul className="space-y-0.5">
                        {rejection.missing.map((m, i) => (
                          <li key={i} className="flex items-start gap-1.5 text-[11.5px]">
                            <span className="opacity-50 mt-0.5 shrink-0">•</span>
                            <span>{m}</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              </motion.div>
            )}
          </AnimatePresence>

          {/* Validated metric chips — driven by store so they appear in DiscoveryCard too */}
          {customMetricNodes.length > 0 && (
            <div className="flex flex-col gap-2 mt-2.5">
              {customMetricNodes.map((m) => (
                <motion.div
                  key={m.id}
                  initial={{ opacity: 0, scale: 0.97 }}
                  animate={{ opacity: 1, scale: 1 }}
                  className="flex items-start gap-2 px-3 py-2 rounded-xl border"
                  style={{ background: 'rgba(99,102,241,0.04)', borderColor: 'rgba(99,102,241,0.2)' }}
                >
                  <CheckCircle size={13} strokeWidth={2.5} className="shrink-0 mt-0.5" style={{ color: '#6366F1' }} />
                  <div className="flex-1 min-w-0">
                    <div className="text-[12.5px] font-semibold" style={{ color: '#6366F1' }}>{m.name}</div>
                    {m.description && (
                      <div className="text-[11.5px] mt-0.5 leading-relaxed" style={{ color: '#6B7280' }}>{m.description}</div>
                    )}
                  </div>
                  <button onClick={() => removeMetric(m.id)} className="shrink-0 opacity-40 hover:opacity-80 transition-opacity mt-0.5">
                    <X size={12} strokeWidth={2.5} style={{ color: '#6366F1' }} />
                  </button>
                </motion.div>
              ))}
            </div>
          )}
        </div>

        {/* Run button */}
        <div className="px-4 pb-4">
          <button
            onClick={handleRun}
            disabled={running}
            className="flex items-center gap-2 px-4 py-2.5 rounded-xl text-[13.5px] font-semibold text-white transition-all w-full justify-center"
            style={{
              background: running ? '#9CA3AF' : '#6366F1',
              boxShadow: running ? 'none' : '0 2px 8px rgba(99,102,241,0.3)',
            }}
            onMouseEnter={e => { if (!running) e.currentTarget.style.background = '#4F46E5'; }}
            onMouseLeave={e => { if (!running) e.currentTarget.style.background = '#6366F1'; }}
          >
            <Play size={15} strokeWidth={2.5} className={running ? 'animate-pulse' : ''} />
            {running ? 'Starting…' : 'Run Analysis'}
          </button>
        </div>
      </div>
    </motion.div>
  );
}
