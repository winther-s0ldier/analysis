import React, { useState } from 'react';
import { RefreshCw } from 'lucide-react';
import { rerunSynthesis } from '../../api';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';

export function RerunSynthesisCard() {
  const [val, setVal] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const { sessionId, setPhase, startPipelineRun } = usePipelineStore();
  const { addMessage } = useChatStore();

  const handleRerun = async () => {
    if (!sessionId || submitting) return;
    setSubmitting(true);
    
    if (val.trim()) {
      addMessage('user', 'text', val.trim());
    }

    try {
      startPipelineRun(); // increment pipelineRunId → triggers SSE reconnection in useSSEStream
      setPhase('synthesizing');
      await rerunSynthesis(sessionId, val);
    } catch (e) {
      addMessage('ai', 'text', `Rerun failed: ${e.message}`);
      setPhase('error');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="w-full mt-2 flex items-center gap-2">
      <input 
        type="text" 
        className="flex-1 bg-bg-surface border border-border-default rounded-md px-3 py-2 text-[14px] text-text-primary outline-none focus:border-accent focus:ring-1 focus:ring-accent-dim"
        placeholder="What would you like more of? (optional)"
        value={val}
        onChange={e => setVal(e.target.value)}
        onKeyDown={e => e.key === 'Enter' && handleRerun()}
        disabled={submitting}
      />
      <button 
        onClick={handleRerun}
        disabled={submitting}
        className="flex shrink-0 items-center justify-center gap-2 bg-bg-surface border border-border-default text-text-secondary hover:text-text-primary hover:bg-bg-elevated px-4 py-2 rounded-md font-medium transition-colors disabled:opacity-50"
      >
        <RefreshCw size={16} className={submitting ? "animate-spin" : ""} />
        <span>Re-run Synthesis</span>
      </button>
    </div>
  );
}
