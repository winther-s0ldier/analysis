import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { usePipelineStore } from '../../store/pipelineStore';

const STATUS_MESSAGES = {
  uploading: [
    'Reading file headers…', 'Validating file format…', 'Normalizing encoding…',
    'Detecting delimiter…', 'Scanning row count…', 'Preparing upload buffer…',
  ],
  profiling: [
    'Scanning column types…', 'Computing distributions…', 'Mapping schema structure…',
    'Identifying date columns…', 'Detecting categorical fields…', 'Computing null ratios…',
    'Estimating cardinality…', 'Analyzing value distributions…', 'Building data profile…',
  ],
  discovering: [
    'Mapping analytical pathways…', 'Building dependency graph…', 'Scoring metric relevance…',
    'Selecting analysis types…', 'Optimizing execution order…', 'Validating column roles…',
    'Identifying key dimensions…', 'Planning analysis strategy…',
  ],
  analyzing: [
    'Running pipeline…', 'Computing models…', 'Generating chart artifacts…',
    'Executing DAG nodes…', 'Aggregating node outputs…', 'Validating results…',
    'Building chart visualizations…', 'Computing statistical tests…', 'Detecting patterns…',
    'Running analysis functions…', 'Scoring severity levels…',
  ],
  synthesizing: [
    'Aggregating analysis results…', 'Extracting key findings…', 'Generating summary…',
    'Cross-referencing metrics…', 'Building narrative thread…', 'Scoring recommendations…',
    'Grounding claims to data…', 'Drafting executive summary…',
  ],
  building_report: [
    'Assembling report sections…', 'Embedding chart visualizations…', 'Finalizing HTML report…',
    'Publishing insights…', 'Generating downloadable report…',
  ],
};

export function ProcessingStatus() {
  const phase = usePipelineStore((s) => s.sessions[s.currentSessionId]?.phase ?? 'idle');
  const [msgIndex, setMsgIndex] = useState(0);

  const messages = STATUS_MESSAGES[phase] || [];

  useEffect(() => {
    setMsgIndex(0);
    if (messages.length === 0) return;
    const interval = setInterval(() => {
      setMsgIndex((idx) => (idx + 1) % messages.length);
    }, 2500);
    return () => clearInterval(interval);
  }, [phase, messages.length]);

  const isVisible = messages.length > 0 && phase !== 'idle' && phase !== 'complete' && phase !== 'error';

  return (
    <AnimatePresence>
      {isVisible && (
        <motion.div
          className="flex items-center gap-2 px-6 py-2 shrink-0 max-w-[720px] w-full mx-auto"
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: 'auto' }}
          exit={{ opacity: 0, height: 0 }}
        >
          <div className="w-3 h-3 border-[1.5px] border-border-default border-t-accent rounded-full animate-spin shrink-0" />
          <AnimatePresence mode="wait">
            <motion.span
              key={msgIndex}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.15 }}
              className="font-mono text-[12px] text-text-muted"
            >
              {messages[msgIndex] || 'Processing...'}
            </motion.span>
          </AnimatePresence>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
