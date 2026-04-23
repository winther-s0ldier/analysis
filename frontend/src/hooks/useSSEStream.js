import { useEffect } from 'react';
import { usePipelineStore } from '../store/pipelineStore';
import { ensureConnection } from '../services/sseManager';

/**
 * Thin React hook that bridges the component lifecycle with the background
 * SSE connection manager.  It does NOT own the connection — the manager does.
 *
 * Responsibilities:
 *   1. Call ensureConnection() when a pipeline is actively running.
 *   2. NO cleanup on unmount — the manager keeps connections alive across
 *      navigation so events continue flowing into the correct session slot.
 */
export function useSSEStream(sessionId) {
  const pipelineRunId = usePipelineStore(
    (s) => s.sessions[sessionId]?.pipelineRunId ?? 0
  );
  const phase = usePipelineStore(
    (s) => s.sessions[sessionId]?.phase
  );

  useEffect(() => {
    if (!sessionId) return;

    // Open / keep a connection if the pipeline is actively running OR a new
    // run was just started. The pipelineRunId>0 guard catches the brief window
    // between startPipelineRun() and setPhase('analyzing') so we never miss
    // early node_started events.
    const isRunning = phase && !['idle', 'complete', 'error'].includes(phase);
    const runKicked = pipelineRunId > 0 && phase !== 'complete' && phase !== 'error';
    if (isRunning || runKicked) {
      ensureConnection(sessionId, pipelineRunId);
      usePipelineStore.getState().setSseConnected(true, sessionId);
    }
    // NO cleanup — manager owns connection lifecycle.
    // Connection self-terminates on stream_end via closeConnection().
  }, [sessionId, pipelineRunId, phase]);
}
