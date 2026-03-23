import { useEffect, useRef } from 'react';
import { flushSync } from 'react-dom';
import { usePipelineStore } from '../store/pipelineStore';
import { useChatStore } from '../store/chatStore';
import { fetchSynthesis, fetchResults } from '../api';


/**
 * Concurrency guard — prevents concurrent /results fetches from piling up
 * when multiple node_complete events arrive in rapid succession.
 * Mirrors GitHub app.js _renderInProgress (line 847).
 */
let _renderInProgress = false;

/**
 * Helper: fetch GET /results/{sessionId} and render any new chart cards.
 * Mirrors the GitHub version's renderNewCharts() — idempotent pull model.
 */
function renderNewCharts(sessionId, renderedCharts, addMessage, updateChartMessage) {
  if (_renderInProgress) return Promise.resolve();
  _renderInProgress = true;
  return fetchResults(sessionId).then(results => {
    if (!Array.isArray(results)) return;
    results.forEach(r => {
      const aid = r.analysis_id;
      if (!aid || r.status === 'error') return;
      const chartPayload = {
        id:                     aid,
        analysisType:           r.analysis_type ?? null,
        finding:                r.top_finding ?? null,
        hasChart:               !!r.chart_path,
        severity:               r.severity ?? null,
        whatItMeans:             r.narrative?.what_it_means ?? null,
        recommendation:         r.insight_summary?.recommendation ?? null,
        proposedFix:            r.narrative?.proposed_fix ?? null,
        decisionMakerTakeaway:  r.insight_summary?.decision_maker_takeaway ?? null,
        keyFinding:             r.insight_summary?.key_finding ?? null,
        topValues:              r.insight_summary?.top_values ?? null,
        anomalies:              r.insight_summary?.anomalies ?? null,
      };
      if (!renderedCharts.has(aid)) {
        renderedCharts.add(aid);
        addMessage('ai', 'chart', chartPayload);
      } else {
        updateChartMessage(aid, chartPayload);
      }
    });
  }).catch(() => {}).finally(() => { _renderInProgress = false; });
}


export function useSSEStream(sessionId) {
  const { setPhase, updateNodeStatus, setHasReport, setSseConnected, setCanvasOpen, setCanvasNarrative, pipelineRunId } = usePipelineStore();
  const { addMessage, updateChartMessage, addOrUpdateChart } = useChatStore();
  const sseRef = useRef(null);
  const synthesisFetchedRef = useRef(false);
  const canvasOpenedByChunkRef = useRef(false);
  const pollIntervalRef = useRef(null);

  useEffect(() => {
    if (!sessionId) return;

    setSseConnected(true);
    synthesisFetchedRef.current = false;
    canvasOpenedByChunkRef.current = false;
    const evtSource = new EventSource(`/stream/${sessionId}`);
    sseRef.current = evtSource;

    // Local closure set for chart card dedup — immune to Zustand stale-state
    const renderedCharts = new Set();

    /** Shared helper: fetch and render synthesis payload into chat messages */
    const handleSynthesisPayload = (synthesis) => {
      const insights = Array.isArray(synthesis.detailed_insights)
        ? synthesis.detailed_insights
        : synthesis.detailed_insights?.insights || [];
      if (insights.length) addMessage('ai', 'insights', insights);

      const _rawPersonas = synthesis.key_segments ?? synthesis.personas;
      const segments = Array.isArray(_rawPersonas)
        ? _rawPersonas
        : _rawPersonas?.segments ?? _rawPersonas?.personas ?? [];
      if (segments.length) addMessage('ai', 'personas', segments);

      const _rawStrat = synthesis.recommendations ?? synthesis.intervention_strategies;
      const strategies = Array.isArray(_rawStrat)
        ? _rawStrat
        : _rawStrat?.strategies ?? [];
      if (strategies.length) addMessage('ai', 'interventions', strategies);

      const _rawConns = synthesis.cross_metric_connections;
      const connections = Array.isArray(_rawConns)
        ? _rawConns
        : _rawConns?.connections ?? [];
      if (connections.length) addMessage('ai', 'connections', connections);

      if (synthesis.executive_summary) addMessage('ai', 'summary', synthesis.executive_summary);

      if (synthesis.conversational_report && !canvasOpenedByChunkRef.current) {
        addMessage('ai', 'narrative', synthesis.conversational_report);
        setCanvasNarrative(synthesis.conversational_report);
      }

      if (synthesis._critic_review) addMessage('ai', 'critic', synthesis._critic_review);
    };

    evtSource.onmessage = (e) => {
      let ev;
      try {
        ev = JSON.parse(e.data);
      } catch {
        return;
      }

      switch (ev.type) {

        // ── Node lifecycle ────────────────────────────────────────────────────
        case 'node_started':
          flushSync(() => {
            updateNodeStatus(ev.data.node_id, 'running');
          });
          break;

        case 'node_complete': {
          flushSync(() => {
            updateNodeStatus(ev.data.analysis_id, 'complete');
            setPhase('analyzing');
          });

          // Pull model (matches GitHub renderNewCharts): fetch ALL results from
          // server and render any new charts. The SSE event is just a nudge.
          renderNewCharts(sessionId, renderedCharts, addMessage, updateChartMessage);
          break;
        }

        case 'node_failed':
          flushSync(() => {
            updateNodeStatus(ev.data.node_id || 'unknown', 'failed', ev.data.error || 'unknown');
          });
          addMessage('ai', 'text', `⚠️ Analysis ${ev.data.node_id} could not complete: ${ev.data.error}. Other analyses will continue.`);
          break;

        // ── Synthesis lifecycle ───────────────────────────────────────────────
        case 'synthesis_started': {
          // Only mark nodes as complete if they're in the completed_nodes payload.
          // This prevents masking real failures with fake "done" status.
          const completedIds = new Set(
            (ev.data.completed_nodes || []).map(n => n.analysis_id)
          );
          flushSync(() => {
            usePipelineStore.getState().nodes.forEach(n => {
              if (n.status !== 'complete' && n.status !== 'failed') {
                if (completedIds.has(n.id)) {
                  updateNodeStatus(n.id, 'complete');
                } else {
                  updateNodeStatus(n.id, 'failed', 'Did not complete');
                }
              }
            });
            setPhase('synthesizing');
          });

          // Final pull: ensure ALL chart cards are visible before showing the
          // "All analyses complete" message. This is the last safety net.
          renderNewCharts(sessionId, renderedCharts, addMessage, updateChartMessage)
            .then(() => {
              addMessage('ai', 'text', '✓ All analyses complete — building synthesis…');
            });
          break;
        }

        case 'synthesis_chunk': {
          const current = usePipelineStore.getState().canvasNarrative || '';
          setCanvasNarrative(current + ev.data.chunk);
          if (ev.data.first && !canvasOpenedByChunkRef.current) {
            canvasOpenedByChunkRef.current = true;
            setCanvasOpen(true);
            setPhase('synthesizing');
          }
          break;
        }

        case 'synthesis_complete': {
          // Only mark nodes that are in completed_nodes payload
          const synthCompletedIds = new Set(
            (ev.data.completed_nodes || []).map(n => n.analysis_id)
          );
          usePipelineStore.getState().nodes.forEach(n => {
            if (n.status !== 'complete' && n.status !== 'failed') {
              if (synthCompletedIds.has(n.id)) {
                updateNodeStatus(n.id, 'complete');
              } else {
                updateNodeStatus(n.id, 'failed', 'Did not complete');
              }
            }
          });
          setPhase('synthesizing');

          if (!canvasOpenedByChunkRef.current) {
            setCanvasOpen(true);
          }

          // Final chart back-fill via pull model
          renderNewCharts(sessionId, renderedCharts, addMessage, updateChartMessage);

          // Fetch full synthesis payload
          if (!synthesisFetchedRef.current) {
            synthesisFetchedRef.current = true;
            fetchSynthesis(sessionId)
              .then(handleSynthesisPayload)
              .catch(() => {});
          }
          break;
        }

        // ── Report lifecycle ──────────────────────────────────────────────────
        case 'report_ready':
          setHasReport(true);
          setPhase('building_report');
          addMessage('ai', 'report', { sessionId });
          addMessage('ai', 'text', '✓ Full report ready — open the side panel to view.');
          break;

        case 'report_error':
          addMessage('ai', 'text', `⚠️ Report could not be generated: ${ev.data.error}`);
          addMessage('ai', 'rerun', {});
          break;

        // ── Stream lifecycle ──────────────────────────────────────────────────
        case 'stream_end':
          evtSource.close();
          setSseConnected(false);
          setPhase(ev.data?.status === 'complete' ? 'complete' : 'error');
          if (ev.data?.error) {
            addMessage('ai', 'text', `⚠️ ${ev.data.error}`);
          } else if (ev.data?.status !== 'complete') {
            addMessage('ai', 'text', 'Pipeline encountered an error. Some results may be available.');
          }
          break;

        case 'status_update':
          if (ev.data?.status === 'clarification_needed') {
            addMessage('ai', 'clarification', {
              message: ev.data?.message || 'Column role confirmation required.',
              ambiguousNodes: ev.data?.ambiguous_nodes || [],
              columns: ev.data?.columns || [],
            });
          }
          break;
      }
    };

    // ── SSE error: fall back to polling (matches GitHub app.js line 567) ──
    evtSource.onerror = () => {
      evtSource.close();
      sseRef.current = null;
      console.warn('SSE error/closed, falling back to polling');

      // Clear any existing poll interval
      if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);

      const pollStartTime = Date.now();
      const MAX_POLL_TIME = 10 * 60 * 1000; // 10 minutes

      pollIntervalRef.current = setInterval(async () => {
        // Timeout after 10 minutes
        if (Date.now() - pollStartTime > MAX_POLL_TIME) {
          clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = null;
          setSseConnected(false);
          setPhase('error');
          addMessage('ai', 'text', 'Pipeline timed out after 10 minutes.');
          return;
        }

        try {
          const statusRes = await fetch(`/status/${sessionId}`);
          if (!statusRes.ok) return;
          const status = await statusRes.json();

          // Update node statuses from poll
          if (status.pipeline?.node_statuses) {
            Object.entries(status.pipeline.node_statuses).forEach(([nid, st]) => {
              updateNodeStatus(nid, st);
            });
          }

          // Render any new charts
          renderNewCharts(sessionId, renderedCharts, addMessage, updateChartMessage);

          // Map session_status to phase
          const phaseMap = {
            analyzing: 'analyzing',
            synthesizing: 'synthesizing',
            building_report: 'synthesizing',
            complete: 'complete',
            error: 'error',
          };
          if (phaseMap[status.session_status]) setPhase(phaseMap[status.session_status]);

          // Stop polling on terminal states
          if (status.session_status === 'complete' || status.session_status === 'error') {
            clearInterval(pollIntervalRef.current);
            pollIntervalRef.current = null;
            setSseConnected(false);

            if (status.has_report) setHasReport(true);

            if (status.session_status === 'complete' && !synthesisFetchedRef.current) {
              synthesisFetchedRef.current = true;
              fetchSynthesis(sessionId)
                .then(handleSynthesisPayload)
                .catch(() => {});
            }
            if (status.session_status === 'error') {
              addMessage('ai', 'text', 'Pipeline encountered an error. Some results may be available.');
            }
          }
        } catch (err) {
          console.error('Poll error:', err);
        }
      }, 2500);
    };

    return () => {
      if (sseRef.current) {
        sseRef.current.close();
        setSseConnected(false);
      }
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    };
  }, [sessionId, pipelineRunId]);
}
