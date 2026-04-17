/**
 * Background SSE Connection Manager
 *
 * Owns the lifecycle of SSE connections independently of the React component
 * tree.  Connections survive navigation between sessions and self-terminate
 * on `stream_end`.  Each session+runId pair gets at most one connection.
 */
import { flushSync } from 'react-dom';
import { usePipelineStore } from '../store/pipelineStore';
import { useChatStore } from '../store/chatStore';
import { fetchSynthesis, fetchResults } from '../api';

// ── Connection registry ────────────────────────────────────────────────────
const _connections = new Map(); // sessionId -> { evtSource, runId, pollInterval }

// ── Per-session render lock (replaces the old module-global boolean) ────────
const _renderInProgress = new Map(); // sessionId -> boolean

// ── Per-session dedup sets for charts (per pipeline run) ───────────────────
const _renderedCharts = new Map(); // sessionId -> Set<analysisId>

// ── Per-session synthesis-fetched flag ──────────────────────────────────────
const _synthesisFetched = new Map(); // sessionId -> boolean

// ── Per-session canvas-opened-by-chunk flag ────────────────────────────────
const _canvasOpenedByChunk = new Map(); // sessionId -> boolean

// ── Public API ─────────────────────────────────────────────────────────────

export function ensureConnection(sessionId, runId) {
  if (!sessionId) return;
  const existing = _connections.get(sessionId);
  if (existing && existing.runId === runId) return; // already connected for this run
  if (existing) closeConnection(sessionId);          // stale run → replace

  // Reset per-run tracking
  _renderedCharts.set(sessionId, new Set(
    // Pre-populate from existing messages so restored charts aren't duplicated
    (useChatStore.getState().sessions[sessionId]?.messages || [])
      .filter(m => m.type === 'chart' && m.payload?.id)
      .map(m => m.payload.id)
  ));
  _synthesisFetched.set(sessionId, false);
  _canvasOpenedByChunk.set(sessionId, false);

  const evtSource = new EventSource(`/stream/${sessionId}`);
  _connections.set(sessionId, { evtSource, runId, pollInterval: null });

  evtSource.onmessage = (e) => handleSSEEvent(sessionId, e);

  evtSource.onerror = () => {
    evtSource.close();
    // Replace with polling fallback for THIS session
    startPollingFallback(sessionId, runId);
  };
}

export function closeConnection(sessionId) {
  const conn = _connections.get(sessionId);
  if (!conn) return;
  conn.evtSource.close();
  if (conn.pollInterval) clearInterval(conn.pollInterval);
  _connections.delete(sessionId);
}

export function resetSessionTracking(sessionId) {
  if (!sessionId) return;
  _renderedCharts.delete(sessionId);
  _synthesisFetched.delete(sessionId);
  _canvasOpenedByChunk.delete(sessionId);
  _renderInProgress.delete(sessionId);
}

export function isConnected(sessionId) {
  return _connections.has(sessionId);
}

// ── Chart rendering (per-session lock) ─────────────────────────────────────

function renderNewCharts(sessionId) {
  if (_renderInProgress.get(sessionId)) return Promise.resolve();
  _renderInProgress.set(sessionId, true);
  const renderedCharts = _renderedCharts.get(sessionId) || new Set();

  return fetchResults(sessionId).then(results => {
    if (!Array.isArray(results)) return;
    const chatStore = useChatStore.getState();
    results.forEach(r => {
      const aid = r.analysis_id;
      if (!aid || r.status === 'error') return;
      const chartPayload = {
        id:                     aid,
        analysisType:           r.analysis_type ?? null,
        finding:                r.top_finding ?? null,
        hasChart:               !!r.chart_path,
        severity:               r.severity ?? null,
        confidence:             r.confidence ?? null,
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
        chatStore.addMessage(sessionId, 'ai', 'chart', chartPayload);
      } else {
        chatStore.updateChartMessage(aid, chartPayload, sessionId);
      }
    });
  }).catch(() => {}).finally(() => { _renderInProgress.set(sessionId, false); });
}

// ── Synthesis payload handler ──────────────────────────────────────────────

function handleSynthesisPayload(sessionId, synthesis) {
  const chatStore = useChatStore.getState();
  const pipelineStore = usePipelineStore.getState();

  const insights = Array.isArray(synthesis.detailed_insights)
    ? synthesis.detailed_insights
    : synthesis.detailed_insights?.insights || [];
  const criticChallenges = synthesis._critic_review?.challenges || [];
  if (insights.length) chatStore.addMessage(sessionId, 'ai', 'insights', { insights, criticChallenges });
  if (insights.length) chatStore.addMessage(sessionId, 'ai', 'action_plan', insights);

  const _rawPersonas = synthesis.key_segments ?? synthesis.personas;
  const segments = Array.isArray(_rawPersonas) ? _rawPersonas : _rawPersonas?.segments ?? _rawPersonas?.personas ?? [];
  if (segments.length) chatStore.addMessage(sessionId, 'ai', 'personas', segments);

  const _rawStrat = synthesis.recommendations ?? synthesis.intervention_strategies;
  const strategies = Array.isArray(_rawStrat) ? _rawStrat : _rawStrat?.strategies ?? [];
  if (strategies.length) chatStore.addMessage(sessionId, 'ai', 'interventions', strategies);

  const _rawConns = synthesis.cross_metric_connections;
  const connections = Array.isArray(_rawConns) ? _rawConns : _rawConns?.connections ?? [];
  if (connections.length) chatStore.addMessage(sessionId, 'ai', 'connections', connections);

  if (synthesis.conversational_report && !_canvasOpenedByChunk.get(sessionId)) {
    chatStore.addMessage(sessionId, 'ai', 'narrative', synthesis.conversational_report);
    pipelineStore.setCanvasNarrative(synthesis.conversational_report, sessionId);
  }
}

// ── SSE event handler ──────────────────────────────────────────────────────

function handleSSEEvent(sessionId, e) {
  let ev;
  try { ev = JSON.parse(e.data); } catch { return; }

  const pipelineStore = usePipelineStore.getState();
  const chatStore = useChatStore.getState();

  switch (ev.type) {

    // ── Node lifecycle ──────────────────────────────────────────────────
    case 'node_started':
      flushSync(() => {
        pipelineStore.updateNodeStatus(ev.data.node_id, 'running', null, sessionId);
        chatStore.addSkeleton(ev.data.node_id, ev.data.analysis_type || ev.data.node_id, sessionId);
      });
      break;

    case 'node_complete':
      flushSync(() => {
        pipelineStore.updateNodeStatus(ev.data.analysis_id, 'complete', null, sessionId);
        pipelineStore.setPhase('analyzing', sessionId);
        chatStore.removeSkeleton(ev.data.analysis_id, sessionId);
      });
      renderNewCharts(sessionId);
      break;

    case 'node_failed':
      flushSync(() => {
        pipelineStore.updateNodeStatus(ev.data.node_id || 'unknown', 'failed', ev.data.error || 'unknown', sessionId);
        chatStore.removeSkeleton(ev.data.node_id || 'unknown', sessionId);
      });
      chatStore.addMessage(sessionId, 'ai', 'text',
        `\u26a0\ufe0f Analysis ${ev.data.node_id} could not complete: ${ev.data.error}. Other analyses will continue.`);
      break;

    // ── Synthesis lifecycle ──────────────────────────────────────────────
    case 'synthesis_started': {
      const completedIds = new Set(
        (ev.data.completed_nodes || []).map(n => n.analysis_id)
      );
      flushSync(() => {
        const sessionData = usePipelineStore.getState().sessions[sessionId];
        if (sessionData) {
          sessionData.nodes.forEach(n => {
            if (n.status !== 'complete' && n.status !== 'failed') {
              if (completedIds.has(n.id)) {
                usePipelineStore.getState().updateNodeStatus(n.id, 'complete', null, sessionId);
              } else {
                usePipelineStore.getState().updateNodeStatus(n.id, 'failed', 'Did not complete', sessionId);
              }
            }
          });
          usePipelineStore.getState().setPhase('synthesizing', sessionId);
        }
      });
      renderNewCharts(sessionId).then(() => {
        chatStore.addMessage(sessionId, 'ai', 'text', '\u2713 All analyses complete \u2014 building synthesis\u2026');
      });
      break;
    }

    case 'synthesis_chunk': {
      const current = usePipelineStore.getState().sessions[sessionId]?.canvasNarrative || '';
      pipelineStore.setCanvasNarrative(current + ev.data.chunk, sessionId);
      if (ev.data.first && !_canvasOpenedByChunk.get(sessionId)) {
        _canvasOpenedByChunk.set(sessionId, true);
        pipelineStore.setCanvasOpen(true, sessionId);
        pipelineStore.setPhase('synthesizing', sessionId);
      }
      break;
    }

    case 'synthesis_complete': {
      const synthCompletedIds = new Set(
        (ev.data.completed_nodes || []).map(n => n.analysis_id)
      );
      const sessionData = usePipelineStore.getState().sessions[sessionId];
      if (sessionData) {
        sessionData.nodes.forEach(n => {
          if (n.status !== 'complete' && n.status !== 'failed') {
            if (synthCompletedIds.has(n.id)) {
              usePipelineStore.getState().updateNodeStatus(n.id, 'complete', null, sessionId);
            } else {
              usePipelineStore.getState().updateNodeStatus(n.id, 'failed', 'Did not complete', sessionId);
            }
          }
        });
      }
      pipelineStore.setPhase('synthesizing', sessionId);

      if (!_canvasOpenedByChunk.get(sessionId)) {
        pipelineStore.setCanvasOpen(true, sessionId);
      }

      renderNewCharts(sessionId);

      if (!_synthesisFetched.get(sessionId)) {
        _synthesisFetched.set(sessionId, true);
        fetchSynthesis(sessionId)
          .then((synthesis) => handleSynthesisPayload(sessionId, synthesis))
          .catch(() => {});
      }
      break;
    }

    // ── Report lifecycle ────────────────────────────────────────────────
    case 'report_ready':
      pipelineStore.setHasReport(true, sessionId);
      pipelineStore.setPhase('building_report', sessionId);
      chatStore.addMessage(sessionId, 'ai', 'text', '\u2713 Full report ready \u2014 open the side panel to view.');
      break;

    case 'report_error':
      chatStore.addMessage(sessionId, 'ai', 'text', `\u26a0\ufe0f Report could not be generated: ${ev.data.error}`);
      chatStore.addMessage(sessionId, 'ai', 'rerun', {});
      break;

    // ── Stream lifecycle ────────────────────────────────────────────────
    case 'stream_end':
      closeConnection(sessionId);
      pipelineStore.setSseConnected(false, sessionId);
      pipelineStore.setPhase(ev.data?.status === 'complete' ? 'complete' : 'error', sessionId);
      if (ev.data?.error) {
        chatStore.addMessage(sessionId, 'ai', 'text', `\u26a0\ufe0f ${ev.data.error}`);
      } else if (ev.data?.status !== 'complete') {
        chatStore.addMessage(sessionId, 'ai', 'text', 'Pipeline encountered an error. Some results may be available.');
      }
      break;

    case 'status_update':
      if (ev.data?.status === 'clarification_needed') {
        chatStore.addMessage(sessionId, 'ai', 'clarification', {
          message: ev.data?.message || 'Column role confirmation required.',
          ambiguousNodes: ev.data?.ambiguous_nodes || [],
          columns: ev.data?.columns || [],
        });
      }
      break;

    default:
      if (ev.type && !['turn_started', 'turn_ended', 'tool_called'].includes(ev.type)) {
        console.warn('[SSE] Unhandled event type:', ev.type, ev.data);
      }
      break;
  }
}

// ── Polling fallback ───────────────────────────────────────────────────────

function startPollingFallback(sessionId, runId) {
  const existing = _connections.get(sessionId);
  // Clear any prior polling interval
  if (existing?.pollInterval) clearInterval(existing.pollInterval);

  console.warn(`[SSE] Falling back to polling for session ${sessionId}`);

  const pollStartTime = Date.now();
  const MAX_POLL_TIME = 10 * 60 * 1000;
  const renderedCharts = _renderedCharts.get(sessionId) || new Set();

  const pollInterval = setInterval(async () => {
    if (Date.now() - pollStartTime > MAX_POLL_TIME) {
      clearInterval(pollInterval);
      _connections.delete(sessionId);
      usePipelineStore.getState().setSseConnected(false, sessionId);
      usePipelineStore.getState().setPhase('error', sessionId);
      useChatStore.getState().addMessage(sessionId, 'ai', 'text', 'Pipeline timed out after 10 minutes.');
      return;
    }

    try {
      const statusRes = await fetch(`/status/${sessionId}`);
      if (!statusRes.ok) return;
      const status = await statusRes.json();

      if (status.pipeline?.node_statuses) {
        Object.entries(status.pipeline.node_statuses).forEach(([nid, st]) => {
          usePipelineStore.getState().updateNodeStatus(nid, st, null, sessionId);
        });
      }

      renderNewCharts(sessionId);

      const phaseMap = {
        analyzing: 'analyzing',
        synthesizing: 'synthesizing',
        building_report: 'synthesizing',
        complete: 'complete',
        error: 'error',
      };
      if (phaseMap[status.session_status]) {
        usePipelineStore.getState().setPhase(phaseMap[status.session_status], sessionId);
      }

      if (status.session_status === 'complete' || status.session_status === 'error') {
        clearInterval(pollInterval);
        _connections.delete(sessionId);
        usePipelineStore.getState().setSseConnected(false, sessionId);

        if (status.has_report) usePipelineStore.getState().setHasReport(true, sessionId);

        if (status.session_status === 'complete' && !_synthesisFetched.get(sessionId)) {
          _synthesisFetched.set(sessionId, true);
          fetchSynthesis(sessionId)
            .then((synthesis) => handleSynthesisPayload(sessionId, synthesis))
            .catch(() => {});
        }
        if (status.session_status === 'error') {
          useChatStore.getState().addMessage(sessionId, 'ai', 'text',
            'Pipeline encountered an error. Some results may be available.');
        }
      }
    } catch (err) {
      console.error('[Poll] Error for session', sessionId, err);
    }
  }, 2500);

  // Store the interval so closeConnection can clean it up
  const conn = _connections.get(sessionId);
  if (conn) {
    conn.pollInterval = pollInterval;
  } else {
    _connections.set(sessionId, { evtSource: null, runId, pollInterval });
  }
}
