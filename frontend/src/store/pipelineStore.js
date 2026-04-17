import { create } from 'zustand';

const DEFAULT_SESSION = {
  outputFolder: null,
  phase: 'idle',
  nodes: [],
  customMetricNodes: [],
  hasReport: false,
  sseConnected: false,
  canvasOpen: false,
  canvasNarrative: null,
  pipelineRunId: 0,
};

// Stable empty-session reference — prevents spurious re-renders when no session is active.
const EMPTY_SESSION = { ...DEFAULT_SESSION };

// Helper: immutably update a single session entry.
function updateSession(state, sid, patch) {
  const session = state.sessions[sid];
  if (!session) return {};
  return {
    sessions: {
      ...state.sessions,
      [sid]: { ...session, ...(typeof patch === 'function' ? patch(session) : patch) },
    },
  };
}

export const usePipelineStore = create((set, get) => ({
  // ── Which session the user is currently viewing ──────────────────────────
  currentSessionId: null,

  // ── Per-session state keyed by sessionId ─────────────────────────────────
  sessions: {},

  // ── UI-global (not per-session) ──────────────────────────────────────────
  sidebarCollapsed: false,
  historyOpen: false,
  lastStartedPipelineSessionId: null, // persists across navigation

  // ── Session management ───────────────────────────────────────────────────

  // Create-or-reset: always start with a fresh session slot.
  // Only InputArea calls this (for new uploads). HistoryPanel uses restoreSessionState().
  setSession: (sessionId, outputFolder) => set((state) => ({
    currentSessionId: sessionId,
    sessions: {
      ...state.sessions,
      [sessionId]: { ...DEFAULT_SESSION, outputFolder },
    },
  })),

  // Atomically rename a session key (temp → real) after an upload resolves.
  // Preserves all accumulated state (messages, phase, nodes) and updates outputFolder.
  migrateSession: (fromId, toId, outputFolder) => set((state) => {
    const session = state.sessions[fromId];
    if (!session) return {};
    const { [fromId]: _, ...rest } = state.sessions;
    return {
      currentSessionId: state.currentSessionId === fromId ? toId : state.currentSessionId,
      sessions: {
        ...rest,
        [toId]: { ...session, outputFolder: outputFolder ?? session.outputFolder },
      },
    };
  }),

  // Switch view to an existing session without modifying its data.
  switchSession: (sessionId) => set({ currentSessionId: sessionId }),

  // Populate a session slot with full restored data (history restore, page reload).
  restoreSessionState: (sessionId, data) => set((state) => ({
    currentSessionId: sessionId,
    sessions: {
      ...state.sessions,
      [sessionId]: {
        ...(state.sessions[sessionId] || { ...DEFAULT_SESSION }),
        outputFolder: data.outputFolder ?? data.output_folder ?? null,
        phase: data.phase || 'complete',
        nodes: data.nodes || [],
        hasReport: data.hasReport ?? data.has_report ?? false,
        canvasNarrative: data.canvasNarrative ?? data.canvas_narrative ?? null,
        canvasOpen: !!(data.canvasNarrative ?? data.canvas_narrative),
      },
    },
  })),

  // ── Per-session setters (targetSessionId defaults to currentSessionId) ───

  setPhase: (phase, targetSessionId) => set((state) => {
    const sid = targetSessionId || state.currentSessionId;
    return sid ? updateSession(state, sid, { phase }) : {};
  }),

  setNodes: (nodes, targetSessionId) => set((state) => {
    const sid = targetSessionId || state.currentSessionId;
    return sid ? updateSession(state, sid, { nodes }) : {};
  }),

  updateNodeStatus: (nodeId, status, error = null, targetSessionId) => set((state) => {
    const sid = targetSessionId || state.currentSessionId;
    if (!sid || !state.sessions[sid]) return {};
    const session = state.sessions[sid];
    const exists = session.nodes.some(n => n.id === nodeId);
    const nodes = exists
      ? session.nodes.map(n => n.id === nodeId ? { ...n, status, ...(error && { error }) } : n)
      : [...session.nodes, { id: nodeId, status, ...(error && { error }) }];
    return updateSession(state, sid, { nodes });
  }),

  addCustomMetricNode: (node) => set((state) => {
    const sid = state.currentSessionId;
    if (!sid || !state.sessions[sid]) return {};
    const session = state.sessions[sid];
    return updateSession(state, sid, {
      customMetricNodes: [...session.customMetricNodes, node],
      nodes: [...session.nodes, { id: node.id, type: node.analysis_type, name: node.name, status: 'pending' }],
    });
  }),

  removeCustomMetricNode: (id) => set((state) => {
    const sid = state.currentSessionId;
    if (!sid || !state.sessions[sid]) return {};
    const session = state.sessions[sid];
    return updateSession(state, sid, {
      customMetricNodes: session.customMetricNodes.filter(n => n.id !== id),
      nodes: session.nodes.filter(n => n.id !== id),
    });
  }),

  setHasReport: (hasReport, targetSessionId) => set((state) => {
    const sid = targetSessionId || state.currentSessionId;
    return sid ? updateSession(state, sid, { hasReport }) : {};
  }),

  setSseConnected: (sseConnected, targetSessionId) => set((state) => {
    const sid = targetSessionId || state.currentSessionId;
    return sid ? updateSession(state, sid, { sseConnected }) : {};
  }),

  setCanvasOpen: (canvasOpen, targetSessionId) => set((state) => {
    const sid = targetSessionId || state.currentSessionId;
    return sid ? updateSession(state, sid, { canvasOpen }) : {};
  }),

  setCanvasNarrative: (canvasNarrative, targetSessionId) => set((state) => {
    const sid = targetSessionId || state.currentSessionId;
    return sid ? updateSession(state, sid, { canvasNarrative }) : {};
  }),

  // ── UI-global setters ────────────────────────────────────────────────────

  setSidebarCollapsed: (sidebarCollapsed) => set({ sidebarCollapsed }),
  setHistoryOpen: (historyOpen) => set({ historyOpen }),

  // ── Pipeline run lifecycle ───────────────────────────────────────────────

  startPipelineRun: (targetSessionId) => set((state) => {
    const sid = targetSessionId || state.currentSessionId;
    if (!sid || !state.sessions[sid]) return {};
    const session = state.sessions[sid];
    return {
      lastStartedPipelineSessionId: sid,
      sessions: {
        ...state.sessions,
        [sid]: {
          ...session,
          pipelineRunId: session.pipelineRunId + 1,
          hasReport: false,
          canvasOpen: false,
          canvasNarrative: null,
        },
      },
    };
  }),

  // ── Full reset (new analysis) ────────────────────────────────────────────

  reset: () => set({
    currentSessionId: null,
    sessions: {},
    lastStartedPipelineSessionId: null,
    sidebarCollapsed: false,
    historyOpen: false,
  }),
}));

// ── Selectors ────────────────────────────────────────────────────────────────

/** Returns the per-session state object for the currently viewed session. */
export const selectCurrentSession = (state) =>
  state.sessions[state.currentSessionId] || EMPTY_SESSION;
