import { create } from 'zustand';

// Message types that belong to the pipeline results block (charts, profile, plan, etc.)
// Everything else (text, canvas_question) is a conversation message shown below.
const PIPELINE_TYPES = new Set([
  'chart', 'skeleton', 'profile', 'discovery', 'terminal',
  'insights', 'personas', 'interventions', 'connections',
  'summary', 'narrative', 'critic', 'report', 'rerun',
  'run_analysis', 'clarification', 'file',
]);

function msgCategory(type) {
  return PIPELINE_TYPES.has(type) ? 'pipeline' : 'conversation';
}

function genId() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID();
  return Date.now().toString(36) + Math.random().toString(36).slice(2);
}

export const useChatStore = create((set) => ({
  messages: [], // { id, role, type, payload, category, timestamp }
  thinking: false, // true while Ask AI / chat is awaiting a backend response

  addMessage: (role, type, payload) => {
    const id = genId();
    set((state) => ({
      messages: [...state.messages, { id, role, type, payload, category: msgCategory(type), timestamp: Date.now() }]
    }));
    return id;
  },

  insertAfterMessage: (afterId, role, type, payload) => {
    const id = genId();
    set((state) => {
      const idx = state.messages.findIndex(m => m.id === afterId);
      const newMsg = { id, role, type, payload, category: msgCategory(type), timestamp: Date.now() };
      if (idx === -1) return { messages: [...state.messages, newMsg] };
      const msgs = [...state.messages];
      msgs.splice(idx + 1, 0, newMsg);
      return { messages: msgs };
    });
    return id;
  },

  // Update payload fields of an existing chart message (matched by payload.id).
  // Used to upgrade hasChart=false cards when a retry produces a chart.
  updateChartMessage: (analysisId, updates) => set((state) => ({
    messages: state.messages.map(msg =>
      msg.type === 'chart' && msg.payload?.id === analysisId
        ? { ...msg, payload: { ...msg.payload, ...updates } }
        : msg
    ),
  })),

  // Add a chart message only if one with this ID doesn't already exist.
  // If it exists, update it instead. Prevents duplicate chart cards when
  // both SSE and HTTP response deliver the same analysis result.
  addOrUpdateChart: (chartPayload) => set((state) => {
    const exists = state.messages.some(
      msg => msg.type === 'chart' && msg.payload?.id === chartPayload.id
    );
    if (exists) {
      return {
        messages: state.messages.map(msg =>
          msg.type === 'chart' && msg.payload?.id === chartPayload.id
            ? { ...msg, payload: { ...msg.payload, ...chartPayload } }
            : msg
        ),
      };
    }
    // Also remove any skeleton for this node when a real chart arrives
    const filtered = state.messages.filter(
      m => !(m.type === 'skeleton' && m.payload?.nodeId === chartPayload.id)
    );
    return {
      messages: [...filtered, {
        id: genId(),
        role: 'ai',
        type: 'chart',
        category: 'pipeline',
        payload: chartPayload,
        timestamp: Date.now(),
      }],
    };
  }),

  setThinking: (thinking) => set({ thinking }),
  clearMessages: () => set({ messages: [], thinking: false }),

  // ── Pre-fill chat input ("Ask about this" on chart/insight cards) ──────────
  pendingMessage: '',
  setPendingMessage: (text) => set({ pendingMessage: text }),

  // ── Skeleton placeholders — shown on node_started, removed on node_complete ─
  addSkeleton: (nodeId, analysisType) => set((state) => {
    const already = state.messages.some(
      m => m.type === 'skeleton' && m.payload?.nodeId === nodeId
    );
    if (already) return {};
    return {
      messages: [...state.messages, {
        id: `skeleton_${nodeId}`,
        role: 'ai',
        type: 'skeleton',
        category: 'pipeline',
        payload: { nodeId, analysisType },
        timestamp: Date.now(),
      }],
    };
  }),

  removeSkeleton: (nodeId) => set((state) => ({
    messages: state.messages.filter(
      m => !(m.type === 'skeleton' && m.payload?.nodeId === nodeId)
    ),
  })),
}));
