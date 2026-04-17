import { create } from 'zustand';
import { usePipelineStore } from './pipelineStore';

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

// Default per-session chat state
const DEFAULT_CHAT = { messages: [], thinking: false };
const EMPTY_CHAT = { ...DEFAULT_CHAT };

// Helper: resolve sessionId — explicit arg or fall back to pipelineStore.currentSessionId
function resolveSid(explicitSid) {
  return explicitSid || usePipelineStore.getState().currentSessionId;
}

// Helper: immutably update a single session's chat state
function updateChat(state, sid, patch) {
  const chat = state.sessions[sid];
  if (!chat) return {};
  return {
    sessions: {
      ...state.sessions,
      [sid]: { ...chat, ...(typeof patch === 'function' ? patch(chat) : patch) },
    },
  };
}

// Helper: ensure a session slot exists
function ensureSession(sessions, sid) {
  if (sessions[sid]) return sessions;
  return { ...sessions, [sid]: { ...DEFAULT_CHAT } };
}

export const useChatStore = create((set) => ({
  // ── Per-session chat state ───────────────────────────────────────────────
  sessions: {},

  // ── Global (not per-session) ─────────────────────────────────────────────
  pendingMessage: '',

  // ── Message operations (sessionId optional — defaults to current) ────────

  addMessage: (roleOrSid, typeOrRole, payloadOrType, maybePayload) => {
    // Support both old signature (role, type, payload) and new (sessionId, role, type, payload)
    let sid, role, type, payload;
    if (maybePayload !== undefined) {
      // New 4-arg: (sessionId, role, type, payload)
      sid = roleOrSid;
      role = typeOrRole;
      type = payloadOrType;
      payload = maybePayload;
    } else {
      // Old 3-arg: (role, type, payload) — use current session
      sid = resolveSid();
      role = roleOrSid;
      type = typeOrRole;
      payload = payloadOrType;
    }
    if (!sid) return undefined;
    const id = genId();
    set((state) => {
      const sessions = ensureSession(state.sessions, sid);
      const chat = sessions[sid];
      return {
        sessions: {
          ...sessions,
          [sid]: {
            ...chat,
            messages: [...chat.messages, { id, role, type, payload, category: msgCategory(type), timestamp: Date.now() }],
          },
        },
      };
    });
    return id;
  },

  insertAfterMessage: (afterId, role, type, payload, explicitSid) => {
    const sid = resolveSid(explicitSid);
    if (!sid) return undefined;
    const id = genId();
    set((state) => {
      const sessions = ensureSession(state.sessions, sid);
      const chat = sessions[sid];
      const idx = chat.messages.findIndex(m => m.id === afterId);
      const newMsg = { id, role, type, payload, category: msgCategory(type), timestamp: Date.now() };
      const msgs = idx === -1 ? [...chat.messages, newMsg] : [...chat.messages.slice(0, idx + 1), newMsg, ...chat.messages.slice(idx + 1)];
      return { sessions: { ...sessions, [sid]: { ...chat, messages: msgs } } };
    });
    return id;
  },

  updateChartMessage: (analysisId, updates, explicitSid) => {
    const sid = resolveSid(explicitSid);
    if (!sid) return;
    set((state) => {
      if (!state.sessions[sid]) return {};
      return updateChat(state, sid, (chat) => ({
        messages: chat.messages.map(msg =>
          msg.type === 'chart' && msg.payload?.id === analysisId
            ? { ...msg, payload: { ...msg.payload, ...updates } }
            : msg
        ),
      }));
    });
  },

  addOrUpdateChart: (chartPayload, explicitSid) => {
    const sid = resolveSid(explicitSid);
    if (!sid) return;
    set((state) => {
      const sessions = ensureSession(state.sessions, sid);
      const chat = sessions[sid];
      const exists = chat.messages.some(
        msg => msg.type === 'chart' && msg.payload?.id === chartPayload.id
      );
      if (exists) {
        return {
          sessions: {
            ...sessions,
            [sid]: {
              ...chat,
              messages: chat.messages.map(msg =>
                msg.type === 'chart' && msg.payload?.id === chartPayload.id
                  ? { ...msg, payload: { ...msg.payload, ...chartPayload } }
                  : msg
              ),
            },
          },
        };
      }
      const filtered = chat.messages.filter(
        m => !(m.type === 'skeleton' && m.payload?.nodeId === chartPayload.id)
      );
      return {
        sessions: {
          ...sessions,
          [sid]: {
            ...chat,
            messages: [...filtered, {
              id: genId(), role: 'ai', type: 'chart', category: 'pipeline',
              payload: chartPayload, timestamp: Date.now(),
            }],
          },
        },
      };
    });
  },

  setThinking: (thinking, explicitSid) => {
    const sid = resolveSid(explicitSid);
    if (!sid) return;
    set((state) => {
      const sessions = ensureSession(state.sessions, sid);
      return updateChat({ sessions }, sid, { thinking });
    });
  },

  clearMessages: (explicitSid) => {
    const sid = resolveSid(explicitSid);
    if (!sid) return;
    set((state) => {
      const sessions = ensureSession(state.sessions, sid);
      return { sessions: { ...sessions, [sid]: { ...DEFAULT_CHAT } } };
    });
  },

  restoreMessages: (msgs, explicitSid) => {
    const sid = resolveSid(explicitSid);
    if (!sid) return;
    set((state) => ({
      sessions: {
        ...state.sessions,
        [sid]: { messages: msgs, thinking: false },
      },
    }));
  },

  // Atomically rename a session key (temp → real) after an upload resolves.
  migrateSession: (fromId, toId) => set((state) => {
    const chat = state.sessions[fromId];
    if (!chat) return {};
    const { [fromId]: _, ...rest } = state.sessions;
    return { sessions: { ...rest, [toId]: chat } };
  }),

  // ── Pre-fill chat input ──────────────────────────────────────────────────
  setPendingMessage: (text) => set({ pendingMessage: text }),

  // ── Skeleton placeholders ────────────────────────────────────────────────

  addSkeleton: (nodeId, analysisType, explicitSid) => {
    const sid = resolveSid(explicitSid);
    if (!sid) return;
    set((state) => {
      const sessions = ensureSession(state.sessions, sid);
      const chat = sessions[sid];
      if (chat.messages.some(m => m.type === 'skeleton' && m.payload?.nodeId === nodeId)) return {};
      return {
        sessions: {
          ...sessions,
          [sid]: {
            ...chat,
            messages: [...chat.messages, {
              id: `skeleton_${nodeId}`, role: 'ai', type: 'skeleton', category: 'pipeline',
              payload: { nodeId, analysisType }, timestamp: Date.now(),
            }],
          },
        },
      };
    });
  },

  removeSkeleton: (nodeId, explicitSid) => {
    const sid = resolveSid(explicitSid);
    if (!sid) return;
    set((state) => {
      if (!state.sessions[sid]) return {};
      return updateChat(state, sid, (chat) => ({
        messages: chat.messages.filter(
          m => !(m.type === 'skeleton' && m.payload?.nodeId === nodeId)
        ),
      }));
    });
  },
}));

// ── Selectors ────────────────────────────────────────────────────────────────

/** Returns the chat state for the currently viewed session. */
export const selectCurrentChat = (state) => {
  const sid = usePipelineStore.getState().currentSessionId;
  return state.sessions[sid] || EMPTY_CHAT;
};
