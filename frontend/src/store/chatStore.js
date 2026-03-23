import { create } from 'zustand';

export const useChatStore = create((set) => ({
  messages: [], // { id, role: 'user' | 'ai', type, payload, timestamp }
  thinking: false, // true while Ask AI / chat is awaiting a backend response

  addMessage: (role, type, payload) => set((state) => ({
    messages: [...state.messages, {
      id: crypto.randomUUID(),
      role,
      type,
      payload,
      timestamp: Date.now()
    }]
  })),

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
    return {
      messages: [...state.messages, {
        id: crypto.randomUUID(),
        role: 'ai',
        type: 'chart',
        payload: chartPayload,
        timestamp: Date.now(),
      }],
    };
  }),

  setThinking: (thinking) => set({ thinking }),
  clearMessages: () => set({ messages: [], thinking: false }),
}));
