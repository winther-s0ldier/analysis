import { create } from 'zustand';

export const usePipelineStore = create((set) => ({
  sessionId: null,
  outputFolder: null,
  phase: 'idle', // 'idle' | 'uploading' | 'profiling' | 'discovering' | 'analyzing' | 'synthesizing' | 'building_report' | 'complete' | 'error'
  nodes: [],
  customMetricNodes: [], // user-validated custom analyses added before pipeline runs
  hasReport: false,
  sseConnected: false,
  canvasOpen: false,
  canvasNarrative: null, // HTML string from synthesis conversational_report
  pipelineRunId: 0, // incremented each time a pipeline starts — triggers SSE reconnection
  sidebarCollapsed: false,

  setSession: (sessionId, outputFolder) => set({
    sessionId, outputFolder,
    phase: 'idle', nodes: [], customMetricNodes: [], hasReport: false,
    canvasOpen: false, canvasNarrative: null,
  }),
  setPhase: (phase) => set({ phase }),

  setNodes: (nodes) => set({ nodes }),
  updateNodeStatus: (nodeId, status, error = null) => set((state) => {
    const exists = state.nodes.some(n => n.id === nodeId);
    if (exists) {
      return {
        nodes: state.nodes.map(n =>
          n.id === nodeId ? { ...n, status, ...(error && { error }) } : n
        ),
      };
    }
    // Node wasn't pre-populated by setNodes (e.g. setNodes wasn't called yet,
    // or this node was filtered out).  Add it now so the terminal shows it.
    return {
      nodes: [...state.nodes, { id: nodeId, status, ...(error && { error }) }],
    };
  }),

  // Custom metric nodes: added by user at RunAnalysisCard before pipeline starts.
  // Each entry is the full node spec {id, name, analysis_type, description, column_roles, priority}.
  // Also appended to nodes[] so DiscoveryCard NodeRow picks up live status via SSE.
  addCustomMetricNode: (node) => set((state) => ({
    customMetricNodes: [...state.customMetricNodes, node],
    nodes: [...state.nodes, { id: node.id, type: node.analysis_type, name: node.name, status: 'pending' }],
  })),
  removeCustomMetricNode: (id) => set((state) => ({
    customMetricNodes: state.customMetricNodes.filter(n => n.id !== id),
    nodes: state.nodes.filter(n => n.id !== id),
  })),

  setHasReport: (hasReport) => set({ hasReport }),
  setSseConnected: (sseConnected) => set({ sseConnected }),
  setCanvasOpen: (canvasOpen) => set({ canvasOpen }),
  setSidebarCollapsed: (sidebarCollapsed) => set({ sidebarCollapsed }),
  setCanvasNarrative: (canvasNarrative) => set({ canvasNarrative }),
  startPipelineRun: () => set((state) => ({
    pipelineRunId: state.pipelineRunId + 1,
    hasReport: false,
    canvasOpen: false,
    canvasNarrative: null,
  })),

  reset: () => set({
    sessionId: null,
    outputFolder: null,
    phase: 'idle',
    nodes: [],
    customMetricNodes: [],
    hasReport: false,
    sseConnected: false,
    canvasOpen: false,
    canvasNarrative: null,
    sidebarCollapsed: false,
  })
}));
