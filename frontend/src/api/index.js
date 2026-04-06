// src/api/index.js
const API_BASE = '';

export const uploadFile = async (file) => {
  const formData = new FormData();
  formData.append('file', file);
  const res = await fetch(`${API_BASE}/upload`, {
    method: 'POST',
    body: formData,
  });
  if (!res.ok) {
    const errorData = await res.json().catch(() => null);
    throw new Error(errorData?.detail || 'Upload failed');
  }
  return res.json();
};

export const profileDataset = async (sessionId) => {
  const res = await fetch(`${API_BASE}/profile/${sessionId}`, { method: 'POST' });
  if (!res.ok) throw new Error('Profiling failed');
  return res.json();
};

export const discoverMetrics = async (sessionId) => {
  const res = await fetch(`${API_BASE}/discover/${sessionId}`, { method: 'POST' });
  if (!res.ok) throw new Error('Discovery failed');
  return res.json();
};

export const validateMetric = async (sessionId, metric) => {
  const res = await fetch(`${API_BASE}/validate-metric/${sessionId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ metric }),
  });
  if (!res.ok) throw new Error('Validation failed');
  return res.json();
};

export const analyzeMetrics = async (sessionId, request = 'Analyze all discovered metrics.', customNodes = []) => {
  const res = await fetch(`${API_BASE}/analyze/${sessionId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    // custom_nodes: structured node specs [{id, name, analysis_type, description, column_roles}]
    // backend appends these to state.dag before the pipeline starts
    body: JSON.stringify({ request, custom_nodes: customNodes }),
  });
  if (!res.ok) throw new Error('Analysis setup failed');
  return res.json();
};

export const fetchResults = async (sessionId) => {
  const res = await fetch(`${API_BASE}/results/${sessionId}`);
  if (!res.ok) throw new Error('Failed to fetch results');
  return res.json();
};

export const fetchSynthesis = async (sessionId) => {
  const res = await fetch(`${API_BASE}/synthesis/${sessionId}`);
  if (!res.ok) throw new Error('Failed to fetch synthesis');
  return res.json();
};

export const rerunSynthesis = async (sessionId, instructions) => {
  const res = await fetch(`${API_BASE}/rerun-synthesis/${sessionId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ instructions }),
  });
  if (!res.ok) throw new Error('Rerun synthesis failed');
  return res.json();
};

export const sendChatMessage = async (sessionId, message) => {
  const res = await fetch(`${API_BASE}/chat/${sessionId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message }),
  });
  if (!res.ok) throw new Error('Chat failed');
  return res.json();
};

export const getHistory = async () => {
  const res = await fetch(`${API_BASE}/history`);
  if (!res.ok) throw new Error('Failed to fetch history');
  return res.json();
};

export const restoreSession = async (sessionId) => {
  const res = await fetch(`${API_BASE}/history/${sessionId}/restore`);
  if (!res.ok) throw new Error('Failed to restore session');
  return res.json();
};
