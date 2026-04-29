// src/api/index.js
const API_BASE = '';

export const uploadFile = async (file, schemaFile = null) => {
  const formData = new FormData();
  formData.append('file', file);
  // Optional companion data-dictionary / schema CSV. Backend treats it
  // as additive metadata — leave null and the upload behaves exactly
  // as before.
  if (schemaFile) formData.append('schema_file', schemaFile);
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

// ── Google Analytics (GA4) data-source API ─────────────────────────────────

export const gaStatus = async () => {
  const res = await fetch(`${API_BASE}/ga/status`);
  if (!res.ok) throw new Error('GA status failed');
  return res.json();
};

export const gaAuthStart = async () => {
  const res = await fetch(`${API_BASE}/ga/auth/start`);
  if (!res.ok) throw new Error('GA auth start failed');
  return res.json();
};

export const gaDisconnect = async () => {
  const res = await fetch(`${API_BASE}/ga/disconnect`, { method: 'POST' });
  if (!res.ok) throw new Error('GA disconnect failed');
  return res.json();
};

export const gaListProperties = async () => {
  const res = await fetch(`${API_BASE}/ga/properties`);
  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.detail || 'GA properties failed');
  }
  return res.json();
};

export const gaIngest = async ({ property_id, start_date = '90daysAgo', end_date = 'today' }) => {
  const res = await fetch(`${API_BASE}/ga/ingest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ property_id, start_date, end_date }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.detail || 'GA ingest failed');
  }
  return res.json();
};

// ── BigQuery data-source API ───────────────────────────────────────────────

export const bqStatus = async () => {
  const res = await fetch(`${API_BASE}/bq/status`);
  if (!res.ok) throw new Error('BQ status failed');
  return res.json();
};

export const bqAuthStart = async () => {
  const res = await fetch(`${API_BASE}/bq/auth/start`);
  if (!res.ok) throw new Error('BQ auth start failed');
  return res.json();
};

export const bqDisconnect = async () => {
  const res = await fetch(`${API_BASE}/bq/disconnect`, { method: 'POST' });
  if (!res.ok) throw new Error('BQ disconnect failed');
  return res.json();
};

export const bqListProjects = async () => {
  const res = await fetch(`${API_BASE}/bq/projects`);
  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.detail || 'BQ projects failed');
  }
  return res.json();
};

export const bqListDatasets = async (project_id) => {
  const res = await fetch(`${API_BASE}/bq/datasets?project_id=${encodeURIComponent(project_id)}`);
  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.detail || 'BQ datasets failed');
  }
  return res.json();
};

export const bqListTables = async (project_id, dataset_id) => {
  const res = await fetch(
    `${API_BASE}/bq/tables?project_id=${encodeURIComponent(project_id)}&dataset_id=${encodeURIComponent(dataset_id)}`
  );
  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.detail || 'BQ tables failed');
  }
  return res.json();
};

export const bqIngest = async ({ project_id, dataset_id, table_id, row_limit = 50000 }) => {
  const res = await fetch(`${API_BASE}/bq/ingest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ project_id, dataset_id, table_id, row_limit }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.detail || 'BQ ingest failed');
  }
  return res.json();
};
