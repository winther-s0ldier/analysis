let sessionId = null;
let outputFolder = null;
let pendingFile = null;
let renderedCharts = new Set();
let pollInterval = null;
let pollStartTime = 0;
const MAX_POLL_TIME = 10 * 60 * 1000;

const $ = sel => document.querySelector(sel);
const chatScroll = $('#chat-scroll');
const chatContainer = $('#chat-container');
const welcomeState = $('#welcome-state');
const chatInput = $('#chat-input');
const sendBtn = $('#send-btn');

// Pipeline Status Panel Elements
const statusPanel = $('#pipeline-status-panel');
const panelStatusBadge = $('#panel-status-badge');
const panelGateWarnings = $('#panel-gate-warnings');
const gateWarningsList = $('#gate-warnings-list');
const panelMonitorAlerts = $('#panel-monitor-alerts');
const monitorAlertsList = $('#monitor-alerts-list');
const statusNodeGrid = $('#node-grid');
const attachBtn = $('#attach-btn');
const fileInput = $('#file-input');
const fileAttach = $('#file-attach');
const fileAttachName = $('#file-attach-name');
const fileAttachSize = $('#file-attach-size');
const fileAttachRemove = $('#file-attach-remove');
const dropOverlay = $('#drop-overlay');
const sessionIdEl = $('#session-id');

attachBtn.addEventListener('click', () => fileInput.click());
fileInput.addEventListener('change', () => {
    if (fileInput.files.length > 0) attachFile(fileInput.files[0]);
});

fileAttachRemove.addEventListener('click', () => {
    pendingFile = null;
    fileAttach.classList.add('hidden');
    fileInput.value = '';
});

document.addEventListener('dragover', e => { e.preventDefault(); dropOverlay.classList.remove('hidden'); });
document.addEventListener('dragleave', e => {
    if (e.relatedTarget === null) dropOverlay.classList.add('hidden');
});
dropOverlay.addEventListener('drop', e => {
    e.preventDefault();
    dropOverlay.classList.add('hidden');
    if (e.dataTransfer.files.length > 0) {
        attachFile(e.dataTransfer.files[0]);
    }
});
dropOverlay.addEventListener('dragover', e => e.preventDefault());

sendBtn.addEventListener('click', handleSend);
chatInput.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSend();
    }
});

chatInput.addEventListener('input', () => {
    chatInput.style.height = 'auto';
    chatInput.style.height = Math.min(chatInput.scrollHeight, 150) + 'px';
});

const sidebarEl = $('#sidebar');
const sidebarExpand = $('#sidebar-expand');

$('#sidebar-toggle')?.addEventListener('click', () => {
    sidebarEl.classList.add('collapsed');
    sidebarExpand.classList.remove('hidden');
});

sidebarExpand?.addEventListener('click', () => {
    sidebarEl.classList.remove('collapsed');
    sidebarExpand.classList.add('hidden');
});

restoreSession();

function attachFile(file) {
    pendingFile = file;
    fileAttachName.textContent = file.name;
    fileAttachSize.textContent = formatSize(file.size);
    fileAttach.classList.remove('hidden');
    chatInput.placeholder = 'Add instructions (optional), then press Enter...';
    chatInput.focus();
}

async function handleSend() {
    const text = chatInput.value.trim();
    const file = pendingFile;

    if (!text && !file) return;

    chatInput.value = '';
    chatInput.style.height = 'auto';

    if (welcomeState) welcomeState.remove();

    if (file) {
        pendingFile = null;
        fileAttach.classList.add('hidden');
        fileInput.value = '';
        chatInput.placeholder = 'Ask anything about your data...';

        if (text) {
            addUserMessage(text);
        }
        addUserMessage(`Uploaded: ${file.name} (${formatSize(file.size)})`, true);

        await uploadAndAnalyze(file, text);
    } else if (sessionId) {
        addUserMessage(text);
        await sendChatMessage(text);
    } else {
        addUserMessage(text);
        addAIMessage('Please upload a data file first. Click the attachment button or drag and drop a file.');
    }
}

async function uploadAndAnalyze(file, instructions) {
    const statusEl = addStatusMessage('Uploading file...');

    const formData = new FormData();
    formData.append('file', file);

    try {
        const res = await fetch('/upload', { method: 'POST', body: formData });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Upload failed');

        sessionId = data.session_id;
        outputFolder = data.output_folder || data.session_id;
        sessionIdEl.textContent = `Session: ${sessionId.substring(0, 8)}`;
        saveSession();

        if (instructions) {
            try {
                await fetch(`/instructions/${sessionId}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ instructions })
                });
            } catch (e) { }
        }

        updateStage(1, 'complete');
        removeStatusMessage(statusEl);

        updateStage(2, 'active');
        const profStatus = addStatusMessage('Profiling dataset structure...');

        const profileRes = await fetch(`/profile/${sessionId}`, { method: 'POST' });
        const profileData = await profileRes.json();
        if (!profileRes.ok) throw new Error(profileData.detail || 'Profiling failed');

        removeStatusMessage(profStatus);
        updateStage(2, 'complete');

        addProfileMessage(profileData);

        updateStage(3, 'active');
        const discStatus = addStatusMessage('Building analytical roadmap...');

        const discoverRes = await fetch(`/discover/${sessionId}`, { method: 'POST' });
        const discoverData = await discoverRes.json();
        if (!discoverRes.ok) throw new Error(discoverData.detail || 'Discovery failed');

        removeStatusMessage(discStatus);
        updateStage(3, 'complete');

        addDiscoveryMessage(discoverData.discovery);

    } catch (err) {
        removeStatusMessage(statusEl);
        addAIMessage(`Error: ${err.message}`);
        updateStage(0, 'error');
    }
}

async function runAnalysis() {
    updateStage(4, 'active');
    const statusEl = addStatusMessage('Running analysis pipeline...');
    renderedCharts.clear();
    
    if (statusPanel) {
        statusPanel.classList.remove('hidden');
        statusNodeGrid.innerHTML = '';
        gateWarningsList.innerHTML = '';
        monitorAlertsList.innerHTML = '';
        panelGateWarnings.classList.add('hidden');
        panelMonitorAlerts.classList.add('hidden');
    }

    fetch(`/analyze/${sessionId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request: 'Analyze all discovered metrics.', custom_metrics: [] })
    });

    pollStartTime = Date.now();
    if (pollInterval) clearInterval(pollInterval);

    pollInterval = setInterval(async () => {
        if (Date.now() - pollStartTime > MAX_POLL_TIME) {
            clearInterval(pollInterval);
            removeStatusMessage(statusEl);
            addAIMessage('Pipeline timed out after 10 minutes.');
            return;
        }

        try {
            const res = await fetch(`/status/${sessionId}`);
            if (!res.ok) return;
            const status = await res.json();

            updateStageFromStatus(status);
            updateMetricCards(status.pipeline?.node_statuses || {});
            await renderNewCharts();
            await updatePipelineStatusPanel();

            if (status.session_status === 'complete') {
                clearInterval(pollInterval);
                removeStatusMessage(statusEl);
                await finishPipeline();
            } else if (status.session_status === 'error') {
                clearInterval(pollInterval);
                removeStatusMessage(statusEl);
                addAIMessage('Pipeline encountered an error. Some results may be available.');
            }
        } catch (err) {
            console.error('Poll error:', err);
        }
    }, 2500);
}

function updateStageFromStatus(status) {
    const map = { 'analyzing': 4, 'synthesizing': 5, 'building_report': 6, 'complete': 6 };
    const active = map[status.session_status] || 4;
    for (let i = 1; i <= 6; i++) {
        if (i <= 3 || i < active) updateStage(i, 'complete');
        else if (i === active) updateStage(i, 'active');
    }
}

async function updatePipelineStatusPanel() {
    if (!statusPanel) return;
    try {
        const res = await fetch(`/api/session/${sessionId}/status`);
        if (!res.ok) return;
        const data = await res.json();
        
        // Update badge
        panelStatusBadge.textContent = (data.pipeline_status || 'UNKNOWN').toUpperCase();
        if (data.pipeline_status === 'complete') {
            panelStatusBadge.className = 'badge success';
        } else if (data.pipeline_status === 'error') {
            panelStatusBadge.className = 'badge critical';
        } else {
            panelStatusBadge.className = 'badge info';
        }
        
        // Update nodes grid
        if (data.nodes && data.nodes.length > 0) {
            statusNodeGrid.innerHTML = data.nodes.map(n => `
                <div class="grid-node" data-status="${n.status}">
                    <div class="grid-node-id">${n.id}</div>
                    <div class="grid-node-type" title="${n.type}">${n.type.replace(/_/g, ' ')}</div>
                </div>
            `).join('');
        }
        
        // Update Gate Warnings (once available from API if they exist)
        const warnings = data.gate_result?.warnings || [];
        if (warnings.length > 0) {
            panelGateWarnings.classList.remove('hidden');
            gateWarningsList.innerHTML = warnings.map(w => `<li>${escapeHtml(w)}</li>`).join('');
        }
        
        // Update Monitor Alerts
        const alerts = data.alerts || [];
        if (alerts.length > 0) {
            panelMonitorAlerts.classList.remove('hidden');
            monitorAlertsList.innerHTML = alerts.map(a => `<li>[${a.event}] ${escapeHtml(JSON.stringify(a.data))}</li>`).join('');
        }
        
    } catch (e) {
        console.error('Failed to update pipeline panel', e);
    }
}

function updateMetricCards(nodeStatuses) {
    Object.entries(nodeStatuses).forEach(([nodeId, status]) => {
        const card = $(`[data-node-id="${nodeId}"]`);
        if (!card) return;
        card.dataset.status = status;
        if (status === 'complete') {
            const finding = card.querySelector('.card-metric-finding');
            if (finding && !finding.classList.contains('visible')) {
                finding.classList.add('visible');
                finding.textContent = '\u2713 Complete';
            }
        }
    });
}

async function renderNewCharts() {
    try {
        const res = await fetch(`/results/${sessionId}`);
        if (!res.ok) return;
        const results = await res.json();

        results.forEach(result => {
            if (result.chart_path && !renderedCharts.has(result.analysis_id)) {
                renderedCharts.add(result.analysis_id);
                addChartMessage(result);

                const card = $(`[data-node-id="${result.analysis_id}"]`);
                if (card) {
                    const finding = card.querySelector('.card-metric-finding');
                    if (finding) {
                        finding.classList.add('visible');
                        finding.textContent = result.top_finding;
                    }
                }
            }
        });
    } catch (err) {
        console.error('Chart fetch error:', err);
    }
}

async function finishPipeline() {
    updateStage(6, 'complete');

    try {
        const res = await fetch(`/synthesis/${sessionId}`);
        const synthesis = await res.json();

        if (synthesis.executive_summary) {
            addExecutiveSummaryMessage(synthesis.executive_summary);
        }

        addAIMessageHTML(`
            <p>Analysis complete! You can view the full report or continue asking questions.</p>
            <div class="card-actions" style="margin-top: 10px;">
                <button class="btn btn-primary" onclick="window.open('/report/${sessionId}', '_blank')">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                    View Report
                </button>
                <button class="btn btn-ghost" onclick="downloadReport()">Download HTML</button>
            </div>
        `);
    } catch (err) {
        console.error('Synthesis error:', err);
    }

    chatInput.placeholder = 'Ask about the analysis results...';
}

async function sendChatMessage(text) {
    const typing = addTypingIndicator();

    try {
        const res = await fetch(`/chat/${sessionId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: text })
        });
        const data = await res.json();
        removeElement(typing);
        addAIMessage(data.response);
    } catch (err) {
        removeElement(typing);
        addAIMessage('Error: ' + err.message);
    }
}

function addUserMessage(text, isFileMsg = false) {
    const el = document.createElement('div');
    el.className = 'msg msg-user';
    el.innerHTML = `<div class="msg-user-bubble${isFileMsg ? ' file-msg' : ''}">${escapeHtml(text)}</div>`;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addAIMessage(text) {
    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `<div class="msg-ai-text">${formatText(text)}</div>`;
    chatContainer.appendChild(el);
    scrollToBottom();
    return el;
}

function addAIMessageHTML(html) {
    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `<div class="msg-ai-text">${html}</div>`;
    chatContainer.appendChild(el);
    scrollToBottom();
    return el;
}

function addProfileMessage(profileData) {
    const p = profileData.profile;
    const c = profileData.classification;

    const cards = [
        { val: p.row_count?.toLocaleString() || '0', label: 'Rows' },
        { val: p.column_count || '0', label: 'Columns' },
    ];

    const dtCol = p.columns?.find(col => col.type_category === 'datetime');
    if (dtCol?.stats?.date_range_days) {
        cards.push({ val: dtCol.stats.date_range_days + 'd', label: 'Date Span' });
    }

    if (c?.dataset_type) {
        cards.push({ val: c.dataset_type.replace(/_/g, ' '), label: 'Type' });
    }

    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text">Here's the <strong>data profile</strong> for your dataset:</div>
        <div class="card-profile">
            ${cards.map(c => `
                <div class="card-profile-item">
                    <div class="card-profile-val">${c.val}</div>
                    <div class="card-profile-label">${c.label}</div>
                </div>
            `).join('')}
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addDiscoveryMessage(discovery) {
    const metrics = discovery?.metrics || [];

    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text">I've identified <strong>${metrics.length} analyses</strong> to run on your data:</div>
        <div class="card-roadmap">
            ${metrics.map((m, i) => `
                <div class="card-metric" data-node-id="${m.id || i}" data-status="pending">
                    <div class="card-metric-accent" style="background: var(--${(m.priority || m.severity || 'medium').toLowerCase()})"></div>
                    <div class="card-metric-body">
                        <div class="card-metric-top">
                            <span class="card-metric-type">${m.analysis_type?.replace(/_/g, ' ') || 'analysis'}</span>
                            <span class="badge ${(m.priority || m.severity || 'medium').toLowerCase()}">${m.feasibility || 'HIGH'}</span>
                        </div>
                        <div class="card-metric-name">${m.name}</div>
                        <div class="card-metric-desc">${m.description}</div>
                        <div class="card-metric-finding"></div>
                    </div>
                </div>
            `).join('')}
        </div>
        <div class="card-actions">
            <button class="btn btn-primary" id="btn-execute" onclick="runAnalysis()">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg>
                Execute Pipeline
            </button>
            <button class="btn btn-ghost" onclick="addCustomAnalysis()">+ Custom Analysis</button>
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addChartMessage(result) {
    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="card-chart">
            <div class="card-chart-header">
                <span class="card-chart-type">${result.analysis_type?.replace(/_/g, ' ').toUpperCase()}</span>
                <span class="badge ${(result.severity || 'low').toLowerCase()}">${result.severity || 'INFO'}</span>
            </div>
            <div class="card-chart-finding">${result.top_finding}</div>
            <div class="card-chart-frame">
                <iframe src="/chart/${sessionId}/${result.analysis_id}" loading="lazy"></iframe>
            </div>
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addExecutiveSummaryMessage(summary) {
    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text"><strong>Executive Summary</strong></div>
        <div class="card-exec">
            <div class="card-exec-overview">${summary.overview || ''}</div>
            ${(summary.key_findings || []).map(f => `<div class="card-exec-finding">${f}</div>`).join('')}
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addStatusMessage(text) {
    const el = document.createElement('div');
    el.className = 'msg msg-status';
    el.innerHTML = `<div class="spinner"></div><span class="msg-status-text">${text}</span>`;
    chatContainer.appendChild(el);
    scrollToBottom();
    return el;
}

function removeStatusMessage(el) {
    if (el && el.parentNode) el.parentNode.removeChild(el);
}

function addTypingIndicator() {
    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = '<div class="typing-dots"><div class="typing-dot"></div><div class="typing-dot"></div><div class="typing-dot"></div></div>';
    el.id = 'typing-indicator';
    chatContainer.appendChild(el);
    scrollToBottom();
    return el;
}

function removeElement(el) {
    if (el && el.parentNode) el.parentNode.removeChild(el);
}

function createAIMessageEl() {
    const el = document.createElement('div');
    el.className = 'msg msg-ai';
    el.innerHTML = `
        <div class="msg-ai-avatar">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none">
                <rect x="3" y="3" width="7" height="7" rx="2" fill="currentColor" opacity="0.9"/>
                <rect x="14" y="3" width="7" height="7" rx="2" fill="currentColor" opacity="0.4"/>
                <rect x="3" y="14" width="7" height="7" rx="2" fill="currentColor" opacity="0.4"/>
                <rect x="14" y="14" width="7" height="7" rx="2" fill="currentColor" opacity="0.7"/>
            </svg>
        </div>
        <div class="msg-ai-content"></div>
    `;
    return el;
}

function formatText(text) {
    return text
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        .replace(/\n/g, '<br>');
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function scrollToBottom() {
    requestAnimationFrame(() => {
        chatScroll.scrollTop = chatScroll.scrollHeight;
    });
}

function updateStage(num, status) {
    const el = $(`#stage-${num}`);
    if (el) el.dataset.status = status;
}

function formatSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function saveSession() {
    if (sessionId) {
        sessionStorage.setItem('adk_sessionId', sessionId);
        sessionStorage.setItem('adk_outputFolder', outputFolder || '');
    }
}

function restoreSession() {
    const saved = sessionStorage.getItem('adk_sessionId');
    if (saved) {
        sessionId = saved;
        outputFolder = sessionStorage.getItem('adk_outputFolder') || '';
        sessionIdEl.textContent = `Session: ${sessionId.substring(0, 8)}`;
    }
}

async function addCustomAnalysis() {
    const text = prompt('Describe the custom analysis you want to run:');
    if (!text) return;
    if (!sessionId) { addAIMessage('No active session. Upload a file first.'); return; }

    addUserMessage(text);

    const customId = 'C' + (document.querySelectorAll('.card-metric[data-custom]').length + 1);
    const roadmap = document.querySelector('.card-roadmap');
    if (roadmap) {
        const card = document.createElement('div');
        card.className = 'card-metric';
        card.dataset.nodeId = customId;
        card.dataset.status = 'running';
        card.dataset.custom = 'true';
        card.innerHTML = `
            <div class="card-metric-accent" style="background: var(--info)"></div>
            <div class="card-metric-body">
                <div class="card-metric-top">
                    <span class="card-metric-type">custom analysis</span>
                    <span class="badge info">CUSTOM</span>
                </div>
                <div class="card-metric-name">${escapeHtml(text)}</div>
                <div class="card-metric-desc">User-requested analysis</div>
                <div class="card-metric-finding"><div class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;vertical-align:middle;margin-right:6px;"></div> Running...</div>
            </div>
        `;
        roadmap.appendChild(card);
        scrollToBottom();
    }

    try {
        const res = await fetch(`/add-metric/${sessionId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ metric: text })
        });
        const data = await res.json();

        const card = document.querySelector(`[data-node-id="${customId}"]`);

        if (data.status === 'success') {
            if (card) {
                card.dataset.status = 'complete';
                const finding = card.querySelector('.card-metric-finding');
                if (finding) {
                    finding.innerHTML = data.top_finding || '✓ Complete';
                    finding.classList.add('visible');
                }
            }
            if (data.chart_path) {
                addChartMessage({
                    analysis_id: data.analysis_id,
                    analysis_type: data.analysis_type || 'custom',
                    top_finding: data.top_finding || '',
                    severity: data.severity || 'info',
                    chart_path: data.chart_path,
                });
            } else {
                addAIMessage(data.top_finding || 'Custom analysis completed.');
            }
        } else if (data.status === 'invalid') {
            if (card) {
                card.dataset.status = 'failed';
                const finding = card.querySelector('.card-metric-finding');
                if (finding) {
                    finding.innerHTML = `✗ ${data.reason || 'Invalid'}`;
                    finding.classList.add('visible');
                }
            }
            addAIMessage(`Can't run that analysis: ${data.reason || 'The data doesn\'t support this analysis.'}`);
        } else {
            if (card) {
                card.dataset.status = 'failed';
                const finding = card.querySelector('.card-metric-finding');
                if (finding) {
                    finding.innerHTML = `✗ ${data.error || 'Error'}`;
                    finding.classList.add('visible');
                }
            }
            addAIMessage(`Analysis error: ${data.error || 'Something went wrong.'}`);
        }
    } catch (err) {
        const card = document.querySelector(`[data-node-id="${customId}"]`);
        if (card) {
            card.dataset.status = 'failed';
            const finding = card.querySelector('.card-metric-finding');
            if (finding) { finding.innerHTML = `✗ ${err.message}`; finding.classList.add('visible'); }
        }
        addAIMessage(`Custom analysis failed: ${err.message}`);
    }
}

window.downloadReport = async function () {
    try {
        const res = await fetch(`/report/${sessionId}`);
        if (!res.ok) return;
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `adk_report_${sessionId}.html`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    } catch (err) {
        addAIMessage('Download failed: ' + err.message);
    }
};
