let sessionId = null;
let outputFolder = null;
let pendingFile = null;
let renderedCharts = new Set();
let pollInterval = null;
let pollStartTime = 0;
const MAX_POLL_TIME = 10 * 60 * 1000;

let _terminalCard = null;
let _terminalNodeStates = {};
let _terminalTotalNodes = 0;
let _warnedMessages = new Set();

const $ = sel => document.querySelector(sel);
const chatScroll = $('#chat-scroll');
const chatContainer = $('#chat-container');
const welcomeState = $('#welcome-state');
const chatInput = $('#chat-input');
const sendBtn = $('#send-btn');
const progressRing = $('#progress-ring');
const ringFill = $('#ring-fill');
const ringLabel = $('#ring-label');
const attachBtn = $('#attach-btn');
const fileInput = $('#file-input');
const fileAttach = $('#file-attach');
const fileAttachName = $('#file-attach-name');
const fileAttachSize = $('#file-attach-size');
const fileAttachRemove = $('#file-attach-remove');
const dropOverlay = $('#drop-overlay');
const processingStatus = $('#processing-status');
const processingText = $('#processing-text');
const topProgress = $('#top-progress');
const topProgressBar = $('#top-progress-bar');

const STATUS_MESSAGES = {
    upload: [
        'Reading file headers…',
        'Validating file format…',
        'Normalizing encoding…',
        'Parsing column types…',
        'Detecting delimiter patterns…',
        'Scanning file schema…',
        'Loading raw data blocks…',
        'Indexing rows for processing…',
        'Validating schema consistency…',
        'Detecting compressed formats…',
        'Streaming file chunks…',
        'Checking file integrity…',
        'Sampling dataset structure…',
        'Allocating ingestion buffers…',
        'Registering dataset metadata…',
        'Initializing ingestion pipeline…',
    ],
    profile: [
        'Scanning column types…',
        'Computing value distributions…',
        'Detecting temporal patterns…',
        'Measuring cardinality ratios…',
        'Identifying entity columns…',
        'Building statistical summary…',
        'Checking null density…',
        'Inferring data semantics…',
        'Mapping structural patterns…',
        'Detecting categorical features…',
        'Estimating feature entropy…',
        'Analyzing text token frequency…',
        'Profiling numeric ranges…',
        'Detecting outliers and anomalies…',
        'Evaluating skewness and kurtosis…',
        'Profiling missing value patterns…',
        'Detecting duplicate records…',
        'Identifying potential primary keys…',
        'Evaluating column correlations…',
        'Inferring feature importance signals…',
        'Analyzing column uniqueness ratios…',
        'Building feature metadata index…',
        'Mapping dataset schema graph…',
    ],
    discover: [
        'Mapping analytical pathways…',
        'Evaluating analysis feasibility…',
        'Building dependency graph…',
        'Scoring analysis relevance…',
        'Selecting optimal analyses…',
        'Constructing execution plan…',
        'Ranking metrics by dataset type…',
        'Identifying candidate KPIs…',
        'Detecting relational joins…',
        'Inferring business entities…',
        'Mapping user activity flows…',
        'Detecting event hierarchies…',
        'Generating hypothesis candidates…',
        'Evaluating signal strength…',
        'Exploring feature interactions…',
        'Modeling analysis dependency chains…',
        'Optimizing analytical execution order…',
        'Selecting statistical techniques…',
        'Mapping dimensional hierarchies…',
        'Identifying metric relationships…',
    ],
    analyze: [
        'Initializing analysis pipeline…',
        'Running session detection…',
        'Computing behavioral funnels…',
        'Fitting distribution models…',
        'Calculating correlation matrix…',
        'Detecting friction points…',
        'Segmenting user cohorts…',
        'Running survival analysis…',
        'Mining sequential patterns…',
        'Building time-series decomposition…',
        'Generating chart artifacts…',
        'Validating analysis outputs…',
        'Executing DAG nodes…',
        'Resolving node dependencies…',
        'Aggregating intermediate results…',
        'Partitioning dataset shards…',
        'Executing parallel aggregations…',
        'Reducing distributed results…',
        'Streaming event logs…',
        'Computing rolling metrics…',
        'Extracting behavioral signals…',
        'Calculating retention curves…',
        'Estimating trend components…',
        'Computing percentile metrics…',
        'Detecting seasonality patterns…',
        'Scanning log events…',
        'Extracting session timelines…',
        'Analyzing event frequency bursts…',
        'Building feature vectors…',
        'Evaluating clustering structures…',
        'Running anomaly detection…',
        'Initializing distributed workers…',
        'Allocating compute shards…',
        'Rebalancing data partitions…',
        'Streaming telemetry events…',
        'Materializing feature store…',
        'Updating analytical cache…',
        'Checkpointing intermediate states…',
        'Executing vectorized computations…',
        'Processing batch aggregations…',
        'Reconciling distributed metrics…',
    ],
    synthesize: [
        'Aggregating analysis results…',
        'Extracting key findings…',
        'Quantifying business impact…',
        'Generating executive summary…',
        'Ranking insights by severity…',
        'Composing actionable recommendations…',
        'Cross-referencing findings…',
        'Linking insights across analyses…',
        'Evaluating statistical significance…',
        'Estimating confidence intervals…',
        'Detecting recurring patterns…',
        'Summarizing behavioral insights…',
        'Synthesizing trend narratives…',
        'Mapping insight dependencies…',
        'Prioritizing high-impact findings…',
        'Validating statistical robustness…',
        'Reconciling cross-analysis signals…',
        'Constructing narrative explanations…',
    ],
    report: [
        'Assembling report sections…',
        'Embedding chart artifacts…',
        'Formatting findings narrative…',
        'Finalizing HTML report…',
        'Compiling visualization assets…',
        'Rendering interactive charts…',
        'Linking supporting metrics…',
        'Generating insight highlights…',
        'Packaging report assets…',
        'Optimizing report layout…',
        'Embedding data summaries…',
        'Publishing analysis report…',
        'Building executive dashboard…',
        'Formatting analytical tables…',
        'Rendering summary visualizations…',
        'Generating downloadable artifacts…',
        'Indexing report sections…',
        'Completing report generation…',
    ],
};

let _rotatingInterval = null;
let _rotatingMsgIndex = 0;
let _rotatingMsgPool = [];

function startProcessingStatus(phase) {
    _rotatingMsgPool = STATUS_MESSAGES[phase] || STATUS_MESSAGES.analyze;
    _rotatingMsgIndex = 0;
    processingStatus.classList.remove('hidden');
    processingText.textContent = _rotatingMsgPool[0];

    clearInterval(_rotatingInterval);
    _rotatingInterval = setInterval(() => {
        _rotatingMsgIndex = (_rotatingMsgIndex + 1) % _rotatingMsgPool.length;
        processingText.style.opacity = '0';
        setTimeout(() => {
            processingText.textContent = _rotatingMsgPool[_rotatingMsgIndex];
            processingText.style.opacity = '1';
        }, 150);
    }, 2500);
}

function stopProcessingStatus() {
    clearInterval(_rotatingInterval);
    processingStatus.classList.add('hidden');
}

function setProgress(pct) {
    topProgress.classList.add('active');
    topProgressBar.style.width = Math.min(pct, 100) + '%';
    if (pct >= 100) {
        setTimeout(() => {
            topProgress.classList.remove('active');
            topProgressBar.style.width = '0%';
        }, 600);
    }
}

let lenis = null;
if (typeof Lenis !== 'undefined') {
    lenis = new Lenis({
        wrapper: chatScroll,
        content: chatContainer,
        duration: 1.0,
        easing: t => Math.min(1, 1.001 - Math.pow(2, -10 * t)),
        smoothWheel: true,
    });

    function lenisRaf(time) {
        lenis.raf(time);
        requestAnimationFrame(lenisRaf);
    }
    requestAnimationFrame(lenisRaf);
}

attachBtn.addEventListener('click', () => fileInput.click());
fileInput.addEventListener('change', () => {
    if (fileInput.files.length > 0) attachFile(fileInput.files[0]);
});

fileAttachRemove.addEventListener('click', () => {
    pendingFile = null;
    fileAttach.classList.add('hidden');
    fileInput.value = '';
    chatInput.placeholder = 'Upload a file to begin, or type a message…';
});

document.addEventListener('dragover', e => {
    e.preventDefault();
    dropOverlay.classList.remove('hidden');
});
document.addEventListener('dragleave', e => {
    if (e.relatedTarget === null) dropOverlay.classList.add('hidden');
});
dropOverlay.addEventListener('drop', e => {
    e.preventDefault();
    dropOverlay.classList.add('hidden');
    if (e.dataTransfer.files.length > 0) attachFile(e.dataTransfer.files[0]);
});
dropOverlay.addEventListener('dragover', e => e.preventDefault());

/* ─── Send ───────────────────────────────────────────────────── */
sendBtn.addEventListener('click', handleSend);
chatInput.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend(); }
});
chatInput.addEventListener('input', () => {
    chatInput.style.height = 'auto';
    chatInput.style.height = Math.min(chatInput.scrollHeight, 150) + 'px';
});

/* ─── Sidebar Toggle ─────────────────────────────────────────── */
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

/* ─── Core Functions ─────────────────────────────────────────── */
function attachFile(file) {
    pendingFile = file;
    fileAttachName.textContent = file.name;
    fileAttachSize.textContent = formatSize(file.size);
    fileAttach.classList.remove('hidden');
    chatInput.placeholder = 'Add instructions (optional), then press Enter…';
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
        chatInput.placeholder = 'Ask anything about your data…';

        if (text) addUserMessage(text);
        addUserMessage(`${file.name}  ·  ${formatSize(file.size)}`, true);
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
    setProgress(10);
    startProcessingStatus('upload');

    const formData = new FormData();
    formData.append('file', file);

    try {
        const res = await fetch('/upload', { method: 'POST', body: formData });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Upload failed');

        sessionId = data.session_id;
        outputFolder = data.output_folder || data.session_id;
        // No session saved to storage — fresh start on reload

        if (instructions) {
            try {
                await fetch(`/instructions/${sessionId}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ instructions }),
                });
            } catch (_) { }
        }

        updateStage(1, 'complete');
        setProgress(25);

        /* ── Profile ── */
        updateStage(2, 'active');
        startProcessingStatus('profile');

        const profileRes = await fetch(`/profile/${sessionId}`, { method: 'POST' });
        const profileData = await profileRes.json();
        if (!profileRes.ok) throw new Error(profileData.detail || 'Profiling failed');

        updateStage(2, 'complete');
        setProgress(45);
        addProfileMessage(profileData);

        /* ── Discover ── */
        updateStage(3, 'active');
        startProcessingStatus('discover');

        const discoverRes = await fetch(`/discover/${sessionId}`, { method: 'POST' });
        const discoverData = await discoverRes.json();
        if (!discoverRes.ok) throw new Error(discoverData.detail || 'Discovery failed');

        updateStage(3, 'complete');
        setProgress(65);
        addDiscoveryMessage(discoverData.discovery);

    } catch (err) {
        stopProcessingStatus();
        setProgress(100);
        addAIMessage(`Error: ${err.message}`);
        updateStage(0, 'error');
    } finally {
        stopProcessingStatus();
    }
}

async function runAnalysis() {
    updateStage(4, 'active');
    startProcessingStatus('analyze');
    renderedCharts.clear();

    // Reset terminal card state for new run
    _terminalCard = null;
    _terminalNodeStates = {};
    _terminalTotalNodes = 0;
    _warnedMessages = new Set();
    if (progressRing) { progressRing.classList.add('hidden'); progressRing.classList.remove('complete'); }

    fetch(`/analyze/${sessionId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request: 'Analyze all discovered metrics.', custom_metrics: [] }),
    });

    pollStartTime = Date.now();
    if (pollInterval) clearInterval(pollInterval);

    pollInterval = setInterval(async () => {
        if (Date.now() - pollStartTime > MAX_POLL_TIME) {
            clearInterval(pollInterval);
            stopProcessingStatus();
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

            // Update rotating messages based on current stage
            const phase = { analyzing: 'analyze', synthesizing: 'synthesize', building_report: 'report' }[status.session_status];
            if (phase) startProcessingStatus(phase);

            if (status.session_status === 'complete') {
                clearInterval(pollInterval);
                stopProcessingStatus();
                setProgress(100);
                await finishPipeline();
            } else if (status.session_status === 'error') {
                clearInterval(pollInterval);
                stopProcessingStatus();
                setProgress(100);
                addAIMessage('Pipeline encountered an error. Some results may be available.');
            }
        } catch (err) {
            console.error('Poll error:', err);
        }
    }, 2500);
}

function updateStageFromStatus(status) {
    const map = { analyzing: 4, synthesizing: 5, building_report: 6, complete: 6 };
    const active = map[status.session_status] || 4;
    for (let i = 1; i <= 6; i++) {
        if (i <= 3 || i < active) updateStage(i, 'complete');
        else if (i === active) updateStage(i, 'active');
    }
}

async function updatePipelineStatusPanel() {
    try {
        const res = await fetch(`/api/session/${sessionId}/status`);
        if (!res.ok) return;
        const data = await res.json();

        const nodes = data.nodes || [];

        // ── Terminal card ──────────────────────────────────────────
        if (nodes.length > 0) {
            if (!_terminalCard) {
                _terminalTotalNodes = nodes.length;
                addTerminalCard(nodes.length);
            }
            updateTerminalCard(nodes);
        }

        // ── Circular progress ring ─────────────────────────────────
        const completed = nodes.filter(n => n.status === 'complete' || n.status === 'failed').length;
        const total = nodes.length || 1;
        if (nodes.length > 0) {
            if (progressRing) progressRing.classList.remove('hidden');
            updateProgressRing(completed, total);
        }

        // ── Gate warnings → AI chat (once each) ───────────────────
        const warnings = data.gate_result?.warnings || [];
        warnings.forEach(w => {
            const key = 'warn:' + w;
            if (!_warnedMessages.has(key)) {
                _warnedMessages.add(key);
                addAIMessage(`⚠️ Gate warning: ${w}`);
            }
        });

        // ── Monitor alerts → AI chat (once each) ──────────────────
        const alerts = data.alerts || [];
        alerts.forEach(a => {
            const key = 'alert:' + a.event + ':' + JSON.stringify(a.data);
            if (!_warnedMessages.has(key)) {
                _warnedMessages.add(key);
                addAIMessage(`🔔 [${a.event}] ${JSON.stringify(a.data)}`);
            }
        });

    } catch (e) {
        console.error('Failed to update pipeline panel', e);
    }
}

/* ─── Terminal Card ──────────────────────────────────────────── */
function addTerminalCard(nodeCount) {
    const wrapper = document.createElement('div');
    wrapper.className = 'msg msg-ai';
    wrapper.innerHTML = `
        <div class="msg-ai-avatar">
            <svg width="14" height="14" viewBox="0 0 20 20" fill="none">
                <rect x="2" y="2" width="7" height="7" rx="1.5" fill="currentColor" opacity="0.9"/>
                <rect x="11" y="2" width="7" height="7" rx="1.5" fill="currentColor" opacity="0.4"/>
                <rect x="2" y="11" width="7" height="7" rx="1.5" fill="currentColor" opacity="0.4"/>
                <rect x="11" y="11" width="7" height="7" rx="1.5" fill="currentColor" opacity="0.7"/>
            </svg>
        </div>
        <div class="msg-ai-content">
            <div class="msg-terminal" id="terminal-card">
                <div class="msg-terminal-header">
                    <div class="t-dot"></div>
                    <div class="t-dot"></div>
                    <div class="t-dot"></div>
                    <span class="msg-terminal-title">pipeline — analysis</span>
                </div>
                <div class="msg-terminal-body" id="terminal-body"></div>
                <div class="msg-terminal-footer">
                    <div class="t-progress-bar"><div class="t-progress-fill" id="t-prog-fill"></div></div>
                    <span class="t-counter" id="terminal-footer-text">0 / ${nodeCount}</span>
                    <span class="t-cursor" id="terminal-cursor"></span>
                </div>
            </div>
        </div>
    `;
    chatContainer.appendChild(wrapper);
    _terminalCard = wrapper;
    scrollToBottom();
}

function updateTerminalCard(nodes) {
    const body = document.getElementById('terminal-body');
    const footer = document.getElementById('terminal-footer-text');
    const cursor = document.getElementById('terminal-cursor');
    const progFill = document.getElementById('t-prog-fill');
    if (!body) return;

    let completed = 0;
    nodes.forEach(n => {
        _terminalNodeStates[n.id] = n.status;

        const isOk = n.status === 'complete';
        const isRun = n.status === 'running';
        const isFail = n.status === 'failed';

        const statusCls = isOk ? 't-status-ok' : isRun ? 't-status-run' : isFail ? 't-status-fail' : 't-status-pend';
        const labelCls = isOk ? 't-label-ok' : isRun ? 't-label-run' : isFail ? 't-label-fail' : 't-label-pend';
        const icon = isOk ? '✓' : isRun ? '⟳' : isFail ? '✗' : '·';
        const statusText = isOk ? 'done' : isRun ? 'running…' : isFail ? 'failed' : 'waiting';
        const label = (n.type || n.id || '').replace(/_/g, ' ');

        let row = document.querySelector(`#terminal-body .t-line[data-node-id="${n.id}"]`);
        if (!row) {
            row = document.createElement('div');
            row.className = 't-line';
            row.dataset.nodeId = n.id;
            body.appendChild(row);
        }
        row.innerHTML = `
            <span class="t-icon ${statusCls}">${icon}</span>
            <span class="t-line-type ${labelCls}">${escapeHtml(label)}</span>
            <span class="t-status-text ${statusCls}">${statusText}</span>
        `;

        if (isOk || isFail) completed++;
    });

    const total = nodes.length || 1;
    if (footer) footer.textContent = `${completed} / ${total}`;
    if (progFill) progFill.style.width = `${Math.round(completed / total * 100)}%`;

    const allDone = nodes.length > 0 && nodes.every(n => n.status === 'complete' || n.status === 'failed');
    if (cursor) cursor.style.display = allDone ? 'none' : '';
}

/* ─── Circular Progress Ring ─────────────────────────────────── */
function updateProgressRing(completed, total) {
    if (!ringFill || !ringLabel) return;
    const circumference = 138.23; // 2π × 22
    const fraction = total > 0 ? completed / total : 0;
    ringFill.style.strokeDashoffset = circumference * (1 - fraction);

    const allDone = total > 0 && completed >= total;
    if (allDone) {
        progressRing.classList.add('complete');
        ringLabel.textContent = '✓';
    } else {
        ringLabel.textContent = `${completed}/${total}`;
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
                finding.textContent = '✓ Complete';
            }
            // Remove retry button if present from a previous failure
            card.querySelector('.btn-retry-node')?.remove();
        } else if (status === 'failed') {
            const finding = card.querySelector('.card-metric-finding');
            if (finding) {
                finding.classList.add('visible');
                if (!finding.querySelector('.btn-retry-node')) {
                    finding.innerHTML = `<span style="color:var(--error)">✗ Failed</span>
                        <button class="btn-retry-node" onclick="retryNode('${escapeHtml(nodeId)}', this)">↺ Retry</button>`;
                }
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
        startProcessingStatus('synthesize');
        const res = await fetch(`/synthesis/${sessionId}`);
        const synthesis = await res.json();
        stopProcessingStatus();

        // Render synthesis sections in order: detailed → personas → interventions → connections → summary
        if (synthesis.detailed_insights?.insights?.length) {
            addDetailedInsightsMessage(synthesis.detailed_insights);
        }
        if (synthesis.personas?.personas?.length) {
            addPersonasMessage(synthesis.personas);
        }
        if (synthesis.intervention_strategies?.strategies?.length) {
            addInterventionsMessage(synthesis.intervention_strategies);
        }
        if (synthesis.cross_metric_connections?.connections?.length) {
            addCrossConnectionsMessage(synthesis.cross_metric_connections);
        }
        if (synthesis.executive_summary) {
            addExecutiveSummaryMessage(synthesis.executive_summary);
        }

        addAIMessageHTML(`
            <p>Analysis complete. You can view the full report or continue asking questions.</p>
            <div class="card-actions" style="margin-top: 12px;">
                <button class="btn btn-primary" onclick="window.open('/report/${sessionId}', '_blank')">
                    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                    View Report
                </button>
                <button class="btn btn-ghost" onclick="downloadReport()">Download HTML</button>
            </div>
        `);
    } catch (err) {
        stopProcessingStatus();
        console.error('Synthesis error:', err);
    }

    chatInput.placeholder = 'Ask about the analysis results…';
}

async function sendChatMessage(text) {
    const typing = addTypingIndicator();
    try {
        const res = await fetch(`/chat/${sessionId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: text }),
        });
        const data = await res.json();
        removeElement(typing);
        addAIMessage(data.response);
    } catch (err) {
        removeElement(typing);
        addAIMessage('Error: ' + err.message);
    }
}

/* ─── Message Builders ───────────────────────────────────────── */
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
    const c = profileData.classification || {};
    const cols = p.columns || [];

    // ── Column type counts ───────────────────────────────────────────
    const colTypes = p.column_types || {};
    const numericCount = (colTypes.numeric || []).length;
    const catCount = (colTypes.categorical || []).length;
    const datetimeCount = (colTypes.datetime || []).length;

    // ── Unique entities (domain-agnostic) ────────────────────────────
    // Use the entity_col identified by the profiler; fall back to the
    // categorical column with the highest unique_count.
    const colRoles = p.column_roles || c.column_roles || {};
    let uniqueEntities = null;
    let entityLabel = 'Unique Records';
    if (colRoles.entity_col) {
        const ecol = cols.find(col => col.name === colRoles.entity_col);
        if (ecol) {
            uniqueEntities = ecol.unique_count;
            // Turn "user_id" → "User Ids" etc.
            entityLabel = 'Unique ' + colRoles.entity_col
                .replace(/_/g, ' ')
                .replace(/\b\w/g, l => l.toUpperCase());
        }
    }
    if (!uniqueEntities) {
        const catCols = cols.filter(col => col.type_category === 'categorical');
        if (catCols.length) {
            const best = catCols.reduce((a, b) => a.unique_count > b.unique_count ? a : b);
            uniqueEntities = best.unique_count;
        }
    }

    // ── Unique categories (event/category column) ────────────────────
    let uniqueCategories = null;
    let catLabel = 'Unique Categories';
    if (colRoles.event_col) {
        const evcol = cols.find(col => col.name === colRoles.event_col);
        if (evcol) {
            uniqueCategories = evcol.unique_count;
            catLabel = 'Unique ' + colRoles.event_col
                .replace(/_/g, ' ')
                .replace(/\b\w/g, l => l.toUpperCase());
        }
    }

    // ── Data completeness (cell-level: non-null cells / total cells) ─
    let completeness = null;
    const rowCount = p.row_count || 0;
    if (cols.length > 0 && rowCount > 0) {
        const totalCells = rowCount * cols.length;
        const nonNullCells = cols.reduce((sum, col) => sum + (col.non_null_count || 0), 0);
        completeness = ((nonNullCells / totalCells) * 100).toFixed(1);
    }

    // ── Date span ────────────────────────────────────────────────────
    const dtCol = cols.find(col => col.type_category === 'datetime');
    const dateSpanDays = dtCol?.stats?.date_range_days;

    // ── Memory ───────────────────────────────────────────────────────
    const memMb = p.memory_mb;
    let memDisplay = null;
    if (memMb != null) {
        memDisplay = memMb < 1
            ? (memMb * 1024).toFixed(0) + ' KB'
            : memMb.toFixed(1) + ' MB';
    }

    // ── Build stat cards ─────────────────────────────────────────────
    const cards = [
        { val: p.row_count?.toLocaleString() || '0', label: 'Rows' },
        { val: p.column_count || '0', label: 'Columns' },
    ];
    if (uniqueEntities) cards.push({ val: uniqueEntities.toLocaleString(), label: entityLabel });
    if (uniqueCategories) cards.push({ val: uniqueCategories.toLocaleString(), label: catLabel });
    if (dateSpanDays) cards.push({ val: dateSpanDays + 'd', label: 'Date Span' });
    if (completeness !== null) cards.push({ val: completeness + '%', label: 'Completeness' });
    if (memDisplay) cards.push({ val: memDisplay, label: 'Memory' });
    if (c?.dataset_type) cards.push({ val: c.dataset_type.replace(/_/g, ' '), label: 'Type' });

    // ── Column type pills ────────────────────────────────────────────
    const pills = [];
    if (numericCount) pills.push({ count: numericCount, label: 'numeric', cls: 'pill-numeric' });
    if (catCount) pills.push({ count: catCount, label: 'categorical', cls: 'pill-cat' });
    if (datetimeCount) pills.push({ count: datetimeCount, label: 'datetime', cls: 'pill-dt' });

    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text">Here is the <strong>data profile</strong> for your dataset:</div>
        <div class="card-profile">
            ${cards.map(card => `
                <div class="card-profile-item">
                    <div class="card-profile-val">${card.val}</div>
                    <div class="card-profile-label">${card.label}</div>
                </div>
            `).join('')}
        </div>
        ${pills.length ? `
        <div class="card-profile-types">
            ${pills.map(pill => `
                <span class="type-pill ${pill.cls}">
                    <strong>${pill.count}</strong>&nbsp;${pill.label}
                </span>
            `).join('')}
        </div>` : ''}
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addDiscoveryMessage(discovery) {
    const metrics = discovery?.metrics || [];

    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text">Identified <strong>${metrics.length} analyses</strong> for your dataset:</div>
        <div class="card-roadmap">
            ${metrics.map((m, i) => {
        const sev = (m.priority || m.severity || 'medium').toLowerCase();
        const accentColors = { critical: '#DC2626', high: '#EA580C', medium: '#CA8A04', low: '#16A34A', info: '#0891B2' };
        const color = accentColors[sev] || accentColors.medium;
        return `
                <div class="card-metric" data-node-id="${m.id || i}" data-status="pending">
                    <div class="card-metric-accent" style="background:${color}"></div>
                    <div class="card-metric-body">
                        <div class="card-metric-top">
                            <span class="card-metric-type">${escapeHtml(m.analysis_type?.replace(/_/g, ' ') || 'analysis')}</span>
                            <span class="badge ${sev}">${m.feasibility || 'HIGH'}</span>
                        </div>
                        <div class="card-metric-name">${escapeHtml(m.name)}</div>
                        <div class="card-metric-desc">${escapeHtml(m.description)}</div>
                        <div class="card-metric-finding"></div>
                    </div>
                </div>`;
    }).join('')}
        </div>
        <div class="card-actions">
            <button class="btn btn-primary" onclick="runAnalysis()">
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polygon points="5 3 19 12 5 21 5 3"/></svg>
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
    const sev = (result.severity || 'low').toLowerCase();
    const ins = result.insight_summary || {};
    const nav = result.narrative || {};

    // Build detail block — show all available insight fields, no fallback duplication
    const detailRows = [];
    if (ins.key_finding) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Key Finding</span><span>${escapeHtml(ins.key_finding)}</span></div>`);
    if (ins.top_values) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Notable Values</span><span>${escapeHtml(ins.top_values)}</span></div>`);
    if (ins.anomalies) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Anomalies</span><span>${escapeHtml(ins.anomalies)}</span></div>`);
    if (ins.recommendation) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Recommendation</span><span>${escapeHtml(ins.recommendation)}</span></div>`);
    if (nav.what_it_means) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">What It Means</span><span>${escapeHtml(nav.what_it_means)}</span></div>`);
    if (nav.proposed_fix) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Proposed Fix</span><span>${escapeHtml(nav.proposed_fix)}</span></div>`);

    const detailHtml = detailRows.length
        ? `<div class="card-chart-detail">${detailRows.join('')}</div>`
        : '';

    content.innerHTML = `
        <div class="card-chart">
            <div class="card-chart-header">
                <span class="card-chart-type">${escapeHtml(result.analysis_type?.replace(/_/g, ' ').toUpperCase() || '')}</span>
                <span class="badge ${sev}">${sev.toUpperCase()}</span>
            </div>
            <div class="card-chart-finding">${escapeHtml(result.top_finding)}</div>
            <div class="card-chart-frame" id="chart-frame-${result.analysis_id}">
                <iframe src="/chart/${sessionId}/${result.analysis_id}" loading="lazy"></iframe>
            </div>
            ${detailHtml}
        </div>
    `;
    // Auto-resize iframe to full chart height (same-origin — no CORS restriction)
    const iframe = content.querySelector('iframe');
    if (iframe) {
        iframe.addEventListener('load', function () {
            try {
                const h = this.contentDocument.documentElement.scrollHeight;
                if (h > 100) this.style.height = h + 'px';
            } catch (e) { /* cross-origin fallback — keep CSS default */ }
        });
    }
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addExecutiveSummaryMessage(summary) {
    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text"><strong>Executive Summary</strong></div>
        <div class="card-exec">
            <div class="card-exec-overview">${escapeHtml(summary.overview || summary.overall_health || '')}</div>
            ${(summary.key_findings || summary.top_priorities || []).map(f => `<div class="card-exec-finding">${escapeHtml(f)}</div>`).join('')}
            ${summary.business_impact ? `<div class="card-exec-impact"><span class="card-exec-label">Business Impact</span>${escapeHtml(summary.business_impact)}</div>` : ''}
            ${summary.timeline ? `<div class="card-exec-impact"><span class="card-exec-label">Timeline</span>${escapeHtml(summary.timeline)}</div>` : ''}
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addDetailedInsightsMessage(detailedInsights) {
    const insights = detailedInsights?.insights || [];
    if (!insights.length) return;

    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text"><strong>Detailed Insights</strong></div>
        <div class="synthesis-section">
            ${insights.map(ins => `
                <div class="synthesis-card synthesis-insight">
                    <div class="synthesis-card-header">
                        <span class="synthesis-card-title">${escapeHtml(ins.title || '')}</span>
                        <span class="badge ${(ins.fix_priority || 'low').toLowerCase()}">${(ins.fix_priority || 'low').toUpperCase()}</span>
                    </div>
                    ${ins.ai_summary ? `<p class="synthesis-body">${escapeHtml(ins.ai_summary)}</p>` : ''}
                    ${ins.root_cause_hypothesis ? `<div class="synthesis-detail-row"><span class="synthesis-label">Root Cause</span><span>${escapeHtml(ins.root_cause_hypothesis)}</span></div>` : ''}
                    ${ins.ux_implications ? `<div class="synthesis-detail-row"><span class="synthesis-label">UX Impact</span><span>${escapeHtml(ins.ux_implications)}</span></div>` : ''}
                    ${ins.how_to_fix?.length ? `
                        <div class="synthesis-detail-row"><span class="synthesis-label">How to Fix</span>
                            <ol class="synthesis-list">${ins.how_to_fix.map(f => `<li>${escapeHtml(f)}</li>`).join('')}</ol>
                        </div>` : ''}
                </div>
            `).join('')}
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addPersonasMessage(personas) {
    const list = personas?.personas || [];
    if (!list.length) return;

    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text"><strong>User Personas</strong></div>
        <div class="synthesis-section synthesis-personas">
            ${list.map(p => `
                <div class="synthesis-card synthesis-persona">
                    <div class="synthesis-card-header">
                        <span class="synthesis-card-title">${escapeHtml(p.name || '')}</span>
                        <span class="badge ${(p.priority_level || 'low').toLowerCase()}">${escapeHtml(p.size || '')}</span>
                    </div>
                    ${p.profile ? `<p class="synthesis-body">${escapeHtml(p.profile)}</p>` : ''}
                    ${p.pain_points?.length ? `<div class="synthesis-detail-row"><span class="synthesis-label">Pain Points</span><ul class="synthesis-list">${p.pain_points.map(x => `<li>${escapeHtml(x)}</li>`).join('')}</ul></div>` : ''}
                    ${p.opportunities?.length ? `<div class="synthesis-detail-row"><span class="synthesis-label">Opportunities</span><ul class="synthesis-list">${p.opportunities.map(x => `<li>${escapeHtml(x)}</li>`).join('')}</ul></div>` : ''}
                </div>
            `).join('')}
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addInterventionsMessage(interventions) {
    const strategies = interventions?.strategies || [];
    if (!strategies.length) return;

    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text"><strong>Intervention Strategies</strong></div>
        <div class="synthesis-section">
            ${strategies.map(s => `
                <div class="synthesis-card synthesis-intervention">
                    <div class="synthesis-card-header">
                        <span class="synthesis-card-title">${escapeHtml(s.title || '')}</span>
                        <span class="badge ${(s.severity || 'low').toLowerCase()}">${(s.severity || 'LOW').toUpperCase()}</span>
                    </div>
                    ${s.realtime_interventions?.length ? `<div class="synthesis-detail-row"><span class="synthesis-label">Real-time</span><ul class="synthesis-list">${s.realtime_interventions.map(x => `<li>${escapeHtml(x)}</li>`).join('')}</ul></div>` : ''}
                    ${s.proactive_outreach?.length ? `<div class="synthesis-detail-row"><span class="synthesis-label">Proactive</span><ul class="synthesis-list">${s.proactive_outreach.map(x => `<li>${escapeHtml(x)}</li>`).join('')}</ul></div>` : ''}
                </div>
            `).join('')}
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addCrossConnectionsMessage(connections) {
    const list = connections?.connections || [];
    if (!list.length) return;

    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text"><strong>Cross-Metric Connections</strong></div>
        <div class="synthesis-section">
            ${list.map(c => `
                <div class="synthesis-card synthesis-connection">
                    <div class="synthesis-connection-pair">
                        <span class="synthesis-finding-tag">${escapeHtml(c.finding_a || '')}</span>
                        <span class="synthesis-arrow">→</span>
                        <span class="synthesis-finding-tag">${escapeHtml(c.finding_b || '')}</span>
                    </div>
                    ${c.synthesized_meaning ? `<p class="synthesis-body synthesis-meaning">${escapeHtml(c.synthesized_meaning)}</p>` : ''}
                </div>
            `).join('')}
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
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
    if (el?.parentNode) el.parentNode.removeChild(el);
}

function createAIMessageEl() {
    const el = document.createElement('div');
    el.className = 'msg msg-ai';
    el.innerHTML = `
        <div class="msg-ai-avatar">
            <svg width="14" height="14" viewBox="0 0 20 20" fill="none">
                <rect x="2" y="2" width="7" height="7" rx="1.5" fill="currentColor" opacity="0.9"/>
                <rect x="11" y="2" width="7" height="7" rx="1.5" fill="currentColor" opacity="0.4"/>
                <rect x="2" y="11" width="7" height="7" rx="1.5" fill="currentColor" opacity="0.4"/>
                <rect x="11" y="11" width="7" height="7" rx="1.5" fill="currentColor" opacity="0.7"/>
            </svg>
        </div>
        <div class="msg-ai-content"></div>
    `;
    return el;
}

/* ─── Custom Analysis ────────────────────────────────────────── */
async function addCustomAnalysis() {
    const text = prompt('Describe the custom analysis you want to run:');
    if (!text) return;
    if (!sessionId) { addAIMessage('No active session. Upload a file first.'); return; }

    addUserMessage(text);
    const customId = 'C' + (document.querySelectorAll('.card-metric[data-custom]').length + 1);
    const roadmap = document.querySelector('.card-roadmap');

    // Helper: update the roadmap card status line
    const setCardStatus = (statusHtml, statusClass) => {
        const card = document.querySelector(`[data-node-id="${customId}"]`);
        if (!card) return;
        card.dataset.status = statusClass;
        const finding = card.querySelector('.card-metric-finding');
        if (finding) finding.innerHTML = statusHtml;
    };

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
                <div class="card-metric-desc">Checking data compatibility…</div>
                <div class="card-metric-finding visible">
                    <span class="processing-spinner" style="display:inline-block;width:10px;height:10px;border:1.5px solid var(--border-default);border-top-color:var(--accent);border-radius:50%;animation:spin 700ms linear infinite;vertical-align:middle;margin-right:6px;"></span>
                    Validating against your data…
                </div>
            </div>
        `;
        roadmap.appendChild(card);
        scrollToBottom();
    }

    try {
        const res = await fetch(`/add-metric/${sessionId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ metric: text }),
        });
        const data = await res.json();

        if (data.status === 'success') {
            // Update card: show AI-generated name + description if available
            const card = document.querySelector(`[data-node-id="${customId}"]`);
            if (card) {
                if (data.metric_name) {
                    const nameEl = card.querySelector('.card-metric-name');
                    if (nameEl) nameEl.textContent = data.metric_name;
                }
                if (data.description) {
                    const descEl = card.querySelector('.card-metric-desc');
                    if (descEl) descEl.textContent = data.description;
                }
            }
            setCardStatus(escapeHtml(data.top_finding || '✓ Complete'), 'complete');

            if (data.chart_path) {
                addChartMessage({
                    analysis_id: data.analysis_id,
                    analysis_type: data.analysis_type || 'custom',
                    top_finding: data.top_finding || '',
                    severity: data.severity || 'info',
                    chart_path: data.chart_path,
                    narrative: { what_it_means: data.description || '' },
                });
            } else {
                addAIMessage(data.top_finding || 'Custom analysis completed.');
            }

        } else if (data.status === 'unsupported') {
            // AI says the data doesn't support this — show what's missing
            setCardStatus('✗ Data doesn\'t support this', 'failed');
            const card = document.querySelector(`[data-node-id="${customId}"]`);
            if (card) {
                const descEl = card.querySelector('.card-metric-desc');
                if (descEl) descEl.textContent = data.reason || 'Unsupported analysis';
            }

            const missing = data.missing_requirements || [];
            let msg = `**Can't run this analysis** — ${data.reason || 'The available data doesn\'t support it.'}`;
            if (missing.length) {
                msg += `\n\n**To run this you'd need:**\n${missing.map(r => `• ${r}`).join('\n')}`;
            }
            addAIMessage(msg);

        } else {
            setCardStatus(`✗ ${escapeHtml(data.reason || data.error || 'Failed')}`, 'failed');
            addAIMessage(`Analysis issue: ${data.reason || data.error || 'Something went wrong.'}`);
        }
    } catch (err) {
        setCardStatus(`✗ ${escapeHtml(err.message)}`, 'failed');
        addAIMessage(`Custom analysis failed: ${err.message}`);
    }
}

/* ─── Report Download ────────────────────────────────────────── */
window.downloadReport = async function () {
    try {
        // Refresh report first so any custom analyses added after the pipeline are included
        startProcessingStatus('report');
        await fetch(`/report/refresh/${sessionId}`, { method: 'POST' });
        stopProcessingStatus();

        const res = await fetch(`/report/${sessionId}`);
        if (!res.ok) { addAIMessage('Report not available yet.'); return; }
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `analysis_report_${Date.now()}.html`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    } catch (err) {
        stopProcessingStatus();
        addAIMessage('Download failed: ' + err.message);
    }
};

/* ─── Node Retry ─────────────────────────────────────────────── */
window.retryNode = async function (nodeId, btnEl) {
    if (!sessionId) return;
    const card = $(`[data-node-id="${nodeId}"]`);
    if (card) {
        card.dataset.status = 'running';
        const finding = card.querySelector('.card-metric-finding');
        if (finding) {
            finding.innerHTML = `<span class="processing-spinner" style="display:inline-block;width:10px;height:10px;border:1.5px solid var(--border-default);border-top-color:var(--accent);border-radius:50%;animation:spin 700ms linear infinite;vertical-align:middle;margin-right:6px;"></span>Retrying…`;
        }
    }
    try {
        const res = await fetch(`/retry/${sessionId}/${encodeURIComponent(nodeId)}`, { method: 'POST' });
        const data = await res.json();
        if (data.status === 'success') {
            if (card) {
                card.dataset.status = 'complete';
                const finding = card.querySelector('.card-metric-finding');
                if (finding) finding.innerHTML = escapeHtml(data.top_finding || '✓ Complete');
            }
            if (data.chart_path) {
                addChartMessage({
                    analysis_id: nodeId,
                    analysis_type: data.analysis_type,
                    top_finding: data.top_finding || '',
                    severity: data.severity || 'info',
                    chart_path: data.chart_path,
                    insight_summary: data.insight_summary || {},
                    narrative: data.narrative || {},
                });
            } else if (data.top_finding) {
                addAIMessage(data.top_finding);
            }
        } else {
            if (card) {
                card.dataset.status = 'failed';
                const finding = card.querySelector('.card-metric-finding');
                if (finding) {
                    finding.innerHTML = `<span style="color:var(--error)">✗ ${escapeHtml(data.error || 'Failed')}</span>
                        <button class="btn-retry-node" onclick="retryNode('${escapeHtml(nodeId)}', this)">↺ Retry</button>`;
                }
            }
            addAIMessage(`Retry failed for ${nodeId}: ${data.error || 'Unknown error'}`);
        }
    } catch (err) {
        if (card) card.dataset.status = 'failed';
        addAIMessage(`Retry error: ${err.message}`);
    }
};

/* ─── Helpers ────────────────────────────────────────────────── */
function formatText(text) {
    return text
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        .replace(/\n/g, '<br>');
}

function escapeHtml(text) {
    if (typeof text !== 'string') text = String(text ?? '');
    const d = document.createElement('div');
    d.textContent = text;
    return d.innerHTML;
}

function scrollToBottom() {
    requestAnimationFrame(() => {
        if (lenis) {
            lenis.scrollTo(chatScroll.scrollHeight, { duration: 0.6 });
        } else {
            chatScroll.scrollTop = chatScroll.scrollHeight;
        }
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
