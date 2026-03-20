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

function showClarificationPanel(data) {
    const ambiguous = (data?.ambiguous_nodes || []);
    const message = data?.message || 'Please confirm column roles to continue.';
    const colRoles = ['entity_col', 'time_col', 'event_col', 'value_col', 'outcome_col'];

    // Get all CSV columns from the profiler output if available
    let allCols = [];
    if (window._lastProfileCols && Array.isArray(window._lastProfileCols)) {
        allCols = window._lastProfileCols;
    }

    const roleOptions = colRoles.map(role => {
        const opts = ['(none)', ...allCols].map(c =>
            `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`
        ).join('');
        return `
          <div class="clarification-row">
            <label>${role}</label>
            <select data-role="${escapeHtml(role)}" class="clarification-select">
              ${opts}
            </select>
          </div>`;
    }).join('');

    const panelHtml = `
      <div class="clarification-panel" id="clarificationPanel">
        <div class="clarification-header">Column Role Confirmation Required</div>
        <p class="clarification-msg">${escapeHtml(message)}</p>
        ${ambiguous.length ? `<p class="clarification-affected">Affected analyses: ${ambiguous.map(n => escapeHtml(n.analysis_type)).join(', ')}</p>` : ''}
        <div class="clarification-roles">${roleOptions}</div>
        <button class="clarification-submit" onclick="submitClarification()">Confirm &amp; Continue</button>
      </div>`;

    addAIMessage(panelHtml, true);
}

async function submitClarification() {
    const selects = document.querySelectorAll('.clarification-select');
    const columnRoles = {};
    selects.forEach(sel => {
        const val = sel.value;
        if (val && val !== '(none)') {
            columnRoles[sel.dataset.role] = val;
        }
    });

    if (Object.keys(columnRoles).length === 0) {
        alert('Please assign at least one column role before continuing.');
        return;
    }

    const btn = document.querySelector('.clarification-submit');
    if (btn) { btn.disabled = true; btn.textContent = 'Submitting…'; }

    try {
        const resp = await fetch(`/clarify/${sessionId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ column_roles: columnRoles }),
        });
        if (!resp.ok) throw new Error(await resp.text());
        const panel = document.getElementById('clarificationPanel');
        if (panel) panel.closest('.ai-message, .message-bubble, .chat-bubble') ?.remove();
        addAIMessage('Column roles confirmed. Re-running discovery…');
        // Re-open SSE stream to track the re-discover progress
        startSSEStream();
    } catch (err) {
        addAIMessage(`Clarification submission failed: ${escapeHtml(String(err))}`);
        if (btn) { btn.disabled = false; btn.textContent = 'Confirm & Continue'; }
    }
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

/*  —  —  —  Send  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
sendBtn.addEventListener('click', handleSend);
chatInput.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend(); }
});
chatInput.addEventListener('input', () => {
    chatInput.style.height = 'auto';
    chatInput.style.height = Math.min(chatInput.scrollHeight, 150) + 'px';
});

/*  —  —  —  Sidebar Toggle  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
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

/*  —  —  —  Core Functions  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
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
    // Show ring immediately from upload  —  visible for entire pipeline
    if (progressRing) progressRing.classList.remove('hidden');
    updateProgressRingStage('upload');

    const formData = new FormData();
    formData.append('file', file);

    try {
        const res = await fetch('/upload', { method: 'POST', body: formData });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Upload failed');

        sessionId = data.session_id;
        outputFolder = data.output_folder || data.session_id;
        // No session saved to storage  —  fresh start on reload

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

        /*  —  —  Profile  —  —  */
        updateStage(2, 'active');
        startProcessingStatus('profile');
        updateProgressRingStage('profiling');

        const profileRes = await fetch(`/profile/${sessionId}`, { method: 'POST' });
        const profileData = await profileRes.json();
        if (!profileRes.ok) throw new Error(profileData.detail || 'Profiling failed');

        updateStage(2, 'complete');
        setProgress(45);
        addProfileMessage(profileData);

        /*  —  —  Discover  —  —  */
        updateStage(3, 'active');
        startProcessingStatus('discover');
        updateProgressRingStage('discovering');

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
    if (progressRing) { progressRing.classList.remove('complete'); }
    updateProgressRingStage('analyzing');

    fetch(`/analyze/${sessionId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request: 'Analyze all discovered metrics.', custom_metrics: [] }),
    }).then(() => {
        // Prime the terminal card and ring before any charts arrive
        updatePipelineStatusPanel();
    });

    pollStartTime = Date.now();
    _finishCalled = false;     // reset for new run
    _hasReport = false;        // reset — a fresh run hasn't generated a report yet
    _renderInProgress = false; // reset render lock
    if (pollInterval) clearInterval(pollInterval);

    // --- #12: Try SSE stream first; fall back to polling if unavailable ---
    startSSEStream();
}

// #12: SSE stream  —  receives real-time events from the pipeline backend.
// Falls back to polling if connection fails (e.g. proxy timeout, server restart).
let _activeSSE = null;
let _finishCalled = false;   // guard: ensure finishPipeline() runs at most once per session
let _hasReport = false;      // set true only when report_ready fires with a real path
function startSSEStream() {
    if (_activeSSE) { try { _activeSSE.close(); } catch (e) { } }

    const evtSource = new EventSource(`/stream/${sessionId}`);
    _activeSSE = evtSource;
    let _streamReceived = false;

    evtSource.onmessage = async (e) => {
        _streamReceived = true;
        let ev;
        try { ev = JSON.parse(e.data); } catch { return; }

        if (ev.type === 'node_complete') {
            await updatePipelineStatusPanel();   // terminal + ring first
            await renderNewCharts();             // then charts below terminal
            const nodeStatuses = {};
            nodeStatuses[ev.data.analysis_id] = 'complete';
            updateMetricCards(nodeStatuses);
            // Keep stage indicator updated
            updateStage(4, 'active');
        } else if (ev.type === 'synthesis_started') {
            updateStage(4, 'complete');
            updateStage(5, 'active');
            startProcessingStatus('synthesize');
            updateProgressRingStage('synthesizing');
        } else if (ev.type === 'synthesis_complete') {
            updateStage(4, 'complete');  // belt-and-suspenders: mark Analyze done if synthesis_started was missed
            updateStage(5, 'complete');
            updateProgressRingStage('synthesizing');
            startProcessingStatus('synthesize');
        } else if (ev.type === 'report_ready') {
            _hasReport = true;           // only fires when an actual report path exists
            updateStage(6, 'active');
            updateProgressRingStage('building_report');
            startProcessingStatus('report');
        } else if (ev.type === 'node_failed') {
            const nodeId = ev.data?.node_id || 'unknown';
            const errMsg = ev.data?.error || 'unknown error';
            addAIMessage(`⚠️ Analysis ${nodeId} could not complete: ${errMsg}. Other analyses will continue  —  results for this node will be unavailable.`);
        } else if (ev.type === 'report_error') {
            const errMsg = ev.data?.error || 'Report build failed';
            addAIMessage(`Report could not be generated: ${errMsg}`);
            addRerunSynthesisButton();
            stopProcessingStatus();
        } else if (ev.type === 'stream_end') {
            evtSource.close();
            _activeSSE = null;
            stopProcessingStatus();
            setProgress(100);
            const status = ev.data?.status || 'complete';
            if (status === 'complete') {
                updateProgressRingStage('complete');
                if (!_finishCalled) { _finishCalled = true; await finishPipeline(); }
            } else {
                addAIMessage('Pipeline encountered an error. Some results may be available.');
            }
        } else if (ev.type === 'status_update' && ev.data?.status === 'clarification_needed') {
            evtSource.close();
            _activeSSE = null;
            stopProcessingStatus();
            showClarificationPanel(ev.data);
        }
    };

    evtSource.onerror = () => {
        evtSource.close();
        _activeSSE = null;
        // Always fall back to polling  —  handles both "never connected" and
        // "stream dropped before stream_end was processed" (which leaves synthesis invisible)
        console.warn('SSE error/closed, falling back to polling');
        _startPolling();
    };

    // Safety timeout: if no stream_end received in MAX_POLL_TIME, switch to polling
    setTimeout(() => {
        if (_activeSSE === evtSource) {
            evtSource.close();
            _activeSSE = null;
            if (Date.now() - pollStartTime < MAX_POLL_TIME) {
                _startPolling();
            }
        }
    }, MAX_POLL_TIME);
}

// Original polling fallback (preserved intact for reliability)
function _startPolling() {
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
            const phase = { analyzing: 'analyze', synthesizing: 'synthesize', building_report: 'report' }[status.session_status];
            if (phase) startProcessingStatus(phase);
            // Update ring with overall pipeline stage
            if (status.session_status) updateProgressRingStage(status.session_status);
            if (status.session_status === 'complete') {
                clearInterval(pollInterval);
                stopProcessingStatus();
                setProgress(100);
                updateProgressRingStage('complete');
                if (!_finishCalled) { _finishCalled = true; await finishPipeline(); }
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

        //  —  —  Terminal card  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  — 
        if (nodes.length > 0) {
            if (!_terminalCard) {
                _terminalTotalNodes = nodes.length;
                addTerminalCard(nodes.length);
            }
            updateTerminalCard(nodes);
        }

        //  —  —  Circular progress ring  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  — 
        const completed = nodes.filter(n => n.status === 'complete' || n.status === 'failed').length;
        const total = nodes.length || 1;
        if (nodes.length > 0) {
            if (progressRing) progressRing.classList.remove('hidden');
            updateProgressRing(completed, total);
        }

        //  —  —  Gate warnings → AI chat (once each)  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  — 
        const warnings = data.gate_result?.warnings || [];
        warnings.forEach(w => {
            const key = 'warn:' + w;
            if (!_warnedMessages.has(key)) {
                _warnedMessages.add(key);
                addAIMessage(`⚠️ Gate warning: ${w}`);
            }
        });

        //  —  —  Monitor alerts → AI chat (once each)  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  — 
        const alerts = data.alerts || [];
        alerts.forEach(a => {
            const key = 'alert:' + a.event + ':' + JSON.stringify(a.data);
            if (!_warnedMessages.has(key)) {
                _warnedMessages.add(key);
                addAIMessage(`ðŸ”” [${a.event}] ${JSON.stringify(a.data)}`);
            }
        });

    } catch (e) {
        console.error('Failed to update pipeline panel', e);
    }
}

/*  —  —  —  Terminal Card  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
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
                    <span class="msg-terminal-title">pipeline  —  analysis</span>
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
        const typeLabel = (n.type || n.id || '').replace(/_/g, ' ');
        const nodeId = (n.id || '').toUpperCase();
        const label = nodeId && n.type ? `${nodeId} · ${typeLabel}` : typeLabel;
        const errorTip = isFail && n.error ? ` title="${escapeHtml(n.error)}"` : '';

        let row = document.querySelector(`#terminal-body .t-line[data-node-id="${n.id}"]`);
        if (!row) {
            row = document.createElement('div');
            row.className = 't-line';
            row.dataset.nodeId = n.id;
            body.appendChild(row);
        }
        row.innerHTML = `
            <span class="t-icon ${statusCls}">${icon}</span>
            <span class="t-line-type ${labelCls}"${errorTip}>${escapeHtml(label)}</span>
            <span class="t-status-text ${statusCls}"${errorTip}>${statusText}</span>
        `;

        if (isOk || isFail) completed++;
    });

    const total = nodes.length || 1;
    if (footer) footer.textContent = `${completed} / ${total}`;
    if (progFill) progFill.style.width = `${Math.round(completed / total * 100)}%`;

    const allDone = nodes.length > 0 && nodes.every(n => n.status === 'complete' || n.status === 'failed');
    if (cursor) cursor.style.display = allDone ? 'none' : '';
}

/*  —  —  —  Circular Progress Ring  —  overall pipeline %  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
// Stage → base percentage of the full pipeline
const STAGE_PCT = {
    upload:         5,
    profiling:      15,
    discovering:    30,
    analyzing:      30,   // slides from 30→75 as nodes complete
    synthesizing:   80,
    building_report:90,
    complete:       100,
};
let _ringCurrentStage = 'upload';
let _ringNodesDone    = 0;
let _ringNodesTotal   = 0;

function _calcOverallPct() {
    if (_ringCurrentStage === 'analyzing' && _ringNodesTotal > 0) {
        const nodeFraction = _ringNodesDone / _ringNodesTotal;
        return Math.round(30 + nodeFraction * 45); // 30 → 75
    }
    return STAGE_PCT[_ringCurrentStage] ?? 5;
}

function updateProgressRing(completed, total) {
    // Legacy call from node-completion path  —  update node counters then redraw
    _ringNodesDone  = completed;
    _ringNodesTotal = total;
    _drawProgressRing(_calcOverallPct());
}

function updateProgressRingStage(stage) {
    _ringCurrentStage = stage;
    if (stage !== 'analyzing') { _ringNodesDone = 0; _ringNodesTotal = 0; }
    _drawProgressRing(_calcOverallPct());
}

function _drawProgressRing(pct) {
    if (!ringFill || !ringLabel || !progressRing) return;
    const circumference = 138.23; // 2Ï€ Ã— 22
    const fraction = Math.min(pct, 100) / 100;
    ringFill.style.strokeDashoffset = circumference * (1 - fraction);

    if (pct >= 100) {
        progressRing.classList.add('complete');
        ringLabel.textContent = '✓';
    } else {
        progressRing.classList.remove('complete');
        ringLabel.textContent = `${pct}%`;
    }
}

function updateMetricCards(nodeStatuses) {
    Object.entries(nodeStatuses).forEach(([nodeId, status]) => {
        const card = document.querySelector(`.card-metric[data-node-id="${nodeId}"]`);
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

let _renderInProgress = false;
async function renderNewCharts() {
    if (_renderInProgress) return;   // don't pile up concurrent fetches
    _renderInProgress = true;
    try {
        const res = await fetch(`/results/${sessionId}`);
        if (!res.ok) return;
        const results = await res.json();

        results.forEach(result => {
            if (result.chart_path && !renderedCharts.has(result.analysis_id)) {
                renderedCharts.add(result.analysis_id);
                addChartMessage(result);
                const card = document.querySelector(`.card-metric[data-node-id="${result.analysis_id}"]`);
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
    } finally {
        _renderInProgress = false;
    }
}

async function finishPipeline() {
    updateStage(6, 'complete');

    try {
        startProcessingStatus('synthesize');
        const res = await fetch(`/synthesis/${sessionId}`);
        const synthesis = await res.json();
        stopProcessingStatus();

        // Normalize synthesis sections  —  handle flat array OR {key: [...]} wrapper object
        // (LLM sometimes returns a bare list; this prevents silent render failures)
        const di = Array.isArray(synthesis.detailed_insights) ? { insights: synthesis.detailed_insights } : (synthesis.detailed_insights || {});
        const ps = Array.isArray(synthesis.personas) ? { personas: synthesis.personas } : (synthesis.personas || {});
        const inv = Array.isArray(synthesis.intervention_strategies) ? { strategies: synthesis.intervention_strategies } : (synthesis.intervention_strategies || {});
        const cx = Array.isArray(synthesis.cross_metric_connections) ? { connections: synthesis.cross_metric_connections } : (synthesis.cross_metric_connections || {});

        // Render synthesis sections in order: detailed → personas → interventions → connections → summary → narrative
        if (di.insights?.length) addDetailedInsightsMessage(di);
        if (ps.personas?.length) addPersonasMessage(ps);
        if (inv.strategies?.length) addInterventionsMessage(inv);
        if (cx.connections?.length) addCrossConnectionsMessage(cx);
        if (synthesis.executive_summary) {
            addExecutiveSummaryMessage(synthesis.executive_summary);
        }
        if (synthesis.conversational_report) {
            addConversationalReportMessage(synthesis.conversational_report);
        }

        // --- #8: Adversarial critic review card ---
        if (synthesis._critic_review) {
            addCriticReviewMessage(synthesis._critic_review);
        }

        // Build reliability badge from critic verdict (shown inline next to report button)
        let reliabilityBadge = '';
        if (synthesis._critic_review) {
            const cr = synthesis._critic_review;
            const approved = cr.approved !== false;
            const conf = typeof cr.confidence_adjustment === 'number' ? cr.confidence_adjustment : 1;
            const confPct = Math.round(conf * 100);
            const badgeColor = approved && conf >= 0.8 ? 'var(--success)' : conf >= 0.6 ? 'var(--warning)' : 'var(--error)';
            const badgeLabel = approved ? `✓ Reliable · ${confPct}%` : `⚠ Issues Found · ${confPct}%`;
            reliabilityBadge = `<span class="reliability-badge" style="background:${badgeColor}15;color:${badgeColor};border:1px solid ${badgeColor}40;border-radius:6px;padding:3px 10px;font-size:11px;font-weight:600;letter-spacing:0.03em;">${badgeLabel}</span>`;
        }

        // Fallback: if SSE missed report_ready (race condition), check via HTTP HEAD
        if (!_hasReport) {
            try {
                const hRes = await fetch(`/report/${sessionId}`, { method: 'HEAD' });
                if (hRes.ok) _hasReport = true;
            } catch (_) { }
        }

        if (_hasReport) {
            addAIMessageHTML(`
                <p>Analysis complete. Full report is ready.</p>
                ${reliabilityBadge ? `<div style="margin-bottom:10px;">${reliabilityBadge}</div>` : ''}
                <div class="card-actions" style="margin-top: 12px;">
                    <button class="btn btn-primary" onclick="window.open('/report/${sessionId}', '_blank')">
                        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                        Open Full Report
                    </button>
                    <button class="btn btn-ghost" onclick="downloadReport()">Download HTML</button>
                </div>
            `);
            addReportEmbed(sessionId);
        } else {
            addAIMessageHTML(`
                <p>Analysis complete. Synthesis results are available below.</p>
                ${reliabilityBadge ? `<div style="margin-bottom:10px;">${reliabilityBadge}</div>` : ''}
            `);
        }

        // "Improve Synthesis" is only shown on failure or user request — not here.
    } catch (err) {
        stopProcessingStatus();
        console.error('Synthesis error:', err);
    }

    chatInput.placeholder = 'Ask about the analysis results…';
}

function addReportEmbed(sid) {
    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="report-embed-card">
            <div class="report-embed-header">
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/>
                </svg>
                <span>Analysis Report</span>
                <button class="report-embed-toggle" onclick="toggleReportEmbed(this)" title="Expand / Collapse">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="6 9 12 15 18 9"/></svg>
                </button>
            </div>
            <div class="report-embed-body collapsed">
                <iframe src="/report/${sid}" class="report-iframe" sandbox="allow-scripts allow-same-origin" loading="lazy"></iframe>
            </div>
        </div>`;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function toggleReportEmbed(btn) {
    const body = btn.closest('.report-embed-card').querySelector('.report-embed-body');
    const isCollapsed = body.classList.toggle('collapsed');
    btn.querySelector('svg polyline').setAttribute('points', isCollapsed ? '6 9 12 15 18 9' : '6 15 12 9 18 15');
    if (!isCollapsed) scrollToBottom();
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

/*  —  —  —  Message Builders  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
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

    const colTypes = p.column_types || {};
    const numericCount = (colTypes.numeric || []).length;
    const catCount = (colTypes.categorical || []).length;
    const datetimeCount = (colTypes.datetime || []).length;

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

    //  —  —  Unique categories (event/category column)  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  — 
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

    let completeness = null;
    const rowCount = p.row_count || 0;
    if (cols.length > 0 && rowCount > 0) {
        const totalCells = rowCount * cols.length;
        const nonNullCells = cols.reduce((sum, col) => sum + (col.non_null_count || 0), 0);
        completeness = ((nonNullCells / totalCells) * 100).toFixed(1);
    }

    //  —  —  Date span  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  — 
    const dtCol = cols.find(col => col.type_category === 'datetime');
    const dateSpanDays = dtCol?.stats?.date_range_days;

    //  —  —  Memory  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  — 
    const memMb = p.memory_mb;
    let memDisplay = null;
    if (memMb != null) {
        memDisplay = memMb < 1
            ? (memMb * 1024).toFixed(0) + ' KB'
            : memMb.toFixed(1) + ' MB';
    }

    //  —  —  Build stat cards  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  — 
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

    //  —  —  Column type pills  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  — 
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

    // Build detail block  —  show all available insight fields, no fallback duplication
    const detailRows = [];
    // Decision-maker takeaway is the most valuable per-node LLM insight  —  show it first
    if (ins.decision_maker_takeaway) detailRows.push(`<div class="chart-detail-row chart-detail-dm"><span class="chart-detail-label">Key Insight</span><span>${escapeHtml(ins.decision_maker_takeaway)}</span></div>`);
    if (ins.key_finding) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Key Finding</span><span>${escapeHtml(ins.key_finding)}</span></div>`);
    if (ins.top_values) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Notable Values</span><span>${escapeHtml(ins.top_values)}</span></div>`);
    if (ins.anomalies) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Anomalies</span><span>${escapeHtml(ins.anomalies)}</span></div>`);
    if (ins.recommendation) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Recommendation</span><span>${escapeHtml(ins.recommendation)}</span></div>`);
    if (nav.what_it_means) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">What It Means</span><span>${escapeHtml(nav.what_it_means)}</span></div>`);
    if (nav.proposed_fix) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Proposed Fix</span><span>${escapeHtml(nav.proposed_fix)}</span></div>`);
    if (result.confidence) detailRows.push(`<div class="chart-detail-row"><span class="chart-detail-label">Confidence</span><span>${Math.round(result.confidence * 100)}%</span></div>`);

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
    // Auto-resize iframe to full chart height (same-origin  —  no CORS restriction)
    const iframe = content.querySelector('iframe');
    if (iframe) {
        iframe.addEventListener('load', function () {
            try {
                const h = this.contentDocument.documentElement.scrollHeight;
                if (h > 100) this.style.height = h + 'px';
            } catch (e) { /* cross-origin fallback  —  keep CSS default */ }
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
            ${summary.resource_allocation ? `<div class="card-exec-impact"><span class="card-exec-label">Resource Allocation</span>${escapeHtml(summary.resource_allocation)}</div>` : ''}
        </div>
    `;
    chatContainer.appendChild(el);
    scrollToBottom();
}

function addConversationalReportMessage(report) {
    if (!report || report.length < 50) return;
    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');

    // Parse markdown into header+body section pairs
    // Split on lines that start with one or two # characters
    const lines = report.split('\n');
    const sections = [];
    let currentHeader = null;
    let currentBody = [];

    for (const line of lines) {
        if (/^#{1,2} /.test(line)) {
            if (currentHeader !== null) {
                sections.push({ header: currentHeader, body: currentBody.join('\n').trim() });
            }
            currentHeader = line.replace(/^#+\s*/, '');
            currentBody = [];
        } else {
            currentBody.push(line);
        }
    }
    if (currentHeader !== null) {
        sections.push({ header: currentHeader, body: currentBody.join('\n').trim() });
    }

    // If no headers found, render as one block
    if (!sections.length) {
        sections.push({ header: 'Intelligence Report', body: report.trim() });
    }

    // Render a markdown table string into an HTML table
    const renderTable = (tableLines) => {
        // Strip \r (Windows line endings) and filter separator rows like |---|---|
        const rows = tableLines
            .map(l => l.replace(/\r$/, ''))
            .filter(l => l.trim().startsWith('|') && !/^\|[\s\-|: ]+\|$/.test(l.trim()));
        if (!rows.length) return '';
        const parseRow = (row) => row.trim().replace(/^\||\|$/g, '').split('|').map(c => c.trim());
        const [head, ...body] = rows;
        const headCells = parseRow(head).map(c => `<th>${escapeHtml(c)}</th>`).join('');
        const bodyRows = body.map(r => `<tr>${parseRow(r).map(c => `<td>${escapeHtml(c)}</td>`).join('')}</tr>`).join('');
        return `<table class="conv-report-table"><thead><tr>${headCells}</tr></thead><tbody>${bodyRows}</tbody></table>`;
    };

    const renderBody = (text) => {
        const bodyLines = text.split('\n');
        let html = '';
        let i = 0;
        while (i < bodyLines.length) {
            const line = bodyLines[i];
            // ### sub-header
            if (/^### /.test(line)) {
                html += `<div class="conv-report-subheader">${escapeHtml(line.replace(/^###\s*/, ''))}</div>`;
                i++; continue;
            }
            // markdown table block
            if (/^\|/.test(line.trim())) {
                const tableBlock = [];
                while (i < bodyLines.length && /^\|/.test(bodyLines[i].trim())) {
                    tableBlock.push(bodyLines[i]);
                    i++;
                }
                html += renderTable(tableBlock);
                continue;
            }
            // normal line  —  inline markdown
            const rendered = escapeHtml(line)
                .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
                .replace(/^•\s+|^-\s+/, '• ');
            html += rendered ? `<span>${rendered}</span><br>` : '<br>';
            i++;
        }
        return html;
    };

    const sectionsHtml = sections.map(s => `
        <div class="conv-report-section">
            <div class="conv-report-header">${escapeHtml(s.header)}</div>
            <div class="conv-report-body">${renderBody(s.body)}</div>
        </div>
    `).join('');

    content.innerHTML = `
        <div class="msg-ai-text"><strong>Full Intelligence Report</strong></div>
        <div class="conv-report">${sectionsHtml}</div>
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
                    ${ins.possible_causes?.length ? `<div class="synthesis-detail-row"><span class="synthesis-label">Possible Causes</span><ul class="synthesis-list">${ins.possible_causes.map(c => `<li>${escapeHtml(c)}</li>`).join('')}</ul></div>` : ''}
                    ${ins.downstream_implications ? `<div class="synthesis-detail-row"><span class="synthesis-label">Downstream Impact</span><span>${escapeHtml(ins.downstream_implications)}</span></div>` : ''}
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

// --- #8: Adversarial Critic Review card ---
function addCriticReviewMessage(critic) {
    if (!critic || typeof critic !== 'object') return;
    const approved = critic.approved !== false;
    const challenges = Array.isArray(critic.challenges) ? critic.challenges : [];
    const confAdj = typeof critic.confidence_adjustment === 'number' ? critic.confidence_adjustment : 1;
    const verdict = critic.overall_verdict || '';

    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="msg-ai-text"><strong>Adversarial Review</strong></div>
        <div class="critic-card">
            <div class="critic-status-row">
                <span class="critic-status-label ${approved ? 'critic-status-ok' : 'critic-status-warn'}">
                    ${approved ? 'Synthesis Approved' : 'Issues Flagged'}
                </span>
                <span class="badge ${approved ? 'low' : 'medium'}" style="font-size:10px;">
                    Confidence ${Math.round(confAdj * 100)}%
                </span>
            </div>
            ${verdict ? `<p class="critic-verdict">${escapeHtml(verdict)}</p>` : ''}
            <div class="critic-challenges">
                ${challenges.length
            ? challenges.map(c => `
                        <div class="critic-challenge">
                            <div class="critic-challenge-top">
                                <span class="badge ${c.severity === 'high' ? 'high' : c.severity === 'low' ? 'low' : 'medium'}">${escapeHtml((c.severity || 'medium').toUpperCase())}</span>
                                <div class="critic-claim">${escapeHtml(c.claim || '')}</div>
                            </div>
                            <div class="critic-issue">${escapeHtml(c.issue || '')}</div>
                        </div>`).join('')
            : '<p class="critic-none">No significant issues found  —  synthesis is well-grounded.</p>'
        }
            </div>
        </div>`;
    chatContainer.appendChild(el);
    scrollToBottom();
}

// --- #5: Re-run synthesis (shown only on failure) ---
function addRerunSynthesisButton() {
    const el = createAIMessageEl();
    const content = el.querySelector('.msg-ai-content');
    content.innerHTML = `
        <div class="rerun-synthesis-card">
            <p style="margin:0 0 10px;font-size:13px;color:var(--text-secondary);">
                Would you like to re-run the synthesis? You can add instructions to guide the analysis.
            </p>
            <div class="rerun-synthesis-input-row">
                <input type="text" class="rerun-synthesis-input"
                    placeholder="What would you like more of? (optional)"
                    onkeydown="if(event.key==='Enter')triggerSynthesisRerun(this.closest('.rerun-synthesis-card').querySelector('.rerun-btn'))" />
                <button class="rerun-btn" onclick="triggerSynthesisRerun(this)">&#8635; Re-run Synthesis</button>
            </div>
        </div>`;
    chatContainer.appendChild(el);
    scrollToBottom();
    setTimeout(() => el.querySelector('.rerun-synthesis-input')?.focus(), 100);
}

async function triggerSynthesisRerun(btn) {
    const card = btn.closest('.rerun-synthesis-card');
    const input = card?.querySelector('.rerun-synthesis-input');
    const instructions = input?.value?.trim() || '';

    btn.disabled = true;
    btn.textContent = 'Running…';
    if (input) input.disabled = true;

    try {
        const r = await fetch(`/rerun-synthesis/${sessionId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ instructions }),
        });
        if (r.ok) {
            const msg = instructions
                ? `Synthesis restarting with your instructions: "${instructions}"`
                : 'Synthesis restarting — new insights will appear shortly.';
            addAIMessage(msg);
            pollStartTime = Date.now();
            _finishCalled = false;
            _hasReport = false;
            startSSEStream();
        } else {
            const err = await r.json().catch(() => ({}));
            addAIMessage(`Rerun failed: ${err.detail || r.status}`);
            btn.disabled = false;
            btn.textContent = '&#8635; Re-run Synthesis';
            if (input) input.disabled = false;
        }
    } catch (e) {
        addAIMessage(`Rerun error: ${e.message}`);
        btn.disabled = false;
        btn.textContent = '&#8635; Re-run Synthesis';
        if (input) input.disabled = false;
    }
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

/*  —  —  —  Custom Analysis  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
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
            // AI says the data doesn't support this  —  show what's missing
            setCardStatus('✗ Data doesn\'t support this', 'failed');
            const card = document.querySelector(`[data-node-id="${customId}"]`);
            if (card) {
                const descEl = card.querySelector('.card-metric-desc');
                if (descEl) descEl.textContent = data.reason || 'Unsupported analysis';
            }

            const missing = data.missing_requirements || [];
            let msg = `**Can't run this analysis**  —  ${data.reason || 'The available data doesn\'t support it.'}`;
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

/*  —  —  —  Report Download  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
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

/*  —  —  —  Node Retry  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
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

/*  —  —  —  Helpers  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  —  */
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

(function initSelectionPopup() {
    const popup = document.createElement('div');
    popup.id = 'sel-popup';
    popup.className = 'sel-popup hidden';
    popup.innerHTML = `
        <button class="sel-popup-btn" id="sel-ask-btn">
            <svg width="12" height="12" viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M10 2C5.58 2 2 5.58 2 10s3.58 8 8 8 8-3.58 8-8-3.58-8-8-8zm.75 12.5h-1.5v-1.5h1.5v1.5zm1.56-5.44c-.63.84-1.31 1.1-1.31 2.19H9.5c0-1.53.76-2.17 1.38-2.84.45-.49.87-.94.87-1.66 0-.97-.78-1.75-1.75-1.75s-1.75.78-1.75 1.75H6.75C6.75 5.54 8.2 4 10 4s3.25 1.54 3.25 3.25c0 1.2-.62 1.95-1.19 2.56-.07.08-.13.17-.19.25z"
                      fill="currentColor"/>
            </svg>
            Ask about this
        </button>
        <div class="sel-popup-arrow"></div>`;
    document.body.appendChild(popup);

    let _pendingText = '';

    // Position + show popup above the current selection
    function showPopup() {
        const sel = window.getSelection();
        if (!sel || sel.rangeCount === 0) return;
        const text = sel.toString().trim();
        if (!text || text.length < 3) { hidePopup(); return; }

        // Only trigger if selection is inside the chat container
        const range = sel.getRangeAt(0);
        if (!chatContainer || !chatContainer.contains(range.commonAncestorContainer)) {
            hidePopup(); return;
        }

        _pendingText = text;
        const rect = range.getBoundingClientRect();
        const popupW = 160; // approximate pill width
        let left = rect.left + rect.width / 2 - popupW / 2;
        // Clamp inside viewport
        left = Math.max(8, Math.min(left, window.innerWidth - popupW - 8));
        const top = rect.top - 48; // 44px pill height + 4px gap

        popup.style.left = left + 'px';
        popup.style.top = top + 'px';
        popup.classList.remove('hidden');
    }

    function hidePopup() {
        popup.classList.add('hidden');
        _pendingText = '';
    }

    // Show on mouse-up inside chat
    document.addEventListener('mouseup', (e) => {
        // Small timeout so selection is finalised before we measure
        setTimeout(() => {
            const sel = window.getSelection();
            if (sel && sel.toString().trim().length >= 3) {
                showPopup();
            } else {
                hidePopup();
            }
        }, 10);
    });

    // Hide when clicking anywhere outside the popup
    document.addEventListener('mousedown', (e) => {
        if (!popup.contains(e.target)) hidePopup();
    });

    // Hide on scroll (position would drift)
    window.addEventListener('scroll', hidePopup, { passive: true });
    if (chatContainer) chatContainer.addEventListener('scroll', hidePopup, { passive: true });

    // Main action  —  send selection as a chat question
    document.getElementById('sel-ask-btn').addEventListener('click', () => {
        const text = _pendingText || window.getSelection().toString().trim();
        if (!text) { hidePopup(); return; }

        if (!sessionId) {
            addAIMessage('Upload a file first to ask questions about your data.');
            hidePopup(); return;
        }

        // Clear highlight
        window.getSelection().removeAllRanges();
        hidePopup();

        // Quote short selections; truncate long ones
        const quoted = text.length <= 120
            ? `"${text}"`
            : `"${text.slice(0, 117)}…"`;
        const question = `What does this mean? ${quoted}`;

        addUserMessage(question);
        sendChatMessage(question);
        scrollToBottom();
    });
})();

