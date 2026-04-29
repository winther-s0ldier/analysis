import React, { useState, useRef, useEffect, useImperativeHandle, forwardRef } from 'react';
import { Paperclip, Send, X, FileUp, Plus, Upload, BarChart3, Database } from 'lucide-react';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';
import { useAutoResize } from '../../hooks/useAutoResize';
import { useAutoAnimate } from '@formkit/auto-animate/react';
import { uploadFile, profileDataset, discoverMetrics, analyzeMetrics, sendChatMessage } from '../../api';
import { closeConnection, resetSessionTracking } from '../../services/sseManager';
import { toast } from 'sonner';
import { cn } from '../ui/Badge';
import { GASourcePicker } from './GASourcePicker';
import { BQSourcePicker } from './BQSourcePicker';

function formatSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + ['B', 'KB', 'MB', 'GB'][i];
}

/**
 * InputArea
 *
 * Backend wiring (uploadFile / profileDataset / discoverMetrics / sendChatMessage,
 * pipelineStore, chatStore, sseManager) is unchanged from the original.
 *
 * Only visual layer changes:
 * - `variant="center"` renders a large floating card for the empty state.
 * - `variant="bottom"` (default) renders the original bottom-pinned bar.
 *
 * Exposes `openFilePicker(accept?)` via ref so the centered empty-state can
 * trigger the file picker from suggestion chips with a pre-narrowed accept list.
 */
export const InputArea = forwardRef(function InputArea({ variant = 'bottom' }, ref) {
  const sessionId = usePipelineStore((s) => s.currentSessionId);
  const setSession = usePipelineStore((s) => s.setSession);
  const setPhase = usePipelineStore((s) => s.setPhase);
  const setNodes = usePipelineStore((s) => s.setNodes);
  const setHasReport = usePipelineStore((s) => s.setHasReport);
  const bumpHistory = usePipelineStore((s) => s.bumpHistory);
  const setCanvasNarrative = usePipelineStore((s) => s.setCanvasNarrative);
  const setCanvasOpen = usePipelineStore((s) => s.setCanvasOpen);
  const migrateSession = usePipelineStore((s) => s.migrateSession);
  const addMessage = useChatStore((s) => s.addMessage);
  const insertAfterMessage = useChatStore((s) => s.insertAfterMessage);
  const addOrUpdateChart = useChatStore((s) => s.addOrUpdateChart);
  const migrateChatSession = useChatStore((s) => s.migrateSession);
  const setThinking = useChatStore((s) => s.setThinking);
  const pendingMessage = useChatStore((s) => s.pendingMessage);
  const setPendingMessage = useChatStore((s) => s.setPendingMessage);
  const [file, setFile] = useState(null);
  // Optional companion data-dictionary / schema CSV. Workaround for the
  // single-file upload constraint: when the user attaches a data file
  // they can also attach a small CSV that maps event_name → description,
  // which the backend persists alongside the data and feeds to the
  // profiler. Leaving this null preserves the original single-file flow.
  const [schemaFile, setSchemaFile] = useState(null);
  const [textValue, setTextValue] = useState('');
  const [isHovering, setIsHovering] = useState(false);
  const fileInputRef = useRef(null);
  const schemaInputRef = useRef(null);
  const [acceptOverride, setAcceptOverride] = useState(null);
  const [parent] = useAutoAnimate();
  const isCenter = variant === 'center';
  // "+" menu state
  const [plusMenuOpen, setPlusMenuOpen] = useState(false);
  const [showGaPicker, setShowGaPicker] = useState(false);
  const [showBqPicker, setShowBqPicker] = useState(false);
  const plusMenuRef = useRef(null);

  const textareaRef = useAutoResize(textValue);

  // Imperative handle so parent (centered empty state) can open the picker
  useImperativeHandle(ref, () => ({
    openFilePicker: (accept) => {
      setAcceptOverride(accept || null);
      // setAcceptOverride is async; trigger click on next tick
      setTimeout(() => fileInputRef.current?.click(), 0);
    },
    focus: () => textareaRef.current?.focus(),
  }), [textareaRef]);

  // Consume pendingMessage (set by "Ask about this" buttons on cards)
  useEffect(() => {
    if (!pendingMessage) return;
    setTextValue(pendingMessage);
    setPendingMessage('');
    setTimeout(() => textareaRef.current?.focus(), 50);
  }, [pendingMessage]);

  // Close the "+" menu on outside click / Escape
  useEffect(() => {
    if (!plusMenuOpen) return;
    const onDoc = (e) => {
      if (plusMenuRef.current && !plusMenuRef.current.contains(e.target)) {
        setPlusMenuOpen(false);
      }
    };
    const onKey = (e) => { if (e.key === 'Escape') setPlusMenuOpen(false); };
    document.addEventListener('mousedown', onDoc);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onDoc);
      document.removeEventListener('keydown', onKey);
    };
  }, [plusMenuOpen]);

  // ── Post-session pipeline (shared by CSV upload and GA4 ingest) ────────
  // Given the response envelope from /upload or /ga/ingest, run the standard
  // profile → discover → render-run-button flow.
  const runPostSessionFlow = async (uploadData, tempId) => {
    const realId = uploadData.session_id;
    const realFolder = uploadData.output_folder || realId;
    closeConnection(tempId);
    resetSessionTracking(tempId);
    closeConnection(realId);
    resetSessionTracking(realId);
    migrateSession(tempId, realId, realFolder);
    migrateChatSession(tempId, realId);

    let profileData;
    if (uploadData.profile_cached && uploadData.profile) {
      toast.success('Identical dataset — loading cached profile');
      profileData = uploadData.profile;
    } else {
      setPhase('profiling');
      profileData = await profileDataset(realId);
      toast.success('Profiling complete');
    }
    addMessage('ai', 'profile', profileData);

    let discoveryPayload;
    if (uploadData.dag_cached && uploadData.discovery) {
      toast.success('Loaded cached analysis plan');
      discoveryPayload = uploadData.discovery;
    } else {
      setPhase('discovering');
      const discoverData = await discoverMetrics(realId);
      discoveryPayload = discoverData.discovery;
      toast.success('Analysis plan ready');
    }
    addMessage('ai', 'discovery', discoveryPayload);
    if (discoveryPayload?.dag) {
      setNodes(discoveryPayload.dag.map(n => ({ id: n.id, type: n.analysis_type, status: 'pending' })));
    }

    addMessage('ai', 'run_analysis', { sessionId: realId });
    toast.success('Review the plan and click Run when ready');
  };

  // ── GA4 ingest handler
  const onGASessionReady = async (ingestData) => {
    const tempId = `__ingesting_${Date.now()}`;
    setSession(tempId, null);
    setPhase('uploading');
    addMessage('user', 'file', {
      name: ingestData.filename || 'GA4 pull',
      size: `${(ingestData.rows || 0).toLocaleString()} rows`,
    });
    setShowGaPicker(false);
    try {
      await runPostSessionFlow(ingestData, tempId);
    } catch (err) {
      toast.error(`Error: ${err.message}`);
      addMessage('ai', 'text', `Sorry, something went wrong: ${err.message}`);
      setPhase('error');
    }
  };

  // ── BigQuery ingest handler
  const onBQSessionReady = async (ingestData) => {
    const tempId = `__ingesting_${Date.now()}`;
    setSession(tempId, null);
    setPhase('uploading');
    addMessage('user', 'file', {
      name: ingestData.filename || 'BigQuery pull',
      size: `${(ingestData.rows || 0).toLocaleString()} rows`,
    });
    setShowBqPicker(false);
    try {
      await runPostSessionFlow(ingestData, tempId);
    } catch (err) {
      toast.error(`Error: ${err.message}`);
      addMessage('ai', 'text', `Sorry, something went wrong: ${err.message}`);
      setPhase('error');
    }
  };

  // ── Submit handler — backend behavior unchanged ─────────────────────────
  const onSubmit = async () => {
    const currentText = textValue.trim();
    if (!currentText && !file) return;

    setTextValue('');

    if (file) {
      const currentFile = file;
      const currentSchema = schemaFile;
      setFile(null);
      setSchemaFile(null);

      const tempId = `__uploading_${Date.now()}`;
      setSession(tempId, null);
      setPhase('uploading');
      if (currentText) addMessage('user', 'text', currentText);
      addMessage('user', 'file', { name: currentFile.name, size: formatSize(currentFile.size) });
      if (currentSchema) {
        addMessage('user', 'file', {
          name: `${currentSchema.name} (data dictionary)`,
          size: formatSize(currentSchema.size),
        });
      }

      try {
        toast.info(`Uploading ${currentFile.name}...`);
        const uploadData = await uploadFile(currentFile, currentSchema);
        await runPostSessionFlow(uploadData, tempId);
      } catch (err) {
        toast.error(`Error: ${err.message}`);
        addMessage('ai', 'text', `Sorry, something went wrong: ${err.message}`);
        setPhase('error');
      }
    } else if (sessionId) {
      const userMsgId = addMessage('user', 'text', currentText);
      setThinking(true);
      try {
        const res = await sendChatMessage(sessionId, currentText);
        if (res?.response) {
          if (res.analysis_status === 'success' && res.chart) {
            addOrUpdateChart(res.chart);
          }
          insertAfterMessage(userMsgId, 'ai', 'text', res.response);
          bumpHistory();
        }
      } catch {
        insertAfterMessage(userMsgId, 'ai', 'text', 'Sorry, could not reach the server.');
      } finally {
        setThinking(false);
      }
    } else {
      addMessage('user', 'text', currentText);
      addMessage('ai', 'text', 'Please upload a data file first. Click the attachment button.');
    }
  };

  const handleAttach = (e) => {
    const f = e.target.files?.[0];
    if (f) {
      setFile(f);
      toast.info(`Attached: ${f.name}`);
    }
    e.target.value = '';
    setAcceptOverride(null);
  };

  // Companion data-dictionary picker. Validated lightly client-side; the
  // backend re-validates extension and size and degrades gracefully on
  // malformed content, so this is just a UX guard.
  const handleAttachSchema = (e) => {
    const f = e.target.files?.[0];
    if (f) {
      if (!/\.csv$/i.test(f.name)) {
        toast.error('Data dictionary must be a .csv file.');
      } else if (f.size > 5 * 1024 * 1024) {
        toast.error('Data dictionary too large (>5 MB).');
      } else {
        setSchemaFile(f);
        toast.info(`Schema attached: ${f.name}`);
      }
    }
    e.target.value = '';
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      onSubmit();
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsHovering(false);
    if (e.dataTransfer.files?.length) {
      setFile(e.dataTransfer.files[0]);
      toast.info(`Dropped: ${e.dataTransfer.files[0].name}`);
    }
  };

  const canSend = Boolean(textValue.trim() || file);

  // ── Layout ─────────────────────────────────────────────────────────────
  return (
    <div
      className={cn(
        'transition-all duration-300',
        isCenter
          ? 'w-full max-w-[640px] mx-auto'
          : 'shrink-0 px-4 py-3 bg-bg-page'
      )}
      style={!isCenter ? {
        borderTop: '1px solid #E2E8F0',
        paddingBottom: typeof window !== 'undefined' && window.innerWidth <= 768
          ? 'max(12px, env(safe-area-inset-bottom, 12px))'
          : 12,
      } : {}}
      onDragOver={e => { e.preventDefault(); setIsHovering(true); }}
      onDragLeave={() => setIsHovering(false)}
      onDrop={handleDrop}
    >
      {/* Full-screen drag overlay */}
      {isHovering && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center"
          style={{
            background: 'rgba(255,255,255,0.85)',
            backdropFilter: 'blur(6px)',
          }}
        >
          <div
            className="flex flex-col items-center gap-3 text-center"
            style={{
              border: '2px dashed rgba(251,113,133,0.5)',
              borderRadius: 20,
              padding: '48px 64px',
              background: 'rgba(251,113,133,0.05)',
            }}
          >
            <FileUp size={40} strokeWidth={1.5} style={{ color: '#FB7185' }} />
            <p className="text-[15px] font-semibold text-text-primary tracking-tight">
              Drop your data file here
            </p>
            <p className="text-[12.5px] text-text-muted">CSV, XLSX, JSON, Parquet supported</p>
          </div>
        </div>
      )}

      <div
        className={cn(
          isCenter ? 'w-full' : 'max-w-[720px] mx-auto w-full'
        )}
        ref={parent}
      >
        {/* Inline GA picker */}
        {showGaPicker && (
          <div className="mb-3 relative">
            <button
              onClick={() => setShowGaPicker(false)}
              className="absolute -top-2 -right-2 z-10 w-6 h-6 flex items-center justify-center rounded-full bg-white border border-border-subtle text-text-muted hover:text-status-error shadow-sm"
              title="Close"
              aria-label="Close GA picker"
            >
              <X size={12} strokeWidth={2.5} />
            </button>
            <GASourcePicker onSessionReady={onGASessionReady} />
          </div>
        )}

        {/* Inline BQ picker */}
        {showBqPicker && (
          <div className="mb-3 relative">
            <button
              onClick={() => setShowBqPicker(false)}
              className="absolute -top-2 -right-2 z-10 w-6 h-6 flex items-center justify-center rounded-full bg-white border border-border-subtle text-text-muted hover:text-status-error shadow-sm"
              title="Close"
              aria-label="Close BQ picker"
            >
              <X size={12} strokeWidth={2.5} />
            </button>
            <BQSourcePicker onSessionReady={onBQSessionReady} />
          </div>
        )}

        {/* File attachment chip */}
        {file && (
          <div className="flex flex-wrap items-center gap-2 mb-2.5">
            <div
              className="inline-flex items-center gap-2.5 pl-3 pr-2 py-2 rounded-xl border bg-white border-border-subtle shadow-sm"
            >
              <div
                className="w-6 h-6 rounded-md flex items-center justify-center shrink-0 bg-accent/10 text-accent"
              >
                <Paperclip size={13} strokeWidth={2} />
              </div>
              <div className="flex flex-col leading-none">
                <span className="text-[12.5px] font-semibold text-text-primary">{file.name}</span>
                <span className="text-[11px] font-mono text-text-muted mt-0.5">{formatSize(file.size)}</span>
              </div>
              <button
                onClick={() => setFile(null)}
                className="ml-1 w-5 h-5 flex items-center justify-center rounded-full text-text-muted hover:text-status-error hover:bg-status-error/10 transition-colors duration-150"
              >
                <X size={12} strokeWidth={2.5} />
              </button>
            </div>

            {/* Optional schema / data-dictionary attachment */}
            {schemaFile ? (
              <div
                className="inline-flex items-center gap-2.5 pl-3 pr-2 py-2 rounded-xl border bg-white border-border-subtle shadow-sm"
                title="Companion data dictionary (event_name → description)"
              >
                <div className="w-6 h-6 rounded-md flex items-center justify-center shrink-0 bg-emerald-500/10 text-emerald-600">
                  <Paperclip size={13} strokeWidth={2} />
                </div>
                <div className="flex flex-col leading-none">
                  <span className="text-[12.5px] font-semibold text-text-primary">
                    {schemaFile.name}
                  </span>
                  <span className="text-[11px] font-mono text-text-muted mt-0.5">
                    schema · {formatSize(schemaFile.size)}
                  </span>
                </div>
                <button
                  onClick={() => setSchemaFile(null)}
                  className="ml-1 w-5 h-5 flex items-center justify-center rounded-full text-text-muted hover:text-status-error hover:bg-status-error/10 transition-colors duration-150"
                  aria-label="Remove schema file"
                >
                  <X size={12} strokeWidth={2.5} />
                </button>
              </div>
            ) : (
              <button
                type="button"
                onClick={() => schemaInputRef.current?.click()}
                className="inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[12px] font-medium text-text-muted hover:text-accent hover:bg-accent/5 border border-dashed border-border-subtle transition-colors"
                title="Optional: attach a data dictionary CSV (Event Name, Description)"
              >
                <Plus size={12} strokeWidth={2.5} />
                Add data dictionary (optional)
              </button>
            )}
          </div>
        )}

        {/* Input box */}
        <div
          className={cn(
            'relative flex items-end transition-all duration-200 bg-white',
            isCenter
              ? 'rounded-3xl shadow-md border border-border-subtle'
              : 'rounded-2xl border border-border-subtle shadow-input'
          )}
          onFocus={e => {
            e.currentTarget.style.borderColor = 'rgba(251,113,133,0.55)';
            e.currentTarget.style.boxShadow = '0 0 0 3px rgba(251,113,133,0.10), 0 4px 12px rgba(15,23,42,0.06)';
          }}
          onBlur={e => {
            e.currentTarget.style.borderColor = '#E2E8F0';
            e.currentTarget.style.boxShadow = isCenter
              ? '0 4px 12px rgba(15,23,42,0.06), 0 1px 3px rgba(15,23,42,0.04)'
              : '0 1px 3px rgba(15,23,42,0.05), 0 1px 1px rgba(15,23,42,0.03)';
          }}
        >
          {/* "+" data-source menu */}
          <div className="relative flex-shrink-0" ref={plusMenuRef}>
            <button
              className={cn(
                'transition-colors duration-150 text-text-muted hover:text-accent',
                isCenter ? 'p-4' : 'p-3 rounded-bl-2xl'
              )}
              onClick={() => setPlusMenuOpen(v => !v)}
              title="Add data source"
              aria-label="Add data source"
              aria-haspopup="menu"
              aria-expanded={plusMenuOpen}
            >
              <Plus
                size={isCenter ? 20 : 19}
                strokeWidth={2.25}
                className={cn('transition-transform duration-200', plusMenuOpen && 'rotate-45')}
              />
            </button>

            {plusMenuOpen && (
              <div
                role="menu"
                className="absolute left-2 bottom-full mb-2 z-40 w-[220px] rounded-xl border bg-white py-1.5 shadow-lg"
                style={{ borderColor: '#E2E8F0', boxShadow: '0 10px 25px rgba(15,23,42,0.10), 0 2px 6px rgba(15,23,42,0.05)' }}
              >
                <button
                  role="menuitem"
                  onClick={() => {
                    setPlusMenuOpen(false);
                    setShowGaPicker(false);
                    setShowBqPicker(false);
                    setAcceptOverride(null);
                    fileInputRef.current?.click();
                  }}
                  className="w-full flex items-center gap-2.5 px-3 py-2 text-[13px] text-text-primary hover:bg-bg-page transition-colors"
                >
                  <Upload size={14} strokeWidth={2} className="text-text-muted" />
                  <div className="flex flex-col items-start leading-tight">
                    <span className="font-medium">Upload file</span>
                    <span className="text-[11px] text-text-muted">CSV, XLSX, JSON, Parquet</span>
                  </div>
                </button>
                <button
                  role="menuitem"
                  onClick={() => {
                    setPlusMenuOpen(false);
                    setShowBqPicker(false);
                    setShowGaPicker(true);
                  }}
                  className="w-full flex items-center gap-2.5 px-3 py-2 text-[13px] text-text-primary hover:bg-bg-page transition-colors"
                >
                  <BarChart3 size={14} strokeWidth={2} className="text-text-muted" />
                  <div className="flex flex-col items-start leading-tight">
                    <span className="font-medium">Google Analytics</span>
                    <span className="text-[11px] text-text-muted">Pull GA4 property data</span>
                  </div>
                </button>
                <button
                  role="menuitem"
                  onClick={() => {
                    setPlusMenuOpen(false);
                    setShowGaPicker(false);
                    setShowBqPicker(true);
                  }}
                  className="w-full flex items-center gap-2.5 px-3 py-2 text-[13px] text-text-primary hover:bg-bg-page transition-colors"
                >
                  <Database size={14} strokeWidth={2} className="text-text-muted" />
                  <div className="flex flex-col items-start leading-tight">
                    <span className="font-medium">BigQuery</span>
                    <span className="text-[11px] text-text-muted">Import a table from BigQuery</span>
                  </div>
                </button>
              </div>
            )}
          </div>

          {/* Textarea */}
          <textarea
            value={textValue}
            onChange={e => setTextValue(e.target.value)}
            ref={e => { textareaRef.current = e; }}
            onKeyDown={handleKeyDown}
            placeholder={
              file
                ? 'Add instructions (optional), then press Enter…'
                : isCenter
                  ? 'Upload a data file or describe what to analyze…'
                  : 'Upload a file to begin, or type a message…'
            }
            className={cn(
              'flex-1 bg-transparent resize-none border-none outline-none text-text-primary leading-relaxed',
              isCenter
                ? 'max-h-[200px] min-h-[56px] py-4 text-[15.5px]'
                : 'max-h-[160px] min-h-[46px] py-3 text-[14.5px]'
            )}
            rows={1}
          />

          {/* Send button */}
          <button
            className={cn(
              'flex-shrink-0 rounded-xl transition-all duration-200',
              isCenter ? 'p-3 m-1.5' : 'p-2.5 m-1',
              canSend ? 'text-white shadow-brand' : 'pointer-events-none'
            )}
            style={
              canSend
                ? { backgroundImage: 'linear-gradient(135deg, #FB7185 0%, #FB923C 100%)' }
                : { background: '#F1F5F9', color: '#CBD5E1' }
            }
            onMouseEnter={e => {
              if (canSend) e.currentTarget.style.backgroundImage = 'linear-gradient(135deg, #F43F5E 0%, #F97316 100%)';
            }}
            onMouseLeave={e => {
              if (canSend) e.currentTarget.style.backgroundImage = 'linear-gradient(135deg, #FB7185 0%, #FB923C 100%)';
            }}
            onClick={onSubmit}
            aria-label="Send message"
          >
            <Send
              size={isCenter ? 17 : 16}
              strokeWidth={2.5}
              className={cn('transition-transform duration-200', canSend && 'rotate-45')}
            />
          </button>

          <input
            type="file"
            ref={fileInputRef}
            className="hidden"
            accept={acceptOverride || '.csv,.xlsx,.xls,.json,.jsonl,.parquet'}
            onChange={handleAttach}
          />
          <input
            type="file"
            ref={schemaInputRef}
            className="hidden"
            accept=".csv"
            onChange={handleAttachSchema}
          />
        </div>
      </div>
    </div>
  );
});
