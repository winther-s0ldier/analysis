import React, { useState, useRef, useEffect } from 'react';
import { Paperclip, Send, X, FileUp } from 'lucide-react';
import { usePipelineStore } from '../../store/pipelineStore';
import { useChatStore } from '../../store/chatStore';
import { useAutoResize } from '../../hooks/useAutoResize';
import { useAutoAnimate } from '@formkit/auto-animate/react';
import { uploadFile, profileDataset, discoverMetrics, analyzeMetrics, sendChatMessage } from '../../api';
import { toast } from 'sonner';
import { cn } from '../ui/Badge';

function formatSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + ['B', 'KB', 'MB', 'GB'][i];
}

export function InputArea() {
  const { sessionId, setSession, setPhase, setNodes, setHasReport, setCanvasNarrative, setCanvasOpen } = usePipelineStore();
  const { addMessage, insertAfterMessage, addOrUpdateChart, clearMessages, setThinking, pendingMessage, setPendingMessage } = useChatStore();
  const [file, setFile] = useState(null);
  const [textValue, setTextValue] = useState('');
  const [isHovering, setIsHovering] = useState(false);
  const fileInputRef = useRef(null);
  const [parent] = useAutoAnimate();

  const textareaRef = useAutoResize(textValue);

  // Consume pendingMessage (set by "Ask about this" buttons on cards)
  useEffect(() => {
    if (!pendingMessage) return;
    setTextValue(pendingMessage);
    setPendingMessage('');
    // Focus the textarea so user can immediately send
    setTimeout(() => textareaRef.current?.focus(), 50);
  }, [pendingMessage]);

  const onSubmit = async () => {
    const currentText = textValue.trim();
    if (!currentText && !file) return;

    setTextValue('');

    if (file) {
      const currentFile = file;
      setFile(null);

      // Clear any messages from a previous session before starting a fresh upload flow.
      // Must happen BEFORE adding the new file chip so we don't wipe our own messages.
      clearMessages();

      if (currentText) addMessage('user', 'text', currentText);
      addMessage('user', 'file', { name: currentFile.name, size: formatSize(currentFile.size) });

      try {
        toast.info(`Uploading ${currentFile.name}...`);
        setPhase('uploading');
        const uploadData = await uploadFile(currentFile);

        setSession(uploadData.session_id, uploadData.output_folder || uploadData.session_id);

        // Profile step — use inline cached data if available, otherwise call /profile
        let profileData;
        if (uploadData.profile_cached && uploadData.profile) {
          toast.success('Identical dataset — loading cached profile');
          profileData = uploadData.profile;
        } else {
          setPhase('profiling');
          profileData = await profileDataset(uploadData.session_id);
          toast.success('Profiling complete');
        }
        addMessage('ai', 'profile', profileData);

        // Discovery step — use inline cached DAG if available, otherwise call /discover
        let discoveryPayload;
        if (uploadData.dag_cached && uploadData.discovery) {
          toast.success('Loaded cached analysis plan');
          discoveryPayload = uploadData.discovery;
        } else {
          setPhase('discovering');
          const discoverData = await discoverMetrics(uploadData.session_id);
          discoveryPayload = discoverData.discovery;
          toast.success('Analysis plan ready');
        }
        addMessage('ai', 'discovery', discoveryPayload);
        if (discoveryPayload?.dag) {
          setNodes(discoveryPayload.dag.map(n => ({ id: n.id, type: n.analysis_type, status: 'pending' })));
        }

        // Pause here — let user review plan, add custom metrics, then click Run
        addMessage('ai', 'run_analysis', { sessionId: uploadData.session_id });
        toast.success('Review the plan and click Run when ready');
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

  return (
    <div
      className="shrink-0 px-4 py-3 bg-bg-page transition-all duration-300"
      style={{
        borderTop: '1px solid #F3F4F6',
        paddingBottom: typeof window !== 'undefined' && window.innerWidth <= 768
          ? 'max(12px, env(safe-area-inset-bottom, 12px))'
          : 12
      }}
      onDragOver={e => { e.preventDefault(); setIsHovering(true); }}
      onDragLeave={() => setIsHovering(false)}
      onDrop={handleDrop}
    >
      {/* Full-screen drag overlay */}
      {isHovering && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center"
          style={{
            background: 'rgba(249,250,251,0.85)',
            backdropFilter: 'blur(6px)',
          }}
        >
          <div
            className="flex flex-col items-center gap-3 text-center"
            style={{
              border: '2px dashed rgba(99,102,241,0.45)',
              borderRadius: 20,
              padding: '48px 64px',
              background: 'rgba(99,102,241,0.04)',
            }}
          >
            <FileUp size={40} strokeWidth={1.5} style={{ color: '#6366F1' }} />
            <p className="text-[15px] font-semibold text-text-primary tracking-tight">
              Drop your data file here
            </p>
            <p className="text-[12.5px] text-text-muted">CSV, XLSX, JSON, Parquet supported</p>
          </div>
        </div>
      )}

      <div className="max-w-[720px] mx-auto w-full" ref={parent}>
        {/* File attachment chip */}
        {file && (
          <div
            className="inline-flex items-center gap-2.5 mb-2.5 pl-3 pr-2 py-2 rounded-xl border transition-all duration-200"
            style={{
              background: '#FFFFFF',
              borderColor: '#E5E7EB',
              boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
            }}
          >
            <div
              className="w-6 h-6 rounded-md flex items-center justify-center shrink-0"
              style={{ background: 'rgba(99,102,241,0.1)', color: '#6366F1' }}
            >
              <Paperclip size={13} strokeWidth={2} />
            </div>
            <div className="flex flex-col leading-none">
              <span className="text-[12.5px] font-semibold text-text-primary">{file.name}</span>
              <span className="text-[11px] font-mono text-text-muted mt-0.5">{formatSize(file.size)}</span>
            </div>
            <button
              onClick={() => setFile(null)}
              className="ml-1 w-5 h-5 flex items-center justify-center rounded-full transition-colors duration-150"
              style={{ color: '#9CA3AF' }}
              onMouseEnter={e => { e.currentTarget.style.color = '#EF4444'; e.currentTarget.style.background = 'rgba(239,68,68,0.08)'; }}
              onMouseLeave={e => { e.currentTarget.style.color = '#9CA3AF'; e.currentTarget.style.background = 'transparent'; }}
            >
              <X size={12} strokeWidth={2.5} />
            </button>
          </div>
        )}

        {/* Input box */}
        <div
          className="relative flex items-end rounded-2xl transition-all duration-200"
          style={{
            background: '#FFFFFF',
            border: '1px solid #E5E7EB',
            boxShadow: '0 1px 4px rgba(0,0,0,0.04)',
          }}
          onFocus={e => {
            e.currentTarget.style.borderColor = 'rgba(99,102,241,0.5)';
            e.currentTarget.style.boxShadow = '0 0 0 3px rgba(99,102,241,0.1), 0 1px 4px rgba(0,0,0,0.04)';
          }}
          onBlur={e => {
            e.currentTarget.style.borderColor = '#E5E7EB';
            e.currentTarget.style.boxShadow = '0 1px 4px rgba(0,0,0,0.04)';
          }}
        >
          {/* Attach button */}
          <button
            className="p-3 flex-shrink-0 transition-colors duration-150 rounded-bl-2xl"
            style={{ color: '#9CA3AF' }}
            onMouseEnter={e => e.currentTarget.style.color = '#6366F1'}
            onMouseLeave={e => e.currentTarget.style.color = '#9CA3AF'}
            onClick={() => fileInputRef.current?.click()}
            title="Attach file"
            aria-label="Attach file"
          >
            <Paperclip size={18} strokeWidth={2} />
          </button>

          {/* Textarea */}
          <textarea
            value={textValue}
            onChange={e => setTextValue(e.target.value)}
            ref={e => { textareaRef.current = e; }}
            onKeyDown={handleKeyDown}
            placeholder={
              file
                ? 'Add instructions (optional), then press Enter…'
                : 'Upload a file to begin, or type a message…'
            }
            className="flex-1 max-h-[160px] min-h-[46px] py-3 bg-transparent resize-none border-none outline-none text-[14.5px] text-text-primary leading-relaxed"
            style={{ color: '#111827' }}
            rows={1}
          />

          {/* Send button */}
          <button
            className={cn(
              'p-2.5 m-1 flex-shrink-0 rounded-xl transition-all duration-200',
              canSend
                ? 'text-white'
                : 'pointer-events-none',
            )}
            style={
              canSend
                ? { background: '#6366F1', boxShadow: '0 2px 8px rgba(99,102,241,0.35)' }
                : { background: '#F3F4F6', color: '#D1D5DB' }
            }
            onMouseEnter={e => { if (canSend) e.currentTarget.style.background = '#4F46E5'; }}
            onMouseLeave={e => { if (canSend) e.currentTarget.style.background = '#6366F1'; }}
            onClick={onSubmit}
            aria-label="Send message"
          >
            <Send
              size={16}
              strokeWidth={2.5}
              className={cn('transition-transform duration-200', canSend && 'rotate-45')}
            />
          </button>

          <input
            type="file"
            ref={fileInputRef}
            className="hidden"
            accept=".csv,.xlsx,.xls,.json,.jsonl,.parquet"
            onChange={handleAttach}
          />
        </div>
      </div>
    </div>
  );
}
