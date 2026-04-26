import React, { useEffect, useState, useCallback } from 'react';
import { Database, Link2, RefreshCw, Unlink } from 'lucide-react';
import { toast } from 'sonner';
import { bqStatus, bqAuthStart, bqListProjects, bqListDatasets, bqListTables, bqIngest, bqDisconnect } from '../../api';
import { cn } from '../ui/Badge';

/**
 * BQSourcePicker
 *
 * Data-source picker for Google BigQuery.
 *
 * Flow:
 *   1. If not connected → "Connect Google" OAuth popup.
 *   2. If connected → cascading dropdowns: Project → Dataset → Table.
 *      Row limit selector, then "Import & analyze" calls POST /bq/ingest.
 *      Returns the same envelope as /upload so the parent runs the standard flow.
 *
 * Props:
 *   onSessionReady(ingestData)  — called with the /bq/ingest response.
 *   disabled                    — when the parent has a session in flight.
 */
export function BQSourcePicker({ onSessionReady, disabled = false }) {
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);

  const [projects, setProjects] = useState([]);
  const [selectedProject, setSelectedProject] = useState('');
  const [loadingProjects, setLoadingProjects] = useState(false);

  const [datasets, setDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState('');
  const [loadingDatasets, setLoadingDatasets] = useState(false);

  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [loadingTables, setLoadingTables] = useState(false);

  const [rowLimit, setRowLimit] = useState(50000);
  const [ingesting, setIngesting] = useState(false);

  const refreshStatus = useCallback(async () => {
    try {
      const s = await bqStatus();
      setConnected(Boolean(s?.connected));
      return Boolean(s?.connected);
    } catch {
      setConnected(false);
      return false;
    }
  }, []);

  const loadProjects = useCallback(async () => {
    setLoadingProjects(true);
    setDatasets([]);
    setSelectedDataset('');
    setTables([]);
    setSelectedTable('');
    try {
      const r = await bqListProjects();
      const list = r?.projects || [];
      setProjects(list);
      if (list.length) setSelectedProject(list[0].project_id);
    } catch (e) {
      toast.error(`Could not list projects: ${e.message}`);
    } finally {
      setLoadingProjects(false);
    }
  }, []);

  const loadDatasets = useCallback(async (project_id) => {
    if (!project_id) return;
    setLoadingDatasets(true);
    setDatasets([]);
    setSelectedDataset('');
    setTables([]);
    setSelectedTable('');
    try {
      const r = await bqListDatasets(project_id);
      const list = r?.datasets || [];
      setDatasets(list);
      if (list.length) setSelectedDataset(list[0].dataset_id);
    } catch (e) {
      toast.error(`Could not list datasets: ${e.message}`);
    } finally {
      setLoadingDatasets(false);
    }
  }, []);

  const loadTables = useCallback(async (project_id, dataset_id) => {
    if (!project_id || !dataset_id) return;
    setLoadingTables(true);
    setTables([]);
    setSelectedTable('');
    try {
      const r = await bqListTables(project_id, dataset_id);
      const list = r?.tables || [];
      setTables(list);
      if (list.length) setSelectedTable(list[0].table_id);
    } catch (e) {
      toast.error(`Could not list tables: ${e.message}`);
    } finally {
      setLoadingTables(false);
    }
  }, []);

  // Initial status check
  useEffect(() => {
    (async () => {
      const ok = await refreshStatus();
      if (ok) loadProjects();
    })();
  }, []);

  // Load datasets when project changes
  useEffect(() => {
    if (selectedProject) loadDatasets(selectedProject);
  }, [selectedProject]);

  // Load tables when dataset changes
  useEffect(() => {
    if (selectedProject && selectedDataset) loadTables(selectedProject, selectedDataset);
  }, [selectedDataset]);

  // Listen for OAuth popup success
  useEffect(() => {
    const onMessage = async (ev) => {
      if (ev?.data?.type === 'bq-oauth' && ev.data.ok) {
        setConnecting(false);
        const ok = await refreshStatus();
        if (ok) {
          toast.success('BigQuery connected');
          loadProjects();
        }
      }
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [refreshStatus, loadProjects]);

  const onConnect = async () => {
    try {
      setConnecting(true);
      const { auth_url } = await bqAuthStart();
      const w = window.open(auth_url, 'bq_oauth', 'width=560,height=720');
      if (!w) {
        toast.error('Popup blocked — please allow popups and try again');
        setConnecting(false);
      }
    } catch (e) {
      toast.error(`BQ auth failed: ${e.message}`);
      setConnecting(false);
    }
  };

  const onDisconnect = async () => {
    try {
      await bqDisconnect();
      setConnected(false);
      setProjects([]);
      setSelectedProject('');
      setDatasets([]);
      setSelectedDataset('');
      setTables([]);
      setSelectedTable('');
      toast.success('BigQuery disconnected');
    } catch (e) {
      toast.error(`Disconnect failed: ${e.message}`);
    }
  };

  const onAnalyze = async () => {
    if (!selectedProject || !selectedDataset || !selectedTable) {
      toast.error('Select a project, dataset, and table first');
      return;
    }
    setIngesting(true);
    try {
      toast.info('Pulling BigQuery table…');
      const res = await bqIngest({
        project_id: selectedProject,
        dataset_id: selectedDataset,
        table_id: selectedTable,
        row_limit: rowLimit,
      });
      if (res.cached) toast.success('Using cached BigQuery pull');
      else toast.success(`Pulled ${res.rows.toLocaleString()} rows from BigQuery`);
      onSessionReady?.(res);
    } catch (e) {
      toast.error(`BigQuery ingest failed: ${e.message}`);
    } finally {
      setIngesting(false);
    }
  };

  const isLoading = loadingProjects || loadingDatasets || loadingTables || ingesting;
  const canAnalyze = Boolean(selectedProject && selectedDataset && selectedTable && !isLoading && !disabled);

  return (
    <div
      className="rounded-2xl border bg-white p-4"
      style={{ borderColor: '#E2E8F0', boxShadow: '0 1px 3px rgba(15,23,42,0.05)' }}
    >
      <div className="flex items-center gap-2 mb-3">
        <div
          className="w-7 h-7 rounded-lg flex items-center justify-center"
          style={{ background: 'linear-gradient(135deg, #3B82F6 0%, #6366F1 100%)' }}
        >
          <Database size={15} strokeWidth={2.5} color="#fff" />
        </div>
        <div className="leading-tight">
          <div className="text-[13.5px] font-semibold text-text-primary">Google BigQuery</div>
          <div className="text-[11.5px] text-text-muted">
            {connected ? 'Connected — pick a table to analyze' : 'Connect your Google account to import BigQuery data'}
          </div>
        </div>
        <div className="ml-auto">
          {connected && (
            <button
              onClick={onDisconnect}
              className="flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11.5px] text-text-muted hover:text-status-error hover:bg-status-error/10 transition-colors"
              title="Disconnect BigQuery"
            >
              <Unlink size={12} strokeWidth={2} />
              Disconnect
            </button>
          )}
        </div>
      </div>

      {!connected ? (
        <button
          onClick={onConnect}
          disabled={connecting || disabled}
          className={cn(
            'w-full flex items-center justify-center gap-2 py-2.5 rounded-xl text-[13px] font-semibold text-white transition-all',
            (connecting || disabled) ? 'opacity-60 pointer-events-none' : ''
          )}
          style={{ backgroundImage: 'linear-gradient(135deg, #3B82F6 0%, #6366F1 100%)' }}
        >
          <Link2 size={14} strokeWidth={2.5} />
          {connecting ? 'Waiting for Google…' : 'Connect Google'}
        </button>
      ) : (
        <div className="flex flex-col gap-2.5">
          {/* Project */}
          <div className="flex items-center gap-2">
            <select
              value={selectedProject}
              onChange={(e) => setSelectedProject(e.target.value)}
              disabled={loadingProjects || isLoading || disabled}
              className="flex-1 rounded-lg border border-border-subtle bg-white px-3 py-2 text-[13px] text-text-primary outline-none focus:border-accent/60"
            >
              {loadingProjects && <option>Loading projects…</option>}
              {!loadingProjects && projects.length === 0 && <option value="">No projects found</option>}
              {projects.map((p) => (
                <option key={p.project_id} value={p.project_id}>
                  {p.name || p.project_id}
                </option>
              ))}
            </select>
            <button
              onClick={loadProjects}
              disabled={isLoading}
              className="p-2 rounded-lg border border-border-subtle text-text-muted hover:text-accent hover:border-accent/50"
              title="Refresh projects"
            >
              <RefreshCw size={13} strokeWidth={2} className={loadingProjects ? 'animate-spin' : ''} />
            </button>
          </div>

          {/* Dataset */}
          <select
            value={selectedDataset}
            onChange={(e) => setSelectedDataset(e.target.value)}
            disabled={!selectedProject || loadingDatasets || isLoading || disabled}
            className="rounded-lg border border-border-subtle bg-white px-3 py-2 text-[13px] text-text-primary outline-none focus:border-accent/60"
          >
            {loadingDatasets && <option>Loading datasets…</option>}
            {!loadingDatasets && datasets.length === 0 && <option value="">No datasets found</option>}
            {datasets.map((d) => (
              <option key={d.dataset_id} value={d.dataset_id}>
                {d.friendly_name || d.dataset_id}
              </option>
            ))}
          </select>

          {/* Table */}
          <select
            value={selectedTable}
            onChange={(e) => setSelectedTable(e.target.value)}
            disabled={!selectedDataset || loadingTables || isLoading || disabled}
            className="rounded-lg border border-border-subtle bg-white px-3 py-2 text-[13px] text-text-primary outline-none focus:border-accent/60"
          >
            {loadingTables && <option>Loading tables…</option>}
            {!loadingTables && tables.length === 0 && <option value="">No tables found</option>}
            {tables.map((t) => (
              <option key={t.table_id} value={t.table_id}>
                {t.table_id}{t.num_rows != null ? `  ·  ${t.num_rows.toLocaleString()} rows` : ''}
              </option>
            ))}
          </select>

          {/* Row limit */}
          <div className="flex items-center gap-2">
            <span className="text-[11.5px] text-text-muted">Row limit:</span>
            {[
              [10000,  '10k'],
              [50000,  '50k'],
              [100000, '100k'],
            ].map(([val, label]) => (
              <button
                key={val}
                onClick={() => setRowLimit(val)}
                className={cn(
                  'px-2.5 py-1 rounded-full text-[11.5px] font-medium border transition-colors',
                  rowLimit === val
                    ? 'bg-accent/10 border-accent/50 text-accent'
                    : 'border-border-subtle text-text-muted hover:border-accent/30 hover:text-text-primary'
                )}
              >
                {label}
              </button>
            ))}
          </div>

          {/* Analyze button */}
          <button
            onClick={onAnalyze}
            disabled={!canAnalyze}
            className={cn(
              'mt-1 w-full py-2.5 rounded-xl text-[13px] font-semibold text-white transition-all',
              !canAnalyze ? 'opacity-60 pointer-events-none' : ''
            )}
            style={{ backgroundImage: 'linear-gradient(135deg, #3B82F6 0%, #6366F1 100%)' }}
          >
            {ingesting ? 'Pulling BigQuery data…' : 'Import & analyze'}
          </button>
        </div>
      )}
    </div>
  );
}
