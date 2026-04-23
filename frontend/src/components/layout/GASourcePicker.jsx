import React, { useEffect, useState, useCallback } from 'react';
import { BarChart3, Link2, RefreshCw, Unlink } from 'lucide-react';
import { toast } from 'sonner';
import { gaStatus, gaAuthStart, gaListProperties, gaIngest, gaDisconnect } from '../../api';
import { cn } from '../ui/Badge';

/**
 * GASourcePicker
 *
 * Phase 3 Step 1 data-source picker for Google Analytics 4.
 *
 * Flow:
 *   1. If not connected → show "Connect Google" button. Opens OAuth in a popup.
 *      On success, the popup posts a message back and we refresh status.
 *   2. If connected → fetch GA4 property list, show dropdown + date-range,
 *      "Analyze" button calls POST /ga/ingest and returns the same envelope
 *      as /upload so the parent runs the same profile/discover flow.
 *
 * Props:
 *   onSessionReady(uploadData)  — called with the /ga/ingest response, which
 *                                 has the same shape as /upload's response.
 *   disabled                    — when the parent has a session in flight.
 */
export function GASourcePicker({ onSessionReady, disabled = false }) {
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [loadingProps, setLoadingProps] = useState(false);
  const [properties, setProperties] = useState([]);
  const [selectedProp, setSelectedProp] = useState('');
  const [dateRange, setDateRange] = useState('90daysAgo');
  const [ingesting, setIngesting] = useState(false);

  const refreshStatus = useCallback(async () => {
    try {
      const s = await gaStatus();
      setConnected(Boolean(s?.connected));
      return Boolean(s?.connected);
    } catch {
      setConnected(false);
      return false;
    }
  }, []);

  const loadProperties = useCallback(async () => {
    setLoadingProps(true);
    try {
      const r = await gaListProperties();
      const props = r?.properties || [];
      setProperties(props);
      if (props.length && !selectedProp) setSelectedProp(props[0].property_id);
    } catch (e) {
      toast.error(`Could not list GA properties: ${e.message}`);
    } finally {
      setLoadingProps(false);
    }
  }, [selectedProp]);

  // Initial status check
  useEffect(() => {
    (async () => {
      const ok = await refreshStatus();
      if (ok) loadProperties();
    })();
  }, []);

  // Listen for the OAuth popup's postMessage on success
  useEffect(() => {
    const onMessage = async (ev) => {
      if (ev?.data?.type === 'ga-oauth' && ev.data.ok) {
        setConnecting(false);
        const ok = await refreshStatus();
        if (ok) {
          toast.success('Google Analytics connected');
          loadProperties();
        }
      }
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [refreshStatus, loadProperties]);

  const onConnect = async () => {
    try {
      setConnecting(true);
      const { auth_url } = await gaAuthStart();
      const w = window.open(auth_url, 'ga_oauth', 'width=560,height=720');
      if (!w) {
        toast.error('Popup blocked — please allow popups and try again');
        setConnecting(false);
      }
    } catch (e) {
      toast.error(`GA auth failed: ${e.message}`);
      setConnecting(false);
    }
  };

  const onDisconnect = async () => {
    try {
      await gaDisconnect();
      setConnected(false);
      setProperties([]);
      setSelectedProp('');
      toast.success('Google Analytics disconnected');
    } catch (e) {
      toast.error(`Disconnect failed: ${e.message}`);
    }
  };

  const onAnalyze = async () => {
    if (!selectedProp) {
      toast.error('Pick a GA4 property first');
      return;
    }
    setIngesting(true);
    try {
      toast.info('Pulling GA4 data…');
      const res = await gaIngest({
        property_id: selectedProp,
        start_date: dateRange,
        end_date: 'today',
      });
      if (res.cached) toast.success('Using cached GA4 pull');
      else toast.success(`Pulled ${res.rows.toLocaleString()} rows from GA4`);
      onSessionReady?.(res);
    } catch (e) {
      toast.error(`GA ingest failed: ${e.message}`);
    } finally {
      setIngesting(false);
    }
  };

  const pretty = (p) =>
    `${p.display_name || p.property_id}  ·  ${p.account_name || ''}`.trim();

  return (
    <div
      className="rounded-2xl border bg-white p-4"
      style={{ borderColor: '#E2E8F0', boxShadow: '0 1px 3px rgba(15,23,42,0.05)' }}
    >
      <div className="flex items-center gap-2 mb-3">
        <div
          className="w-7 h-7 rounded-lg flex items-center justify-center"
          style={{ background: 'linear-gradient(135deg, #F59E0B 0%, #EF4444 100%)' }}
        >
          <BarChart3 size={15} strokeWidth={2.5} color="#fff" />
        </div>
        <div className="leading-tight">
          <div className="text-[13.5px] font-semibold text-text-primary">Google Analytics 4</div>
          <div className="text-[11.5px] text-text-muted">
            {connected ? 'Connected — pick a property and date range' : 'Connect your Google account to import GA4 data'}
          </div>
        </div>
        <div className="ml-auto">
          {connected && (
            <button
              onClick={onDisconnect}
              className="flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11.5px] text-text-muted hover:text-status-error hover:bg-status-error/10 transition-colors"
              title="Disconnect Google"
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
          style={{ backgroundImage: 'linear-gradient(135deg, #FB7185 0%, #FB923C 100%)' }}
        >
          <Link2 size={14} strokeWidth={2.5} />
          {connecting ? 'Waiting for Google…' : 'Connect Google'}
        </button>
      ) : (
        <div className="flex flex-col gap-2.5">
          {/* Property dropdown */}
          <div className="flex items-center gap-2">
            <select
              value={selectedProp}
              onChange={(e) => setSelectedProp(e.target.value)}
              disabled={loadingProps || ingesting || disabled}
              className="flex-1 rounded-lg border border-border-subtle bg-white px-3 py-2 text-[13px] text-text-primary outline-none focus:border-accent/60"
            >
              {loadingProps && <option>Loading properties…</option>}
              {!loadingProps && properties.length === 0 && <option value="">No GA4 properties found</option>}
              {properties.map((p) => (
                <option key={p.property_id} value={p.property_id}>
                  {pretty(p)}
                </option>
              ))}
            </select>
            <button
              onClick={loadProperties}
              disabled={loadingProps || ingesting}
              className="p-2 rounded-lg border border-border-subtle text-text-muted hover:text-accent hover:border-accent/50"
              title="Refresh property list"
            >
              <RefreshCw size={13} strokeWidth={2} className={loadingProps ? 'animate-spin' : ''} />
            </button>
          </div>

          {/* Date range picker */}
          <div className="flex items-center gap-2">
            <span className="text-[11.5px] text-text-muted">Range:</span>
            {[
              ['7daysAgo',  'Last 7 days'],
              ['30daysAgo', 'Last 30 days'],
              ['90daysAgo', 'Last 90 days'],
              ['365daysAgo', 'Last year'],
            ].map(([val, label]) => (
              <button
                key={val}
                onClick={() => setDateRange(val)}
                className={cn(
                  'px-2.5 py-1 rounded-full text-[11.5px] font-medium border transition-colors',
                  dateRange === val
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
            disabled={!selectedProp || ingesting || disabled}
            className={cn(
              'mt-1 w-full py-2.5 rounded-xl text-[13px] font-semibold text-white transition-all',
              (!selectedProp || ingesting || disabled) ? 'opacity-60 pointer-events-none' : ''
            )}
            style={{ backgroundImage: 'linear-gradient(135deg, #FB7185 0%, #FB923C 100%)' }}
          >
            {ingesting ? 'Pulling GA4 data…' : 'Import & analyze'}
          </button>
        </div>
      )}
    </div>
  );
}
